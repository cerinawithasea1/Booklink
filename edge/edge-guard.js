// BookLink edge guard — runs at Cloudflare, in front of the BookLink hosts.
//
// Why this exists: one rented server once walked an entire published link
// catalogue — 989 requests in a day, 81% of that day's traffic — without
// downloading a single book. The zone-level block that stopped it answers 403,
// which tells the crawler it was blocked and invites it back from a new
// address. This hands out a plain 404 instead, identical to a dead link, so
// there is nothing to learn and nowhere to go.
//
// Blocking by hand only works if somebody notices, and a new address defeats it
// anyway. So the decisions live in a Durable Object: one instance per IP, and
// because it is a single thing rather than one per edge machine, the counters
// are the real ones. An earlier version kept them in module scope and a burst
// of 42 requests sailed straight past a limit of 40.
//
// Two shapes get blocked, because two shapes have actually happened:
//   - a burst, which is the 989-request day
//   - a slow walk, which is ~100 links over 8 hours, each one a different book
//
// Deliberately generous, and deliberately not permanent. A reader who clicks a
// handful of books from a forum will never come near either number, and the
// blocks expire, so a mistake costs a day rather than a reader.
//
// Attached to: books.lightweave.uk/* , cerinawithasea.download/b/* and
// cerinawithasea.download/status* (forwarded to BOOKLINK_HOST below).

// Populated per deployment. Deliberately empty in the published copy: naming
// the addresses you block in public tells the crawler its 404 was a decision
// rather than a dead link, which is the whole thing the 404 avoids.
const BLOCKED_IPS = new Set([
  // "203.0.113.10",
]);

// The host the BookLink origin answers on. The zone that only routes /b/* has no
// origin of its own for anything else, so /status is forwarded here by name —
// otherwise the dashboard would only be reachable on a second, less memorable
// URL, and the whole point is that the same link can be handed to a few people.
const BOOKLINK_HOST = "books.lightweave.uk";

// A burst. The crawler was doing ~80/minute; a reader does a handful, and the
// bulk of their traffic goes to the copy box, not through here.
const BURST_LIMIT = 40;
const BURST_WINDOW_MS = 60_000;
const BURST_BLOCK_MS = 3_600_000; // 1h

// A `Range` request is not somebody opening a link — it is somebody's player
// already part way through a book, seeking. Those come in bursts too (an
// audiobook player issues them constantly, and one reader accounted for 64 in
// the log), so counting them against BURST_LIMIT would block a listener for
// listening. They get their own, much higher ceiling purely so that adding a
// Range header is not a way to opt out of the limit entirely.
const RANGE_BURST_LIMIT = 300;

// A slow walk. This is the one that catches "100 links in 8 hours": each request
// is for a different book, so no per-minute limit ever sees it. Someone reading
// their way through a series stays well under this; someone enumerating a
// catalogue does not.
const WALK_LIMIT = 60;
const WALK_WINDOW_MS = 86_400_000;
const WALK_BLOCK_MS = 86_400_000; // 24h

function notFound() {
  return new Response("Not Found", {
    status: 404,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
}

// One instance per client IP. Single-threaded and long-lived, so the counters
// are authoritative in a way module scope never was.
export class Gate {
  constructor(state) {
    this.state = state;
    this.data = { secs: [], paths: {}, blocked_until: 0, block_count: 0 };
    this.ready = state.blockConcurrencyWhile(async () => {
      this.data = (await state.storage.get("s")) || this.data;
    });
  }

  async fetch(request) {
    const { path, ranged } = await request.json();
    return Response.json(await this.check(path, ranged));
  }

  async check(path, ranged) {
    await this.ready;
    const now = Date.now();
    const d = this.data;
    if (!d.rsecs) d.rsecs = [];

    if (d.blocked_until > now) {
      return { blocked: true, until: d.blocked_until, reason: "existing" };
    }

    // Prune the windows before deciding, so a cooled-down IP gets a clean slate.
    d.secs = d.secs.filter((t) => now - t < BURST_WINDOW_MS);
    d.rsecs = d.rsecs.filter((t) => now - t < BURST_WINDOW_MS);
    for (const [p, t] of Object.entries(d.paths)) {
      if (now - t > WALK_WINDOW_MS) delete d.paths[p];
    }

    if (ranged) {
      d.rsecs.push(now);
    } else {
      d.secs.push(now);
      d.paths[path] = now;
    }

    let until = 0;
    let reason = "";
    if (d.rsecs.length > RANGE_BURST_LIMIT) {
      until = now + BURST_BLOCK_MS;
      reason = "range";
    } else if (d.secs.length > BURST_LIMIT) {
      until = now + BURST_BLOCK_MS;
      reason = "burst";
    } else if (Object.keys(d.paths).length > WALK_LIMIT) {
      until = now + WALK_BLOCK_MS;
      reason = "walk";
    }

    if (until) {
      d.blocked_until = until;
      d.block_count = (d.block_count || 0) + 1;
      // Clear the evidence as well as the verdict: otherwise the window still
      // reads as over-limit the moment this block expires and it re-blocks
      // forever. A fresh budget after a block is the point of the block ending.
      d.secs = [];
      d.rsecs = [];
      d.paths = {};
    }

    await this.state.storage.put("s", d);
    return {
      blocked: until > 0, until, reason, count: d.block_count,
      burst: d.secs.length, ranged: d.rsecs.length, walk: Object.keys(d.paths).length,
    };
  }
}

export default {
  async fetch(request, env) {
    const ip = request.headers.get("CF-Connecting-IP") || "";
    const url = new URL(request.url);
    const { pathname } = url;

    if (ip && BLOCKED_IPS.has(ip)) return notFound();

    // Guarded on hostname so the forwarded request — which arrives back here on
    // the BookLink host — falls through to the origin instead of looping.
    if (pathname.startsWith("/status") && url.hostname !== BOOKLINK_HOST) {
      url.hostname = BOOKLINK_HOST;
      return fetch(new Request(url, request));
    }

    // Anyone who has logged into the dashboard is not a crawler. Without this
    // she would be blocked by her own guard the first time she checked a batch
    // of links before posting them — which is the exact behaviour the walk
    // limit treats as a crawler. The cookie is only ever issued by the origin
    // behind the dashboard login, and it never rides along in a link.
    const exempt = env.PASS && (request.headers.get("Cookie") || "").includes(`bl_pass=${env.PASS}`);
    if (pathname.startsWith("/b/") && ip && !exempt) {
      try {
        const id = env.GATE.idFromName(ip);
        const res = await env.GATE.get(id).fetch("https://gate/check", {
          method: "POST",
          body: JSON.stringify({
            path: pathname,
            ranged: request.headers.has("range"),
          }),
        });
        const verdict = await res.json();
        if (verdict.blocked) {
          console.log(`block ${ip} (${verdict.reason}) until ${new Date(verdict.until).toISOString()}`);
          return notFound();
        }
      } catch (e) {
        // Fail open. This sits in front of every published link, and a broken
        // counter must never be the reason a working link stops working.
        console.log(`gate error for ${ip}: ${e}`);
      }
    }

    // Everything else is untouched: Range requests, seeking, resuming and
    // streaming all pass straight through to the origin.
    return fetch(request);
  },
};
