// BookLink edge guard — runs at Cloudflare, in front of the two BookLink hosts.
//
// Why this exists: one rented server once walked an entire published link
// catalogue — 989 requests, 81% of a day's traffic — without downloading a
// single book. The zone-level block that stopped it answers 403, which tells
// the crawler it was blocked and invites it to come
// back from a new address. This hands out a plain 404 instead, identical to a
// dead link, so there is nothing to learn from the response.
//
// It also caps requests per IP on /b/, which is the part that survives an IP
// change. A reader clicking books from Mobilism will never come close to it.
//
// Attached only to: books.lightweave.uk/* and cerinawithasea.download/b/*
// plus cerinawithasea.download/status*, which is forwarded to the host below.

const BLOCKED_IPS = new Set([
  // "203.0.113.10",
]);

// The host the BookLink origin answers on. The zone that only routes /b/* has no
// origin of its own for anything else, so /status is forwarded here by name —
// otherwise the dashboard would only be reachable on a second, less memorable
// URL, and the whole point is that the same link can be handed to a few people.
const BOOKLINK_HOST = "books.lightweave.uk";

// Per-IP ceiling on /b/ within the window. Deliberately generous: the crawler
// was doing ~80/minute in bursts, a real reader does a handful.
const RATE_LIMIT = 40;
const WINDOW_MS = 60_000;

// Soft state: one isolate's view. Good enough to blunt a burst, and it resets
// when the isolate recycles. The blocked-IP list above needs no state at all.
const hits = new Map();

function notFound() {
  return new Response("Not Found", {
    status: 404,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
}

export default {
  async fetch(request) {
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

    if (pathname.startsWith("/b/") && ip) {
      const now = Date.now();
      const recent = (hits.get(ip) || []).filter((t) => now - t < WINDOW_MS);
      recent.push(now);
      hits.set(ip, recent);
      if (recent.length > RATE_LIMIT) return notFound();
    }

    // Everything else is untouched: Range requests, seeking, resuming and
    // streaming all pass straight through to the origin.
    return fetch(request);
  },
};
