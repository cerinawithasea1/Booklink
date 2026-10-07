# BookLink

Permanent, shareable **streaming links for Telegram files**.

Forward a file to a librarian account and get back a URL that streams it —
with HTTP Range support so audio/video players can seek and resume. Links never
expire: they re-fetch live from Telegram on every request.

```
books.yourdomain.com/b/<key>/Any Book Title.m4b
                              └─ cosmetic, the <key> does the work
```

The point is that **Telegram stays the storage**. Nothing is mirrored, nothing
has to fit on the server's disk, and a link never breaks because a copy expired
— because there is no copy to expire. Links are pointers, not files.

## How it works

A link encodes `chat_id:message_id`. When it's opened, the librarian account
(which must be a member of the chat the file lives in) fetches that message and
serves the bytes over HTTP.

Three layers, each doing one job:

| Layer | Role |
|---|---|
| **Telegram** | the library. The source of truth. Nothing is ever moved off it. |
| **Local cache** | a short-lived working copy on disk, so seeks/resumes and repeat hits don't re-hit Telegram. Evicted by age and by total size. |
| **buzzheavier** *(optional)* | a public copy tier. While a copy is live, `/b/` redirects readers there — which takes the load off Telegram entirely. |

**If any layer fails, the link still works.** A dead or expired buzz copy just
means the request falls through to the local cache, and a cache miss just means
we fetch from Telegram again. That's the whole design: each layer is an
optimisation, and none of them is required for a link to resolve.

## Setup

1. **Install**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure**

   ```bash
   cp .env.example .env
   ```

3. **Log in the librarian account**

   ```bash
   python3 make_session.py phone +15551234567
   python3 make_session.py sign-in 12345        # code Telegram sends
   # (add "password <pw>" if the account has 2FA)
   ```

   Paste the printed string into `.env` as `BOOKLINK_SESSION`.

4. **Run**

   ```bash
   python3 streamer.py
   ```

   Forward any file to the librarian account — it replies with a permanent
   link.

5. **Put it on a domain** (optional but recommended)

   Front `127.0.0.1:8094` with any reverse proxy (nginx, Caddy, Apache) or a
   Cloudflare Tunnel, and set `BOOKLINK_BASE` to the public URL.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `BOOKLINK_API_ID` / `BOOKLINK_API_HASH` | — | from [my.telegram.org](https://my.telegram.org) → API development tools |
| `BOOKLINK_SESSION` | — | StringSession for the primary librarian account |
| `BOOKLINK_SESSION_NEW` | — | *optional* second librarian account |
| `BOOKLINK_BASE` | local URL | the public base URL links are minted with |
| `BOOKLINK_PORT` / `BOOKLINK_HOST` | `8091` / `127.0.0.1` | web listener |
| `BOOKLINK_SECRET` | — | *optional* HMAC-signs private links (`?s=`) so only you can mint them |
| `BOOKLINK_CACHE_DIR` | `/opt/booklink/cache` | where the local cache lives |
| `BOOKLINK_CACHE_MAX_AGE` | 24h | drop cache entries older than this |
| `BOOKLINK_CACHE_MAX_SIZE` | 18 GB | drop oldest entries above this total |
| `BOOKLINK_MAX_DOWNLOADS` | `4` | concurrent downloads on the fresh account |
| `BOOKLINK_MAX_DOWNLOADS_OLD` | `1` | concurrent downloads on the legacy account |
| `BOOKLINK_DOWNLOAD_VIA` | `auto` | which account fetches files: `old`, `new`, or `auto` |
| `BOOKLINK_NO_PRECACHE_CHATS` | — | chats that get a **link only** — no cache, no offload |
| `BOOKLINK_GROUP_CHATS` | — | groups where posts are treated as first-class sends (see below) |
| `BOOKLINK_BUZZ_TOKEN` | — | buzzheavier Auth ID; enables the offload tier |
| `BOOKLINK_BUZZ_DIR` | — | buzz folder id to upload into |
| `BOOKLINK_BUZZ_MAP` | `/opt/booklink/buzz_map.json` | the id/expiry map |
| `BOOKLINK_ALERT_CHAT` | — | where operational alerts are sent |
| `BOOKLINK_DISK_ALERT_MB` / `_COOLDOWN` | 10 GB / 4h | warn when free space drops below this |

## Design notes

### Why the cache

Telegram rate-limits raw file fetches (`GetFileRequest flood wait`). A
short-lived on-disk cache absorbs repeat hits and makes seeks/resumes instant
instead of hitting Telegram over and over. It is *not* a mirror — it's a hot
set, bounded by age and size.

### The atomic-rename trick

Downloads never write directly to the served filename. Bytes stream into a
hidden `.tmp` file first, then the file is published with a single
`os.replace()`. Because rename is atomic on POSIX, the real filename only ever
exists as a *complete* file — a reader can never observe a half-written
download. This is what fixed the old "corrupt stream on resume" bug.

### One download per file

Concurrent clicks on the same uncached link must not each start a full duplicate
download — that exhausts the download pool and starves everything else. Each
cache file has a single shared download task that all callers await, so the
second click waits for the first download rather than racing it.

### The link is a pointer, so links survive everything

Because `/b/` re-resolves on every request, the failure modes are all
degradations rather than breakages. A link whose buzz copy was deleted serves
from cache; a link whose cache was evicted re-fetches from Telegram. There is no
state a link can be in where it stops working while the file still exists on
Telegram.

### Two accounts, and why

A second librarian account is optional, but it buys two things:

- **A throttle parachute.** Telegram throttles `GetFileRequest` per account.
  When one account is being throttled, `BOOKLINK_DOWNLOAD_VIA` routes fetches
  through the other without touching a single link — links stay keyed to the
  account encoded in the token, only *who fetches* changes.
- **Hot-standby bandwidth.** Two accounts means two throttles.

**Use accounts with Telegram Premium.** This is not cosmetic. A non-Premium
account is throttled so aggressively that a single 300 MB book can take twenty
minutes, and the whole pipeline backs up behind it — silently, because every
individual link still works. With `BOOKLINK_DOWNLOAD_VIA=auto` the service
checks both accounts at startup and periodically, routes through one that
actually has Premium, and alerts if it has to move.

### Group intake: two lanes into the same pipe

By default you get links by forwarding files to the librarian account. In a
**private chat** that's the only way it can work — private-chat message IDs
differ per account, so only the account that received a file can fetch it.

A **group** is different: message IDs are shared, so *either* librarian account
can fetch any file posted there. List such a group in `BOOKLINK_GROUP_CHATS` and
posts in it are handled like personal sends, with the fetcher alternating
between accounts — so a batch posted to the group gets worked by two pipes at
once instead of one.

Notes if you use this:

- Only one account answers, so you get one link per book, not two.
- The group must not be listed in `BOOKLINK_NO_PRECACHE_CHATS` (that's the
  link-only lane).
- Keep chat history visible in the group, so an account that joins later can
  still fetch older posts.
- Grant the librarian accounts **membership, not admin** — membership is all
  they need.

### Upload priority

Offloads to the public copy tier run one at a time, because they share the same
pipe as downloads. But not all uploads are equally urgent, so there are two
lanes:

- **Send-time uploads go first** — someone is waiting on a book they just
  posted.
- **Reader-triggered uploads wait** — a click on an older link whose copy had
  lapsed.

Without this, a fresh batch of sends can queue behind hours of reader traffic
and appear to have silently failed.

## Notes

- Files larger than ~2 GB can't be re-uploaded to Telegram by non-premium
  accounts, but can still be streamed this way (the librarian account fetches
  them directly).
- The librarian account must be a member of the chat whose files you want to
  share.
- Links are permanent *as long as the Telegram message exists*. Deleting the
  message in Telegram is the one thing that breaks a link.

## Licence

MIT — see [LICENSE](LICENSE).
