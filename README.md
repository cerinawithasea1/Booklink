# BookLink

Permanent, shareable **streaming links for Telegram files**.

Forward a file to a librarian account and get back a URL that streams it —
with HTTP Range support so audio/video players can seek and resume. Links never
expire: they re-fetch live from Telegram on every request.

```
books.yourdomain.com/b/<key>/Any Book Title.m4b
                              └─ cosmetic, the <key> does the work
```

## How it works

- A link encodes `chat_id:message_id`. When opened, the librarian account (an
  admin in the group the file lives in) fetches that message and streams the
  bytes back over HTTP.
- Files are cached on disk for a short window so seeks/resumes don't re-hit
  Telegram (which throttles `GetFileRequest`).
- The service runs its own Telethon session, so it never conflicts with any
  other bot or script using the same account.

## Setup

1. **Install**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure**

   ```bash
   cp .env.example .env
   ```

   Fill in `.env`:
   - `BOOKLINK_API_ID` / `BOOKLINK_API_HASH` — from
     [my.telegram.org](https://my.telegram.org) → API development tools.
   - `BOOKLINK_SESSION` — generate with `make_session.py` (below).

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

## Why the cache?

Telegram rate-limits raw file fetches (`GetFileRequest flood wait`). Serving
from a short-lived on-disk cache absorbs repeat hits and makes seeks/resumes
instant instead of hitting Telegram over and over.

## The atomic-rename trick

Downloads never write directly to the served filename. Bytes stream into a
hidden `.tmp` file first, then the file is published with a single
`os.replace()`. Because rename is atomic on POSIX, the real filename only ever
exists as a *complete* file — a reader can never observe a half-written
download. This is what fixed the old "corrupt stream on resume" bug.

## Notes

- Files larger than ~2 GB can't be re-uploaded to Telegram by non-premium
  accounts, but can still be streamed this way (the librarian account fetches
  them directly).
- The librarian account must be a member of the group whose files you want to
  share.
