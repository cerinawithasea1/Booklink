#!/usr/bin/env python3
"""
BookLink — permanent, shareable streaming links for Telegram files.

A link encodes (chat_id, message_id); when opened, a librarian account (an
admin in the groups whose files you want to share) fetches that message and
streams the bytes back — with HTTP Range support so audio/video players can
seek and resume.

Links are permanent: they re-fetch live every time, so nothing expires.
Files are cached on disk for a short window so seeks/resumes don't re-hit
Telegram (which throttles GetFileRequest and can make large downloads slow).

The service runs as its OWN Telethon session (a separate "device"), so it never
conflicts with any other bot or script using the same account.

Quick start
-----------
1. Fill in .env (copy .env.example) — you need a Telegram API id/hash and a
   StringSession for the librarian account.
2. `python3 make_session.py` to log in and print the session string.
3. `python3 streamer.py`
4. Forward a file to the librarian account — it replies with a permanent link.

"""
import os
import time
import asyncio
import base64
import hmac
import hashlib
import logging
import shutil
from pathlib import Path
from aiohttp import web
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import FileReferenceExpiredError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("booklink")

API_ID = int(os.environ["BOOKLINK_API_ID"])
API_HASH = os.environ["BOOKLINK_API_HASH"]
SESSION = os.environ["BOOKLINK_SESSION"]           # StringSession for the librarian account
SESSION_NEW = os.environ.get("BOOKLINK_SESSION_NEW", "")  # optional: second librarian account for new links
PORT = int(os.environ.get("BOOKLINK_PORT", "8094"))
HOST = os.environ.get("BOOKLINK_HOST", "127.0.0.1")  # localhost-only until a reverse proxy/tunnel fronts it
PUBLIC_BASE = os.environ.get("BOOKLINK_BASE", f"http://127.0.0.1:{PORT}")
LINK_SECRET = os.environ.get("BOOKLINK_SECRET", "")  # optional: signs private links

CACHE_DIR = Path(os.environ.get("BOOKLINK_CACHE_DIR", "/opt/booklink/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_MAX_AGE = int(os.environ.get("BOOKLINK_CACHE_MAX_AGE", str(24 * 3600)))  # seconds
CACHE_MAX_SIZE = int(os.environ.get("BOOKLINK_CACHE_MAX_SIZE", str(18 * 1024**3)))  # bytes (18GB hard cap)
DISK_ALERT_CHAT = os.environ.get("BOOKLINK_ALERT_CHAT", "")  # optional: ping this chat when disk is low
DISK_ALERT_MB = int(os.environ.get("BOOKLINK_DISK_ALERT_MB", "5120"))  # ping if free drops below this
DISK_ALERT_COOLDOWN = 24 * 3600  # ping at most once per day
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("BOOKLINK_MAX_DOWNLOADS", "1"))
DOWNLOAD_SEM = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
new_client = None
if SESSION_NEW:
    new_client = TelegramClient(StringSession(SESSION_NEW), API_ID, API_HASH)

CHUNK = 512 * 1024  # 512KB stream chunks


def slug_name(name: str) -> str:
    """Neat cosmetic URL name: 'Some Author  - A Book Title .m4b' -> 'Some-Author-A-Book-Title.m4b'"""
    import re as _re
    base, dot, ext = name.rpartition(".")
    if not dot:
        base, ext = name, ""
    s = _re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return f"{s}.{ext}" if ext else s


ACCOUNT_OLD = 0  # primary account (legacy links — no version tag in the token)
ACCOUNT_NEW = 1  # optional second account (token carries a "1" version tag)


def encode_ref(chat_id: int, message_id: int, account: int = ACCOUNT_OLD) -> str:
    raw = f"{chat_id}:{message_id}"
    if account == ACCOUNT_NEW:
        raw += ":1"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_ref(token: str):
    pad = "=" * (-len(token) % 4)
    parts = base64.urlsafe_b64decode(token + pad).decode().split(":")
    if len(parts) == 2:
        return int(parts[0]), int(parts[1]), ACCOUNT_OLD
    if len(parts) == 3 and parts[2] == "1":
        return int(parts[0]), int(parts[1]), ACCOUNT_NEW
    raise ValueError("bad link")


def sign(token: str) -> str:
    return hmac.new(LINK_SECRET.encode(), token.encode(), hashlib.sha256).hexdigest()[:16]


def cache_path(chat_id: int, message_id: int, account: int = ACCOUNT_OLD) -> Path:
    if account == ACCOUNT_NEW:
        return CACHE_DIR / f"1_{chat_id}_{message_id}.part"
    return CACHE_DIR / f"{chat_id}_{message_id}.part"


async def get_media_message(chat_id, message_id, cli):
    msg = await cli.get_messages(chat_id, ids=message_id)
    if not msg or not msg.media:
        return None
    return msg


def media_info(msg):
    doc = getattr(msg.media, "document", None)
    name, size, mime = None, None, "application/octet-stream"
    if doc:
        size = doc.size
        mime = doc.mime_type or mime
        for attr in doc.attributes:
            if hasattr(attr, "file_name") and attr.file_name:
                name = attr.file_name
    if not name:
        name = f"file_{getattr(msg, 'id', 0)}.bin"
    return name, size, mime


async def cache_cleanup_loop():
    while True:
        try:
            now = time.time()
            # 1) age-based: drop anything (finished or orphaned) older than the window
            for f in CACHE_DIR.iterdir():
                if f.is_file() and now - f.stat().st_mtime > CACHE_MAX_AGE:
                    f.unlink()
                    log.info(f"cache cleanup (age): removed {f.name}")
            # 2) size-based: oldest finished files first, until under the cap
            finished = [f for f in CACHE_DIR.iterdir() if f.is_file() and f.suffix != ".tmp"]
            finished.sort(key=lambda f: f.stat().st_mtime)
            total = sum(f.stat().st_size for f in finished)
            while total > CACHE_MAX_SIZE and finished:
                f = finished.pop(0)
                total -= f.stat().st_size
                f.unlink()
                log.info(f"cache cleanup (size): removed {f.name}")
        except Exception as e:
            log.error(f"cache cleanup error: {e}")
        await asyncio.sleep(3600)


_last_disk_alert = 0.0


async def disk_alert_loop():
    """Ping the configured chat if free disk on / drops too low.
    Uses the already-connected client, so no second session/token. No-op if
    BOOKLINK_ALERT_CHAT is not set."""
    global _last_disk_alert
    if not DISK_ALERT_CHAT:
        return
    while True:
        try:
            free_mb = shutil.disk_usage("/").free // (1024 * 1024)
            now = time.time()
            if free_mb < DISK_ALERT_MB and now - _last_disk_alert > DISK_ALERT_COOLDOWN:
                await client.send_message(
                    DISK_ALERT_CHAT,
                    f"⚠️ Low disk: {free_mb} MB free on /",
                )
                _last_disk_alert = now
                log.info(f"disk alert sent to {DISK_ALERT_CHAT} ({free_mb} MB free)")
        except Exception as e:
            log.error(f"disk alert error: {e}")
        await asyncio.sleep(600)


def _rm(path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass


async def download_to_cache(chat_id, message_id, msg, cli, account=ACCOUNT_OLD):
    """Download a file fully into cache and return its path. Raises on failure.

    Atomic-rename trick: bytes stream into a hidden `.tmp` file first, then the
    file is published with a single os.replace(). Because rename is atomic on
    POSIX, the real filename only ever exists as a *complete* file — a reader
    can never observe a half-written download (the old "corrupt stream" bug)."""
    cache_file = cache_path(chat_id, message_id, account)
    if cache_file.is_file():
        return cache_file
    tmp = cache_file.with_suffix(".tmp")
    async with DOWNLOAD_SEM:
        if cache_file.is_file():  # another task finished while we waited
            return cache_file
        media = msg.media
        try:
            with open(tmp, "wb") as fh:
                async for chunk in cli.iter_download(media, chunk_size=CHUNK):
                    fh.write(chunk)
        except FileReferenceExpiredError:
            # refresh the file reference once and retry
            fresh = await cli.get_messages(chat_id, ids=message_id)
            if fresh and fresh.media:
                media = fresh.media
                try:
                    with open(tmp, "wb") as fh:
                        async for chunk in cli.iter_download(media, chunk_size=CHUNK):
                            fh.write(chunk)
                except Exception:
                    _rm(tmp)
                    raise
            else:
                _rm(tmp)
                raise
        except Exception:
            _rm(tmp)
            raise
        os.replace(tmp, cache_file)
        log.info(f"cached {cache_file.name} ({cache_file.stat().st_size} bytes)")
        return cache_file


async def precache_in_background(chat_id, message_id, msg, cli, account=ACCOUNT_OLD):
    try:
        await download_to_cache(chat_id, message_id, msg, cli, account)
    except Exception as e:
        log.warning(f"background precache failed for {chat_id}/{message_id}: {e}")


async def handle_book(request: web.Request):
    token = request.match_info["token"]

    if LINK_SECRET:
        sig = request.query.get("s", "")
        if not hmac.compare_digest(sig, sign(token)):
            return web.Response(status=403, text="invalid or missing signature")

    try:
        chat_id, message_id, account = decode_ref(token)
    except Exception:
        return web.Response(status=400, text="bad link")

    cli = new_client if account == ACCOUNT_NEW else client
    if cli is None:
        return web.Response(status=503, text="second librarian account not configured")

    msg = await get_media_message(chat_id, message_id, cli)
    if msg is None:
        return web.Response(status=404, text="file not found (moved or deleted in Telegram)")

    name, size, mime = media_info(msg)
    safe_name = name.replace('"', "'")
    disposition = f'attachment; filename="{safe_name}"'

    cache_file = cache_path(chat_id, message_id, account)
    if not cache_file.is_file():
        try:
            await download_to_cache(chat_id, message_id, msg, cli, account)
        except Exception as e:
            log.error(f"download failed for {chat_id}/{message_id}: {e}")
            return web.Response(status=502, text="download failed — please try again")

    # Serve the complete cached file. FileResponse handles Range/Content-Length
    # correctly, so no live-stream corruption.
    return web.FileResponse(
        cache_file,
        headers={
            "Content-Type": mime,
            "Content-Disposition": disposition,
            "Access-Control-Allow-Origin": "*",
        },
    )


async def handle_health(request):
    me = (await client.get_me()).username
    me_new = (await new_client.get_me()).username if new_client else None
    return web.json_response({"ok": True, "me": me, "new": me_new})


@events.register(events.NewMessage(incoming=True, func=lambda e: e.is_private and e.media))
async def on_forwarded_book(event):
    """Forward any file to the librarian account → get its permanent link back."""
    try:
        doc = getattr(event.message.media, "document", None)
        if not doc:
            return
        cli = event.client
        account = ACCOUNT_NEW if (new_client is not None and cli is new_client) else ACCOUNT_OLD
        name = None
        for a in doc.attributes:
            if hasattr(a, "file_name") and a.file_name:
                name = a.file_name
        token = encode_ref(event.chat_id, event.message.id, account)
        link = f"{PUBLIC_BASE}/b/{token}"
        if LINK_SECRET:
            link += f"?s={sign(token)}"
        size_mb = doc.size / 1024 / 1024
        await event.reply(f"📚 {name or 'file'}\n{size_mb:.1f} MB\n\n🔗 Permanent link:\n{link}")
        asyncio.create_task(precache_in_background(event.chat_id, event.message.id, event.message, cli, account))
        log.info(f"link issued for {name} ({event.chat_id}/{event.message.id}, account {account})")
    except Exception as e:
        log.error(f"link handler error: {e}")
        await event.reply(f"couldn't make a link: {e}")


async def on_startup(app):
    await client.start()
    client.add_event_handler(on_forwarded_book)
    if new_client is not None:
        await new_client.start()
        new_client.add_event_handler(on_forwarded_book)
    asyncio.create_task(cache_cleanup_loop())
    asyncio.create_task(disk_alert_loop())
    me = await client.get_me()
    log.info(f"BookLink librarian logged in as @{me.username} (id {me.id}) — forward it a file to get a link")
    if new_client is not None:
        me_new = await new_client.get_me()
        log.info(f"BookLink second librarian logged in as @{me_new.username} (id {me_new.id})")


def build_app():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_get("/b/{token}", handle_book)
    # Pretty form: /b/<key>/Any Book Name.m4b  — the name is cosmetic, the key does the work
    app.router.add_get("/b/{token}/{name:.*}", handle_book)
    app.on_startup.append(on_startup)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host=HOST, port=PORT)
