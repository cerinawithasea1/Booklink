#!/usr/bin/env python3
"""
BookLink — permanent, shareable streaming links for Cerina's library.

A link encodes (chat_id, message_id); when opened, the librarian account
(Cerina2, already admin in all her groups) fetches that message and streams
the bytes — with HTTP Range support so audio players can seek/resume.

Links are permanent: they re-fetch live every time, so nothing expires.
Files are cached on disk for a short window so seeks/resumes don't re-hit
Telegram (which throttles GetFileRequest and made downloads take hours).

Runs as its OWN Telethon session so it never conflicts with fairybot or the
session-scheduler (same account, separate login = fine, like a second device).

Built by Uncle Fable with Cerina, July 11 2026. See BOOKLINK_PLAN.md.
"""
import os
import re
import time
import json
import asyncio
import base64
import hmac
import html
import itertools
from collections import deque
import hashlib
import logging
import shutil
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, unquote
import aiohttp
from aiohttp import web
from aiohttp.web_log import AccessLogger
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import FileReferenceExpiredError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("booklink")

API_ID = int(os.environ["BOOKLINK_API_ID"])
API_HASH = os.environ["BOOKLINK_API_HASH"]
SESSION = os.environ["BOOKLINK_SESSION"]           # StringSession for Cerina2 (librarian)
SESSION_NEW = os.environ.get("BOOKLINK_SESSION_NEW", "")  # optional: fresh librarian account for new links
PORT = int(os.environ.get("BOOKLINK_PORT", "8091"))
HOST = os.environ.get("BOOKLINK_HOST", "127.0.0.1")  # localhost-only until a tunnel fronts it
PUBLIC_BASE = os.environ.get("BOOKLINK_BASE", f"http://127.0.0.1:{PORT}")
LINK_SECRET = os.environ.get("BOOKLINK_SECRET", "")  # optional: signs private links

CACHE_DIR = Path(os.environ.get("BOOKLINK_CACHE_DIR", "/opt/booklink/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_MAX_AGE = int(os.environ.get("BOOKLINK_CACHE_MAX_AGE", str(24 * 3600)))  # seconds
CACHE_MAX_SIZE = int(os.environ.get("BOOKLINK_CACHE_MAX_SIZE", str(18 * 1024**3)))  # bytes (18GB hard cap)
DISK_ALERT_CHAT = os.environ.get("BOOKLINK_ALERT_CHAT", "cerinawithasea")
DISK_ALERT_MB = int(os.environ.get("BOOKLINK_DISK_ALERT_MB", "10240"))  # ping if free drops below this
DISK_ALERT_COOLDOWN = int(os.environ.get("BOOKLINK_DISK_ALERT_COOLDOWN", str(4 * 3600)))  # re-alert at most this often
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("BOOKLINK_MAX_DOWNLOADS", "4"))
# Parachute when Telegram throttles one account's GetFileRequest: set
# BOOKLINK_DOWNLOAD_VIA=old (or new) to route ALL media fetches/downloads through
# that account. Links and cache files stay keyed to the account encoded in each
# /b/ token, so public URLs are never affected — only who does the fetching.
DOWNLOAD_VIA = os.environ.get("BOOKLINK_DOWNLOAD_VIA", "").strip().lower()
# Resolved at startup and re-checked by premium_watch_loop. "old"/"new" pins the
# choice; "auto"/"" lets us pick an account that actually has Telegram Premium.
_download_via = DOWNLOAD_VIA
# Group books alternate between the two librarian accounts (see pick_cli_for).
_group_rr = itertools.cycle(("new", "old"))
# @Cerina2 is Telegram-throttled: parallel downloads on it just stack flood-waits
# and keep the account pegged. Run it strictly serial so it can cool down. The
# fresh account isn't throttled, so it may fetch several books at once.
DOWNLOAD_SEM_OLD = asyncio.Semaphore(int(os.environ.get("BOOKLINK_MAX_DOWNLOADS_OLD", "1")))
DOWNLOAD_SEM_NEW = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# One download per cache file, so concurrent clicks on the same uncached link
# don't each start a full duplicate Telegram download (which floods GetFileRequest).
_inflight = {}  # cache_file path -> asyncio.Task (single-flight download)

# Chats whose forwards get a link only (no precache). These are the group books the
# notifier forwards to the librarian — they're a list/database for the ABB, not
# hot links, so pre-downloading them just wastes Telegram bandwidth and evicts the
# real (personal) singles from cache. Set to the forwarder->librarian DM id.
NO_PRECACHE_CHATS = {
    int(x) for x in os.environ.get("BOOKLINK_NO_PRECACHE_CHATS", "").replace(" ", "").split(",")
    if x.lstrip("-").isdigit()
}

# Groups where books are posted so that BOTH librarian accounts can fetch them.
# A private chat's message IDs differ per account, so a DM book can only ever be
# downloaded by the account that received it — which is why account-swapping can't
# speed up personal sends. A GROUP has shared message IDs, so either account can
# fetch any book posted in it. Posts here are handled like personal sends (link +
# precache + buzz); they are NOT on the NO_PRECACHE list.
GROUP_CHATS = {
    int(x) for x in os.environ.get("BOOKLINK_GROUP_CHATS", "").replace(" ", "").split(",")
    if x.lstrip("-").isdigit()
}

FILEBOX_URL = os.environ.get("BOOKLINK_FILEBOX_URL", "http://127.0.0.1:12345")

# --- buzzheavier offload tier (dormant unless BOOKLINK_BUZZ_TOKEN is set) ---
# Shared personal singles get a background copy uploaded to buzzheavier after they
# are cached; while that copy is alive, /b/ requests redirect to its landing page.
# Group books (Book Fairy / ABS, chat in NO_PRECACHE_CHATS) are NEVER offloaded.
# Telegram stays the source of truth: a dead/expired buzz copy just means we serve
# locally again, so public links can never break. Token = the account's Auth ID
# (single Bearer credential, no separate token).
BUZZ_TOKEN = os.environ.get("BOOKLINK_BUZZ_TOKEN", "")
BUZZ_DIR = os.environ.get("BOOKLINK_BUZZ_DIR", "")  # buzz folder id to upload into
BUZZ_MAP_PATH = Path(os.environ.get("BOOKLINK_BUZZ_MAP", "/opt/booklink/buzz_map.json"))


class OffloadGate:
    """One upload at a time, with a fast lane for Cerina's own sends.

    Uploads stay serial because they share the same pipe as the downloads. But
    the two kinds of upload are not equally urgent: a send-time upload is a book
    she just posted and is waiting to see, while a click-triggered upload is a
    reader whose buzz copy had lapsed. On 2026-10-07 a batch of her sends sat
    behind ~105 reader-triggered uploads and took over an hour to appear — and
    readers who hit a slow link often don't come back. So sends go first.
    """

    def __init__(self):
        self._busy = False
        self._high = deque()
        self._low = deque()

    async def acquire(self, high: bool):
        if not self._busy and not self._high and not self._low:
            self._busy = True
            return
        fut = asyncio.get_running_loop().create_future()
        (self._high if high else self._low).append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            if fut.cancelled():
                self.release()  # hand the slot on rather than lose it
            raise

    def release(self):
        # Hand the slot straight to the next waiter, high lane first.
        for lane in (self._high, self._low):
            while lane:
                fut = lane.popleft()
                if not fut.done():
                    fut.set_result(None)
                    return
        self._busy = False


OFFLOAD_GATE = OffloadGate()
_buzz_map = {}  # "chat:msg:account" -> {"id", "expiry", "expiry_epoch", "name"}
_buzz_lock = asyncio.Lock()


def _buzz_load():
    global _buzz_map
    if BUZZ_MAP_PATH.is_file():
        try:
            _buzz_map = json.loads(BUZZ_MAP_PATH.read_text())
            log.info(f"buzz map loaded: {len(_buzz_map)} entries")
        except Exception as e:
            log.warning(f"buzz map unreadable, starting empty: {e}")


def _buzz_save():
    tmp = BUZZ_MAP_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(_buzz_map, indent=1))
    os.replace(tmp, BUZZ_MAP_PATH)


def _buzz_key(chat_id: int, message_id: int, account: int) -> str:
    return f"{chat_id}:{message_id}:{account}"


def _buzz_live(key: str):
    """Return the map entry if its buzz copy is (conservatively) still alive."""
    ent = _buzz_map.get(key)
    if ent and ent.get("expiry_epoch", 0) > time.time():
        return ent
    return None


async def _buzz_list_ids():
    """Every file id in the buzz upload directory, or None if the listing failed."""
    ids = set()
    cursor = None
    pages = 0
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        while True:
            url = f"https://buzzheavier.com/api/fs/{BUZZ_DIR}"
            if cursor:
                url += f"?cursor={cursor}"
            try:
                async with s.get(url, headers={"Authorization": f"Bearer {BUZZ_TOKEN}"}) as r:
                    data = (await r.json(content_type=None)).get("data") or {}
            except Exception as e:
                log.warning(f"buzz listing failed: {e}")
                return None
            for child in data.get("children") or []:
                ids.add(child["id"])
            cursor = data.get("nextCursor")
            pages += 1
            if not cursor or pages > 60:
                break
    return ids


async def buzz_reconcile_loop():
    """Drop map entries whose buzz copy no longer exists.

    Files get deleted on buzz by hand, but the map would still call them live —
    so /b/ would redirect a reader to a dead page instead of quietly falling
    back to Telegram. Reconciling against the real directory listing keeps that
    redirect honest. A book pruned here simply offloads again if it's clicked."""
    await asyncio.sleep(60)
    while True:
        try:
            if BUZZ_TOKEN and BUZZ_DIR:
                live = await _buzz_list_ids()
                if live is not None:
                    gone = [k for k, v in _buzz_map.items() if v.get("id") and v["id"] not in live]
                    if gone:
                        for k in gone:
                            _buzz_map.pop(k, None)
                        _buzz_save()
                        log.info(f"buzz reconcile: dropped {len(gone)} stale entries")
        except Exception as e:
            log.warning(f"buzz reconcile error: {e}")
        await asyncio.sleep(6 * 3600)


async def _buzz_existing(session, raw_name: str, size: int):
    """Return (id, expiry) of a buzz copy we already hold for this exact file.

    Two identical files split the download count between them, which halves the
    chance either reaches buzz's permanence threshold (15+ downloads in 45 days)
    — the thing that eventually stops a /b/ link pointing at Telegram. Match on
    name AND byte size, so a different book that happens to share a filename can
    never be served in its place. The API also reports each file's live expiry,
    which can be later than the one we recorded (downloads extend it)."""
    ids = {v["id"] for v in _buzz_map.values() if v.get("name") == raw_name and v.get("id")}
    for fid in ids:
        try:
            async with session.get(
                f"https://buzzheavier.com/api/fs/{fid}",
                headers={"Authorization": f"Bearer {BUZZ_TOKEN}"},
            ) as r:
                data = (await r.json(content_type=None)).get("data") or {}
        except Exception:
            continue
        if data.get("size") != size:
            continue
        exp = data.get("expiry")
        if exp:  # a copy that has already lapsed can't serve anyone
            try:
                if datetime.fromisoformat(exp.replace("Z", "+00:00")).timestamp() <= time.time():
                    continue
            except Exception:
                pass
        return data.get("id"), exp
    return None


async def buzz_upload(cache_file: Path, name: str, high: bool = True) -> tuple[str, str | None]:
    """PUT the cached file to buzzheavier; returns (file_id, expiry ISO or None).

    `high` puts this upload in the fast lane — True for books Cerina just sent,
    False for reader-click-triggered ones (see OffloadGate)."""
    raw_name = name
    name = quote(name)[:500]
    note = base64.b64encode(name.encode()).decode()
    base = f"https://w.buzzheavier.com/{BUZZ_DIR}/{name}" if BUZZ_DIR else f"https://w.buzzheavier.com/{name}"
    await OFFLOAD_GATE.acquire(high)
    try:
        log.info(f"buzz: uploading {raw_name[:60]} (priority={'send' if high else 'click'})")
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=60)
        size = cache_file.stat().st_size
        async with aiohttp.ClientSession(timeout=timeout) as s:

            async def put(url):
                # aiohttp closes the file object after a request, so each
                # attempt must open its own handle.
                with open(cache_file, "rb") as fh:
                    async with s.put(
                        url,
                        data=fh,
                        headers={"Authorization": f"Bearer {BUZZ_TOKEN}"},
                    ) as resp:
                        return resp.status, await resp.json(content_type=None)

            existing = await _buzz_existing(s, raw_name, size)
            if existing:
                log.info(f"buzz reuse for {raw_name}")
                return existing

            status, body = await put(f"{base}?note={note}")
            if status == 400 and "already exists" in str(body):
                # A copy exists that we don't know about locally. Look it up
                # rather than adding a second one; only if that fails do we fall
                # back to a unique name so the upload still succeeds.
                existing = await _buzz_existing(s, raw_name, size)
                if existing:
                    return existing
                p = Path(unquote(name))
                alt = f"{p.stem}__{uuid.uuid4().hex[:6]}{p.suffix}"
                alt_url = (
                    f"https://w.buzzheavier.com/{BUZZ_DIR}/{quote(alt)}"
                    if BUZZ_DIR else f"https://w.buzzheavier.com/{quote(alt)}"
                )
                status, body = await put(f"{alt_url}?note={note}")
            if status not in (200, 201):
                raise RuntimeError(f"buzz upload {status}: {body}")
            data = body.get("data") or {}
            return data["id"], data.get("expiry")
    finally:
        OFFLOAD_GATE.release()


_offload_inflight = set()


async def buzz_offload_task(key, chat_id, message_id, account, cache_file, name, high=True):
    """Upload one book to buzz. `high` = Cerina's own send (see OffloadGate).

    One upload per key at a time, tracked per-key rather than under the global
    map lock: that lock used to be held across the whole upload, which queued
    every waiter in arrival order and would have made the priority lanes
    meaningless.
    """
    if key in _offload_inflight or _buzz_live(key):
        return
    _offload_inflight.add(key)
    try:
        try:
            fid, expiry = await buzz_upload(cache_file, name, high)
        except Exception as e:
            log.warning(f"buzz offload failed for {key}: {e}")
            async with _buzz_lock:
                _buzz_map[key] = {"retry_after": time.time() + 3600}
                _buzz_save()
            return
        epoch = 0.0
        if expiry:
            try:
                epoch = datetime.fromisoformat(expiry.replace("Z", "+00:00")).timestamp()
            except Exception:
                pass
        async with _buzz_lock:
            _buzz_map[key] = {"id": fid, "expiry": expiry, "expiry_epoch": epoch, "name": name}
            _buzz_save()
        log.info(f"buzz offloaded {key} -> {fid} (expires {expiry})")
    finally:
        _offload_inflight.discard(key)

# ---------------------------------------------------------------------------
# Public offload tier — the seeding box
#
# Replaces buzzheavier, disabled 2026-10-10 after its ad-funded landing page
# served a reader a macOS infostealer lure. The public copy now goes to Cerina's
# own seeding box, whose nginx serves static files openly from ~/www/books (the
# auth there covers only PHP and WebDAV). A book becomes a copy over SSH with a
# dedicated key, so the reader's path contains no third party and no ads at all.
#
# Telegram stays the source of truth exactly as before: if the box is full,
# unreachable, or the copy is deleted, /b/ simply serves locally again.
# ---------------------------------------------------------------------------
ULTRA_BASE = os.environ.get("BOOKLINK_ULTRA_BASE", "").rstrip("/")
ULTRA_SSH = os.environ.get("BOOKLINK_ULTRA_SSH", "")       # user@host
ULTRA_KEY = os.environ.get("BOOKLINK_ULTRA_KEY", "")       # private key for that host
ULTRA_DIR = os.environ.get("BOOKLINK_ULTRA_DIR", "www/books")
ULTRA_MAP_PATH = Path(os.environ.get("BOOKLINK_ULTRA_MAP", "/opt/booklink/ultra_map.json"))
ULTRA_ENABLED = bool(ULTRA_BASE and ULTRA_SSH and ULTRA_KEY)

_ultra_map = {}  # "chat:msg:account" -> {"url", "name", "file", "size", "added", "hits"}
_ultra_lock = asyncio.Lock()
_ultra_inflight = set()
_ultra_dirty = False  # hit counters changed since the last flush


def _ultra_hit(key: str) -> None:
    """Record a reader being sent to the public copy. Cheap: memory only.

    Counting here rather than flushing on every click keeps a burst of traffic
    from turning into a burst of disk writes; ultra_save_loop flushes later.
    """
    global _ultra_dirty
    ent = _ultra_map.get(key)
    if ent and ent.get("url"):
        ent["hits"] = ent.get("hits", 0) + 1
        ent["last_hit"] = time.time()
        _ultra_dirty = True


async def ultra_save_loop():
    global _ultra_dirty
    while True:
        await asyncio.sleep(30)
        if _ultra_dirty:
            async with _ultra_lock:
                try:
                    _ultra_save()
                    _ultra_dirty = False
                except Exception as e:
                    log.warning(f"ultra map flush failed: {e}")


def _ultra_load():
    global _ultra_map
    if ULTRA_MAP_PATH.is_file():
        try:
            _ultra_map = json.loads(ULTRA_MAP_PATH.read_text())
            log.info(f"ultra map loaded: {len(_ultra_map)} entries")
        except Exception as e:
            log.warning(f"ultra map unreadable, starting empty: {e}")


def _ultra_save():
    tmp = ULTRA_MAP_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(_ultra_map, indent=1))
    os.replace(tmp, ULTRA_MAP_PATH)


def _ultra_live(key: str):
    """Return the entry if a public copy has been recorded for this book."""
    ent = _ultra_map.get(key)
    return ent if ent and ent.get("url") else None


def _ultra_filename(key: str, name: str) -> str:
    """A readable, collision-proof public name: the book's title plus a short hash."""
    stem, dot, ext = name.rpartition(".")
    if not dot:
        stem, ext = name, "bin"
    slug = re.sub(r"[^A-Za-z0-9]+", "-", stem).strip("-")[:80] or "book"
    digest = hashlib.sha1(key.encode()).hexdigest()[:10]
    return f"{slug}-{digest}.{ext.lower()}"


async def ultra_offload_task(key, chat_id, message_id, account, cache_file, name, high=True):
    """Copy one cached book to the public box and record its URL.

    One copy per key at a time, and serialised against every other copy through
    the shared gate, because the transfer shares the pipe with the downloads.
    """
    if not ULTRA_ENABLED or key in _ultra_inflight or _ultra_live(key):
        return
    _ultra_inflight.add(key)
    try:
        await OFFLOAD_GATE.acquire(high)
        try:
            remote_name = _ultra_filename(key, name)
            log.info(f"offload: copying {remote_name} ({'send' if high else 'click'})")
            proc = await asyncio.create_subprocess_exec(
                "rsync", "--inplace",
                "-e", f"ssh -i {ULTRA_KEY} -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new",
                str(cache_file), f"{ULTRA_SSH}:{ULTRA_DIR}/{remote_name}",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, err = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError((err or b"").decode()[-300:] or f"rsync exit {proc.returncode}")
        finally:
            OFFLOAD_GATE.release()
        url = f"{ULTRA_BASE}/{quote(remote_name)}"
        try:
            size = Path(cache_file).stat().st_size
        except OSError:
            size = 0
        async with _ultra_lock:
            _ultra_map[key] = {
                "url": url,
                "name": name,
                "file": remote_name,
                "size": size,
                "added": time.time(),
                "hits": 0,
            }
            _ultra_save()
        log.info(f"offloaded {key} -> {url}")
    except Exception as e:
        log.warning(f"offload failed for {key}: {e}")
        async with _ultra_lock:
            _ultra_map[key] = {"retry_after": time.time() + 3600}
            _ultra_save()
    finally:
        _ultra_inflight.discard(key)


client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
new_client = None
if SESSION_NEW:
    new_client = TelegramClient(StringSession(SESSION_NEW), API_ID, API_HASH)

CHUNK = 512 * 1024  # 512KB stream chunks

# Search/AI crawlers that follow public Mobilism links and download whole m4b files.
# They generate worthless GetFileRequest load and keep the account throttled.
_CRAWLER_UAS = (
    "bingbot", "googlebot", "gptbot", "claudebot", "anthropic-ai",
    "amazonbot", "duckduckbot", "facebookexternalhit", "slurp",
    "baiduspider", "yandex", "bytespider", "petalbot", "mj12bot",
    "semrushbot", "ahrefsbot", "ccbot", "applebot", "perplexitybot",
)


def is_crawler(request: web.Request) -> bool:
    ua = (request.headers.get("User-Agent") or "").lower()
    return any(bot in ua for bot in _CRAWLER_UAS)


def slug_name(name: str) -> str:
    """Neat cosmetic URL name: 'Laura Sims  - How Can I Help You .m4b' → 'Laura-Sims-How-Can-I-Help-You.m4b'"""
    import re as _re
    base, dot, ext = name.rpartition(".")
    if not dot:
        base, ext = name, ""
    s = _re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return f"{s}.{ext}" if ext else s


ACCOUNT_OLD = 0  # @Cerina2 (legacy links — no version tag in the token)
ACCOUNT_NEW = 1  # the fresh account (token carries a "1" version tag)


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


async def _warm_entities(cli):
    """Populate Telethon's in-memory entity cache so numeric-peer lookups work.

    StringSession does not persist access hashes across restarts, so the first
    get_messages() for a peer the librarian hasn't heard from since startup
    fails with "Could not find the input entity". Fetching dialogs once re-caches
    every known peer (including each forwarder's DM)."""
    try:
        dialogs = await cli.get_dialogs()
        log.info("warmed entity cache: %d dialogs", len(dialogs))
    except Exception as e:
        log.warning("entity warm-up failed (links may 500 until the peer re-messages): %s", e)


def pick_cli(account):
    """Client used for Telegram fetches/downloads, honoring the throttle parachute."""
    if _download_via == "old" and client is not None:
        return client
    if _download_via == "new" and new_client is not None:
        return new_client
    return new_client if account == ACCOUNT_NEW else client


def own_cli(account):
    """The token account's own client — the only one that sees its private chats."""
    return new_client if account == ACCOUNT_NEW else client


def pick_cli_for(account, chat_id):
    """Download client for one book. Groups alternate between the two accounts.

    In a group both librarians see the same message IDs, so either can fetch — and
    since each Premium account has its own throttle, alternating means a batch
    posted to the group gets worked by TWO pipes instead of one. Personal (private)
    books are untouched: only the account that received them can fetch those.
    """
    if (
        chat_id in GROUP_CHATS
        and DOWNLOAD_VIA not in ("old", "new")  # honour an explicit pin
        and client is not None
        and new_client is not None
    ):
        choice = next(_group_rr)
        log.info(f"group book {chat_id}: fetching via the {choice} account")
        return new_client if choice == "new" else client
    return pick_cli(account)


async def _alert(text):
    """Message Cerina directly. Used for the things that break quietly."""
    sender = new_client or client
    try:
        await sender.send_message(DISK_ALERT_CHAT, text)
    except Exception as e:
        log.warning(f"alert send failed: {e}")


async def _premium_flags():
    """{'old': bool, 'new': bool} — Telegram Premium on each librarian account."""
    flags = {}
    for label, cli in (("old", client), ("new", new_client)):
        if cli is None:
            continue
        try:
            me = await cli.get_me()
            flags[label] = bool(getattr(me, "premium", False))
        except Exception as e:
            log.warning(f"premium check failed for {label}: {e}")
    return flags


async def resolve_download_account(initial=False):
    """Route downloads through an account that actually has Telegram Premium.

    Premium is not cosmetic here. A non-Premium account is throttled so hard that
    a 300 MB book takes ~20 minutes, and the whole precache -> buzz pipeline backs
    up behind it. On 2026-09-25 @Cerina2 quietly lost Premium and every download
    through it collapsed to 72 KB/s — the only visible symptom was books arriving
    late, and nothing in the pipeline noticed the cause. So we check.
    """
    global _download_via

    if DOWNLOAD_VIA in ("old", "new"):
        # Explicitly pinned. Honour it, but say so if it's the wrong one.
        _download_via = DOWNLOAD_VIA
        flags = await _premium_flags()
        if flags and not flags.get(DOWNLOAD_VIA, False):
            log.warning(
                f"BOOKLINK_DOWNLOAD_VIA={DOWNLOAD_VIA} is pinned but that account has no "
                "Telegram Premium — downloads will be throttled"
            )
        return

    flags = await _premium_flags()
    premium = [k for k in ("new", "old") if flags.get(k)]  # prefer the fresh account
    if not premium:
        if flags:
            log.warning("neither librarian account has Telegram Premium — downloads will be throttled")
        return
    if _download_via in premium:
        return
    previous, _download_via = _download_via, premium[0]
    if not initial:
        log.info(f"download account moved {previous or 'auto'} -> {_download_via} (has Premium)")
        await _alert(f"⚠️ BookLink: downloads were re-routed to the {_download_via} account — the other one has no Telegram Premium")


async def premium_watch_loop():
    """A Premium lapse costs ~30x download speed and looks like nothing at all."""
    await asyncio.sleep(600)
    while True:
        try:
            await resolve_download_account()
        except Exception as e:
            log.warning(f"premium watch error: {e}")
        await asyncio.sleep(6 * 3600)


async def get_media_message(chat_id, message_id, cli):
    try:
        msg = await cli.get_messages(chat_id, ids=message_id)
    except ValueError as e:
        # Cold entity cache (e.g. just restarted) — warm once and retry.
        log.warning("input entity miss for %s; warming cache and retrying: %s", chat_id, e)
        await _warm_entities(cli)
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
    """Ping Cerina if free disk on / drops too low.

    Sent from @lightweavebooks (the pinned librarian account) so it isn't buried
    like @Cerina2. Falls back to the legacy client if the fresh session is absent."""
    global _last_disk_alert
    sender = new_client or client
    while True:
        try:
            free_mb = shutil.disk_usage("/").free // (1024 * 1024)
            now = time.time()
            if free_mb < DISK_ALERT_MB and now - _last_disk_alert > DISK_ALERT_COOLDOWN:
                await sender.send_message(
                    DISK_ALERT_CHAT,
                    f"⚠️ Low disk on ready-1986: {free_mb} MB free on /",
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
    """Download a book fully into cache and return its path. Raises on failure.
    Used by both the background pre-fetch and the on-demand serve path, so a file
    is always served from a complete on-disk copy (never streamed live)."""
    cache_file = cache_path(chat_id, message_id, account)
    if cache_file.is_file():
        return cache_file

    # Single-flight per file: repeated clicks / retries on one uncached link must
    # not each start a full duplicate Telegram download. One shared task per
    # cache file is what makes that true — a dict of Locks does not, because the
    # entry is popped while waiters still hold the lock, so after a failure each
    # arriving request builds a fresh lock and starts yet another full download.
    # That race filled the download semaphore with duplicate copies of one
    # popular book and starved send-time precache.
    task = _inflight.get(cache_file)
    if task is None or task.done():
        task = asyncio.ensure_future(
            _download_once(cache_file, chat_id, message_id, msg, cli, account)
        )
        _inflight[cache_file] = task
        task.add_done_callback(lambda t, cf=cache_file: _inflight_done(cf, t))
    return await asyncio.shield(task)


def _inflight_done(cache_file, task):
    _inflight.pop(cache_file, None)
    if not task.cancelled():
        task.exception()  # mark retrieved; awaiters still get the raise


async def _download_once(cache_file, chat_id, message_id, msg, cli, account):
    tmp = cache_file.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
    sem = DOWNLOAD_SEM_NEW if cli is new_client else DOWNLOAD_SEM_OLD
    async with sem:
        media = msg.media
        try:
            with open(tmp, "wb") as fh:
                async for chunk in cli.iter_download(media, chunk_size=CHUNK):
                    fh.write(chunk)
        except FileReferenceExpiredError:
            # refresh the file reference once and retry. Under the parachute the
            # download client may not see this peer (private-chat IDs differ per
            # account) — then borrow a fresh reference from the token account.
            fresh = await cli.get_messages(chat_id, ids=message_id)
            if not (fresh and fresh.media):
                own = own_cli(account)
                if own is not None and own is not cli:
                    fresh = await own.get_messages(chat_id, ids=message_id)
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
        dcli = pick_cli_for(account, chat_id)
        if dcli is None:
            return
        if dcli is not cli:
            # Throttle parachute active: refetch the message with the download
            # account so the media access hash matches the client that reads it.
            fresh = await get_media_message(chat_id, message_id, dcli)
            if fresh is not None:
                msg, cli = fresh, dcli
            else:
                # The parachute client can't see this peer (private-chat IDs
                # differ per account) — download via the token account instead.
                own = own_cli(account)
                if own is None:
                    return
                msg = await get_media_message(chat_id, message_id, own)
                if msg is None:
                    return
                cli = own
        await download_to_cache(chat_id, message_id, msg, cli, account)
        # The public copy belongs here too: the point of precaching personal sends
        # is to have copies ready BEFORE anyone clicks, so copy right away
        # (link-only chats excluded — they only ever get click-triggered offload).
        if ULTRA_ENABLED and chat_id not in NO_PRECACHE_CHATS:
            key = _buzz_key(chat_id, message_id, account)
            if not _ultra_live(key) and time.time() >= (_ultra_map.get(key) or {}).get("retry_after", 0):
                name, _, _ = media_info(msg)
                cache_file = cache_path(chat_id, message_id, account)
                asyncio.create_task(ultra_offload_task(key, chat_id, message_id, account, cache_file, name))
    except Exception as e:
        log.warning(f"background precache failed for {chat_id}/{message_id}: {e}")


def real_ip(request: web.Request) -> str:
    """The actual visitor address.

    BookLink sits behind a Cloudflare tunnel, so the socket peer is always the
    local proxy and %a in the access log is useless. Cloudflare passes the true
    address in CF-Connecting-IP.
    """
    return (
        request.headers.get("CF-Connecting-IP")
        or (request.headers.get("X-Forwarded-For", "").split(",")[0].strip())
        or request.remote
        or "-"
    )


class DownloadedFileResponse(web.FileResponse):
    """A file response that only counts as a download once the client took it.

    aiohttp sends a FileResponse body inside prepare(), so a client that drops
    mid-transfer raises ConnectionResetError out of it and an intact one returns
    normally. Firing a callback only when prepare() returns therefore fires only
    for transfers that actually finished — which is the distinction that matters:

    the buzz offload used to fire on the click alone, so a crawler that merely
    poked at a URL still cost a full download plus a public upload. On 2026-10-07
    a link crawler produced 476 dropped downloads and every single one of them
    still queued an upload.
    """

    def __init__(self, *args, on_download=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_download = on_download

    async def prepare(self, request):
        result = await super().prepare(request)
        callback, self._on_download = self._on_download, None
        # A HEAD carries no body, and a 206 is a ranged seek — neither proves
        # anybody wanted the whole file, so neither mints a copy.
        if callback is not None and request.method == "GET" and self.status == 200:
            try:
                asyncio.create_task(callback())
            except Exception as e:
                log.warning(f"post-download offload hook failed: {e}")
        return result


async def handle_book(request: web.Request):
    if is_crawler(request):
        return web.Response(status=403, text="forbidden")
    token = request.match_info["token"]

    if LINK_SECRET:
        sig = request.query.get("s", "")
        if not hmac.compare_digest(sig, sign(token)):
            return web.Response(status=403, text="invalid or missing signature")

    try:
        chat_id, message_id, account = decode_ref(token)
    except Exception:
        return web.Response(status=400, text="bad link")

    cache_file = cache_path(chat_id, message_id, account)
    meta_file = Path(str(cache_file) + ".meta.json")

    # The public copy is checked BEFORE the local-cache check. The cache is
    # evicted after 24h but the copy persists, so requiring a cached file to
    # reach the redirect would re-download every book older than a day from
    # Telegram on every single click — and readers hammering one popular link
    # then keep the download semaphore full. A copy on the box is a complete
    # answer on its own; if it's absent we fall through to cache/Telegram.
    if ULTRA_ENABLED and chat_id not in NO_PRECACHE_CHATS:
        key = _buzz_key(chat_id, message_id, account)
        ent = _ultra_live(key)
        if ent:
            # Only GETs count as a reader: players and link-preview bots send
            # HEADs, and inflating the count would break exactly the pruning
            # decision the number exists to support.
            if request.method == "GET":
                _ultra_hit(key)
            return web.HTTPFound(ent["url"])

    # Fast path: fully cached book with known metadata. Served with NO Telegram
    # round-trip, so Telegram throttling/outages can never break posted links.
    if cache_file.is_file() and meta_file.is_file():
        try:
            meta = json.loads(meta_file.read_text())
            name, mime = meta["name"], meta.get("mime", "application/octet-stream")
        except Exception:
            name = None
        if name:
            safe_name = name.replace('"', "'")
            # While a public copy exists, send readers to it — that keeps the
            # load off Telegram and off this box entirely. Never offload
            # link-only chats.
            on_download = None
            if ULTRA_ENABLED and chat_id not in NO_PRECACHE_CHATS:
                key = _buzz_key(chat_id, message_id, account)
                ent = _ultra_live(key)
                if ent:
                    if request.method == "GET":
                        _ultra_hit(key)
                    return web.HTTPFound(ent["url"])
                if time.time() >= (_ultra_map.get(key) or {}).get("retry_after", 0):
                    # Copy the book only once the client has actually taken the
                    # file, behind Cerina's own sends (high=False). A click on
                    # its own proves nothing — a crawler can click anything,
                    # and on 2026-10-07 one did exactly that.
                    who = real_ip(request)

                    def _mint_copy(key=key, who=who):
                        log.info(f"download completed ({who}) — queuing public copy for {key}")
                        return ultra_offload_task(
                            key, chat_id, message_id, account, cache_file, name, high=False
                        )

                    on_download = _mint_copy
            return DownloadedFileResponse(
                cache_file,
                headers={
                    "Content-Type": mime,
                    "Content-Disposition": f'attachment; filename="{safe_name}"',
                    "Access-Control-Allow-Origin": "*",
                },
                on_download=on_download,
            )

    # Slow path: uncached, or cached before sidecars existed — needs Telegram.
    cli = pick_cli_for(account, chat_id)
    if cli is None:
        return web.Response(status=503, text="new librarian account not configured yet")

    msg = await get_media_message(chat_id, message_id, cli)
    if msg is None and own_cli(account) is not cli:
        # The parachute client can't see this peer: private-chat message IDs
        # differ per account. Fetch with the token account and borrow its
        # document for the download client (GetFile works on the document
        # location); if that download fails we fall back to the token account.
        own = own_cli(account)
        if own is not None:
            msg = await get_media_message(chat_id, message_id, own)
    if msg is None:
        return web.Response(status=404, text="file not found (moved or deleted in Telegram)")

    name, size, mime = media_info(msg)
    safe_name = name.replace('"', "'")

    if not cache_file.is_file():
        # Run the download as its own task and shield it: when the reader's
        # browser gives up (timeout), the handler is cancelled but the download
        # keeps going, so the next click finds (or waits on) a real cache file.
        try:
            dl_task = asyncio.create_task(
                download_to_cache(chat_id, message_id, msg, cli, account)
            )
            try:
                await asyncio.shield(dl_task)
            except asyncio.CancelledError:
                raise  # request cancelled — download continues in background
        except asyncio.CancelledError:
            raise
        except Exception as e:
            own = own_cli(account)
            if own is None or own is cli:
                log.error(f"download failed for {chat_id}/{message_id}: {e}")
                return web.Response(status=502, text="download failed — please try again")
            log.warning(
                f"parachute download failed for {chat_id}/{message_id}: {e}; "
                "falling back to the token account"
            )
            own_msg = await get_media_message(chat_id, message_id, own)
            if own_msg is None:
                return web.Response(status=404, text="file not found (moved or deleted in Telegram)")
            name, size, mime = media_info(own_msg)
            safe_name = name.replace('"', "'")
            dl_task = asyncio.create_task(
                download_to_cache(chat_id, message_id, own_msg, own, account)
            )
            try:
                await asyncio.shield(dl_task)
            except asyncio.CancelledError:
                raise
            except Exception as e2:
                log.error(f"download failed for {chat_id}/{message_id}: {e2}")
                return web.Response(status=502, text="download failed — please try again")

    try:
        meta_file.write_text(json.dumps({"name": name, "mime": mime}))
    except Exception as e:
        log.warning(f"meta write failed for {cache_file.name}: {e}")

    # Public offload (same rule as the fast path above).
    on_download = None
    if ULTRA_ENABLED and chat_id not in NO_PRECACHE_CHATS:
        key = _buzz_key(chat_id, message_id, account)
        ent = _ultra_live(key)
        if ent:
            if request.method == "GET":
                _ultra_hit(key)
            return web.HTTPFound(ent["url"])
        if time.time() >= (_ultra_map.get(key) or {}).get("retry_after", 0):
            # Same rule as the fast path: only once the file is really taken.
            who = real_ip(request)

            def _mint_copy(key=key, who=who):
                log.info(f"download completed ({who}) — queuing public copy for {key}")
                return ultra_offload_task(
                    key, chat_id, message_id, account, cache_file, name, high=False
                )

            on_download = _mint_copy

    # Serve the complete cached file. FileResponse handles Range/Content-Length
    # correctly, so no live-stream corruption (the old "Bad Gateway" bug).
    return DownloadedFileResponse(
        cache_file,
        headers={
            "Content-Type": mime,
            "Content-Disposition": f'attachment; filename="{safe_name}"',
            "Access-Control-Allow-Origin": "*",
        },
        on_download=on_download,
    )


async def handle_download(request: web.Request):
    """Resolve a FileCodeBox share code and relay it as a plain full download.

    FileCodeBox stays bound to 127.0.0.1 — only this handler is publicly reachable,
    and it only ever proxies downloads (the upload endpoint is never exposed)."""
    if is_crawler(request):
        return web.Response(status=403, text="forbidden")
    code = request.match_info["code"]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{FILEBOX_URL}/share/select/",
                json={"code": code, "password": ""},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    return web.Response(status=404, text="link not found")
                data = await resp.json()
                detail = (data or {}).get("detail") or {}
                name = detail.get("name") or f"{code}.bin"
                dl = detail.get("download_url")
                if not dl:
                    return web.Response(status=404, text="link not found")

            safe_name = name.replace('"', "'")
            async with session.get(
                f"{FILEBOX_URL}{dl}",
                timeout=aiohttp.ClientTimeout(total=None),
            ) as r:
                if r.status != 200:
                    return web.Response(status=502, text="file unavailable")
                headers = {
                    "Content-Type": r.headers.get("Content-Type", "application/octet-stream"),
                    "Content-Disposition": f'attachment; filename="{safe_name}"',
                    "Access-Control-Allow-Origin": "*",
                }
                if r.headers.get("Content-Length"):
                    headers["Content-Length"] = r.headers["Content-Length"]
                out = web.StreamResponse(headers=headers)
                await out.prepare(request)
                async for chunk in r.content.iter_chunked(CHUNK):
                    await out.write(chunk)
                await out.write_eof()
                return out
    except Exception as e:
        log.error(f"download proxy failed for code {code}: {e}")
        return web.Response(status=502, text="download failed — please try again")


async def handle_health(request):
    me = (await client.get_me()).username
    me_new = (await new_client.get_me()).username if new_client else None
    return web.json_response({"ok": True, "me": me, "new": me_new})


async def handle_robots(request):
    return web.Response(text="User-agent: *\nDisallow: /\n", content_type="text/plain")


# ---------------------------------------------------------------------------
# /status — a glance at the public copies
#
# Deliberately shaped like the file list buzzheavier showed: name, size, how
# many times taken, when it was added, and the link. The point is that deciding
# what to rotate off the box becomes a look rather than a guess — and that
# "have the links landed" is answerable at a glance for a recent batch.
# ---------------------------------------------------------------------------
STATUS_USER = os.environ.get("BOOKLINK_STATUS_USER", "")
STATUS_PASS = os.environ.get("BOOKLINK_STATUS_PASS", "")
STATUS_KEY = os.environ.get("BOOKLINK_STATUS_KEY", "")
FILES_DB = os.environ.get("BOOKLINK_FILES_DB", "")
EDGE_PASS = os.environ.get("BOOKLINK_EDGE_PASS", "")
_disk_cache = (0.0, "")


def _catalogue_query(query: str, limit: int = 100) -> list:
    """Search the recorded link catalogue by filename.

    Runs in an executor: it is an unindexed scan over a few hundred thousand
    rows, which is fine as a one-off but not something to hold the event loop
    for.
    """
    like = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    con = sqlite3.connect(f"file:{FILES_DB}?mode=ro", uri=True, timeout=5)
    try:
        return con.execute(
            "SELECT filename, size, stored, created_at FROM files "
            "WHERE filename LIKE ? ESCAPE '\\' ORDER BY created_at DESC LIMIT ?",
            (like, limit),
        ).fetchall()
    except sqlite3.Error as e:
        log.warning(f"catalogue search failed: {e}")
        return []
    finally:
        con.close()


def key_to_token(key: str) -> str:
    """The /b/ token for an offload map key ("chat:msg:account")."""
    try:
        chat, msg, account = key.split(":")
        return encode_ref(int(chat), int(msg), int(account))
    except Exception:
        return ""


def link_for(key: str) -> str:
    """Rebuild the permanent /b/ link from an offload map key.

    The link is the thing that gets posted, so the dashboard shows it rather than
    only the file behind it — checking a link has landed means checking the link.
    """
    token = key_to_token(key)
    return f"{PUBLIC_BASE}/b/{token}" if token else ""


def _ago(ts: float) -> str:
    if not ts:
        return "—"
    d = max(0, time.time() - ts)
    if d < 3600:
        return f"{int(d // 60)}m ago"
    if d < 86400:
        return f"{int(d // 3600)}h ago"
    return f"{int(d // 86400)}d ago"


def _human(n: float) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024


def _to_gb(tok: str) -> float:
    """Normalise a `quota -s` figure ("55951M", "1863G", "1.8T") to GB."""
    mult = {"K": 1 / 1024 / 1024, "M": 1 / 1024, "G": 1, "T": 1024}
    try:
        return float(tok[:-1]) * mult.get(tok[-1].upper(), 1)
    except (ValueError, IndexError):
        return 0.0


async def _box_disk() -> str:
    """Used-vs-quota on the seeding box, cached 5 min so the page stays snappy.

    `df` on these hosts reports the whole shared volume (26 TB across every
    customer), which says nothing about how much room *she* has — `quota -s`
    is her own 2 TB allowance.
    """
    global _disk_cache
    ts, val = _disk_cache
    if time.time() - ts < 300:
        return val
    try:
        proc = await asyncio.create_subprocess_exec(
            "ssh", "-i", ULTRA_KEY,
            "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
            ULTRA_SSH, "quota -s 2>/dev/null | tail -1 | awk '{print $2\" \"$3}'",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=20)
        used, quota = out.decode().split()[:2]
        u, q = _to_gb(used), _to_gb(quota)
        val = f"{_human(u * 1024**3)} / {_human(q * 1024**3)} ({u / q * 100:.0f}% full)" if q else ""
    except Exception:
        val = ""
    _disk_cache = (time.time(), val)
    return val


async def handle_status(request: web.Request):
    # One shared login for the handful of people who watch the links land. It is
    # a full catalogue of everything on the box, so it is never served openly.
    # A `?k=` key is also accepted, for a bookmark or a quick curl.
    if STATUS_KEY and hmac.compare_digest(request.query.get("k", ""), STATUS_KEY):
        pass
    elif STATUS_USER and STATUS_PASS:
        auth = request.headers.get("Authorization", "")
        ok = False
        if auth.startswith("Basic "):
            try:
                user, _, pwd = base64.b64decode(auth[6:]).decode().partition(":")
                ok = (hmac.compare_digest(user, STATUS_USER)
                      and hmac.compare_digest(pwd, STATUS_PASS))
            except Exception:
                ok = False
        if not ok:
            return web.Response(
                status=401, text="login required",
                headers={"WWW-Authenticate": 'Basic realm="BookLink"'})
    else:
        return web.Response(status=404, text="not found")

    query = (request.query.get("q") or "").strip()
    matches = []
    if query and FILES_DB and Path(FILES_DB).exists():
        matches = await asyncio.get_running_loop().run_in_executor(
            None, _catalogue_query, query)

    ready, pending = [], []
    for key, ent in _ultra_map.items():
        (ready if ent.get("url") else pending).append((key, ent))
    ready.sort(key=lambda kv: kv[1].get("added", 0), reverse=True)

    total = sum(e.get("size", 0) for _, e in ready)
    hits = sum(e.get("hits", 0) for _, e in ready)
    never = sum(1 for _, e in ready if not e.get("hits"))

    def row(key, ent):
        name = html.escape(ent.get("name") or key)
        if not ent.get("url"):
            return f'<tr class=p data-added="0"><td class=n>{name}</td><td colspan=5 class=dim>waiting — retrying</td></tr>'
        url = html.escape(ent["url"])
        b_link = html.escape(link_for(key))
        added = ent.get("added") or 0
        last = ent.get("last_hit") or 0
        hit = ent.get("hits", 0)
        return (
            f'<tr data-name="{html.escape((ent.get("name") or "").lower())}" '
            f'data-size="{ent.get("size", 0)}" data-hits="{hit}" '
            f'data-added="{added}" data-last="{last}">'
            f'<td class=n>{name}</td>'
            f'<td class=num>{_human(ent["size"]) if ent.get("size") else "—"}</td>'
            f'<td class="num{"" if hit else " z"}">{hit}</td>'
            f'<td class=dim>{_ago(added)}</td>'
            f'<td class=dim>{_ago(last) if last else "never"}</td>'
            f'<td class=nowrap><a href="{b_link}" target=_blank rel=noopener class=lk'
            f' title="{b_link}">/b/{html.escape(key_to_token(key))}</a>'
            f' <a href="#" class=cp data-u="{b_link}">copy</a>'
            f' <a href="{url}" target=_blank rel=noopener class=dim title="the copy on the box">file</a></td>'
            "</tr>"
        )

    # Which copies are on the box, to tell a catalogue hit from a live one. Keyed
    # on the FILENAME, not the link: the same book forwarded to a second chat
    # mints a second, equally valid token, so a token-keyed lookup reports
    # perfectly good links as "not copied yet". Tokens are still checked, as a
    # second chance for a copy whose name was changed on the way in.
    def _norm(s: str) -> str:
        return " ".join((s or "").lower().split())

    token_index = {key_to_token(k): (k, e) for k, e in _ultra_map.items() if e.get("url")}
    name_index = {_norm(e.get("name")): (k, e)
                  for k, e in _ultra_map.items() if e.get("url") and e.get("name")}

    def match_row(filename, size, stored, created_at):
        token = (stored or "").rsplit("/", 1)[-1]
        have = name_index.get(_norm(filename)) or token_index.get(token)
        link = html.escape(stored or "")
        name = html.escape(filename or "")
        if have:
            _, ent = have
            state = f'<span class=ok>on the box</span> &middot; {ent.get("hits", 0)} takes'
        else:
            state = '<span class=dim>not copied yet</span>'
        return (
            f'<tr data-name="{html.escape((filename or "").lower())}" data-size="{size or 0}" '
            f'data-hits="{have[1].get("hits", 0) if have else -1}" data-added="{created_at or 0}" '
            f'data-last="{have[1].get("last_hit", 0) if have else 0}">'
            f'<td class=n>{name}</td><td class=num>{_human(size)}</td>'
            f'<td class=dim>{state}</td>'
            f'<td class=dim>{_ago(created_at)}</td>'
            f'<td class=nowrap><a href="{link}" target=_blank rel=noopener class=lk title="{link}">'
            f'/b/{html.escape(token)}</a> <a href="#" class=cp data-u="{link}">copy</a></td>'
            "</tr>"
        )

    disk = await _box_disk()
    body = f"""<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>BookLink &middot; public copies</title>
<style>
 :root{{color-scheme:dark}}
 body{{background:#0f1115;color:#dfe3ea;font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;margin:0;padding:28px}}
 h1{{font-size:19px;margin:0 0 4px}}
 h2{{font-size:15px;margin:26px 0 4px;font-weight:600}} h2.mt{{margin-top:30px}}
 .sub{{color:#8b93a1;font-size:13px;margin-bottom:18px}}
 .cards{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px}}
 .card{{background:#171a21;border:1px solid #232833;border-radius:9px;padding:11px 15px;min-width:110px}}
 .card b{{display:block;font-size:20px;font-weight:650}}
 .card span{{color:#8b93a1;font-size:11.5px;text-transform:uppercase;letter-spacing:.5px}}
 table{{width:100%;border-collapse:collapse}}
 th{{text-align:left;color:#8b93a1;font-weight:500;font-size:11.5px;text-transform:uppercase;letter-spacing:.5px;padding:8px 10px;border-bottom:1px solid #232833;cursor:pointer;user-select:none;white-space:nowrap}}
 th:hover{{color:#dfe3ea}} th.s::after{{content:" ↓"}} th.s.a::after{{content:" ↑"}}
 td{{padding:8px 10px;border-bottom:1px solid #1b1f28}}
 td.n{{max-width:520px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
 td.num{{text-align:right;font-variant-numeric:tabular-nums;color:#a8b0be}}
 td.z{{color:#6f7787}}
 td.dim{{color:#6f7787}}
 tr.p td{{color:#6f7787}}
 a{{color:#7cc4ff;text-decoration:none;cursor:pointer}} a:hover{{text-decoration:underline}}
 a.dim{{color:#6f7787}} td.nowrap{{white-space:nowrap}}
 a.lk{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}}
 .ok{{color:#7ee0a4}}
 form{{display:flex;gap:8px;margin:0 0 20px}}
 input[type=search]{{flex:1;max-width:520px;background:#171a21;border:1px solid #232833;color:#dfe3ea;
   border-radius:9px;padding:10px 13px;font:inherit}}
 input[type=search]:focus{{outline:none;border-color:#3a76aa}}
 button{{background:#1d2a38;border:1px solid #2c4050;color:#cfe1f3;border-radius:9px;
   padding:10px 18px;font:inherit;cursor:pointer}}
 button:hover{{background:#243444}}
</style></head><body>
<h1>Public copies</h1>
<div class=sub>Everything offloaded to the seeding box &mdash; readers are served from here. Click a column to sort: <b>last taken</b> with <b>takes</b> is the pruning view.</div>
<form method=get action=/status>
 <input type=search name=q value="{html.escape(query)}" placeholder="Find a book by name &mdash; gives you its link whether it is on the box or not" autocomplete=off>
 <button type=submit>Find</button>
</form>
<div class=cards>
 <div class=card><b>{len(ready)}</b><span>books</span></div>
 <div class=card><b>{_human(total)}</b><span>on the box</span></div>
 <div class=card><b>{hits:,}</b><span>reader clicks</span></div>
 <div class=card><b>{never:,}</b><span>never taken</span></div>
 <div class=card><b>{html.escape(disk) if disk else '&mdash;'}</b><span>box quota used</span></div>
</div>
{"".join(f'''
<h2>Matches for &ldquo;{html.escape(query)}&rdquo;</h2>
<div class=sub>{len(matches)} matched, newest first. "not copied yet" still works &mdash;
it just has no copy on the box, so a reader waits for it to come off Telegram.</div>
<table id=matches><thead><tr>
 <th data-k=name>Name</th>
 <th data-k=size style="text-align:right">Size</th>
 <th>Copy</th>
 <th data-k=added>Linked</th>
 <th>Link</th>
</tr></thead>
<tbody>{''.join(match_row(*m) for m in matches) or '<tr><td colspan=5 class=dim>nothing matched</td></tr>'}</tbody></table>
''') if query else ''}
<h2 class=mt>Copies on the box</h2>
<table id=copies><thead><tr>
 <th data-k=name>Name</th>
 <th data-k=size style="text-align:right">Size</th>
 <th data-k=hits style="text-align:right">Takes</th>
 <th data-k=added>Added</th>
 <th data-k=last>Last taken</th>
 <th>Link</th>
</tr></thead>
<tbody>{''.join(row(k, e) for k, e in ready)}{''.join(row(k, e) for k, e in pending)}</tbody></table>
<p class=sub style="margin-top:18px">Books with no row have not been downloaded since the move &mdash; they still work, served from the local cache or Telegram, and get a copy here the next time somebody takes one.</p>
<script>
for (const tb of document.querySelectorAll("table[id] tbody")) {{
  const ths=[...tb.closest("table").querySelectorAll("th[data-k]")];
  let sk=null, sa=false;
  const cmp=(k,a,b)=>{{
    const x=a.dataset[k], y=b.dataset[k];
    const n=k==="name"?x.localeCompare(y):(parseFloat(x)-parseFloat(y));
    return sa?n:-n;
  }};
  const sort=(k)=>{{
    if(k===sk) sa=!sa; else {{ sk=k; sa=(k!=="name"); }}
    [...tb.rows].sort((a,b)=>cmp(sk,a,b)).forEach(r=>tb.appendChild(r));
    ths.forEach(t=>{{t.classList.toggle("s",t.dataset.k===sk);t.classList.toggle("a",t.dataset.k===sk&&sa);}});
  }};
  ths.forEach(t=>t.onclick=()=>sort(t.dataset.k));
  tb.addEventListener("click",e=>{{
    const a=e.target.closest("a.cp"); if(!a) return;
    e.preventDefault();
    navigator.clipboard.writeText(a.dataset.u);
    const was=a.textContent; a.textContent="copied";
    setTimeout(()=>{{a.textContent=was;}},1200);
  }});
  sort("added");
}}
</script>
</body></html>"""
    resp = web.Response(text=body, content_type="text/html")
    if EDGE_PASS:
        # Tells the edge guard this browser is one of ours. Checking a batch of
        # links before posting them is indistinguishable from a crawler from the
        # outside, so people who can log in here are exempt by that fact alone.
        resp.set_cookie("bl_pass", EDGE_PASS, secure=True, httponly=True,
                        samesite="Lax", max_age=90 * 86400)
    return resp


async def on_forwarded_book(event):
    """Forward any book to a librarian account → get its permanent link back.

    Registered on BOTH clients, but with different filters: the fresh account also
    accepts the shared group, the legacy one only private chats. That matters —
    both accounts are members of the group, so if both handled group posts every
    book would come back with two identical links.
    """
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
        await event.reply(
            f"📚 {name or 'file'}\n{size_mb:.1f} MB\n\n"
            f"🔗 Permanent link:\n{link}\n\n"
            f"📋 Easy copy (tap to copy):\n`{link}`",
            link_preview=False,
        )
        # Group books (notifier forwards from the watched group) get a link only — no
        # precache. They're a list/database for the ABB, not hot public links, so
        # pre-downloading them just burns Telegram bandwidth and evicts real singles.
        # Personal singles (Cerina's own forwards) keep the immediate cache.
        is_group_forward = event.chat_id in NO_PRECACHE_CHATS
        if not is_group_forward:
            asyncio.create_task(precache_in_background(event.chat_id, event.message.id, event.message, cli, account))
        log.info(f"link issued for {name} ({event.chat_id}/{event.message.id}, account {account}, group={is_group_forward})")
    except Exception as e:
        log.error(f"link handler error: {e}")
        await event.reply(f"couldn't make a link: {e}")


_seen_unlisted_groups = set()


async def on_unlisted_group_media(event):
    """Say so, once, when media turns up in a group we aren't wired to.

    Otherwise a new group just looks broken — posts land, nothing happens, and
    there's no trace of why.
    """
    if event.chat_id in _seen_unlisted_groups:
        return
    _seen_unlisted_groups.add(event.chat_id)
    title = getattr(event.chat, "title", "") if event.chat else ""
    log.info(
        f"media posted in group {event.chat_id} ({title!r}) is NOT handled — "
        "add it to BOOKLINK_GROUP_CHATS if it should mint links"
    )


async def on_startup(app):
    _buzz_load()
    _ultra_load()
    await client.start()
    client.add_event_handler(
        on_forwarded_book,
        events.NewMessage(incoming=True, func=lambda e: e.is_private and e.media),
    )
    if new_client is not None:
        await new_client.start()
        new_client.add_event_handler(
            on_forwarded_book,
            events.NewMessage(
                incoming=True,
                func=lambda e: e.media and (e.is_private or e.chat_id in GROUP_CHATS),
            ),
        )
        new_client.add_event_handler(
            on_unlisted_group_media,
            events.NewMessage(
                incoming=True,
                func=lambda e: e.media and not e.is_private and e.chat_id not in GROUP_CHATS,
            ),
        )
    asyncio.create_task(ultra_save_loop())
    asyncio.create_task(cache_cleanup_loop())
    asyncio.create_task(disk_alert_loop())
    asyncio.create_task(buzz_reconcile_loop())
    asyncio.create_task(premium_watch_loop())
    await resolve_download_account(initial=True)
    flags = await _premium_flags()
    log.info(
        f"download account: {_download_via or 'per-book default'} "
        f"(premium: {', '.join(f'{k}={v}' for k, v in flags.items()) or 'unknown'})"
    )
    me = await client.get_me()
    log.info(f"BookLink librarian logged in as @{me.username} (id {me.id}) — forward it a book to get a link")
    if new_client is not None:
        me_new = await new_client.get_me()
        log.info(f"BookLink new librarian logged in as @{me_new.username} (id {me_new.id})")
    await _warm_entities(client)
    if new_client is not None:
        await _warm_entities(new_client)


def build_app():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_get("/robots.txt", handle_robots)
    app.router.add_get("/b/{token}", handle_book)
    # Pretty form: /b/<key>/Any Book Name.m4b  — the name is cosmetic, the key does the work
    app.router.add_get("/b/{token}/{name:.*}", handle_book)
    app.router.add_get("/d/{code}", handle_download)
    app.router.add_get("/status", handle_status)
    app.on_startup.append(on_startup)
    return app


if __name__ == "__main__":
    # %a is always the local proxy (Cloudflare tunnel), so the real visitor is
    # only visible via CF-Connecting-IP. Without this the log cannot attribute a
    # click flood to anybody.
    web.run_app(
        build_app(), host=HOST, port=PORT,
        access_log_class=AccessLogger,
        access_log_format='%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" cf=%{CF-Connecting-IP}i',
    )
