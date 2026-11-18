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
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, unquote
import aiohttp
from aiohttp import web
from aiohttp.web_log import AccessLogger
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import (
    FileReferenceExpiredError, PeerIdInvalidError, FloodWaitError,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("booklink")

API_ID = int(os.environ["BOOKLINK_API_ID"])
API_HASH = os.environ["BOOKLINK_API_HASH"]
SESSION = os.environ["BOOKLINK_SESSION"]           # StringSession for Cerina2 (librarian)
SESSION_NEW = os.environ.get("BOOKLINK_SESSION_NEW", "")  # optional: fresh librarian account for new links
SESSION_THIRD = os.environ.get("BOOKLINK_SESSION_THIRD", "")  # optional: spare account for group intake
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
BOX_ALERT_GB = int(os.environ.get("BOOKLINK_BOX_ALERT_GB", "20"))       # free space on the cache box
BOX_QUOTA_ALERT_PCT = float(os.environ.get("BOOKLINK_BOX_QUOTA_ALERT_PCT", "85"))  # public copies
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
# How many books each account may fetch at once. The legacy account was run
# strictly serial for a while because parallel downloads on it stacked flood-waits
# and kept it pegged — but that was a month when its Premium had lapsed, and the
# waits stop being a symptom once Premium is back. Both accounts are now on paid
# Premium, so the two lanes are symmetric by default; drop OLD back to 1 if
# flood-waits reappear, which is the signal that the lane is the problem.
DOWNLOAD_SEM_OLD = asyncio.Semaphore(int(os.environ.get("BOOKLINK_MAX_DOWNLOADS_OLD",
                                                        str(MAX_CONCURRENT_DOWNLOADS))))
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
# precache + offload); they are NOT on the NO_PRECACHE list.
GROUP_CHATS = {
    int(x) for x in os.environ.get("BOOKLINK_GROUP_CHATS", "").replace(" ", "").split(",")
    if x.lstrip("-").isdigit()
}

# Chats that get a link recorded but never a reply. The Backroom has always had a
# link for every book in the catalogue — that is its whole purpose, one link to put
# in each book's tags — but the replies were never wanted there; they are noise in
# a room people scroll. Silently recorded is the shape, not a separate lane: the
# link is still minted and filed, only the message back into the room is skipped.
SILENT_CHATS = {
    int(x) for x in os.environ.get("BOOKLINK_SILENT_CHATS", "").replace(" ", "").split(",")
    if x.lstrip("-").isdigit()
}

# Every chat whose media should mint a link: private chats, plus any group listed
# in either lane. The two lanes mean different things for copies, not for links —
# a NO_PRECACHE group is a catalogue, and a catalogue still needs its links or
# there is nothing to put in the tags. Without this, a group added to the
# link-only lane would mint nothing at all and only ever say "not handled".
INTAKE_CHATS = GROUP_CHATS | NO_PRECACHE_CHATS

FILEBOX_URL = os.environ.get("BOOKLINK_FILEBOX_URL", "http://127.0.0.1:12345")

# The link catalogue. Every link this service mints is reported here so the
# catalogue fills as books are posted. It used to be filled only by the watcher
# bridge, which watches a single group, records what it forwards keyed to a DM
# rather than to the group, and left everything else to quarterly bulk imports —
# which is why the catalogue drifted and why its live rows were the fragile kind.
NOTIFY_URL = os.environ.get("BOOKLINK_NOTIFY_URL", "")
NOTIFY_SECRET = os.environ.get("BOOKLINK_NOTIFY_SECRET", "")


class OffloadGate:
    """One upload at a time, with a fast lane for Cerina's own sends.

    Uploads stay serial because they share the same pipe as the downloads. But
    the two kinds of upload are not equally urgent: a send-time upload is a book
    she just posted and is waiting to see, while a click-triggered upload is a
    reader who clicked an older link whose copy had lapsed. On 2026-10-07 a batch
    of her sends sat behind ~105 reader-triggered uploads and took over an hour to
    appear — and readers who hit a slow link often don't come back. So sends go
    first, ahead of every queued reader copy, though not ahead of one already
    running: it is still one upload at a time.
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


def _offload_key(chat_id: int, message_id: int, account: int) -> str:
    return f"{chat_id}:{message_id}:{account}"


# ---------------------------------------------------------------------------
# Public offload tier — the seeding box
#
# Replaces buzzheavier, disabled 2026-10-10 after its ad-funded landing page
# served a reader a macOS infostealer lure. The public copy now goes to Cerina's
# own seeding box, over SSH with a dedicated key, so the reader's path contains
# no third party and no ads at all. Two settings there are load-bearing: nginx
# must force Content-Disposition (the browser acts on the redirect target, not on
# the /b/ response) and must not list the directory.
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

# The public box may sit behind its provider's own HTTP auth, which cannot be
# changed from the account. Rather than hand readers a password, serve the copies
# through here: this service holds the credentials and streams the file, so the
# URL a reader sees is ours and the password never appears in a link.
BOX_PROXY_BASE = os.environ.get("BOOKLINK_BOX_PROXY_BASE", "").rstrip("/")
BOX_PROXY_USER = os.environ.get("BOOKLINK_BOX_PROXY_USER", "")
BOX_PROXY_PASS = os.environ.get("BOOKLINK_BOX_PROXY_PASS", "")
_box_session = None

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
            size = (await _aio(Path(cache_file).stat)).st_size
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
        # The copy on the box is what readers are redirected to, so the local
        # file has done its job and the disk is small. Drop it — but only after
        # a copy exists, never before.
        try:
            await _aio(Path(cache_file).unlink)
        except OSError as e:
            log.warning(f"could not drop local cache for {key}: {e}")
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
# An optional third librarian. It exists so group intake can move to a healthy
# account without disturbing the first two: a link minted by an account can only
# be fetched by that account when it points at a private chat, so the sessions
# that own old DM links must stay loaded even if those accounts are restricted.
third_client = None
if SESSION_THIRD:
    third_client = TelegramClient(StringSession(SESSION_THIRD), API_ID, API_HASH)

# Group downloads rotate across the librarians that can still work. When a spare
# account is present it replaces the new one in the rotation: a frozen account
# can read — which is what keeps the links it minted serving — but it should not
# be a workhorse while Telegram is restricting it.
_group_rr = itertools.cycle(("third", "old") if third_client is not None else ("new", "old"))

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


def catalogue_link(name: str, size: int) -> str | None:
    """The link already recorded for this exact file, if we could still serve it.

    The bridge records one link per book, keyed on filename+size, and refuses to
    record a second. But it can only drop the duplicate *after* the fact — by then
    the bot has already replied with a freshly minted link, so a re-forwarded book
    ends up with a canonical link in the catalogue and a stray one in the chat.
    Asking the catalogue first means the second forward just gets the first link
    back, which is what everyone already assumes is happening.

    Indexed on (filename, size), so this is a single lookup rather than a scan.
    """
    if not (FILES_DB and Path(FILES_DB).is_file()):
        return None
    try:
        con = sqlite3.connect(f"file:{FILES_DB}?mode=ro", uri=True, timeout=5)
        try:
            row = con.execute(
                "SELECT stored FROM files WHERE filename = ? AND size = ? LIMIT 1",
                (name, size),
            ).fetchone()
        finally:
            con.close()
    except sqlite3.Error as e:
        log.warning(f"catalogue lookup failed: {e}")
        return None
    if not row or not row[0]:
        return None
    stored = row[0]
    try:
        chat_id, _, account = decode_ref(stored.rsplit("/", 1)[-1].split("?")[0])
    except Exception:
        return None
    # A link we could no longer fetch would be worse than a new one: the whole
    # point is that the reply works. Private chats are bound to the account that
    # received them, so hand back that link only while that account is still here.
    if chat_id > 0 and own_cli(account) is None:
        return None
    return stored


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
    if chat_id > 0:
        # A private chat belongs to exactly one account — its message ids are
        # not shared with anybody — so the download parachute must not redirect
        # it. Asking the other account for a peer it has never seen raises
        # "could not find the input entity" and used to surface as a 500.
        return own_cli(account) or client or new_client
    if chat_id in GROUP_CHATS and DOWNLOAD_VIA not in ("old", "new"):  # honour a pin
        choice = next(_group_rr)
        chosen = {"third": third_client, "new": new_client, "old": client}.get(choice)
        if chosen is not None:
            log.info(f"group book {chat_id}: fetching via the {choice} account")
            return chosen
    cli = pick_cli(account)
    if cli is None and chat_id < 0:
        # The token names an account that no longer exists. In a group any
        # librarian can read the message, so serve it from one that still can
        # rather than refusing a link that is still perfectly good.
        for candidate in (third_client, client, new_client):
            if candidate is not None:
                log.info(f"group book {chat_id}: token account is gone, using another librarian")
                return candidate
    return cli


async def _alert(text):
    """Message Cerina directly. Used for the things that break quietly."""
    sender = new_client or client
    try:
        await sender.send_message(DISK_ALERT_CHAT, text)
    except Exception as e:
        log.warning(f"alert send failed: {e}")


async def _premium_flags():
    """Telegram Premium on each librarian account, by label."""
    flags = {}
    for label, cli in (("old", client), ("new", new_client), ("third", third_client)):
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
    a 300 MB book takes ~20 minutes, and the whole precache -> offload pipeline backs
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


def other_cli(cli):
    """The other librarian client, when there are two and they differ."""
    if client is None or new_client is None or cli is None:
        return None
    if cli is client:
        return new_client
    if cli is new_client:
        return client
    return None


async def _fetch_once(chat_id, message_id, cli):
    try:
        return await cli.get_messages(chat_id, ids=message_id)
    except ValueError as e:
        # Cold entity cache (e.g. just restarted) — warm once and retry.
        log.warning("input entity miss for %s; warming cache and retrying: %s", chat_id, e)
        await _warm_entities(cli)
        return await cli.get_messages(chat_id, ids=message_id)


async def get_media_message(chat_id, message_id, cli):
    """Fetch a book's message, falling back to the other account for a group.

    Returns (message, client_used) — the caller must fetch the *bytes* with the
    same client, because the media reference belongs to the session that read it.

    The fallback exists because of how Telegram scopes messages. A private chat's
    message ids differ per account, so a DM link can only ever be fetched by the
    account that received it and no second account can help. A group shares
    message ids, so either librarian can fetch it — which means a group link can
    survive losing an account. Until now it only survived if somebody noticed and
    flipped BOOKLINK_DOWNLOAD_VIA by hand, which is a bad thing to have to
    remember at the worst possible moment.

    A private chat is left alone on purpose: the retry would be rejected, and
    failing fast says more than a second identical failure.
    """
    msg, err = None, None
    try:
        msg = await _fetch_once(chat_id, message_id, cli)
    except Exception as e:
        err = e
        log.warning("fetch %s/%s failed on this account: %s", chat_id, message_id, e)

    if not msg or not msg.media:
        alt_cli = other_cli(cli) if chat_id < 0 else None
        if alt_cli is not None:
            try:
                alt = await _fetch_once(chat_id, message_id, alt_cli)
            except Exception as e:
                err = err or e
                alt = None
            if alt is not None and alt.media:
                log.info("fetch %s/%s recovered via the other account", chat_id, message_id)
                return alt, alt_cli
        if err is not None:
            raise err

    return msg, cli


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


def _cache_cleanup_once() -> list:
    """One synchronous sweep of CACHE_DIR, for `cache_cleanup_loop` to run in a thread.

    Touches each directory entry once. The old version walked the directory
    twice with an `is_file()` and one or two `stat()` calls per file — roughly
    five round trips each, several hundred entries — on the event loop. Against
    the sshfs mount that froze the entire service for minutes, which is what
    took the dashboard down.
    """
    now = time.time()
    removed, kept = [], []
    with os.scandir(CACHE_DIR) as entries:
        for entry in entries:
            try:
                if not entry.is_file():
                    continue
                st = entry.stat()
            except OSError:
                continue
            name = entry.name
            if ".tmp." in name:
                # In-flight downloads keep their writer's pid. A temp file whose
                # process is gone is a leftover from a restart, and it is not
                # small: 16 of them were holding 4.9 GB.
                try:
                    pid = int(name.split(".tmp.", 1)[1].split(".")[0])
                except (ValueError, IndexError):
                    pid = None
                if pid is not None and not _pid_alive(pid):
                    removed.append(name)
                continue
            # A file written minutes ago may still be being copied to the public
            # box, and evicting it mid-rsync is how a copy fails with "file has
            # vanished" and lands in the retry queue. Give fresh files room.
            if now - st.st_mtime < 1800:
                kept.append((st.st_mtime, st.st_size, name))
                continue
            if now - st.st_mtime > CACHE_MAX_AGE:
                removed.append(name)
                continue
            kept.append((st.st_mtime, st.st_size, name))
    kept.sort()
    total = sum(size for _, size, _ in kept)
    while total > CACHE_MAX_SIZE and kept:
        _, size, name = kept.pop(0)
        total -= size
        removed.append(name)
    gone = []
    for name in removed:
        try:
            os.unlink(os.path.join(CACHE_DIR, name))
            gone.append(name)
        except OSError as e:
            log.warning(f"cache cleanup: could not remove {name}: {e}")
    return gone


async def cache_cleanup_loop():
    while True:
        try:
            removed = await _aio(_cache_cleanup_once)
            if removed:
                log.info(f"cache cleanup: removed {len(removed)} files")
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
            now = time.time()
            if now - _last_disk_alert > DISK_ALERT_COOLDOWN:
                warnings = []
                free_mb = shutil.disk_usage("/").free // (1024 * 1024)
                if free_mb < DISK_ALERT_MB:
                    warnings.append(f"{free_mb} MB free on /")
                # The cache box is a different machine, so its disk filling up
                # never showed on the local check — which is exactly the one
                # nobody looks at until downloads start failing.
                box_free = await _aio(lambda: shutil.disk_usage(CACHE_DIR).free // (1024 ** 3))
                if box_free < BOX_ALERT_GB:
                    warnings.append(f"{box_free} GB free on the cache box")
                # Public copies have no size cap, so this is the one that can
                # genuinely run out; the quota readout is already cached.
                pct = _box_disk_pct()
                if pct >= BOX_QUOTA_ALERT_PCT:
                    warnings.append(f"public copies at {pct:.0f}% of quota")
                if warnings:
                    await sender.send_message(
                        DISK_ALERT_CHAT, "⚠️ BookLink: " + "; ".join(warnings)
                    )
                    _last_disk_alert = now
                    log.info(f"disk alert sent to {DISK_ALERT_CHAT}: {'; '.join(warnings)}")
        except Exception as e:
            log.error(f"disk alert error: {e}")
        await asyncio.sleep(600)


async def _aio(fn, *args):
    """Run one blocking filesystem call off the event loop.

    Everything under CACHE_DIR lives on the sshfs mount to ultra, so a plain
    stat or unlink is a network round trip — measured at ~1.9s when the box is
    busy. Blocking on one does not slow a single request, it stops the loop that
    serves every /b/ click and the dashboard.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fn(*args))


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


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
    if await _aio(cache_file.is_file):
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


async def _download_media_to(cli, media, path):
    """Stream a Telegram file into `path` without blocking the event loop.

    The cache is the sshfs mount to ultra, so every write is a network round
    trip. Writing them inline stalled the loop for seconds at a time and took
    the dashboard down with it. The buffer is the chunk size, so one chunk is
    one round trip rather than the sixty-four an 8 KB default buffer would make.
    """
    loop = asyncio.get_running_loop()
    fh = await loop.run_in_executor(None, lambda: open(path, "wb", buffering=CHUNK))
    try:
        async for chunk in cli.iter_download(media, chunk_size=CHUNK):
            await loop.run_in_executor(None, fh.write, chunk)
    finally:
        await loop.run_in_executor(None, fh.close)


async def _download_once(cache_file, chat_id, message_id, msg, cli, account):
    tmp = cache_file.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
    sem = DOWNLOAD_SEM_NEW if cli is new_client else DOWNLOAD_SEM_OLD
    async with sem:
        media = msg.media
        try:
            await _download_media_to(cli, media, tmp)
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
                    await _download_media_to(cli, media, tmp)
                except Exception:
                    _rm(tmp)
                    raise
            else:
                _rm(tmp)
                raise
        except Exception:
            _rm(tmp)
            raise
        await _aio(os.replace, tmp, cache_file)
        try:
            size = (await _aio(cache_file.stat)).st_size
        except OSError:
            size = 0
        log.info(f"cached {cache_file.name} ({size} bytes)")
    return cache_file


async def precache_in_background(chat_id, message_id, msg, cli, account=ACCOUNT_OLD,
                                 force_copy=False):
    try:
        dcli = pick_cli_for(account, chat_id)
        if dcli is None:
            return
        if dcli is not cli:
            # Throttle parachute active: refetch the message with the download
            # account so the media access hash matches the client that reads it.
            fresh, fcli = await get_media_message(chat_id, message_id, dcli)
            if fresh is not None:
                msg, cli = fresh, fcli
            else:
                # The parachute client can't see this peer (private-chat IDs
                # differ per account) — download via the token account instead.
                own = own_cli(account)
                if own is None:
                    return
                msg, cli = await get_media_message(chat_id, message_id, own)
                if msg is None:
                    return
        await download_to_cache(chat_id, message_id, msg, cli, account)
        # The public copy belongs here too: the point of precaching personal sends
        # is to have copies ready BEFORE anyone clicks, so copy right away.
        #
        # Link-only chats are the exception — that lane exists so a catalogue feed
        # doesn't drag thousands of books through the cache and the box. `force_copy`
        # is for the one case where that reasoning doesn't hold: a book from a
        # link-only lane that somebody deliberately pulled out and re-sent, which is
        # a person asking for a copy rather than a feed producing one.
        if ULTRA_ENABLED and (force_copy or chat_id not in NO_PRECACHE_CHATS):
            key = _offload_key(chat_id, message_id, account)
            if not _ultra_live(key) and time.time() >= (_ultra_map.get(key) or {}).get("retry_after", 0):
                name, _, _ = media_info(msg)
                cache_file = cache_path(chat_id, message_id, account)
                asyncio.create_task(ultra_offload_task(key, chat_id, message_id, account, cache_file, name))
    except Exception as e:
        log.warning(f"background precache failed for {chat_id}/{message_id}: {e}")


async def record_in_catalogue(name: str, size: int, link: str) -> None:
    """Report a link we just handed out, so the catalogue knows about it.

    The catalogue is what the dashboard searches, what `catalogue_link` dedupes
    against, and what Cerina's library app reads. Recording from here means it
    fills as books are posted rather than in bulk jobs, and that every row is a
    link this service actually minted — so a book re-sent later finds its own
    link again instead of a stranger's.

    Best-effort on purpose: a catalogue that is a bit behind is an annoyance, but
    a link that fails because the catalogue was unreachable is a breakage.
    """
    if not (NOTIFY_URL and NOTIFY_SECRET) or not name or not size:
        return
    body = json.dumps({"filename": name, "size": size, "link": link}).encode()
    sig = hmac.new(NOTIFY_SECRET.encode(), body, hashlib.sha256).hexdigest()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                NOTIFY_URL, data=body, timeout=aiohttp.ClientTimeout(total=15),
                headers={"X-Booklink-Signature": sig, "Content-Type": "application/json"},
            ) as r:
                if r.status != 200:
                    log.warning(f"catalogue refused {name!r}: HTTP {r.status}")
    except Exception as e:
        log.warning(f"catalogue unreachable, {name!r} not recorded: {e}")


async def warm_reused_link(link: str) -> None:
    """Give a reused link the send-time treatment the new message would have had.

    Reuse means "this book is already in the catalogue", which is not the same as
    "this book is already handled". The catalogue's copy of a book often points
    into a link-only lane — the forwarder DM, usually, because the forwarder saw
    the book somewhere else first — and those lanes are never pre-cached or
    offloaded. So handing that link to somebody posting into a lane that keeps
    copies would mean the book is slow the first time *everyone* opens it, for as
    long as it exists, which is the opposite of what the fast lane is for.

    Re-sending a book is a person saying it matters, so that overrides where it
    happens to have been filed. Warm the canonical link itself, so there is still
    exactly one copy on the box and the link everyone gets is the quick one.
    """
    try:
        token = link.rstrip("/").rsplit("/", 1)[-1].split("?")[0]
        chat_id, message_id, account = decode_ref(token)
    except Exception as e:
        log.warning(f"reused link {link!r} is not a token I can read: {e}")
        return
    cli = own_cli(account) or pick_cli_for(account, chat_id)
    if cli is None:
        return
    msg, cli = await get_media_message(chat_id, message_id, cli)
    if msg is None:
        log.warning(f"reused link points at {chat_id}/{message_id} but the file is not reachable")
        return
    log.info(f"warming the reused link {chat_id}/{message_id} (account {account})")
    await precache_in_background(chat_id, message_id, msg, cli, account, force_copy=True)


def trusted_request(request: web.Request) -> bool:
    """True when the request carries the dashboard's cookie.

    That cookie is only ever issued by the origin behind the dashboard login, so
    its presence means a person we know opened the link — which is the same thing
    a completed transfer was standing in for, without making them wait for the
    whole book to arrive first.
    """
    if not EDGE_PASS:
        return False
    return f"bl_pass={EDGE_PASS}" in (request.headers.get("Cookie") or "")


async def copy_when_downloaded(dl_task, key, chat_id, message_id, account, name):
    """Copy a book to the box when it lands on disk, for a reader we know.

    The completed-transfer rule exists so a crawler poking links cannot make us do
    work. But it also means a big book never copies at all: the server has to pull
    the whole thing from Telegram before it can send a single byte, and the
    reader's browser gives up long before that, so the transfer never finishes and
    the copy never fires. Cancelling the spinning tab was doing exactly this —
    cached, never copied, every time.

    So for a request carrying the dashboard cookie the signal is the download
    landing instead. A crawler never has that cookie, so the protection is intact.
    """
    if not ULTRA_ENABLED or key in _ultra_inflight or _ultra_live(key):
        return
    try:
        await asyncio.shield(dl_task)
    except Exception:
        return
    path = cache_path(chat_id, message_id, account)
    if not await _aio(path.is_file):
        return
    log.info(f"downloaded for a known reader — queuing public copy for {key}")
    await ultra_offload_task(key, chat_id, message_id, account, path, name)


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

    the offload used to fire on the click alone, so a crawler that merely
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

    # A repointed link keeps its published URL and serves from a copy that is
    # still there. Done before the cache lookup so the file is cached and
    # offloaded under the location it actually came from.
    target = _repoints.get(token)
    if target:
        try:
            chat_id, message_id, account = decode_ref(target)
        except Exception:
            log.warning(f"repoint for {token} is malformed, ignoring it")

    cache_file = cache_path(chat_id, message_id, account)
    meta_file = Path(str(cache_file) + ".meta.json")

    # The public copy is checked BEFORE the local-cache check. The cache is
    # evicted after 24h but the copy persists, so requiring a cached file to
    # reach the redirect would re-download every book older than a day from
    # Telegram on every single click — and readers hammering one popular link
    # then keep the download semaphore full. A copy on the box is a complete
    # answer on its own; if it's absent we fall through to cache/Telegram.
    if ULTRA_ENABLED:
        key = _offload_key(chat_id, message_id, account)
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
    if await _aio(lambda: cache_file.is_file() and meta_file.is_file()):
        try:
            meta = json.loads(await _aio(meta_file.read_text))
            name, mime = meta["name"], meta.get("mime", "application/octet-stream")
        except Exception:
            name = None
        if name:
            safe_name = name.replace('"', "'")
            # While a public copy exists, send readers to it — that keeps the
            # load off Telegram and off this box entirely.
            #
            # Link-only chats are included here, and that is the distinction the
            # lane is really about: a catalogue must not be *pre*-downloaded, but a
            # book somebody actually opened is a person asking for that one book,
            # and copying it is what makes their second visit instant. Excluding
            # them here meant every later click was a cold fetch forever.
            on_download = None
            if ULTRA_ENABLED:
                key = _offload_key(chat_id, message_id, account)
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

    try:
        msg, cli = await get_media_message(chat_id, message_id, cli)
    except ValueError as e:
        # An account that has never seen the peer cannot resolve it. That is a
        # dead end for this link, not a server fault: say "not found", not 500.
        log.warning(f"link {token}: {e}")
        return web.Response(status=404, text="file not found (moved or deleted in Telegram)")
    if msg is None and own_cli(account) is not cli:
        # The parachute client can't see this peer: private-chat message IDs
        # differ per account. Fetch with the token account and borrow its
        # document for the download client (GetFile works on the document
        # location); if that download fails we fall back to the token account.
        own = own_cli(account)
        if own is not None:
            try:
                msg, cli = await get_media_message(chat_id, message_id, own)
            except ValueError as e:
                log.warning(f"link {token}: {e}")
                msg = None
    if msg is None:
        return web.Response(status=404, text="file not found (moved or deleted in Telegram)")

    name, size, mime = media_info(msg)
    safe_name = name.replace('"', "'")

    dl_task = None
    if not await _aio(cache_file.is_file):
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
            own_msg, own = await get_media_message(chat_id, message_id, own)
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

    # A known reader shouldn't have to sit through the first fetch to earn a copy.
    # The transfer completing is the wrong signal for them — see copy_when_downloaded.
    if dl_task is not None and trusted_request(request):
        asyncio.create_task(copy_when_downloaded(
            dl_task, _offload_key(chat_id, message_id, account),
            chat_id, message_id, account, name))

    # Public offload (same rule as the fast path above).
    on_download = None
    if ULTRA_ENABLED:
        key = _offload_key(chat_id, message_id, account)
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
DEAD_LINKS_FILE = Path(os.environ.get("BOOKLINK_DEAD_LINKS", "/opt/booklink/dead_links.json"))
REPOINT_FILE = Path(os.environ.get("BOOKLINK_REPOINTS", "/opt/booklink/repoints.json"))
EDGE_PASS = os.environ.get("BOOKLINK_EDGE_PASS", "")
_disk_cache = (0.0, "")

# Links whose Telegram message is gone. Kept beside the service rather than in
# files.db: the bridge owns that database, and a search that quietly hides a
# handful of rows needs no write access to somebody else's table.
_dead_links: set = set()

# Old token -> new token. A published link must never change, but what it points
# at can: when a book's original message is gone or is owned by an account that
# is about to be, the same /b/ URL serves the copy that is still readable.
_repoints: dict = {}


def _load_repoints():
    global _repoints
    try:
        if REPOINT_FILE.is_file():
            _repoints = json.loads(REPOINT_FILE.read_text())
    except Exception as e:
        log.warning(f"repoint map unreadable, ignoring it: {e}")
        _repoints = {}
    if _repoints:
        log.info(f"{len(_repoints)} links repointed to a surviving copy")


def _save_repoints():
    tmp = Path(str(REPOINT_FILE) + ".tmp")
    tmp.write_text(json.dumps(_repoints, indent=0, sort_keys=True))
    tmp.replace(REPOINT_FILE)


def _load_dead_links():
    global _dead_links
    try:
        if DEAD_LINKS_FILE.is_file():
            _dead_links = set(json.loads(DEAD_LINKS_FILE.read_text()))
    except Exception as e:
        log.warning(f"dead-link list unreadable, ignoring it: {e}")
        _dead_links = set()
    if _dead_links:
        log.info(f"{len(_dead_links)} catalogue links are known dead and hidden from search")


def _save_dead_links():
    tmp = Path(str(DEAD_LINKS_FILE) + ".tmp")
    tmp.write_text(json.dumps(sorted(_dead_links)))
    tmp.replace(DEAD_LINKS_FILE)


def _recent_filed(limit: int = 120) -> list:
    """The newest rows in the catalogue, copied or not."""
    con = sqlite3.connect(f"file:{FILES_DB}?mode=ro", uri=True, timeout=5)
    try:
        return con.execute(
            "SELECT filename, size, stored, created_at FROM files "
            "WHERE stored IS NOT NULL ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    except sqlite3.Error as e:
        log.warning(f"recent-filed query failed: {e}")
        return []
    finally:
        con.close()


def _catalogue_query(query: str, limit: int = 100) -> list:
    """Search the recorded link catalogue by filename.

    Runs in an executor: it is an unindexed scan over a few hundred thousand
    rows, which is fine as a one-off but not something to hold the event loop
    for.
    """
    like = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    con = sqlite3.connect(f"file:{FILES_DB}?mode=ro", uri=True, timeout=5)
    try:
        rows = con.execute(
            "SELECT filename, size, stored, created_at FROM files "
            "WHERE filename LIKE ? ESCAPE '\\' ORDER BY created_at DESC LIMIT ?",
            (like, limit * 4 if _dead_links else limit),
        ).fetchall()
        # A row whose message is gone is not an answer — it is a 404 the reader
        # only discovers after clicking. Hide it and let a live sibling show.
        if _dead_links:
            rows = [r for r in rows if r[2] not in _dead_links][:limit]
        return rows
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


def _read_box_disk() -> str:
    """Used-vs-quota on the seeding box. Blocking — only ever called from a thread.

    `df` on these hosts reports the whole shared volume (26 TB across every
    customer), which says nothing about how much room *she* has — `quota -s`
    is her own 2 TB allowance.
    """
    try:
        out = subprocess.run(
            ["ssh", "-i", ULTRA_KEY,
             "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
             "-o", "StrictHostKeyChecking=accept-new",
             ULTRA_SSH, "quota -s 2>/dev/null | tail -1 | awk '{print $2\" \"$3}'"],
            capture_output=True, timeout=20,
        ).stdout.decode()
        used, quota = out.split()[:2]
        u, q = _to_gb(used), _to_gb(quota)
        return f"{_human(u * 1024**3)} / {_human(q * 1024**3)} ({u / q * 100:.0f}% full)" if q else ""
    except Exception:
        return ""


def _box_disk() -> str:
    """The quota readout, always from cache.

    That ssh lands on the offload box while it is being written to and has taken
    the better part of a minute under load. It is one number on the dashboard,
    so the page renders from cache and the refresh happens behind it.
    """
    return _disk_cache[1]


def _box_disk_pct() -> float:
    """Percent of the public-copy quota in use, from the cached readout."""
    m = re.search(r"\(([0-9.]+)% full\)", _box_disk() or "")
    return float(m.group(1)) if m else 0.0


# --- intake watchdog --------------------------------------------------------
# A stopped pipeline looks like silence: no links, no errors, nothing in the
# room. This notices that nothing has been filed for a while and says so, so a
# dead intake is a ping rather than an empty dashboard discovered hours later.
WATCHDOG_HOURS = float(os.environ.get("BOOKLINK_WATCHDOG_HOURS", "6"))
WATCHDOG_COOLDOWN = 12 * 3600
_last_mint = 0.0
_watchdog_alerted = 0.0


def note_mint():
    global _last_mint
    _last_mint = time.time()


def _last_mint_from_catalogue() -> float:
    """The newest row in the catalogue, so a restart doesn't look like silence."""
    if not (FILES_DB and Path(FILES_DB).is_file()):
        return 0.0
    try:
        con = sqlite3.connect(f"file:{FILES_DB}?mode=ro", uri=True, timeout=5)
        try:
            row = con.execute("SELECT MAX(created_at) FROM files").fetchone()
        finally:
            con.close()
        return float(row[0] or 0)
    except Exception:
        return 0.0


async def intake_watchdog_loop():
    global _last_mint, _watchdog_alerted
    await asyncio.sleep(60)
    _last_mint = (await _aio(_last_mint_from_catalogue)) or time.time()
    while True:
        try:
            idle = time.time() - _last_mint
            if (idle > WATCHDOG_HOURS * 3600
                    and time.time() - _watchdog_alerted > WATCHDOG_COOLDOWN):
                await _alert(
                    f"BookLink: nothing filed for {idle / 3600:.0f} hours. "
                    f"Existing links still serve — it's new posts that have stopped."
                )
                _watchdog_alerted = time.time()
                log.warning(f"intake watchdog: no book filed for {idle / 3600:.1f} hours")
        except Exception as e:
            log.warning(f"intake watchdog failed: {e}")
        await asyncio.sleep(900)


async def box_disk_loop():
    global _disk_cache
    while True:
        try:
            val = await _aio(_read_box_disk)
            _disk_cache = (time.time(), val)
        except Exception as e:
            log.warning(f"box disk read failed: {e}")
        await asyncio.sleep(300)


async def handle_backfill(request: web.Request):
    """One-off repair: mint and file links for books posted while nothing could act.

    Walks a chat's history between two unix timestamps and reports every media
    message to the catalogue. It deliberately does NOT reply in the room — a
    backfill is repair, not conversation — and the bridge ignores books it
    already has, so running it twice is harmless.

    Behind the dashboard login: it mints permanent links, which is the same power
    as posting to the room.
    """
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
            return web.Response(status=401, text="login required",
                                headers={"WWW-Authenticate": 'Basic realm="BookLink"'})
    else:
        return web.Response(status=404, text="not found")

    try:
        chat_id = int(request.query["chat"])
        since = float(request.query["since"])
        until = float(request.query.get("until", time.time()))
    except (KeyError, ValueError):
        return web.json_response(
            {"error": "chat, since and until (unix seconds) are required"}, status=400)

    cli = third_client or new_client or client
    filed = skipped = 0
    async for msg in cli.iter_messages(
            chat_id, offset_date=datetime.fromtimestamp(until), limit=None):
        if msg.date.timestamp() < since:
            break
        doc = getattr(getattr(msg, "media", None), "document", None)
        if not doc:
            continue
        name = None
        for a in getattr(doc, "attributes", []):
            if getattr(a, "file_name", None):
                name = a.file_name
        if not name:
            skipped += 1
            continue
        token = encode_ref(chat_id, msg.id, ACCOUNT_OLD)
        await record_in_catalogue(name, doc.size, f"{PUBLIC_BASE}/b/{token}")
        filed += 1
    log.info(f"backfill {chat_id}: filed {filed} links, skipped {skipped}")
    return web.json_response({"chat": chat_id, "filed": filed, "skipped": skipped})


def _login_failure(request):
    """None when the caller may proceed, otherwise the response to send instead."""
    if STATUS_KEY and hmac.compare_digest(request.query.get("k", ""), STATUS_KEY):
        return None
    if STATUS_USER and STATUS_PASS:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                user, _, pwd = base64.b64decode(auth[6:]).decode().partition(":")
                if (hmac.compare_digest(user, STATUS_USER)
                        and hmac.compare_digest(pwd, STATUS_PASS)):
                    return None
            except Exception:
                pass
        return web.Response(status=401, text="login required",
                            headers={"WWW-Authenticate": 'Basic realm="BookLink"'})
    return web.Response(status=404, text="not found")


_sweep = {"running": False, "done": 0, "total": 0, "checked": 0,
          "dead": 0, "skipped": 0, "error": ""}
_sweep_stop = False


def _catalogue_refs(chat):
    """Every distinct (chat, message, account, link) the catalogue knows."""
    con = sqlite3.connect(f"file:{FILES_DB}?mode=ro", uri=True, timeout=10)
    try:
        rows = con.execute("SELECT stored FROM files WHERE stored IS NOT NULL").fetchall()
    finally:
        con.close()
    refs = {}
    for (stored,) in rows:
        if not stored or "/b/" not in stored:
            continue
        token = stored.rsplit("/b/", 1)[1].split("/")[0].split("?")[0]
        try:
            c, m, a = decode_ref(token)
        except Exception:
            continue
        if chat is not None and c != chat:
            continue
        refs[(c, m, a)] = stored
    return [(c, m, a, s) for (c, m, a), s in refs.items()]


def _sweep_client(chat_id, account):
    """Who can read this message: a DM belongs to one account, a group to any."""
    if chat_id > 0:
        return own_cli(account)
    return third_client or client or new_client


async def _sweep_run(chat, limit):
    """Ask Telegram about every catalogue message and record the missing ones.

    Batched 100 ids per call: one request per hundred links instead of one per
    link, which is the difference between an hour and a week.
    """
    refs = await _aio(_catalogue_refs, chat)
    if limit:
        refs = refs[:limit]
    _sweep.update(total=len(refs), checked=0, dead=0, skipped=0, error="")
    by_chat = {}
    for c, m, a, s in refs:
        by_chat.setdefault((c, a), []).append((m, s))

    found_dead = []
    for (c, a), items in by_chat.items():
        cli = _sweep_client(c, a)
        if cli is None:
            log.info(f"sweep {c}: no account can read it, skipping {len(items)}")
            _sweep["skipped"] += len(items)
            continue
        live_seen = False
        for i in range(0, len(items), 100):
            if _sweep_stop:
                log.info("sweep stopped on request")
                break
            chunk = items[i:i + 100]
            msgs = None
            for attempt in range(3):
                try:
                    msgs = await cli.get_messages(c, ids=[m for m, _ in chunk])
                    break
                except FloodWaitError as e:
                    log.warning(f"sweep {c}: flood wait {e.seconds}s")
                    await asyncio.sleep(e.seconds + 1)
                except Exception as e:
                    log.warning(f"sweep {c}: batch failed ({type(e).__name__}: {e})")
                    break
            if msgs is None:
                _sweep["skipped"] += len(chunk)
                continue
            by_id = {m.id: m for m in msgs if m is not None}
            batch_dead = []
            for m, stored in chunk:
                msg = by_id.get(m)
                if msg is not None and getattr(msg, "media", None):
                    live_seen = True
                    _sweep["checked"] += 1
                else:
                    batch_dead.append(stored)
            # A whole batch missing means this account cannot see the chat's
            # history, not that a hundred consecutive books vanished. Treat it
            # as unreadable rather than marking good links dead.
            if not live_seen and len(batch_dead) == len(chunk) and len(chunk) > 5:
                log.warning(f"sweep {c}: nothing readable — account cannot see history, skipping")
                _sweep["skipped"] += len(chunk)
            else:
                found_dead.extend(batch_dead)
                _sweep["dead"] += len(batch_dead)
            # The full catalogue is hours of requests; keep what has been found
            # so far rather than losing it to a restart.
            if batch_dead and len(found_dead) % 500 < len(batch_dead):
                _dead_links.update(found_dead)
                await _aio(_save_dead_links)
            await asyncio.sleep(0.4)

    _dead_links.update(found_dead)
    await _aio(_save_dead_links)
    log.info(f"sweep finished: {_sweep['checked']} live, {_sweep['dead']} dead, "
             f"{_sweep['skipped']} skipped")
    # A dead link is invisible until somebody clicks it. Say it out loud instead,
    # while there is still a copy somewhere to point it at.
    if found_dead:
        try:
            await _alert(f"BookLink sweep: {len(found_dead)} dead link(s) found and hidden "
                         f"from search ({_sweep['checked']} checked).")
        except Exception as e:
            log.warning(f"dead-link alert failed: {e}")


async def handle_sweep(request: web.Request):
    """Find catalogue links whose Telegram message is gone. Read-only.

    Hidden from search rather than deleted: a dead row is still evidence of a
    book, and the sweep is meant to be safe to run again and again.
    """
    global _sweep_stop
    failure = _login_failure(request)
    if failure is not None:
        return failure
    if request.query.get("stop"):
        _sweep_stop = True
        return web.json_response({"stopping": True, **_sweep})
    if request.query.get("status") or _sweep["running"]:
        return web.json_response(_sweep)
    _sweep_stop = False
    try:
        limit = int(request.query.get("limit", 0))
        chat = request.query.get("chat")
        chat = int(chat) if chat else None
    except ValueError:
        return web.json_response({"error": "limit and chat must be integers"}, status=400)

    _sweep.update(running=True, done=0)

    async def _run():
        try:
            await _sweep_run(chat, limit)
        except Exception as e:
            log.error(f"sweep failed: {type(e).__name__}: {e}")
            _sweep["error"] = f"{type(e).__name__}: {e}"
        finally:
            _sweep["running"] = False
            _sweep["done"] = 1

    asyncio.create_task(_run())
    return web.json_response({"started": True, "chat": chat, "limit": limit})


def _norm_name(name: str) -> str:
    """Filename reduced to its bones: case, spacing, punctuation and extension.

    The catalogue keys on the exact filename, which is why the same book arrives
    as several rows. For matching a copy against an original, the bones are what
    matter — and the size check alongside it is what keeps this honest.
    """
    s = re.sub(r"\.[a-z0-9]{2,5}$", "", name.lower())
    return re.sub(r"[^a-z0-9]+", "", s)


_recover = {"running": False, "done": 0, "scanned": 0, "matched": 0,
            "total": 0, "error": "", "chat": None}
_recover_stop = False


def _dm_targets():
    """Private-chat links whose exact file we still want a group copy of.

    Keyed on (normalised name, size) so a match is the same file rather than a
    different bitrate of the same book — a repointed link must not quietly hand
    a reader a worse copy than the one it replaced.
    """
    con = sqlite3.connect(f"file:{FILES_DB}?mode=ro", uri=True, timeout=10)
    try:
        rows = con.execute("SELECT filename, size, stored FROM files "
                           "WHERE stored IS NOT NULL").fetchall()
    finally:
        con.close()
    targets = {}
    for fn, sz, stored in rows:
        if not stored or "/b/" not in stored:
            continue
        token = stored.rsplit("/b/", 1)[1].split("/")[0].split("?")[0]
        try:
            chat, _msg, _acct = decode_ref(token)
        except Exception:
            continue
        if chat <= 0 or token in _repoints:      # only private chats, only once
            continue
        targets.setdefault((_norm_name(fn), sz), []).append(token)
    return targets


async def _recover_run(chat, limit):
    """Find each private-chat book's identical copy in a group and repoint to it."""
    targets = await _aio(_dm_targets)
    _recover.update(total=sum(len(v) for v in targets.values()),
                    scanned=0, matched=0, chat=chat, error="")
    # Deliberately the long-standing account, not the intake one: the sweep runs
    # on intake, and Telegram's flood budget is per account, so the two jobs can
    # work at once without stealing each other's allowance.
    cli = client or third_client or new_client
    if cli is None:
        _recover["error"] = "no account can read that chat"
        return
    log.info(f"recover: {len(targets)} files wanted, scanning {chat}")

    scanned = 0
    async for msg in cli.iter_messages(chat, limit=limit or None):
        if _recover_stop:
            log.info("recover stopped on request")
            break
        scanned += 1
        if scanned % 200 == 0:
            _recover["scanned"] = scanned
        doc = getattr(getattr(msg, "media", None), "document", None)
        if not doc:
            continue
        name = None
        for a in getattr(doc, "attributes", []):
            if getattr(a, "file_name", None):
                name = a.file_name
        if not name:
            continue
        key = (_norm_name(name), doc.size)
        tokens = targets.pop(key, None)
        if not tokens:
            continue
        new_token = encode_ref(chat, msg.id, ACCOUNT_OLD)
        for token in tokens:
            _repoints[token] = new_token
            _recover["matched"] += 1
        log.info(f"recover: {name} -> message {msg.id} ({len(tokens)} link(s))")
        if _recover["matched"] % 200 < len(tokens):
            await _aio(_save_repoints)
        if not targets:
            break

    _recover["scanned"] = scanned
    await _aio(_save_repoints)
    log.info(f"recover finished: scanned {scanned}, repointed {_recover['matched']}, "
             f"{len(targets)} still missing")


async def handle_recover(request: web.Request):
    """Repoint private-chat links at their identical copy in a group. Read-only."""
    global _recover_stop
    failure = _login_failure(request)
    if failure is not None:
        return failure
    if request.query.get("stop"):
        _recover_stop = True
        return web.json_response({"stopping": True, **_recover})
    if request.query.get("status") or _recover["running"]:
        return web.json_response(_recover)
    try:
        chat = int(request.query["chat"])
        limit = int(request.query.get("limit", 0))
    except (KeyError, ValueError):
        return web.json_response({"error": "chat (and optional limit) required"}, status=400)
    _recover_stop = False
    _recover.update(running=True, done=0)

    async def _run():
        try:
            await _recover_run(chat, limit)
        except Exception as e:
            log.error(f"recover failed: {type(e).__name__}: {e}")
            _recover["error"] = f"{type(e).__name__}: {e}"
        finally:
            _recover["running"] = False
            _recover["done"] = 1

    asyncio.create_task(_run())
    return web.json_response({"started": True, "chat": chat, "limit": limit})


_copy = {"running": False, "done": 0, "total": 0, "copied": 0,
         "skipped": 0, "failed": 0, "error": "", "chat": None}
_copy_stop = False


async def _copy_run(chat, account, limit):
    """Copy every media message in a chat to the public box, one at a time.

    This is what makes a private-chat link survive its account: once a copy is
    on the box, /b/ redirects to it and Telegram is never asked again. A group
    book would be fine without this — any librarian can fetch it — but a DM book
    has exactly one account that can, and that account is the thing at risk.
    """
    cli = _account_client("1" if account == ACCOUNT_NEW else "0")
    if cli is None:
        _copy["error"] = "that account is not available"
        return
    items = []
    async for m in cli.iter_messages(chat, limit=limit or None):
        doc = getattr(getattr(m, "media", None), "document", None)
        if not doc:
            continue
        name = None
        for a in getattr(doc, "attributes", []):
            if getattr(a, "file_name", None):
                name = a.file_name
        items.append((m.id, name or f"file_{m.id}.bin"))
    _copy.update(total=len(items), done=0, copied=0, skipped=0, failed=0, chat=chat)
    log.info(f"copy: {len(items)} files from {chat}")

    for message_id, name in items:
        if _copy_stop:
            log.info("copy stopped on request")
            break
        key = _offload_key(chat, message_id, account)
        if _ultra_live(key):
            _copy["skipped"] += 1
            _copy["done"] += 1
            continue
        try:
            cache_file = cache_path(chat, message_id, account)
            if not await _aio(cache_file.is_file):
                msg, used = await get_media_message(chat, message_id, cli)
                if msg is None:
                    _copy["failed"] += 1
                    _copy["done"] += 1
                    continue
                await download_to_cache(chat, message_id, msg, used, account)
            await ultra_offload_task(key, chat, message_id, account,
                                     cache_file, name, high=False)
            _copy["copied"] += 1
        except Exception as e:
            log.warning(f"copy failed for {key}: {type(e).__name__}: {e}")
            _copy["failed"] += 1
        _copy["done"] += 1

    log.info(f"copy finished: {_copy['copied']} copied, {_copy['skipped']} already there, "
             f"{_copy['failed']} failed")


async def handle_copy(request: web.Request):
    """Copy a chat's books to the public box so their links outlive the account."""
    global _copy_stop
    failure = _login_failure(request)
    if failure is not None:
        return failure
    if request.query.get("stop"):
        _copy_stop = True
        return web.json_response({"stopping": True, **_copy})
    if request.query.get("status") or _copy["running"]:
        return web.json_response(_copy)
    try:
        chat = int(request.query["chat"])
        account = int(request.query.get("account", ACCOUNT_NEW))
        limit = int(request.query.get("limit", 0))
    except (KeyError, ValueError):
        return web.json_response({"error": "chat required; account and limit optional"}, status=400)
    _copy_stop = False
    _copy.update(running=True)

    async def _run():
        try:
            await _copy_run(chat, account, limit)
        finally:
            _copy["running"] = False

    asyncio.create_task(_run())
    return web.json_response({"started": True, "chat": chat, "account": account})


def _account_client(which: str):
    return {"1": new_client, "0": client, "third": third_client}.get(which)


async def handle_dialogs(request: web.Request):
    """List an account's private chats — who has sent it books."""
    failure = _login_failure(request)
    if failure is not None:
        return failure
    cli = _account_client(request.query.get("account", "1"))
    if cli is None:
        return web.json_response({"error": "that account is not available"}, status=503)
    out = []
    async for d in cli.iter_dialogs():
        if not d.is_user:
            continue
        out.append({"id": d.id, "name": d.name,
                    "username": getattr(d.entity, "username", None)})
    return web.json_response(out)


async def handle_audit(request: web.Request):
    """Every media message in one chat, as (id, name, size, date)."""
    failure = _login_failure(request)
    if failure is not None:
        return failure
    try:
        chat = int(request.query["chat"])
        limit = int(request.query.get("limit", 0))
    except (KeyError, ValueError):
        return web.json_response({"error": "chat (and optional limit) required"}, status=400)
    cli = _account_client(request.query.get("account", "1")) or client
    if cli is None:
        return web.json_response({"error": "no account available"}, status=503)
    out = []
    try:
        async for m in cli.iter_messages(chat, limit=limit or None):
            doc = getattr(getattr(m, "media", None), "document", None)
            if not doc:
                continue
            name = None
            for a in getattr(doc, "attributes", []):
                if getattr(a, "file_name", None):
                    name = a.file_name
            out.append({"id": m.id, "name": name, "size": doc.size, "date": str(m.date)})
    except Exception as e:
        return web.json_response({"error": f"{type(e).__name__}: {e}"}, status=502)
    return web.json_response(out)


async def handle_inspect(request: web.Request):
    """What a catalogue message actually is — read-only, for diagnosing links.

    Used to answer "where did this book come from" without downloading it: a
    forwarded copy carries the chat it came from, which is how a link minted off
    a copy can be traced back to the original post.
    """
    failure = _login_failure(request)
    if failure is not None:
        return failure
    try:
        chat = int(request.query["chat"])
        ids = [int(x) for x in request.query["ids"].split(",")][:100]
    except (KeyError, ValueError):
        return web.json_response({"error": "chat and ids (comma separated) required"}, status=400)
    cli = _sweep_client(chat, ACCOUNT_NEW)
    if cli is None:
        return web.json_response({"error": "no account can read that chat"}, status=503)
    try:
        msgs = await cli.get_messages(chat, ids=ids)
    except Exception as e:
        return web.json_response({"error": f"{type(e).__name__}: {e}"}, status=502)
    out = []
    for m in msgs:
        if m is None:
            out.append(None)
            continue
        fwd = getattr(m, "fwd_from", None)
        doc = getattr(getattr(m, "media", None), "document", None)
        name = None
        for a in getattr(doc, "attributes", []) if doc else []:
            if getattr(a, "file_name", None):
                name = a.file_name
        out.append({
            "id": m.id,
            "date": str(getattr(m, "date", "")),
            "media": bool(m.media),
            "name": name,
            "size": getattr(doc, "size", None),
            "forwarded_from": str(fwd) if fwd is not None else None,
        })
    return web.json_response(out)


async def handle_books_proxy(request: web.Request):
    """Stream a public copy from the box, adding its credentials on the way.

    The box's provider enforces HTTP auth that the account cannot change, so a
    reader given its URL directly would get a password prompt. Here the service
    holds the credentials and passes the bytes through, which keeps the URL a
    reader sees on our own domain and keeps the password out of every link.
    """
    global _box_session
    if not BOX_PROXY_BASE:
        return web.Response(status=404, text="not found")
    if _box_session is None or _box_session.closed:
        # Bounded on purpose. Phone players abort and restart range requests
        # constantly, and each abandoned stream used to leave an upstream
        # connection held open until its read timeout — which is how a burst of
        # ordinary listening starved the pool and took the service with it.
        _box_session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(BOX_PROXY_USER, BOX_PROXY_PASS),
            # force_close: never pool connections to the box. Aborted downloads
            # left half-dead ones in the pool, and a pool full of those stops
            # serving entirely — 25 of them, some idle 30 hours, took every link
            # down. One connection per request costs nothing by comparison.
            connector=aiohttp.TCPConnector(limit=24, limit_per_host=24, force_close=True),
            timeout=aiohttp.ClientTimeout(total=1800, sock_connect=20, sock_read=120),
        )
    name = request.match_info.get("path", "")
    url = f"{BOX_PROXY_BASE}/books/{quote(name)}"
    fwd = {}
    if request.headers.get("Range"):
        fwd["Range"] = request.headers["Range"]
    try:
        upstream = await _box_session.get(url, headers=fwd)
    except Exception as e:
        log.warning(f"book proxy failed for {name}: {type(e).__name__}: {e}")
        return web.Response(status=502, text="box unreachable")
    if upstream.status >= 400:
        upstream.release()
        return web.Response(status=upstream.status, text="not found")

    headers = {}
    for h in ("Content-Type", "Content-Length", "Content-Range",
              "Accept-Ranges", "Last-Modified"):
        if upstream.headers.get(h):
            headers[h] = upstream.headers[h]
    headers["Content-Disposition"] = f'attachment; filename="{Path(name).name}"'
    headers["Access-Control-Allow-Origin"] = "*"
    if request.method == "HEAD":
        upstream.release()
        return web.Response(status=upstream.status, headers=headers)

    resp = web.StreamResponse(status=upstream.status, headers=headers)
    await resp.prepare(request)
    try:
        async for chunk in upstream.content.iter_chunked(256 * 1024):
            await resp.write(chunk)
    except Exception as e:
        # A reader that walked away mid-download: drop the upstream connection
        # outright rather than handing a half-read one back to the pool.
        log.warning(f"book proxy stream broke for {name}: {type(e).__name__}: {e}")
        upstream.close()
    else:
        upstream.release()
    return resp



async def handle_whoami(request: web.Request):
    """Which account each client is actually logged in as."""
    failure = _login_failure(request)
    if failure is not None:
        return failure
    out = {}
    for label, cli in (("old", client), ("new", new_client), ("third", third_client)):
        if cli is None:
            out[label] = None
            continue
        try:
            me = await cli.get_me()
            out[label] = {"id": me.id, "username": me.username, "name": me.first_name,
                          "premium": bool(getattr(me, "premium", False))}
        except Exception as e:
            out[label] = {"error": f"{type(e).__name__}: {e}"}
    return web.json_response(out)


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

    # What has been filed lately, copy or no copy. Posting a book to a room is
    # how Cerina gets its link — she never opens the links afterwards — so a list
    # that only shows copies hides every book she just posted.
    recent = []
    if FILES_DB and Path(FILES_DB).exists():
        try:
            recent = await asyncio.get_running_loop().run_in_executor(None, _recent_filed)
        except Exception as e:
            log.warning(f"recent-filed list failed: {e}")

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

    disk = _box_disk()
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
<h2 class=mt>Recently filed</h2>
<div class=sub>The last {len(recent)} books that got a link, newest first &mdash; whether or not a copy
is on the box. This is the list to read after posting.</div>
<table id=recent><thead><tr>
 <th data-k=name>Name</th>
 <th data-k=size style="text-align:right">Size</th>
 <th>Copy</th>
 <th data-k=added>Filed</th>
 <th>Link</th>
</tr></thead>
<tbody>{''.join(match_row(*r) for r in recent) or '<tr><td colspan=5 class=dim>nothing filed yet</td></tr>'}</tbody></table>
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


async def _reply_in_chat(cli, event, text, **kw):
    """Reply to the book, healing the two ways a librarian's reply fails.

    1. The peer isn't in the entity cache. StringSession keeps no access hashes
       across a restart, and `_warm_entities` only knows the dialogs that existed
       at startup, so a room joined later delivers a message the client can
       receive but cannot address. Re-reading the dialog list once caches it.
    2. The receiving account cannot send at all — a read-only or frozen account
       still *reads* (which is why existing links keep serving) but every reply
       is refused, so no new link would ever be minted. A group message can be
       answered by either librarian, so fall back to the other one rather than
       losing the link. A DM cannot: its message ids are per-account.
    """
    try:
        return await event.reply(text, **kw)
    except Exception as e:
        log.warning(f"reply to {event.chat_id} failed ({type(e).__name__}: {e})")
        alt = other_cli(cli) if event.chat_id < 0 else None
        if alt is None:
            raise
        if isinstance(e, PeerIdInvalidError):
            await _warm_entities(cli)
        log.info(f"replying to {event.chat_id} from the other librarian instead")
        return await alt.send_message(event.chat_id, text,
                                      reply_to=event.message.id, **kw)


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
        # A signed link can't be reused: the signature is over the token, and a
        # stored one was written without it.
        reuse = None if LINK_SECRET else (catalogue_link(name, doc.size) if name else None)
        if reuse:
            link = reuse
        else:
            token = encode_ref(event.chat_id, event.message.id, account)
            link = f"{PUBLIC_BASE}/b/{token}"
            if LINK_SECRET:
                link += f"?s={sign(token)}"
        size_mb = doc.size / 1024 / 1024
        # A room can want the link without the post. The link is the deliverable
        # and it is filed below either way; the reply is the part that is noise
        # in a room people scroll.
        if event.chat_id not in SILENT_CHATS:
            await _reply_in_chat(
                cli, event,
                f"📚 {name or 'file'}\n{size_mb:.1f} MB\n\n"
                f"🔗 Permanent link:\n{link}\n\n"
                f"📋 Easy copy (tap to copy):\n`{link}`",
                link_preview=False,
            )
        # Group books (notifier forwards from the watched group) get a link only — no
        # precache. They're a list/database for the ABB, not hot public links, so
        # pre-downloading them just burns Telegram bandwidth and evicts real singles.
        # Personal singles (Cerina's own forwards) keep the immediate cache.
        #
        # A reused link doesn't pre-cache *this* message — the copy has to belong
        # to the link people will actually open. But it does warm that link, since
        # re-sending a book is a person saying it matters, and the catalogue's copy
        # of it may be sitting in a lane that never keeps copies.
        # Tell the catalogue about it, whether it was minted or reused: this is
        # what keeps files.db current, and what makes the next re-send find the
        # same link.
        asyncio.create_task(record_in_catalogue(name, doc.size, link))
        is_group_forward = event.chat_id in NO_PRECACHE_CHATS
        if not is_group_forward:
            if reuse:
                asyncio.create_task(warm_reused_link(reuse))
            else:
                asyncio.create_task(precache_in_background(event.chat_id, event.message.id, event.message, cli, account))
        note_mint()
        log.info(
            f"link {'reused' if reuse else 'issued'} for {name} "
            f"({event.chat_id}/{event.message.id}, account {account}, group={is_group_forward})"
            f"{' [silent: filed, no reply]' if event.chat_id in SILENT_CHATS else ''}"
        )
    except Exception as e:
        log.error(f"link handler error: {e}")
        try:
            await _reply_in_chat(event.client, event, f"couldn't make a link: {e}")
        except Exception as e2:
            # The reply itself can fail for the same reason the link did; say so
            # in the log rather than raising out of the handler.
            log.error(f"could not even report that failure to {event.chat_id}: {e2}")


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


async def _pick_intake_client():
    """The librarian that should handle shared group chats.

    A frozen account still reads — which is why the links it minted keep serving —
    but Telegram refuses its action methods (GetParticipants and friends) and
    stops delivering group updates to it, so it can neither see a group post nor
    answer one. Pick the account that can actually act instead of assuming.
    """
    probe = min(INTAKE_CHATS) if INTAKE_CHATS else None
    for cli, label in ((third_client, "@Little Darling"),
                       (new_client, "@lightweavebooks"),
                       (client, "@Cerina2")):
        if cli is None:
            continue
        try:
            if probe is not None:
                await cli.get_participants(probe, limit=1)
            return cli, label
        except Exception as e:
            log.warning(f"intake probe failed on {label}: {type(e).__name__}: {e}")
    return None, None


async def _start_or_drop(attr):
    """Start one librarian; if Telegram refuses it, log it and carry on.

    A deleted account's session raises out of start() — AuthKeyUnregistered, or
    UserDeactivated. Letting that propagate takes the whole service down, so one
    dead account would silence all three and every link would 404 at once. The
    accounts are independent: whichever ones still log in serve what they can,
    and a group link whose token names the missing account still recovers through
    the other one (see get_media_message).
    """
    cli = globals()[attr]
    if cli is None:
        return
    try:
        await cli.start()
    except Exception as e:
        log.error(f"{attr} could not start ({type(e).__name__}: {e}) — continuing without it")
        globals()[attr] = None


async def on_startup(app):
    _ultra_load()
    _load_dead_links()
    _load_repoints()
    await client.start()
    await _start_or_drop("new_client")
    await _start_or_drop("third_client")
    if third_client is not None:
        # Cache its dialogs before the intake probe runs, or a group it has just
        # joined is still unresolvable by numeric id and the probe wrongly fails.
        await _warm_entities(third_client)

    # Private chats: each account answers its own, because a DM's message ids
    # belong to the account that received it and neither can see the other's.
    client.add_event_handler(
        on_forwarded_book,
        events.NewMessage(incoming=True, func=lambda e: e.is_private and e.media),
    )
    if new_client is not None:
        new_client.add_event_handler(
            on_forwarded_book,
            events.NewMessage(incoming=True, func=lambda e: e.is_private and e.media),
        )

    # Groups are shared, so exactly one account handles them — and it has to be
    # one that can still act (see _pick_intake_client).
    intake_cli, intake_label = await _pick_intake_client()
    if intake_cli is None:
        log.error("no librarian can act on group chats — group posts will mint nothing")
    else:
        log.info(f"group intake handled by {intake_label}")
        intake_cli.add_event_handler(
            on_forwarded_book,
            events.NewMessage(
                incoming=True,
                func=lambda e: e.media and not e.is_private and e.chat_id in INTAKE_CHATS,
            ),
        )
        intake_cli.add_event_handler(
            on_unlisted_group_media,
            events.NewMessage(
                incoming=True,
                func=lambda e: e.media and not e.is_private and e.chat_id not in INTAKE_CHATS,
            ),
        )
    asyncio.create_task(ultra_save_loop())
    asyncio.create_task(cache_cleanup_loop())
    asyncio.create_task(disk_alert_loop())
    asyncio.create_task(box_disk_loop())
    asyncio.create_task(premium_watch_loop())
    asyncio.create_task(intake_watchdog_loop())
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
    app.router.add_get("/books/{path:.*}", handle_books_proxy)
    app.router.add_get("/whoami", handle_whoami)
    app.router.add_get("/status", handle_status)
    app.router.add_get("/backfill", handle_backfill)
    app.router.add_get("/sweep", handle_sweep)
    app.router.add_get("/inspect", handle_inspect)
    app.router.add_get("/dialogs", handle_dialogs)
    app.router.add_get("/audit", handle_audit)
    app.router.add_get("/copy", handle_copy)
    app.router.add_get("/recover", handle_recover)
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
