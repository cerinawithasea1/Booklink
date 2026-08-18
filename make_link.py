#!/usr/bin/env python3
"""Find a recent media message in a group and print its permanent BookLink.
Usage: python3 make_link.py <chat_id> [search_limit]"""
import os, sys, base64
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.environ["BOOKLINK_API_ID"])
API_HASH = os.environ["BOOKLINK_API_HASH"]
SESSION = os.environ["BOOKLINK_SESSION"]
BASE = os.environ.get("BOOKLINK_BASE", "http://127.0.0.1:" + os.environ.get("BOOKLINK_PORT", "8094"))


def encode_ref(chat_id, message_id):
    return base64.urlsafe_b64encode(f"{chat_id}:{message_id}".encode()).decode().rstrip("=")


def _slug(n):
    import re as _re
    base, dot, ext = n.rpartition(".")
    if not dot:
        base, ext = n, ""
    s = _re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return f"{s}.{ext}" if ext else s


chat_id = int(sys.argv[1])
limit = int(sys.argv[2]) if len(sys.argv) > 2 else 30

with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as c:
    found = 0
    for msg in c.iter_messages(chat_id, limit=limit):
        if msg.media and getattr(msg.media, "document", None):
            name = None
            for a in msg.media.document.attributes:
                if hasattr(a, "file_name"):
                    name = a.file_name
            size_mb = msg.media.document.size / 1024 / 1024
            token = encode_ref(chat_id, msg.id)
            pretty = _slug(name) if name else ""
            print(f"[{name or 'file'}] {size_mb:.1f} MB")
            print(f"  {BASE}/b/{token}/{pretty}")
            found += 1
            if found >= 3:
                break
    if not found:
        print(f"No document media found in last {limit} messages of {chat_id}")
