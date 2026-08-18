#!/usr/bin/env python3
"""Generate a StringSession for a BookLink librarian account.

Reads API id/hash from the environment (see .env / .env.example).

Usage:
  python3 make_session.py phone <number>      # send the login code
  python3 make_session.py sign-in <code>      # enter the code → prints session
  python3 make_session.py password <pw>       # only if the account has 2FA
"""
import sys
import os
import json
import asyncio
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

STATE_PATH = Path("/tmp/booklink_newsession.json")


def load_state():
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {}


def save_state(s):
    STATE_PATH.write_text(json.dumps(s))


async def main():
    args = sys.argv[1:]
    if len(args) < 1:
        print(__doc__)
        return

    api_id = int(os.environ["BOOKLINK_API_ID"])
    api_hash = os.environ["BOOKLINK_API_HASH"]

    state = load_state()
    client = TelegramClient(StringSession(state.get("session", "")), api_id, api_hash)
    await client.connect()

    mode = args[0]
    try:
        if mode == "phone":
            phone = args[1]
            sent = await client.send_code_request(phone)
            save_state({"phone": phone, "session": client.session.save(),
                        "phone_code_hash": sent.phone_code_hash})
            print("code sent. next: python3 make_session.py sign-in <code>")
        elif mode == "sign-in":
            code = args[1]
            try:
                await client.sign_in(state["phone"], code,
                                     phone_code_hash=state.get("phone_code_hash"))
            except SessionPasswordNeededError:
                save_state({"phone": state["phone"], "session": client.session.save(),
                            "phone_code_hash": state.get("phone_code_hash")})
                print("2FA is on. next: python3 make_session.py password <pw>")
                return
            print("\nBOOKLINK_SESSION=" + client.session.save())
            STATE_PATH.unlink(missing_ok=True)
        elif mode == "password":
            pw = args[1]
            await client.sign_in(password=pw)
            print("\nBOOKLINK_SESSION=" + client.session.save())
            STATE_PATH.unlink(missing_ok=True)
        else:
            print(__doc__)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
