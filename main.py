# main.py
import os
import logging
import threading
import time
import json
import uuid
import sqlite3
from typing import Optional, Tuple, List, Dict, Any

from flask import Flask, jsonify
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler

import db
import worker
from validator import validate_and_parse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("quizbot")

# Config from env
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

# SUDO_USERS: CSV of ints
def get_sudo_users() -> List[int]:
    s = os.environ.get("SUDO_USERS", "")
    out = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            continue
    return out

SUDO_USERS = get_sudo_users()

OWNER_TG_ID = None
owner_env = os.environ.get("OWNER_TG_ID")
if owner_env:
    try:
        OWNER_TG_ID = int(owner_env)
        if OWNER_TG_ID not in SUDO_USERS:
            SUDO_USERS.append(OWNER_TG_ID)
    except Exception:
        pass

def is_owner(user_id: int) -> bool:
    try:
        return int(user_id) in SUDO_USERS
    except Exception:
        return False

# initialize DB
DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")
db.init_db(DB_PATH)

# Flask app for health checks
app = Flask(__name__)

@app.route("/")
def index():
    return "OK", 200

@app.route("/health")
def health():
    hb = db.get_meta("last_heartbeat") or ""
    return jsonify({"status": "ok", "last_heartbeat": hb}), 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# Bot init
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Helper for TARGET_CHATS env
def get_target_chats_from_env() -> List[Tuple[str,int]]:
    # Format: Name1:chatid1,Name2:chatid2
    val = os.environ.get("TARGET_CHATS", "")
    pairs = []
    for part in val.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            name, cid = part.split(":", 1)
            try:
                cid_val = int(cid.strip())
                pairs.append((name.strip(), cid_val))
            except Exception:
                continue
    return pairs

# Handlers
def start_cmd(update: Update, context: CallbackContext):
    user = update.effective_user
    if user and is_owner(user.id):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by authorized owners.")

def help_cmd(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Send the quiz in the format (DES/Q/A/B/C/D/ANS/EXP). Only SUDO_USERS can post.")

# Owner message -> validate -> ask poll type
def owner_only_message_handler(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        logger.info("Ignored message from non-owner: %s", uid)
        return

    text = update.effective_message.text or ""
    res = validate_and_parse(text)
    if not res.get('ok', False):
        errs = res.get('errors', [])
        warns = res.get('warnings', [])
        reply = "⚠️ Format errors:\n" + ("\n".join(errs) if errs else "Unknown format error.")
        if warns:
            reply += "\n⚠️ Warnings:\n" + ("\n".join(warns))
        context.bot.send_message(chat_id=uid, text=reply)
        return

    job_id = str(uuid.uuid4())
    payload = {"id": job_id, "text": text, "parsed": res}
    try:
        db.save_job(job_id=job_id, payload=json.dumps(payload), status="await_poll_type", owner_id=uid)
    except Exception:
        logger.exception("Failed to save job to DB.")
        context.bot.send_message(chat_id=uid, text="⚠️ Failed to save job. Try again.")
        return

    keyboard = [
        [InlineKeyboardButton("🔒 Anonymous", callback_data=f"polltype|{job_id}|anon"),
         InlineKeyboardButton("🌍 Public", callback_data=f"polltype|{job_id}|public")]
    ]
    context.bot.send_message(chat_id=uid, text="Choose poll type:", reply_markup=InlineKeyboardMarkup(keyboard))

# Callback: poll type selected
def callback_poll_type(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    try:
        _, job_id, ptype = q.data.split("|")
    except Exception:
        q.edit_message_text("Invalid selection.")
        return

    job_row = db.get_job(job_id)
    if not job_row:
        q.edit_message_text("Job not found or expired.")
        return

    # store poll_type
    extra = {"poll_type": ptype}
    db.update_job_status(job_id, "await_target", extra=json.dumps(extra))

    pairs = get_target_chats_from_env()
    if not pairs:
        q.edit_message_text("No TARGET_CHATS set. Reply with chat id to post, or set TARGET_CHATS env.")
        return

    keyboard = []
    for name, cid in pairs:
        keyboard.append([InlineKeyboardButton(f"{name}", callback_data=f"target|{job_id}|{cid}")])
    q.edit_message_text("Choose channel/group to post:", reply_markup=InlineKeyboardMarkup(keyboard))

# Callback: target selected
def callback_target_selected(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    try:
        _, job_id, cid_str = q.data.split("|")
        cid = int(cid_str)
    except Exception:
        q.edit_message_text("Invalid target selection.")
        return

    job_row = db.get_job(job_id)
    if not job_row:
        q.edit_message_text("Job missing/expired.")
        return

    payload = json.loads(job_row['payload'])
    parsed = payload["parsed"]
    extra = json.loads(job_row.get("extra") or "{}")
    poll_type = extra.get("poll_type", "public")
    is_anon = (poll_type == "anon")

    db.update_job_status(job_id, "queued")
    owner_id = job_row.get("owner_id")

    # Start posting in background thread
    def run_job():
        try:
            ok = worker.post_quiz_questions_background(bot, cid, parsed, is_anon, owner_id=owner_id)
            db.update_job_status(job_id, "done" if ok else "failed")
        except Exception:
            logger.exception("Uncaught exception in job thread")
            db.update_job_status(job_id, "failed")

    threading.Thread(target=run_job, daemon=True).start()
    q.edit_message_text("✅ Job queued. Posting will start now.")

# Fallback: owner replies with chat id (when TARGET_CHATS not present)
def owner_text_general(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        return
    text = (update.effective_message.text or "").strip()
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        # find latest awaiting job for this owner
        job_row = db.get_latest_job_for_owner(uid, status="await_target")
        if not job_row:
            context.bot.send_message(uid, "No pending job found. Send formatted quiz text first.")
            return
        try:
            cid = int(text)
        except Exception:
            context.bot.send_message(uid, "Invalid chat id.")
            return

        payload = json.loads(job_row["payload"])
        parsed = payload["parsed"]
        db.update_job_status(job_row["id"], "queued")
        def run_job():
            worker.post_quiz_questions_background(bot, cid, parsed, is_anon=False, owner_id=uid)
            db.update_job_status(job_row["id"], "done")
        threading.Thread(target=run_job, daemon=True).start()
        context.bot.send_message(uid, f"✅ Job queued. Posting to {cid} shortly.")
        return

    context.bot.send_message(uid, "Reply with chat id number to post a saved job (e.g. -1001234567890).")

# Resilient polling start (no idle in background threads)
def start_polling_resilient(updater: Updater):
    backoff = 1
    while True:
        try:
            logger.info("Starting polling (resilient mode)...")
            # start_polling is non-blocking
            updater.start_polling(poll_interval=1.0, timeout=20, clean=True)
            # keep thread alive; check periodically if updater is running
            while True:
                if not updater.running:
                    raise RuntimeError("Updater stopped unexpectedly")
                time.sleep(5)
        except Exception as e:
            logger.exception("Polling crashed: %s", e)
            if SUDO_USERS:
                try:
                    bot.send_message(SUDO_USERS[0], f"⚠️ Bot polling crashed with error: {e}. Restarting in {backoff}s.")
                except Exception:
                    logger.exception("Failed to notify owner about crash.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            # attempt restart by continuing loop

def main():
    # Start Flask in its own thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    # Setup Updater and handlers
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=SUDO_USERS) & Filters.regex(r'Q:'), owner_only_message_handler))
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=SUDO_USERS), owner_text_general))

    dp.add_handler(CallbackQueryHandler(callback_poll_type, pattern=r'^polltype\|'))
    dp.add_handler(CallbackQueryHandler(callback_target_selected, pattern=r'^target\|'))

    # Start resilient polling in a dedicated thread (we avoid updater.idle())
    poll_thread = threading.Thread(target=start_polling_resilient, args=(updater,), daemon=True)
    poll_thread.start()

    # Heartbeat loop
    try:
        while True:
            ts = str(int(time.time()))
            db.set_meta("last_heartbeat", ts)
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt; stopping updater.")
        updater.stop()
    except Exception:
        logger.exception("Main loop exception; stopping updater.")
        updater.stop()

if __name__ == "__main__":
    main()
