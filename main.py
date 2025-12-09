# main.py
import os
import logging
import time
import threading
import json
from typing import Dict, Set

from flask import Flask, jsonify
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler

import db
from validator import validate_and_parse
import worker

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# Config
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

SUDO_USERS = os.environ.get("SUDO_USERS", "")
SUDO_USERS_SET: Set[int] = set()
if SUDO_USERS:
    for s in SUDO_USERS.split(","):
        try:
            SUDO_USERS_SET.add(int(s.strip()))
        except Exception:
            continue

TARGET_CHATS_ENV = os.environ.get("TARGET_CHATS", "")
def parse_target_chats(env: str):
    out = {}
    if not env:
        return out
    for p in [x.strip() for x in env.split(",") if x.strip()]:
        if ":" in p:
            name, cid = p.split(":",1)
            try:
                out[name.strip()] = int(cid.strip())
            except:
                continue
    return out

TARGET_CHATS_MAP = parse_target_chats(TARGET_CHATS_ENV)

DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")

# init db
db.init_db(DB_PATH)

# Flask app for health
app = Flask(__name__)
@app.route("/")
def index():
    return "OK", 200

@app.route("/health")
def health():
    hb = db.get_meta("last_heartbeat") or ""
    return jsonify({"status":"ok", "last_heartbeat": hb}), 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# Bot
bot = Bot(token=TELEGRAM_BOT_TOKEN)

def kb_choose_anonymous(job_id: str):
    # callback_data encodes job + choice
    kb = [
        [InlineKeyboardButton("Public", callback_data=json.dumps({"job": job_id, "type":"public"}))],
        [InlineKeyboardButton("Anonymous", callback_data=json.dumps({"job": job_id, "type":"anonymous"}))]
    ]
    return InlineKeyboardMarkup(kb)

def kb_choose_chat(job_id: str, type_choice: str):
    kb = []
    for name, cid in TARGET_CHATS_MAP.items():
        payload = {"job": job_id, "type": type_choice, "chat_id": cid}
        kb.append([InlineKeyboardButton(name, callback_data=json.dumps(payload))])
    return InlineKeyboardMarkup(kb)

# /start
def start_cmd(update: Update, context: CallbackContext):
    uid = update.effective_user.id if update.effective_user else None
    if uid in SUDO_USERS_SET:
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
    else:
        msg = "🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner."
    context.bot.send_message(chat_id=update.effective_chat.id, text=msg)

# handle formatted messages from owners (permissive)
def owner_message_handler(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if uid not in SUDO_USERS_SET:
        logger.info("Ignored message from non-owner: %s", uid)
        return
    text = (update.effective_message.text or "").strip()
    if not text:
        context.bot.send_message(uid, "Empty message.")
        return
    # quick ack
    context.bot.send_message(uid, "✅ Received. Quick validation in progress...")
    # validate
    res = validate_and_parse(text)
    if not isinstance(res, dict):
        context.bot.send_message(uid, "Validator returned unexpected result.")
        return
    if not res.get("ok", False):
        errs = res.get("errors", []) or []
        warns = res.get("warnings", []) or []
        reply = ""
        if errs:
            reply += "⚠️ Format errors:\n" + "\n".join(errs) + "\n"
        if warns:
            reply += "⚠️ Warnings:\n" + "\n".join(warns)
        context.bot.send_message(uid, reply or "Validation failed.")
        return
    # ok -> create job id and save sequence in memory (context.bot_data) and DB for persistence
    job_id = f"job:{int(time.time()*1000)}:{uid}"
    sequence = res.get("sequence", [])
    # save to context and to DB
    pending = context.bot_data.setdefault("pending_jobs", {})
    pending[job_id] = {"owner_id": uid, "sequence": sequence}
    db.save_job(job_id=job_id, owner_id=uid, payload={"sequence": sequence}, status="waiting")
    # ask for public/anonymous
    context.bot.send_message(uid, "Choose poll type (no cancel):", reply_markup=kb_choose_anonymous(job_id))

# callback when owner chooses public/anonymous
def callback_choose_type(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    data = query.data
    try:
        payload = json.loads(data)
    except Exception:
        query.answer("Invalid action.")
        return
    job_id = payload.get("job")
    choice = payload.get("type")
    if not job_id or choice not in ("public","anonymous"):
        query.answer("Invalid selection.")
        return
    # verify job exists in context or DB
    pending = context.bot_data.get("pending_jobs", {})
    job = pending.get(job_id)
    if not job:
        # try DB
        j = db.get_job(job_id)
        if j:
            job = {"owner_id": j["owner_id"], "sequence": j["payload"].get("sequence")}
            pending[job_id] = job
        else:
            query.answer("Job not found or expired.")
            return
    # ask to choose chat
    query.answer(f"Selected {choice}. Now choose target chat.")
    query.edit_message_text(text=f"Selected {choice}. Now pick chat to post:", reply_markup=kb_choose_chat(job_id, choice))

# callback when owner chooses chat
def callback_choose_chat(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    try:
        payload = json.loads(query.data)
    except Exception:
        query.answer("Invalid action.")
        return
    job_id = payload.get("job")
    ctype = payload.get("type")
    chat_id = payload.get("chat_id")
    if not job_id or ctype not in ("public","anonymous") or not chat_id:
        query.answer("Invalid action.")
        return
    # find job
    pending = context.bot_data.get("pending_jobs", {})
    job = pending.get(job_id)
    if not job:
        j = db.get_job(job_id)
        if j:
            job = {"owner_id": j["owner_id"], "sequence": j["payload"].get("sequence")}
            pending[job_id] = job
        else:
            query.answer("Job not found or expired.")
            return
    # run posting in background thread so callback returns quickly
    owner_id = job.get("owner_id")
    sequence = job.get("sequence", [])
    # remove job (one-shot)
    try:
        pending.pop(job_id, None)
    except:
        pass
    # mark DB job queued
    db.save_job(job_id=job_id, owner_id=owner_id, payload={"sequence": sequence}, status="queued")
    query.answer("Posting started.")
    query.edit_message_text(text=f"Posting to selected chat...")

    def do_post():
        try:
            chat_name = None
            # find name
            for name, cid in TARGET_CHATS_MAP.items():
                if cid == chat_id:
                    chat_name = name
                    break
            anonymous = True if ctype == "anonymous" else False
            ok = worker.post_sequence(context.bot, chat_id, sequence, anonymous, owner_id=owner_id, chat_name=chat_name)
            # after posting, remove DB job
            # we saved earlier; now delete
            # (pop_next_waiting will delete older ones; to keep it simple just delete by id)
            # We'll reuse db.get_job to check, then delete by marking status done
            if ok:
                db.set_meta("last_job_success", {"job_id": job_id, "owner": owner_id, "chat": chat_name, "sent": True, "time": int(time.time())})
            else:
                db.set_meta("last_job_success", {"job_id": job_id, "owner": owner_id, "chat": chat_name, "sent": False, "time": int(time.time())})
        except Exception:
            logger.exception("Error in do_post thread for job %s", job_id)

    th = threading.Thread(target=do_post, daemon=True)
    th.start()

# helper: a tiny debug command to list target chats
def list_chats_cmd(update: Update, context: CallbackContext):
    uid = update.effective_user.id if update.effective_user else None
    if uid not in SUDO_USERS_SET:
        update.message.reply_text("Not allowed.")
        return
    if not TARGET_CHATS_MAP:
        update.message.reply_text("No target chats configured in TARGET_CHATS.")
        return
    text = "Configured target chats:\n"
    for name, cid in TARGET_CHATS_MAP.items():
        text += f"- {name}: {cid}\n"
    update.message.reply_text(text)

def main():
    # run flask in thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    # commands
    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("list_chats", list_chats_cmd))

    # message handler for owner formatted text
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS_SET)), owner_message_handler))

    # callbacks
    dp.add_handler(CallbackQueryHandler(callback_choose_type, pattern=r'^\{.*"type"\s*:\s*".*"\}'))
    dp.add_handler(CallbackQueryHandler(callback_choose_chat, pattern=r'^\{.*"chat_id".*\}'))

    # Start polling in main thread (so signal works)
    logger.info("Starting polling (main thread)...")
    updater.start_polling(poll_interval=1.0, timeout=20)
    # heartbeat meta update
    try:
        while True:
            db.set_meta("last_heartbeat", int(time.time()))
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        updater.stop()

if __name__ == "__main__":
    main()
