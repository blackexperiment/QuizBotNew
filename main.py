# main.py
import os
import logging
import threading
import time
import uuid
from typing import Dict, Optional

from flask import Flask, jsonify
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler

import db
from validator import validate_and_parse
import worker

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# ENV / config
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

# Parse SUDO_USERS (CSV of ids)
SUDO_RAW = os.environ.get("SUDO_USERS", "")
SUDO_USERS = set()
for part in (SUDO_RAW.split(",") if SUDO_RAW else []):
    p = part.strip()
    if not p:
        continue
    try:
        SUDO_USERS.add(int(p))
    except Exception:
        logger.warning("Ignoring non-integer SUDO_USERS value: %s", p)

# Parse TARGET_CHATS in Name:chatid,Name2:chatid2 form
TARGET_RAW = os.environ.get("TARGET_CHATS", "")
TARGET_CHATS: Dict[str, int] = {}
if TARGET_RAW:
    for item in TARGET_RAW.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            name, cid = item.split(":", 1)
            name = name.strip()
            try:
                cid_val = int(cid.strip())
            except Exception:
                logger.warning("Invalid chat id for target '%s' -> '%s'", name, cid)
                continue
            TARGET_CHATS[name] = cid_val
        else:
            # fallback: unnamed chat id
            try:
                cid_val = int(item)
                TARGET_CHATS[item] = cid_val
            except Exception:
                logger.warning("Invalid TARGET_CHATS entry: %s", item)

# Start DB
db.init_db()

# Flask app for health
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

# helper
def is_owner(user_id: int) -> bool:
    return int(user_id) in SUDO_USERS

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# /start handler
def start_cmd(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user:
        return
    uid = user.id
    if is_owner(uid):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")

def help_cmd(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id, text="Send formatted quiz text (owner only).")

# When owner sends formatted quiz text
def owner_message_handler(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user:
        return
    uid = user.id
    if not is_owner(uid):
        logger.info("Ignored message from non-owner: %s", uid)
        return

    text = update.effective_message.text or ""
    # quick heuristic: must contain 'Q:' to be considered quiz
    if "Q:" not in text:
        context.bot.send_message(uid, "I expected a formatted quiz input containing 'Q:'.")
        return

    res = validate_and_parse(text)
    if not res['ok']:
        errs = "\n".join(res.get('errors', []))
        warns = "\n".join(res.get('warnings', []))
        reply = f"⚠️ Format errors:\n{errs}"
        if warns:
            reply += f"\n⚠️ Warnings:\n{warns}"
        context.bot.send_message(chat_id=uid, text=reply)
        return

    # Save job in DB
    job_id = f"job:{uuid.uuid4().hex}"
    db.save_job(job_id, text, owner_id=uid, status="waiting_target")

    # Build inline keyboard from TARGET_CHATS
    if not TARGET_CHATS:
        context.bot.send_message(uid, "⚠️ No TARGET_CHATS configured. Set TARGET_CHATS env (Name:chatid,...).")
        return

    keyboard = []
    for name, cid in TARGET_CHATS.items():
        # callback data encodes job id and target chat id
        cb = f"post::{job_id}::{cid}"
        keyboard.append([InlineKeyboardButton(text=name, callback_data=cb)])
    reply_markup = InlineKeyboardMarkup(keyboard)
    context.bot.send_message(uid, text="Choose channel/group where you want to post this quiz 📨", reply_markup=reply_markup)

# Callback when owner selects target chat
def callback_post_to_target(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    uid = query.from_user.id
    data = query.data or ""
    # parse callback_data
    if not data.startswith("post::"):
        query.answer()
        return
    _, job_id, cid = data.split("::", 2)
    try:
        cid_val = int(cid)
    except Exception:
        query.answer(text="Invalid chat id", show_alert=True)
        return

    # verify owner
    if not is_owner(uid):
        query.answer(text="Unauthorized", show_alert=True)
        return

    # fetch job
    job = db.get_job(job_id)
    if not job or job.get("status") != "waiting_target":
        query.answer(text="This job is no longer available.", show_alert=True)
        return

    payload = job.get("payload")
    # mark as queued
    db.mark_job_status(job_id, "queued")
    query.answer(text="Queued. Posting will start shortly.", show_alert=False)

    # parse payload again
    res = validate_and_parse(payload)
    if not res['ok']:
        # report back
        try:
            context.bot.send_message(uid, "Stored job is no longer valid. Aborting.")
        except Exception:
            logger.exception("Failed to notify owner.")
        db.mark_job_status(job_id, "failed")
        return

    title = res.get('des')
    questions_input = []
    for q in res["questions"]:
        labels = sorted(q["options"].keys())
        options = {lbl: q["options"][lbl] for lbl in labels}
        questions_input.append({
            "raw_question": q["raw_question"],
            "options": options,
            "ans": q["ans"],
            "exp": q.get("exp")
        })

    # run posting in background thread
    def run_job():
        try:
            ok = worker.post_quiz_questions(context.bot, cid_val, title, questions_input, owner_id=uid)
            if ok:
                # Mark job done
                db.mark_job_status(job_id, "done")
            else:
                db.mark_job_status(job_id, "aborted")
        except Exception:
            logger.exception("Uncaught error in job execution.")
            db.mark_job_status(job_id, "failed")

    threading.Thread(target=run_job, daemon=True).start()
    # send immediate confirmation to owner
    # find friendly name for cid_val
    target_name = None
    for n, c in TARGET_CHATS.items():
        if c == cid_val:
            target_name = n
            break
    target_label = target_name if target_name else str(cid_val)
    context.bot.send_message(uid, f"✅ Job queued. Posting to {target_label} now.")

# General text handler (owner replies w/ chat id numeric) - kept for compatibility
def owner_text_general(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user:
        return
    uid = user.id
    if not is_owner(uid):
        return
    txt = (update.effective_message.text or "").strip()
    if txt.isdigit() or (txt.startswith("-") and txt[1:].isdigit()):
        # pick most recent waiting job for this owner
        job = db.fetch_most_recent_waiting(owner_id=uid)
        if not job:
            context.bot.send_message(uid, "No pending job found.")
            return
        job_id = job["id"]
        payload = job["payload"]
        db.mark_job_status(job_id, "queued")
        res = validate_and_parse(payload)
        if not res['ok']:
            context.bot.send_message(uid, "Stored job invalid. Aborting.")
            db.mark_job_status(job_id, "failed")
            return
        title = res.get('des')
        questions_input = []
        for q in res["questions"]:
            labels = sorted(q["options"].keys())
            options = {lbl: q["options"][lbl] for lbl in labels}
            questions_input.append({
                "raw_question": q["raw_question"],
                "options": options,
                "ans": q["ans"],
                "exp": q.get("exp")
            })
        target_chat = int(txt)
        def run_job():
            try:
                ok = worker.post_quiz_questions(context.bot, target_chat, title, questions_input, owner_id=uid)
                if ok:
                    db.mark_job_status(job_id, "done")
                else:
                    db.mark_job_status(job_id, "aborted")
            except Exception:
                logger.exception("run_job error")
                db.mark_job_status(job_id, "failed")
        threading.Thread(target=run_job, daemon=True).start()
        context.bot.send_message(uid, f"✅ Job queued. Posting to {target_chat} now.")
        return

    # fallback help
    context.bot.send_message(uid, "Reply with chat id number to post latest saved job (e.g. -1001234567890).")

# Polling start/resilience (no updater.idle() to avoid signal-in-thread issues)
def start_polling_loop(updater: Updater):
    backoff = 1
    while True:
        try:
            logger.info("Starting updater.start_polling()")
            updater.start_polling(poll_interval=1.0, timeout=20)
            # Now run until an exception occurs (we keep main thread alive in main loop)
            return  # we started polling successfully; return to main thread
        except Exception as e:
            logger.exception("Start polling failed: %s", e)
            # notify owners
            for uid in SUDO_USERS:
                try:
                    bot.send_message(uid, f"⚠️ Bot failed to start polling: {e}. Restarting in {backoff}s.")
                except Exception:
                    pass
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

def main():
    # run flask in background
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))

    # owner handlers
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS)), owner_message_handler))
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS)), owner_text_general))

    # callback for inline buttons
    dp.add_handler(CallbackQueryHandler(callback_post_to_target, pattern=r"^post::"))

    # Start polling (non-blocking in main thread)
    start_polling_loop(updater)

    # Heartbeat loop - maintian last_heartbeat in DB
    try:
        while True:
            db.set_meta("last_heartbeat", str(int(time.time())))
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt - shutting down")
        updater.stop()
    except Exception:
        logger.exception("Main loop exception")
        updater.stop()

if __name__ == "__main__":
    main()
