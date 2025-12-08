# main.py
import os
import logging
import threading
import time
import signal
import sys
from typing import Optional, List, Dict, Any

from flask import Flask, jsonify
from telegram import Bot, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

# local modules
import db
import worker
from validator import validate_and_parse  # your validator.py

# Logging
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

OWNER_TG_ID = os.environ.get("OWNER_TG_ID")
try:
    OWNER_TG_ID = int(OWNER_TG_ID) if OWNER_TG_ID else None
except Exception:
    OWNER_TG_ID = None

REDIS_URL = os.environ.get("REDIS_URL")
DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")

# Initialize DB
db.init_db()

# Flask app for health (so Render can probe)
app = Flask(__name__)

@app.route("/")
def index():
    return "OK", 200

@app.route("/health")
def health():
    # we can check a simple meta key
    hb = db.get_meta("last_heartbeat") or ""
    return jsonify({"status": "ok", "last_heartbeat": hb}), 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    # Bind to 0.0.0.0 so Render routing works
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# Telegram bot initialization and handlers
bot = Bot(token=TELEGRAM_BOT_TOKEN)

def is_owner(user_id: int) -> bool:
    return OWNER_TG_ID is not None and int(user_id) == int(OWNER_TG_ID)

def start_cmd(update: Update, context: CallbackContext):
    user = update.effective_user
    if is_owner(user.id):
        # owner view
        msg = "🛡️ BLACK RHINO CONTROL PANEL\nWelcome, Owner.\nSend formatted quiz text (DES/Q/A/ANS/EXP) and I will post it."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        # normal user view
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")

def help_cmd(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Send formatted quiz text (owner only).")

def owner_only_message_handler(update: Update, context: CallbackContext):
    """
    Accepts only messages from owner; validates format and enqueues posting.
    """
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        # ignore non-owner messages
        logger.info("Ignored message from non-owner: %s", uid)
        return

    text = update.effective_message.text or ""
    # Validate
    res = validate_and_parse(text)
    if not res['ok']:
        # send errors back to owner
        errs = "\n".join(res.get('errors', []))
        warns = "\n".join(res.get('warnings', []))
        reply = f"⚠️ Format errors:\n{errs}"
        if warns:
            reply += f"\n⚠️ Warnings:\n{warns}"
        context.bot.send_message(chat_id=uid, text=reply)
        return

    # All good — prepare job
    title = res.get('des')
    questions_input = []
    for q in res["questions"]:
        # build question dict for worker
        # options must be listed in alphabetical order of labels
        labels = sorted(q["options"].keys())
        options = {lbl: q["options"][lbl] for lbl in labels}
        questions_input.append({
            "raw_question": q["raw_question"],
            "options": options,
            "ans": q["ans"],
            "exp": q.get("exp")
        })

    # Request owner for target chat (simple behavior: if TARGET_CHAT_IDS env exists use first; else ask owner)
    target_env = os.environ.get("TARGET_CHAT_IDS")
    target_chat = None
    if target_env:
        # take first id
        try:
            target_chat = int(target_env.split(",")[0].strip())
        except Exception:
            target_chat = None

    if target_chat is None:
        # ask owner which chat - but for simplicity, post back quick question to owner:
        context.bot.send_message(uid, "Choose channel/group where you want to post this quiz 📨\n(Reply with chat id number or set TARGET_CHAT_IDS env)")
        # store job payload temporarily in DB and wait for owner to respond: to keep code minimal, we ask owner to reply with chat id
        db.save_job(job_id="pending:"+str(int(time.time())), payload=text, status="waiting_target")
        return

    # if target provided, create a background thread to run posting so main thread is not blocked
    def run_job():
        try:
            success = worker.post_quiz_questions(context.bot, target_chat, title, questions_input, OWNER_TG_ID)
            if not success:
                logger.error("Job failed for target %s", target_chat)
        except Exception:
            logger.exception("Uncaught error in run_job thread")

    t = threading.Thread(target=run_job, daemon=True)
    t.start()
    context.bot.send_message(uid, f"✅ Job queued. Posting to chat {target_chat} shortly.")

# Generic handler for replies to choose chat id if owner had pending job (very simple)
def owner_text_general(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        return
    text = (update.effective_message.text or "").strip()
    # crude: if text looks like an integer and pending job exists, use it as chat id
    if text.isdigit():
        # find a pending job
        # For simplicity, pick the most recent waiting_target job
        # This is a simple flow; you can expand to robust mapping later
        # We'll just scan DB rows for job with status waiting_target
        try:
            conn = db._get_conn()
            cur = conn.cursor()
            cur.execute("SELECT id, payload FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT 1", ("waiting_target",))
            row = cur.fetchone()
            if row:
                job_payload = row["payload"]
                # remove job or mark as queued
                cur.execute("UPDATE jobs SET status = ? WHERE id = ?", ("queued", row["id"]))
                conn.commit()
                conn.close()
                # now validate payload and post
                res = validate_and_parse(job_payload)
                if not res['ok']:
                    context.bot.send_message(uid, "Stored job content is no longer valid. Aborting.")
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
                target_chat = int(text)
                def run_job():
                    worker.post_quiz_questions(context.bot, target_chat, title, questions_input, OWNER_TG_ID)
                threading.Thread(target=run_job, daemon=True).start()
                context.bot.send_message(uid, f"✅ Job queued and will be posted to {target_chat}.")
                return
        except Exception:
            logger.exception("Error processing pending job for owner-provided chat id.")
            context.bot.send_message(uid, "Error processing pending job. See logs.")
            return

    # else treat it as a normal message — help text
    context.bot.send_message(uid, "To post a saved job reply with chat id number (e.g. -1001234567890).")

# resilience: start polling in a loop with reconnect attempts
def start_polling_resilient(updater: Updater):
    backoff = 1
    while True:
        try:
            logger.info("Starting polling...")
            updater.start_polling(poll_interval=1.0, timeout=20)
            updater.idle()  # blocks until stop() is called
            logger.info("Updater.idle() returned; exiting polling loop.")
            break
        except Exception as e:
            logger.exception("Polling crashed: %s", e)
            # notify owner
            if OWNER_TG_ID:
                try:
                    bot.send_message(OWNER_TG_ID, f"⚠️ Bot polling crashed with error: {e}. Restarting in {backoff}s.")
                except Exception:
                    logger.exception("Failed to notify owner about crash.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)  # cap backoff

def main():
    # run flask in separate thread for health
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    # Setup Updater and handlers
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))

    # For owner message handling: if message looks like formatted quiz (contains 'Q:'), go to owner handler
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=OWNER_TG_ID) & Filters.regex(r'Q:'), owner_only_message_handler))
    # fallback owner text handler (for chat id replies)
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=OWNER_TG_ID), owner_text_general))

    # Start resilient polling loop in a thread so main can manage signals
    poll_thread = threading.Thread(target=start_polling_resilient, args=(updater,), daemon=True)
    poll_thread.start()

    # heartbeat: update DB meta every 30s so health can show last heartbeat
    try:
        while True:
            ts = str(int(time.time()))
            db.set_meta("last_heartbeat", ts)
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; shutting down.")
        updater.stop()
    except Exception:
        logger.exception("Main loop exception; exiting.")
        updater.stop()

if __name__ == "__main__":
    main()
