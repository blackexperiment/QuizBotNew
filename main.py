# main.py
import os
import logging
import threading
import time
import sqlite3
from typing import Optional

from flask import Flask, jsonify
from telegram import Bot, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

import db
import worker
from validator import validate_and_parse

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

# owner(s) support: SUDO_USERS comma-separated OR single OWNER_TG_ID
SUDO_USERS_ENV = os.environ.get("SUDO_USERS", "")
SUDO_USERS = set()
if SUDO_USERS_ENV:
    for x in SUDO_USERS_ENV.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            SUDO_USERS.add(int(x))
        except Exception:
            logger.warning("Ignoring invalid SUDO_USERS entry: %s", x)

OWNER_TG_ID = os.environ.get("OWNER_TG_ID")
if OWNER_TG_ID:
    try:
        OWNER_TG_ID = int(OWNER_TG_ID)
        SUDO_USERS.add(OWNER_TG_ID)
    except Exception:
        logger.warning("OWNER_TG_ID invalid, ignoring.")

REDIS_URL = os.environ.get("REDIS_URL")
DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")

# Init DB
db.init_db(db_path=DB_PATH)

# Flask app
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

# Telegram bot init
bot = Bot(token=TELEGRAM_BOT_TOKEN)

def is_owner(user_id: int) -> bool:
    try:
        return int(user_id) in SUDO_USERS
    except Exception:
        return False

def start_cmd(update: Update, context: CallbackContext):
    user = update.effective_user
    if user and is_owner(user.id):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. Only authorized owner(s) may use it.")

def help_cmd(update: Update, context: CallbackContext):
    if update.effective_user and is_owner(update.effective_user.id):
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="Send formatted quiz text (DES/Q/A/ANS/EXP).")
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="No access.")

def owner_only_message_handler(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        logger.info("Ignored message from non-owner: %s", uid)
        return

    text = (update.effective_message.text or "").strip()
    if not text:
        context.bot.send_message(uid, "Empty message.")
        return

    # Validate
    res = validate_and_parse(text)
    if not res['ok']:
        errs = "\n".join(res.get('errors', [])) or "Unknown format error."
        warns = "\n".join(res.get('warnings', []))
        reply = f"⚠️ Format errors:\n{errs}"
        if warns:
            reply += f"\n⚠️ Warnings:\n{warns}"
        context.bot.send_message(chat_id=uid, text=reply)
        return

    # prepare questions payload
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

    # target chat selection: prefer TARGET_CHAT_IDS env (first), else ask owner for chat id
    target_env = os.environ.get("TARGET_CHAT_IDS")
    target_chat = None
    if target_env:
        try:
            target_chat = int(target_env.split(",")[0].strip())
        except Exception:
            target_chat = None

    if target_chat is None:
        context.bot.send_message(uid, "Choose channel/group where you want to post this quiz 📨\nReply with chat id (e.g. -100123...) or set TARGET_CHAT_IDS env.")
        db.save_job(job_id="pending:"+str(int(time.time())), payload=text, status="waiting_target")
        return

    # run worker in background daemon thread (non-blocking)
    def run_job():
        try:
            worker.post_quiz_questions(bot, target_chat, title, questions_input, notify_owner_id=uid)
        except Exception:
            logger.exception("Uncaught error in run_job")
    threading.Thread(target=run_job, daemon=True).start()
    context.bot.send_message(uid, f"✅ Job queued. Posting to chat {target_chat} shortly.")

def owner_text_general(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        return
    text = (update.effective_message.text or "").strip()
    if not text:
        context.bot.send_message(uid, "Please send a chat id (e.g. -100123...) to post saved job.")
        return

    if text.lstrip("-").isdigit():
        # find most recent waiting_target job
        try:
            conn = db._get_conn(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT id, payload FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT 1", ("waiting_target",))
            row = cur.fetchone()
            if row:
                job_id = row["id"]
                job_payload = row["payload"]
                cur.execute("UPDATE jobs SET status = ? WHERE id = ?", ("queued", job_id))
                conn.commit()
                conn.close()

                res = validate_and_parse(job_payload)
                if not res['ok']:
                    context.bot.send_message(uid, "Stored job content invalid. Aborting.")
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
                    worker.post_quiz_questions(bot, target_chat, title, questions_input, notify_owner_id=uid)
                threading.Thread(target=run_job, daemon=True).start()
                context.bot.send_message(uid, f"✅ Job queued and will be posted to {target_chat}.")
                return
            else:
                context.bot.send_message(uid, "No pending job found.")
                return
        except Exception:
            logger.exception("Error processing pending job")
            context.bot.send_message(uid, "Error processing pending job. See logs.")
            return

    context.bot.send_message(uid, "Invalid chat id. Send numeric chat id like -1001234567890.")

def setup_handlers(dp):
    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))

    # owner message handler: messages containing 'Q:' go to validator -> enqueue
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS)) & Filters.regex(r'Q:'), owner_only_message_handler))
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS)), owner_text_general))

def main():
    # Run Flask in background
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Remove any webhook that might be active (avoid conflicts)
    try:
        bot.delete_webhook()
    except Exception:
        logger.debug("delete_webhook failed (maybe none).")

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    setup_handlers(dp)

    # Start polling once (do NOT call updater.idle() from a non-main thread)
    try:
        logger.info("Starting polling (single start)...")
        # start_polling spawns worker threads internally; we do not call idle()
        updater.start_polling(poll_interval=1.0, timeout=20)
    except Exception:
        logger.exception("Failed to start polling; exiting.")
        raise

    # Heartbeat + basic watchdog loop in main thread
    try:
        backoff = 1
        while True:
            # update heartbeat
            db.set_meta("last_heartbeat", str(int(time.time())), db_path=DB_PATH)
            # If the updater threads have died unexpectedly, attempt a restart
            if not updater.running:
                logger.warning("Updater not running; trying to restart polling.")
                try:
                    updater.start_polling(poll_interval=1.0, timeout=20)
                    backoff = 1
                    if SUDO_USERS:
                        for uid in list(SUDO_USERS)[:1]:
                            try:
                                bot.send_message(uid, "⚠️ Bot polling was restarted automatically.")
                            except Exception:
                                pass
                except Exception:
                    logger.exception("Failed to restart updater; sleeping then retrying.")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; shutting down.")
        updater.stop()
    except Exception:
        logger.exception("Main loop exception; stopping updater.")
        updater.stop()

if __name__ == "__main__":
    main()
