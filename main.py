# main.py
import os
import logging
import threading
import time
import signal
import sys
from typing import Optional, List, Dict, Any

from flask import Flask, jsonify
from telegram import Bot, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

# local modules (assumed present in repo)
import db
import worker
from validator import validate_and_parse  # your validator.py

# ----- Logging -----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("quizbot")

# ----- Config from env -----
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

# Primary owner (single id) - optional but recommended
OWNER_TG_ID = os.environ.get("OWNER_TG_ID")
try:
    OWNER_TG_ID = int(OWNER_TG_ID) if OWNER_TG_ID else None
except Exception:
    OWNER_TG_ID = None

# SUDO_USERS - comma separated list of ids (owner + admins)
SUDO_USERS = []
sudo_env = os.environ.get("SUDO_USERS") or os.environ.get("SUDO_USERS_LIST")
if sudo_env:
    for part in sudo_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            SUDO_USERS.append(int(part))
        except Exception:
            logger.warning("Skipping invalid SUDO_USERS entry: %s", part)

# If OWNER_TG_ID present but SUDO_USERS empty, add owner
if OWNER_TG_ID and OWNER_TG_ID not in SUDO_USERS:
    SUDO_USERS.insert(0, OWNER_TG_ID)

REDIS_URL = os.environ.get("REDIS_URL")
DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")
TARGET_CHAT_IDS = os.environ.get("TARGET_CHAT_IDS")  # optional CSV of default target chats

# ----- Initialize DB -----
db.init_db()

# ----- Flask app for health -----
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
    # Bind to 0.0.0.0 so Render routing works
    try:
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    except Exception:
        logger.exception("Flask failed")

# ----- Helper functions -----
bot = Bot(token=TELEGRAM_BOT_TOKEN)

def is_owner(user_id: int) -> bool:
    return user_id in SUDO_USERS

def notify_owner(text: str):
    if OWNER_TG_ID:
        try:
            bot.send_message(OWNER_TG_ID, text)
        except Exception:
            logger.exception("Failed to notify owner")

# ----- Handlers -----
def start_cmd(update: Update, context: CallbackContext):
    user = update.effective_user
    if user is None:
        return
    if is_owner(user.id):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        # optional inline settings button
        buttons = [
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings")]
        ]
        try:
            context.bot.send_message(chat_id=update.effective_chat.id, text=msg,
                                     reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            logger.exception("Failed to send owner start message")
    else:
        try:
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")
        except Exception:
            logger.exception("Failed to send private notice")

def help_cmd(update: Update, context: CallbackContext):
    try:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="Send formatted quiz text (owner/sudo only).")
    except Exception:
        logger.exception("Failed to send help")

def owner_only_message_handler(update: Update, context: CallbackContext):
    """
    Accepts messages (containing 'Q:') only from sudo users.
    Validates, stores job and asks for target if needed, then queues posting in background thread.
    """
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        logger.info("Ignored message from non-sudo user: %s", uid)
        return

    text = update.effective_message.text or ""
    # Validate with validator.py
    try:
        res = validate_and_parse(text)
    except Exception:
        logger.exception("Validator crashed")
        context.bot.send_message(uid, "⚠️ Validator crashed. Check logs.")
        return

    if not res.get('ok'):
        errs = "\n".join(res.get('errors', []))
        warns = "\n".join(res.get('warnings', []))
        reply = f"⚠️ Format errors:\n{errs}" if errs else "⚠️ Format errors."
        if warns:
            reply += f"\n⚠️ Warnings:\n{warns}"
        context.bot.send_message(chat_id=uid, text=reply)
        return

    title = res.get('des')
    questions_input = []
    for q in res["questions"]:
        labels = sorted(q["options"].keys())
        options = {lbl: q["options"][lbl] for lbl in labels}
        questions_input.append({
            "raw_question": q.get("raw_question"),
            "options": options,
            "ans": q.get("ans"),
            "exp": q.get("exp")
        })

    # Determine target chat:
    target_chat = None
    # First, if OWNER sent inline selection of chat id earlier - (not implemented complex mapping)
    # Next, check TARGET_CHAT_IDS env
    if TARGET_CHAT_IDS:
        try:
            tgt = TARGET_CHAT_IDS.split(",")[0].strip()
            target_chat = int(tgt)
        except Exception:
            target_chat = None

    if target_chat is None:
        # Ask owner to reply with chat id (simple flow)
        context.bot.send_message(uid, "📨 Choose channel/group where you want to post this quiz. Reply with chat id (e.g. -1001234567890) or set TARGET_CHAT_IDS env.")
        # Save the job in DB for later pick-up when owner replies with chat id
        job_id = "pending:" + str(int(time.time()))
        db.save_job(job_id=job_id, payload=text, status="waiting_target")
        return

    # enqueue posting but run in background thread (worker.post_quiz_questions handles delays/retries)
    def run_job():
        try:
            ok = worker.post_quiz_questions(bot, target_chat, title, questions_input, uid)
            if ok:
                try:
                    bot.send_message(uid, f"✅ {len(questions_input)} quiz questions posted successfully in {target_chat}.")
                except Exception:
                    logger.exception("Failed to send completion message")
            else:
                bot.send_message(uid, "⚠️ Posting job failed. Check logs.")
        except Exception as e:
            logger.exception("Uncaught error in run_job")
            try:
                bot.send_message(uid, f"❌ Job crashed: {e}")
            except Exception:
                logger.exception("Failed to notify owner about job crash")

    t = threading.Thread(target=run_job, daemon=True)
    t.start()
    context.bot.send_message(uid, f"✅ Job queued. Posting to chat {target_chat} shortly.")

def owner_text_general(update: Update, context: CallbackContext):
    """
    Simple handler to accept plain text replies from owner.
    If owner replies with a numeric chat id and there's a pending job -> post it.
    """
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        return
    text = (update.effective_message.text or "").strip()
    if not text:
        return

    # If text is digits (allow negative for supergroups)
    is_chatid = False
    try:
        # support -100... ids
        if text.startswith("-") and text[1:].isdigit():
            is_chatid = True
        elif text.isdigit():
            is_chatid = True
    except Exception:
        is_chatid = False

    if is_chatid:
        # find a pending waiting_target job
        try:
            conn = db._get_conn()
            cur = conn.cursor()
            cur.execute("SELECT id, payload FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT 1", ("waiting_target",))
            row = cur.fetchone()
            if row:
                job_id = row["id"]
                job_payload = row["payload"]
                # mark queued
                cur.execute("UPDATE jobs SET status = ? WHERE id = ?", ("queued", job_id))
                conn.commit()
                conn.close()
                # validate and post
                res = validate_and_parse(job_payload)
                if not res.get('ok'):
                    context.bot.send_message(uid, "Stored job content invalid. Aborting.")
                    return
                title = res.get('des')
                questions_input = []
                for q in res["questions"]:
                    labels = sorted(q["options"].keys())
                    options = {lbl: q["options"][lbl] for lbl in labels}
                    questions_input.append({
                        "raw_question": q.get("raw_question"),
                        "options": options,
                        "ans": q.get("ans"),
                        "exp": q.get("exp")
                    })
                target_chat = int(text)

                def run_job():
                    try:
                        ok = worker.post_quiz_questions(bot, target_chat, title, questions_input, uid)
                        if ok:
                            bot.send_message(uid, f"✅ {len(questions_input)} quiz questions posted successfully in {target_chat}.")
                        else:
                            bot.send_message(uid, "⚠️ Posting job failed. Check logs.")
                    except Exception:
                        logger.exception("run_job error")
                        try:
                            bot.send_message(uid, "❌ Job crashed during posting.")
                        except Exception:
                            pass

                threading.Thread(target=run_job, daemon=True).start()
                context.bot.send_message(uid, f"✅ Job queued and will be posted to {target_chat}.")
                return
            else:
                context.bot.send_message(uid, "No pending job found. Send formatted quiz first.")
                return
        except Exception:
            logger.exception("Error processing pending job for owner-provided chat id.")
            context.bot.send_message(uid, "Error processing pending job. See logs.")
            return

    # fallback
    context.bot.send_message(uid, "Reply with a chat id to post pending job (e.g. -1001234567890).")

# ----- Polling resilience / main -----
def heartbeat_worker():
    """Update last_heartbeat every 30s so /health shows liveness."""
    try:
        while True:
            ts = str(int(time.time()))
            try:
                db.set_meta("last_heartbeat", ts)
            except Exception:
                logger.exception("Failed to write heartbeat to DB")
            time.sleep(30)
    except Exception:
        logger.exception("Heartbeat thread died")

def main():
    # Start Flask health in background
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Start heartbeat thread
    hb_thread = threading.Thread(target=heartbeat_worker, daemon=True)
    hb_thread.start()

    # Setup Updater and handlers
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))

    # Message handlers:
    # Owner messages that contain 'Q:' -> main quiz handler
    if OWNER_TG_ID:
        user_filter_for_owner = Filters.user(user_id=SUDO_USERS)
    else:
        user_filter_for_owner = Filters.user(user_id=SUDO_USERS) if SUDO_USERS else Filters.user(user_id=[])
    dp.add_handler(MessageHandler(Filters.text & user_filter_for_owner & Filters.regex(r'Q:'), owner_only_message_handler))
    dp.add_handler(MessageHandler(Filters.text & user_filter_for_owner, owner_text_general))

    # Start polling in MAIN THREAD (this is important)
    backoff = 1
    while True:
        try:
            logger.info("Starting polling (main thread)...")
            updater.start_polling(poll_interval=1.0, timeout=20)
            # idle() will block here in main thread and handle signals correctly
            updater.idle()
            logger.info("Updater.idle() returned; exiting.")
            break
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down")
            try:
                updater.stop()
            except Exception:
                pass
            break
        except Exception as e:
            logger.exception("Polling crashed: %s", e)
            notify_owner(f"⚠️ Bot polling crashed with error: {e}. Restarting in {backoff}s.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            # try again (loop)

if __name__ == "__main__":
    main()
