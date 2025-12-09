# main.py
import os
import logging
import time
import threading
import uuid
from typing import Optional

from flask import Flask, jsonify
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler

import db
import worker
from validator import validate_and_parse

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# Configs
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

SUDO_USERS_RAW = os.environ.get("SUDO_USERS", "")
SUDO_USERS = set()
if SUDO_USERS_RAW:
    for part in SUDO_USERS_RAW.split(","):
        part = part.strip()
        if part:
            try:
                SUDO_USERS.add(int(part))
            except:
                pass

# Target chats parsed in worker.parse_target_chats_env
TARGET_CHATS = worker.TARGET_CHATS

# other envs used in worker through env
POLL_DELAY_SHORT = os.environ.get("POLL_DELAY_SHORT", "1")
POLL_DELAY_LONG = os.environ.get("POLL_DELAY_LONG", "2")
MAX_RETRIES = os.environ.get("MAX_RETRIES", "4")

# initialize DB
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

# Bot init
bot = Bot(token=TELEGRAM_BOT_TOKEN)
updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
dispatcher = updater.dispatcher

# Utilities
def is_owner(user_id: int) -> bool:
    return int(user_id) in SUDO_USERS

def format_target_buttons():
    buttons = []
    for c in TARGET_CHATS:
        # callback data uses a uuid to map to pending job id later
        buttons.append([InlineKeyboardButton(c["name"], callback_data=f"CH_{c['id']}")])
    return InlineKeyboardMarkup(buttons)

# State flow:
# Owner sends formatted text -> bot parses -> stores job in DB with status 'waiting_mode'
# Bot replies with Public/Anonymous buttons -> owner clicks -> update job with mode and reply with target chat buttons -> owner clicks chat -> posting starts and job status set to 'posting'
# job id uses uuid

def start_cmd(update: Update, context):
    user = update.effective_user
    if user and is_owner(user.id):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")

def help_cmd(update: Update, context):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Send formatted quiz text (owner only).")

def owner_text_handler(update: Update, context):
    user = update.effective_user
    if user is None or not is_owner(user.id):
        return
    text = (update.message.text or "").strip()
    if not text:
        return
    # parse quickly to produce events; we will still accept even if validator reported minor errors
    parsed = validate_and_parse(text)
    # Save job in DB with status waiting_mode
    job_id = str(uuid.uuid4())
    expires_at = int(time.time()) + 60 * 10  # expires in 10 minutes
    payload = {"text": text, "events": parsed.get("events", [])}
    db.save_job_row(job_id=job_id, owner_id=user.id, payload=payload, status="waiting_mode", expires_at=expires_at)
    # Send mode selection (Public / Anonymous) - no Cancel
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Public", callback_data=f"MODE_PUBLIC|{job_id}")],
        [InlineKeyboardButton("🔒 Anonymous", callback_data=f"MODE_ANON|{job_id}")]
    ])
    context.bot.send_message(chat_id=user.id, text="Choose poll type:", reply_markup=keyboard)
    # also inform minor warnings/errors to owner privately
    if parsed.get("errors"):
        errs = "\n".join(parsed["errors"])
        context.bot.send_message(chat_id=user.id, text=f"⚠️ Parsing errors (please check format):\n{errs}")
    if parsed.get("warnings"):
        warns = "\n".join(parsed["warnings"])
        context.bot.send_message(chat_id=user.id, text=f"ℹ️ Warnings:\n{warns}")

def callback_mode_handler(update: Update, context):
    query = update.callback_query
    data = query.data or ""
    # expected: MODE_PUBLIC|<job_id> or MODE_ANON|<job_id>
    if not data.startswith("MODE_"):
        return
    _, mode, job_id = None, None, None
    try:
        prefix, rest = data.split("_", 1)
        # rest is like "PUBLIC|<jobid>" or "ANON|<jobid>"
        parts = rest.split("|", 1)
        mode = parts[0]
        job_id = parts[1] if len(parts) > 1 else None
    except Exception:
        query.answer("Invalid action")
        return

    owner_id = query.from_user.id
    job = db.get_job(job_id)
    if not job or job["owner_id"] != owner_id:
        query.answer("Job not found or expired.")
        return

    # update job mode and status waiting_target
    db.update_job_status(job_id, "waiting_target", mode=mode)
    # show target chat buttons (no cancel)
    if not worker.TARGET_CHATS:
        query.edit_message_text("No target chats configured. Set TARGET_CHATS env.")
        return
    kb = []
    for c in worker.TARGET_CHATS:
        kb.append([InlineKeyboardButton(c["name"], callback_data=f"CHAT|{job_id}|{c['id']}")])
    reply_markup = InlineKeyboardMarkup(kb)
    query.edit_message_text(text="Choose target chat:", reply_markup=reply_markup)

def callback_chat_handler(update: Update, context):
    query = update.callback_query
    data = query.data or ""
    # expected format: CHAT|<job_id>|<chat_id>
    if not data.startswith("CHAT|"):
        return
    try:
        _, job_id, chat_id_str = data.split("|", 2)
        chat_id = int(chat_id_str)
    except Exception:
        query.answer("Invalid chat selection")
        return

    owner_id = query.from_user.id
    job = db.get_job(job_id)
    if not job or job["owner_id"] != owner_id:
        query.answer("Job not found or expired.")
        return

    # Update status to posting to avoid re-use
    db.update_job_status(job_id, "posting", mode=job.get("mode"))
    # Acknowledge owner quickly
    query.edit_message_text(text="Posting — starting now. You will receive a single confirmation when done.")

    # Now run posting in a background thread
    def do_post():
        try:
            payload = job["payload"]
            events = payload.get("events", [])
            mode = job.get("mode", "PUBLIC")
            is_anonymous = (mode == "ANON")
            success = worker.post_quiz_events(context.bot, chat_id, events, is_anonymous, owner_id=owner_id, chat_name=None)
            # final confirmation: only one message as you requested
            chat_name = None
            for c in worker.TARGET_CHATS:
                if c["id"] == chat_id:
                    chat_name = c["name"]
                    break
            if chat_name is None:
                chat_name = str(chat_id)
            if success:
                try:
                    context.bot.send_message(owner_id, f"✅ {len([e for e in events if e['type']=='question'])} quiz(es) sent successfully to {chat_name} 🎉")
                except Exception:
                    logger.exception("Failed to send confirmation to owner.")
            else:
                try:
                    context.bot.send_message(owner_id, f"❌ Posting failed for job {job_id}. Check logs.")
                except Exception:
                    logger.exception("Failed to notify owner about failure.")
        except Exception:
            logger.exception("Unhandled error during posting for job %s", job_id)
        finally:
            # delete job row to avoid re-use
            try:
                db.delete_job(job_id)
            except Exception:
                logger.exception("Failed to delete job row %s", job_id)

    t = threading.Thread(target=do_post, daemon=True)
    t.start()

# Handlers
dispatcher.add_handler(CommandHandler("start", start_cmd))
dispatcher.add_handler(CommandHandler("help", help_cmd))
dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), owner_text_handler))
dispatcher.add_handler(CallbackQueryHandler(callback_mode_handler, pattern=r'^MODE_'))
dispatcher.add_handler(CallbackQueryHandler(callback_chat_handler, pattern=r'^CHAT\|'))

def main():
    # Start Flask in background thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    # Start polling (do not call idle in worker thread to avoid signal issues)
    updater.start_polling(poll_interval=1.0, timeout=20)

    # heartbeat and keep process alive
    try:
        while True:
            ts = str(int(time.time()))
            db.set_meta("last_heartbeat", ts)
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; stopping.")
        updater.stop()
    except Exception:
        logger.exception("Main loop exception; stopping.")
        updater.stop()

if __name__ == "__main__":
    main()
