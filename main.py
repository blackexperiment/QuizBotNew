# main.py
import os
import logging
import threading
import time
import uuid
import json

from flask import Flask, jsonify
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler

import db
from validator import validate_and_parse
import worker

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# load env
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN is required")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

SUDO_USERS = os.environ.get("SUDO_USERS", "")
SUDO_USERS_SET = set()
for s in [x.strip() for x in SUDO_USERS.split(",") if x.strip()]:
    try:
        SUDO_USERS_SET.add(int(s))
    except Exception:
        pass

TARGET_CHATS_RAW = os.environ.get("TARGET_CHATS", "")
def parse_target_chats(env_value: str):
    pairs = {}
    for part in env_value.split(","):
        if ":" not in part:
            continue
        name, cid = part.split(":", 1)
        try:
            pairs[name.strip()] = int(cid.strip())
        except Exception:
            continue
    return pairs

TARGET_CHATS_MAP = parse_target_chats(TARGET_CHATS_RAW)

DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")
# initialize DB
db.init_db()

app = Flask(__name__)

@app.route("/")
def index():
    return "OK", 200

@app.route("/health")
def health():
    hb = db.get_meta("last_heartbeat") or ""
    return jsonify({"status": "ok", "last_heartbeat": hb}), 200

bot = Bot(token=TELEGRAM_BOT_TOKEN)

def is_owner(user_id: int) -> bool:
    return int(user_id) in SUDO_USERS_SET

# /start
def start_cmd(update: Update, context: CallbackContext):
    user = update.effective_user
    if user and is_owner(user.id):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")

def help_cmd(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id, text="Send formatted quiz text (owner only).")

# owner message handler - receives raw formatted payload
def owner_only_message_handler(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        logger.info("Ignored message from non-owner: %s", uid)
        return

    text = update.effective_message.text or ""
    parsed = validate_and_parse(text)
    if not parsed.get("ok", False):
        errs = "\n".join(parsed.get("errors", [])) or "Unknown format error."
        warns = "\n".join(parsed.get("warnings", []))
        reply = f"⚠️ Format errors:\n{errs}"
        if warns:
            reply += f"\n⚠️ Warnings:\n{warns}"
        context.bot.send_message(chat_id=uid, text=reply)
        return

    # Good: store parsed into job
    job_id = str(uuid.uuid4())
    now = int(time.time())
    expires = now + 60 * 60  # 1 hour TTL
    # store the parsed object (it contains questions, des_list etc.)
    db.save_job_row(job_id=job_id, owner_id=uid, payload=parsed, status="pending_mode", expires_at=expires)

    # Show mode selection (Public / Anonymous)
    kb = [
        [InlineKeyboardButton("Public", callback_data=f"MODE|{job_id}|public"),
         InlineKeyboardButton("Anonymous", callback_data=f"MODE|{job_id}|anonymous")]
    ]
    context.bot.send_message(chat_id=uid, text="Choose poll type:", reply_markup=InlineKeyboardMarkup(kb))

# callback: mode chosen
def handle_mode_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    query.answer()
    data = query.data or ""
    parts = data.split("|")
    if len(parts) < 3:
        query.edit_message_text("Invalid action.")
        return
    _, job_id, mode = parts[0], parts[1], parts[2]
    if query.from_user.id is None:
        query.edit_message_text("Invalid user.")
        return

    job = db.get_job(job_id)
    if not job:
        query.edit_message_text("❗ Job not found or expired. Please resend your quiz.")
        return
    if job["owner_id"] != query.from_user.id:
        query.edit_message_text("Not authorized to operate this job.")
        return
    # update job
    db.update_job_status(job_id, "pending_chat", mode=mode)

    # prepare chat selection buttons from TARGET_CHATS_MAP
    kb = []
    for name in TARGET_CHATS_MAP.keys():
        kb.append([InlineKeyboardButton(name, callback_data=f"POST|{job_id}|{name}")])
    # also allow numeric manual chat id entry? For simplicity, only configured target chats are offered.
    kb.append([InlineKeyboardButton("Cancel", callback_data=f"CANCEL|{job_id}")])
    query.edit_message_text("Choose channel/group where you want to post this quiz 📨", reply_markup=InlineKeyboardMarkup(kb))

# callback: cancel
def handle_cancel(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    query.answer()
    parts = query.data.split("|", 1)
    if len(parts) < 2:
        query.edit_message_text("Invalid action.")
        return
    job_id = parts[1]
    job = db.get_job(job_id)
    if job and job["owner_id"] == query.from_user.id:
        db.update_job_status(job_id, "cancelled")
        query.edit_message_text("Job cancelled.")
    else:
        query.edit_message_text("Job not found or you are not authorized.")

# callback: POST -> chosen chat
def handle_post_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    query.answer()
    data = query.data or ""
    parts = data.split("|", 2)
    if len(parts) < 3:
        query.edit_message_text("Invalid action.")
        return
    _, job_id, chat_name = parts
    job = db.get_job(job_id)
    if not job:
        query.edit_message_text("❗ Job not found or expired. Please resend your quiz.")
        return
    if job["owner_id"] != query.from_user.id:
        query.edit_message_text("Not authorized to operate this job.")
        return

    # map chat_name to id
    if chat_name not in TARGET_CHATS_MAP:
        query.edit_message_text("Selected chat not found in configuration.")
        return
    chat_id = TARGET_CHATS_MAP[chat_name]

    # mark queued
    db.update_job_status(job_id, "queued", mode=job.get("mode"))

    # run posting in background
    def do_post():
        parsed_payload = job["payload"]  # contains questions and des_list
        # worker expects a wrapper with questions and des_list possibly; we pass wrapper directly
        try:
            success = worker.post_quiz_questions(context.bot, chat_id, parsed_payload.get("des"), parsed_payload, owner_id=job["owner_id"], mode=job.get("mode") or "public")
            if success:
                db.update_job_status(job_id, "done")
            else:
                db.update_job_status(job_id, "failed")
        except Exception:
            logger.exception("Posting thread crashed")
            db.update_job_status(job_id, "failed")

    threading.Thread(target=do_post, daemon=True).start()
    query.edit_message_text(f"Queued posting to {chat_name}. You will be notified when complete.")

def owner_text_general(update: Update, context: CallbackContext):
    # fallback messages by owner - keep minimal
    uid = update.effective_user.id if update.effective_user else None
    if not uid or not is_owner(uid):
        return
    context.bot.send_message(uid, "Send formatted quiz text to start. After sending you'll be asked to choose Public/Anonymous and target chat.")

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

def main():
    # Start flask in background
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    # register handlers
    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS_SET)), owner_only_message_handler))
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS_SET)), owner_text_general))
    dp.add_handler(CallbackQueryHandler(handle_mode_callback, pattern=r"^MODE\|"))
    dp.add_handler(CallbackQueryHandler(handle_post_callback, pattern=r"^POST\|"))
    dp.add_handler(CallbackQueryHandler(handle_cancel, pattern=r"^CANCEL\|"))

    # start polling (non-blocking). Do NOT call updater.idle() from a thread.
    updater.start_polling()
    logger.info("Polling started")

    try:
        while True:
            db.set_meta("last_heartbeat", str(int(time.time())))
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt — stopping updater")
        updater.stop()
    except Exception:
        logger.exception("Main loop exception — stopping updater")
        updater.stop()

if __name__ == "__main__":
    main()
