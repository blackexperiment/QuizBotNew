# main.py
import os
import json
import logging
import threading
import time
from typing import Dict, Any, Optional

from flask import Flask, jsonify
from telegram import (
    Bot,
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackQueryHandler,
    CallbackContext,
)

# use your validator (the permissive sequence-based one)
from validator import validate_and_parse
import worker  # worker.py provided below

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# Config from env
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

SUDO_USERS = os.environ.get("SUDO_USERS", "")
SUDO_USERS = [int(x.strip()) for x in SUDO_USERS.split(",") if x.strip().isdigit()]

TARGET_CHATS_RAW = os.environ.get("TARGET_CHATS", "")
# parse format Name:ID,Name2:ID2
TARGET_CHATS: Dict[str, int] = {}
for part in [p.strip() for p in TARGET_CHATS_RAW.split(",") if p.strip()]:
    if ":" in part:
        name, idpart = part.split(":", 1)
        try:
            TARGET_CHATS[name.strip()] = int(idpart.strip())
        except Exception:
            logger.warning("Ignoring invalid TARGET_CHATS entry: %r", part)

POLL_DELAY_SHORT = int(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = int(os.environ.get("POLL_DELAY_LONG", "2"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))

DB_PATH = os.environ.get("DB_PATH", "./quizbot.json")  # used for simple job stash persistence

# persist small job stash so owner can click callback after short redeploys
JOB_STASH_PATH = os.path.join(os.path.dirname(DB_PATH), "job_stash.json")

def load_job_stash() -> Dict[str, Any]:
    try:
        if os.path.exists(JOB_STASH_PATH):
            with open(JOB_STASH_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        logger.exception("Failed to load job stash")
    return {}

def save_job_stash(stash: Dict[str, Any]):
    try:
        os.makedirs(os.path.dirname(JOB_STASH_PATH), exist_ok=True)
        with open(JOB_STASH_PATH, "w", encoding="utf-8") as f:
            json.dump(stash, f)
    except Exception:
        logger.exception("Failed to save job stash")

JOB_STASH = load_job_stash()  # key -> payload dict

# Flask app for health (so Render can probe)
app = Flask(__name__)

@app.route("/")
def index():
    return "OK", 200

@app.route("/health")
def health():
    return jsonify({"status": "ok", "jobs_cached": len(JOB_STASH)}), 200

# Telegram init
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# helper
def is_sudo(user_id: int) -> bool:
    return int(user_id) in SUDO_USERS

def start_handler(update: Update, context: CallbackContext):
    user = update.effective_user
    if user and is_sudo(user.id):
        text = "🛡️ BLACK RHINO CONTROL PANEL\n\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
    else:
        text = "🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner."
    context.bot.send_message(chat_id=update.effective_chat.id, text=text)

def help_handler(update: Update, context: CallbackContext):
    update.message.reply_text("Send formatted quiz text (owner only).")

# owner sends formatted quiz text — validate and present Public / Anonymous inline buttons
def formatted_quiz_received(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or not is_sudo(user.id):
        logger.info("Non-sudo tried to send formatted quiz: %s", user and user.id)
        return

    raw_text = update.message.text or ""
    res = validate_and_parse(raw_text)
    if not res.get("ok"):
        errs = "\n".join(res.get("errors", [])) or "Unknown format error."
        warns = "\n".join(res.get("warnings", []))
        msg = f"⚠️ Format errors:\n{errs}"
        if warns:
            msg += f"\n\n⚠️ Warnings:\n{warns}"
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
        return

    # create a job id and stash the sequence + meta for callback flow
    job_id = str(int(time.time() * 1000))
    JOB_STASH[job_id] = {
        "owner_id": user.id,
        "raw_text": raw_text,
        "sequence": res.get("sequence", []),
        "questions": res.get("questions", []),
    }
    save_job_stash(JOB_STASH)

    # Present two inline buttons: Public, Anonymous (no Cancel)
    kb = [
        [InlineKeyboardButton("Public ✅", callback_data=f"publish|{job_id}|public"),
         InlineKeyboardButton("Anonymous 🔒", callback_data=f"publish|{job_id}|anonymous")]
    ]
    reply_markup = InlineKeyboardMarkup(kb)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Choose poll type — Public or Anonymous (no cancel).",
                             reply_markup=reply_markup)

# callback when owner chooses Public/Anonymous
def cb_publish_choice(update: Update, context: CallbackContext):
    cq = update.callback_query
    data = cq.data or ""
    # data format: publish|<job_id>|public|anonymous
    parts = data.split("|")
    if len(parts) != 3:
        cq.answer("Invalid action")
        return
    _, job_id, ptype = parts
    job = JOB_STASH.get(job_id)
    if not job:
        cq.edit_message_text("Job not found or expired. Please resend the formatted quiz.")
        return
    owner_id = job["owner_id"]
    if update.effective_user.id != owner_id:
        cq.answer("Not authorized", show_alert=True)
        return

    # Build chat selection inline buttons from TARGET_CHATS
    kb = []
    # one button per chat; callback_data: selectchat|<job_id>|<ptype>|<chatkey>
    for name, cid in TARGET_CHATS.items():
        cb = f"selectchat|{job_id}|{ptype}|{name}"
        kb.append([InlineKeyboardButton(text=name, callback_data=cb)])
    if not kb:
        cq.edit_message_text("No target chats configured. Set TARGET_CHATS env like Name:-100123...,Name2:-100...")
        return

    cq.edit_message_text(f"Selected: {ptype.capitalize()}. Now choose target chat:", reply_markup=InlineKeyboardMarkup(kb))

# callback when owner picks chat — start posting job (no further confirmations)
def cb_select_chat(update: Update, context: CallbackContext):
    cq = update.callback_query
    data = cq.data or ""
    # data: selectchat|<job_id>|<ptype>|<chatname>
    parts = data.split("|", 3)
    if len(parts) != 4:
        cq.answer("Invalid action")
        return
    _, job_id, ptype, chatname = parts
    job = JOB_STASH.get(job_id)
    if not job:
        cq.edit_message_text("Job not found or expired. Please resend the formatted quiz.")
        return
    owner_id = job["owner_id"]
    if update.effective_user.id != owner_id:
        cq.answer("Not authorized", show_alert=True)
        return

    # find chat id
    if chatname not in TARGET_CHATS:
        cq.answer("Chat not available", show_alert=True)
        return
    chat_id = TARGET_CHATS[chatname]
    is_anonymous = True if ptype == "anonymous" else False

    # acknowledge to owner and start background posting
    try:
        cq.edit_message_text(f"Queued: {len(job.get('questions', []))} question(s). Posting to *{chatname}* …",
                             parse_mode="Markdown")
    except Exception:
        try:
            cq.answer("Queued. Starting...")
        except Exception:
            pass

    # start worker in background thread
    def run_post():
        try:
            seq = job.get("sequence", [])
            # call worker to post sequence into chat_id
            success = worker.post_quiz_sequence(bot=bot,
                                                chat_id=chat_id,
                                                sequence=seq,
                                                anonymous=is_anonymous,
                                                owner_id=owner_id,
                                                chat_name=chatname,
                                                poll_delay_short=POLL_DELAY_SHORT,
                                                poll_delay_long=POLL_DELAY_LONG,
                                                max_retries=MAX_RETRIES,
                                                sequential_fail_abort=SEQUENTIAL_FAIL_ABORT)
            # notify owner on result (worker already notifies on failures; still do final notify)
            if success:
                try:
                    bot.send_message(owner_id, f"✅ {len([x for x in seq if x.get('type')=='question'])} quiz(es) sent successfully to {chatname} 🎉")
                except Exception:
                    logger.exception("Failed to send final success msg to owner")
            else:
                try:
                    bot.send_message(owner_id, f"⚠️ Job failed while posting to {chatname}. Check logs.")
                except Exception:
                    logger.exception("Failed to send fail msg to owner")
        finally:
            # remove job stash entry to avoid duplicates
            try:
                JOB_STASH.pop(job_id, None)
                save_job_stash(JOB_STASH)
            except Exception:
                logger.exception("Failed to remove job stash entry")

    threading.Thread(target=run_post, daemon=True).start()

# optional: allow owner to list jobs (small utility)
def list_chats_cmd(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or not is_sudo(user.id):
        return
    if not TARGET_CHATS:
        update.message.reply_text("No TARGET_CHATS configured.")
        return
    msg = "Configured target chats:\n" + "\n".join([f"{n} -> {i}" for n, i in TARGET_CHATS.items()])
    update.message.reply_text(msg)

def unknown_text_fallback(update: Update, context: CallbackContext):
    # If owner sends raw chat id to reply to a stored job (legacy), we can accept it.
    user = update.effective_user
    if not user or not is_sudo(user.id):
        return
    text = (update.message.text or "").strip()
    # if looks like job id and 'jobs' in stash, skip. For simplicity we only accept formatted quiz texts via main handler.
    # If text seems numeric chat id and we have a single pending job for this owner, start posting to that chat.
    if text.startswith("-") and text.replace("-", "").isdigit():
        # find most recent job for owner
        owner_jobs = [(k, v) for k, v in JOB_STASH.items() if v.get("owner_id") == user.id]
        if owner_jobs:
            job_id = sorted(owner_jobs, key=lambda x: x[0])[-1][0]
            job = JOB_STASH[job_id]
            chat_id = int(text)
            cq = None
            # start posting in background: default to public
            threading.Thread(target=lambda: worker.post_quiz_sequence(bot, chat_id, job.get("sequence", []), False, user.id,
                                                                     chat_name=str(chat_id),
                                                                     poll_delay_short=POLL_DELAY_SHORT,
                                                                     poll_delay_long=POLL_DELAY_LONG,
                                                                     max_retries=MAX_RETRIES,
                                                                     sequential_fail_abort=SEQUENTIAL_FAIL_ABORT), daemon=True).start()
            update.message.reply_text(f"Queued job {job_id} to {chat_id}")
            JOB_STASH.pop(job_id, None)
            save_job_stash(JOB_STASH)
            return
    # else ignore or send help
    update.message.reply_text("Send formatted quiz text (owner only) or use /list_chats to view configured chat names.")

def main():
    # run flask in a thread
    flask_thread = threading.Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False, use_reloader=False), daemon=True)
    flask_thread.start()

    # ensure no webhook interfering
    try:
        bot.delete_webhook()
    except Exception:
        logger.debug("delete_webhook failed or not necessary")

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_handler))
    dp.add_handler(CommandHandler("help", help_handler))
    dp.add_handler(CommandHandler("list_chats", list_chats_cmd))

    # Message handler for formatted quiz (we assume presence of "Q:" is a good sign)
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=SUDO_USERS) & Filters.regex(r'Q:'), formatted_quiz_received))
    # fallback for any text from sudo users
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=SUDO_USERS), unknown_text_fallback))

    # Callback handlers
    dp.add_handler(CallbackQueryHandler(cb_publish_choice, pattern=r"^publish\|"))
    dp.add_handler(CallbackQueryHandler(cb_select_chat, pattern=r"^selectchat\|"))

    # Start polling in main thread (this avoids the signal-in-thread error)
    logger.info("Starting polling (main thread)...")
    updater.start_polling(poll_interval=1.0, timeout=20)
    updater.idle()
    logger.info("Updater finished. Exiting.")

if __name__ == "__main__":
    main()
