# main.py
import os
import logging
import threading
import time
from typing import List, Dict, Any

from flask import Flask, jsonify
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler, CallbackContext

import db
import worker
from validator import validate_and_parse

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# Config
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

SUDO_USERS = os.environ.get("SUDO_USERS", "")
SUDO_USERS = [int(x.strip()) for x in SUDO_USERS.split(",") if x.strip().isdigit()]

TARGET_CHATS_ENV = os.environ.get("TARGET_CHATS", "")  # format: Name:id,Name:id
TARGET_CHAT_MAP = {}
for pair in [p.strip() for p in TARGET_CHATS_ENV.split(",") if p.strip()]:
    if ":" in pair:
        name, cid = pair.split(":", 1)
        try:
            TARGET_CHAT_MAP[name.strip()] = int(cid.strip())
        except Exception:
            continue

# DB init
db.init_db()

# Flask app for health check
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

# Telegram bot
bot = Bot(token=TELEGRAM_BOT_TOKEN)

def is_sudo(uid: int) -> bool:
    return int(uid) in SUDO_USERS

def start_cmd(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if is_sudo(uid):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")

def help_cmd(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id, text="Send formatted quiz text (owner only).")

# store pending job payloads in DB table 'jobs' (simple)
def enqueue_pending_job(owner_id: int, payload: str):
    job_id = "pending:" + str(int(time.time() * 1000))
    db.save_job(job_id=job_id, payload=payload, status="waiting_target")
    return job_id

def parse_and_request_target(update: Update, context: CallbackContext, payload: str):
    uid = update.effective_user.id
    res = validate_and_parse(payload)
    if not res["ok"]:
        errs = "\n".join(res.get("errors", []))
        warns = "\n".join(res.get("warnings", []))
        reply = f"⚠️ Format errors:\n{errs}"
        if warns:
            reply += f"\n⚠️ Warnings:\n{warns}"
        context.bot.send_message(chat_id=uid, text=reply)
        return

    # Save job and ask for poll type (Anonymous / Public) via inline keyboard
    job_id = enqueue_pending_job(uid, payload)
    kb = [
        [InlineKeyboardButton("Anonymous", callback_data=f"polltype:{job_id}:anon"),
         InlineKeyboardButton("Public", callback_data=f"polltype:{job_id}:public")]
    ]
    context.bot.send_message(chat_id=uid, text="Choose poll privacy: Anonymous or Public", reply_markup=InlineKeyboardMarkup(kb))

def owner_message_handler(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_sudo(uid):
        logger.info("Ignored message from non-sudo %s", uid)
        return
    text = (update.effective_message.text or "").strip()
    if not text:
        return
    # if message contains Q: assume it's a quiz payload
    if "Q:" in text:
        parse_and_request_target(update, context, text)
        return
    # If message looks like a chat id reply to pending job, process here (fallback)
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        # find latest waiting_target job
        row = db.get_latest_job_with_status("waiting_target")
        if not row:
            context.bot.send_message(chat_id=uid, text="Job not found or expired.")
            return
        job_payload = row["payload"]
        db.update_job_status(row["id"], "queued")
        # do immediate posting using default poll type public
        post_job_from_payload(context.bot, int(text), job_payload, uid, poll_type="public")
        context.bot.send_message(chat_id=uid, text=f"✅ Job queued and will be posted to {text}.")
        return
    context.bot.send_message(chat_id=uid, text="Send preformatted quiz text (contains Q:) or a chat id to post saved job.")

def post_job_from_payload(bot_obj: Bot, chat_id: int, payload: str, owner_id: int = None, poll_type: str = "public") -> bool:
    """
    Validate and post: will send DES messages in-place based on des_list question_index positioning.
    """
    res = validate_and_parse(payload)
    if not res["ok"]:
        # notify owner if provided
        if owner_id:
            bot_obj.send_message(owner_id, "Job validation failed. See errors:\n" + "\n".join(res.get("errors", [])))
        return False

    questions = res["questions"]
    des_list = res.get("des_list", [])  # list of dicts with question_index
    # build mapping from question index -> list of DES texts
    des_map = {}
    for d in des_list:
        des_map.setdefault(d["question_index"], []).append(d["text"])

    # determine poll options for privacy
    is_anonymous = poll_type == "anon"

    # iterate questions and post
    for i, q in enumerate(questions):
        # send any DESs that belong before this question (position == i)
        for dt in des_map.get(i, []):
            try:
                bot_obj.send_message(chat_id=chat_id, text=dt)
            except Exception:
                logger.exception("Failed to send DES before question %s", i+1)

        # create options list in alphabetical order of labels
        labels = sorted(q["options"].keys())
        options = [q["options"][lbl] for lbl in labels]
        # send poll
        try:
            message = bot_obj.send_poll(chat_id=chat_id, question=q["raw_question"], options=options,
                                        is_anonymous=is_anonymous, type="quiz", correct_option_id=labels.index(q["ans"]))
            time.sleep(0.5)  # tiny spacing
            # add explanation if exists (Telegram Bot API doesn't support sending explanation on quiz creation,
            # but python-telegram-bot supports editing message to set explanation? Usually explanation is part of Quiz result message.
            # We'll skip complicated edit and instead send explanation as a reply immediately (safe).
            if q.get("exp"):
                try:
                    bot_obj.send_message(chat_id=chat_id, text=f"Explanation: {q['exp']}", reply_to_message_id=message.message_id)
                except Exception:
                    logger.exception("Failed to send EXP for question %s", i+1)
        except Exception as e:
            logger.exception("Network issue posting Q%s. Aborting job.", i+1)
            # notify owner and abort if configured - here we abort and inform owner
            if owner_id:
                try:
                    bot_obj.send_message(owner_id, f"❌ Network issue posting Q{i+1}. Aborting.")
                except Exception:
                    logger.exception("Failed to notify owner about abort.")
            return False

    # after all questions, send DES entries that were after last question (index == len(questions))
    for dt in des_map.get(len(questions), []):
        try:
            bot_obj.send_message(chat_id=chat_id, text=dt)
        except Exception:
            logger.exception("Failed to send DES after all questions.")

    # success notify owner
    if owner_id:
        try:
            # translate chat_id to name if provided in TARGET_CHAT_MAP
            friendly = None
            for nm, cid in TARGET_CHAT_MAP.items():
                if cid == chat_id:
                    friendly = nm
                    break
            dest_name = friendly if friendly else str(chat_id)
            bot_obj.send_message(owner_id, f"✅ {len(questions)} quiz(es) sent successfully to {dest_name} 🎉")
        except Exception:
            logger.exception("Failed to notify owner on success.")
    return True

# Callback query handler for polltype selection -> ask for chat selection next
def callback_polltype(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    payload = query.data  # format: polltype:<job_id>:anon|public
    try:
        _, job_id, polltype = payload.split(":", 2)
    except Exception:
        query.answer("Invalid action")
        return
    uid = query.from_user.id
    if not is_sudo(uid):
        query.answer("Not authorized")
        return
    # Build chat selection keyboard from TARGET_CHAT_MAP
    kb = []
    for name, cid in TARGET_CHAT_MAP.items():
        kb.append([InlineKeyboardButton(name, callback_data=f"postjob:{job_id}:{polltype}:{cid}")])
    # If no target chats in env, allow manual chat id entry instruction
    if not kb:
        query.edit_message_text("No preset target chats. Reply with chat id (e.g. -100123...) to post.")
        return
    query.edit_message_text("Choose channel/group where you want to post this quiz 📨", reply_markup=InlineKeyboardMarkup(kb))

# Callback to actually post job (postjob:<job_id>:<polltype>:<chatid>)
def callback_postjob(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    try:
        _, job_id, polltype, chatid = query.data.split(":", 3)
        chatid = int(chatid)
    except Exception:
        query.answer("Invalid action")
        return
    uid = query.from_user.id
    if not is_sudo(uid):
        query.answer("Not authorized")
        return
    # fetch pending job payload
    row = db.get_job(job_id)
    if not row or row["status"] != "waiting_target":
        query.edit_message_text("Job not found or expired.")
        return
    payload = row["payload"]
    # mark queued
    db.update_job_status(job_id, "queued")
    query.edit_message_text("Posting quiz now...")

    # post in background so callback returns quickly
    def bg():
        ok = post_job_from_payload(bot, chatid, payload, owner_id=uid, poll_type=("anon" if polltype == "anon" else "public"))
        if ok:
            # mark job done
            db.update_job_status(job_id, "done")
        else:
            db.update_job_status(job_id, "failed")
    threading.Thread(target=bg, daemon=True).start()

# Start bot: **polling must run in main thread** to avoid signal error
def main():
    # start flask in background
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))
    dp.add_handler(CallbackQueryHandler(callback_polltype, pattern=r'^polltype:'))
    dp.add_handler(CallbackQueryHandler(callback_postjob, pattern=r'^postjob:'))

    # message handlers
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=SUDO_USERS) & Filters.regex(r'Q:'), owner_message_handler))
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=SUDO_USERS), owner_message_handler))

    # Start polling in main thread (so updater.idle() uses signal in main)
    try:
        logger.info("Starting polling in main thread...")
        updater.start_polling(poll_interval=1.0, timeout=20)
        # heartbeat loop while polling - this code runs while updater is running
        while True:
            db.set_meta("last_heartbeat", str(int(time.time())))
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, stopping")
    except Exception as e:
        logger.exception("Unhandled exception in main: %s", e)
    finally:
        try:
            updater.stop()
        except Exception:
            pass

if __name__ == "__main__":
    main()
