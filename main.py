# main.py
import os
import time
import json
import logging
import threading
from typing import Optional, List, Tuple, Dict, Any

from flask import Flask, jsonify
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler, CallbackContext

import db
import worker  # your worker module that actually posts polls

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# Config
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

# SUDO / owners
def parse_sudo_users() -> List[int]:
    vals = os.environ.get("SUDO_USERS", "")
    res = []
    for part in vals.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            res.append(int(part))
        except:
            logger.warning("Invalid SUDO_USERS entry ignored: %s", part)
    return res

SUDO_USERS = parse_sudo_users()

# Target chats
def parse_target_chats() -> List[Tuple[str, int]]:
    raw = os.environ.get("TARGET_CHATS", "")
    out = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if ":" in token:
            name, idstr = token.split(":", 1)
            try:
                out.append((name.strip(), int(idstr.strip())))
            except:
                logger.warning("Invalid TARGET_CHAT entry ignored: %s", token)
        else:
            # fallback try parse as number-only
            try:
                out.append((token, int(token)))
            except:
                logger.warning("Invalid TARGET_CHAT entry ignored: %s", token)
    return out

TARGET_CHATS = parse_target_chats()

# DB init (uses DB_PATH from env in db.py)
db.init_db()

# Flask health app (run in background thread)
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

def is_owner(user_id: int) -> bool:
    return int(user_id) in SUDO_USERS

# Helpers for job storage
def make_job_id() -> str:
    return f"pending:{int(time.time()*1000)}"

def save_pending_job(job_id: str, owner_id: int, raw_text: str, expires_in: int = 3600):
    payload = {"text": raw_text}
    expires_at = int(time.time()) + expires_in
    db.save_job_row(job_id=job_id, owner_id=owner_id, payload=payload, status="waiting_target", expires_at=expires_at)

def get_job_row(job_id: str) -> Optional[Dict[str, Any]]:
    return db.get_job(job_id)

# UI builders
def build_mode_keyboard(job_id: str):
    keyboard = [
        [InlineKeyboardButton("Public", callback_data=f"mode:{job_id}:public")],
        [InlineKeyboardButton("Anonymous", callback_data=f"mode:{job_id}:anonymous")],
    ]
    return InlineKeyboardMarkup(keyboard)

def build_chat_keyboard(job_id: str, mode: str):
    kb = []
    for name, cid in TARGET_CHATS:
        # keep callback_data short: include chat id directly
        kb.append([InlineKeyboardButton(name, callback_data=f"selectchat:{job_id}:{mode}:{cid}")])
    return InlineKeyboardMarkup(kb)

# /start handler
def start_cmd(update: Update, context: CallbackContext):
    uid = update.effective_user.id if update.effective_user else None
    if uid and is_owner(uid):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")

def help_cmd(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id, text="Send formatted quiz text (owner only).")

# Owner sends formatted text -> validate minimal (we keep validator light in worker or external), but we only queue job
def owner_message_handler(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        logger.info("Ignored message from non-owner: %s", uid)
        return

    text = (update.effective_message.text or "").strip()
    if not text:
        context.bot.send_message(chat_id=uid, text="Empty message.")
        return

    # Create pending job
    job_id = make_job_id()
    save_pending_job(job_id, uid, text)
    # Send mode selection (Public/Anonymous) to owner
    context.bot.send_message(chat_id=uid, text="Choose poll type:", reply_markup=build_mode_keyboard(job_id))
    # no other progress messages

# Callback for mode buttons
def mode_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    user = query.from_user
    query.answer()
    data = query.data or ""
    parts = data.split(":", 2)
    if len(parts) != 3:
        query.edit_message_text("Invalid action.")
        return
    _, job_id, mode = parts

    # owner check
    if not is_owner(user.id):
        query.edit_message_text("Unauthorized.")
        return

    # build chat selection keyboard (no Cancel)
    if not TARGET_CHATS:
        query.edit_message_text("No target chats configured. Set TARGET_CHATS env.")
        return

    query.edit_message_text("Choose which chat to post to:", reply_markup=build_chat_keyboard(job_id, mode))

# Callback for chat selection
def chat_select_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return
    user = query.from_user
    query.answer()
    data = query.data or ""
    parts = data.split(":", 3)
    if len(parts) != 4:
        query.edit_message_text("Invalid action.")
        return
    _, job_id, mode, chat_id_raw = parts
    try:
        chat_id = int(chat_id_raw)
    except:
        query.edit_message_text("Invalid chat id.")
        return

    if not is_owner(user.id):
        query.edit_message_text("Unauthorized.")
        return

    job = get_job_row(job_id)
    if not job:
        query.edit_message_text("Job not found or expired.")
        return

    # update job to queued with mode
    try:
        db.update_job_status(job_id, "queued", mode)
    except Exception:
        logger.exception("Failed update job status.")

    # run posting in background thread
    def _run_post():
        try:
            payload = job.get("payload")
            raw_text = payload.get("text") if isinstance(payload, dict) else payload
            # Let worker parse text (validator) and post. Worker should:
            # - parse DES blocks and Q blocks in order
            # - send DES messages where they appear (before/after polls according to order)
            # - send polls with correct options and set explanation (EXP) to add explanation
            # - respect anonymous flag
            parsed_ok = None
            try:
                # if worker provides a helper parse function, use it; else worker is expected to handle raw_text
                # Try to call worker.post_quiz_questions with parsed questions if available
                # We'll call worker.post_quiz_questions(bot, chat_id, des, questions_input, owner_id, anonymous)
                # For backward compatibility, pass raw_text and let worker decide.
                anonymous_flag = (mode == "anonymous")
                worker.post_quiz_questions(context.bot, chat_id, raw_text, owner_id=user.id, anonymous=anonymous_flag)
            except TypeError:
                # fallback: older worker signature: post_quiz_questions(bot, chat_id, des, questions_input, owner_id, anonymous)
                # try to parse minimally here (very simple): NOT ideal; prefer worker to accept raw_text
                logger.exception("Worker.post_quiz_questions signature mismatch. Please update worker to accept raw_text.")
                context.bot.send_message(user.id, "Internal worker signature error. See logs.")
                return

            # mark done
            db.update_job_status(job_id, "done", mode)
            # send single confirmation to owner with human-friendly chat name
            chat_name = None
            for name, cid in TARGET_CHATS:
                if cid == chat_id:
                    chat_name = name
                    break
            chat_display = chat_name if chat_name else str(chat_id)
            try:
                context.bot.send_message(user.id, f"✅ 1 quiz(es) sent successfully to {chat_display} 🎉")
            except Exception:
                logger.exception("Failed to notify owner on success.")
        except Exception:
            logger.exception("Uncaught error in job posting thread")
            try:
                context.bot.send_message(user.id, "❌ Failed to post quiz. See logs.")
            except:
                pass

    threading.Thread(target=_run_post, daemon=True).start()
    # edit message to acknowledge immediate action (owner will get final confirmation later)
    query.edit_message_text("Queued and posting to selected chat. You will be notified on completion.")

# Generic text handler for owner for other flows (not used heavily)
def owner_text_general(update: Update, context: CallbackContext):
    if update.effective_user is None:
        return
    uid = update.effective_user.id
    if not is_owner(uid):
        return
    text = (update.effective_message.text or "").strip()
    # If text looks like an integer and there's a pending job, let owner use it as quick chat id (legacy)
    if text.isdigit():
        # find latest waiting_target job
        try:
            conn_job = None
            # scan jobs table for status waiting_target
            # We rely on db.get_job to fetch by id; to keep simple, we won't implement heavy queries here.
            context.bot.send_message(uid, "Submitting chat id directly is unsupported in this version. Use the inline buttons shown after sending quiz text.")
        except Exception:
            logger.exception("Error while handling owner_text_general")
            context.bot.send_message(uid, "Error processing request.")
        return
    # default help
    context.bot.send_message(uid, "Send formatted quiz text to start (you will get Public/Anonymous options).")

def setup_dispatcher(updater: Updater):
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))
    # Owner message: any text message from sudo users is treated as formatted quiz input
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=SUDO_USERS), owner_message_handler))
    # fallback owner handler
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=SUDO_USERS), owner_text_general))
    # Callback handlers
    dp.add_handler(CallbackQueryHandler(mode_callback, pattern=r'^mode:'))
    dp.add_handler(CallbackQueryHandler(chat_select_callback, pattern=r'^selectchat:'))
    return dp

def start_polling_and_heartbeat():
    # Updater pattern: start_polling in main thread so signal works correctly on platforms like Render.
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    setup_dispatcher(updater)
    logger.info("Starting polling (drop_pending_updates=True)...")
    # drop pending updates on start to avoid "Conflict" with previous instances sometimes
    updater.start_polling(poll_interval=1.0, timeout=20, clean=True)
    # run flask in background
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask health endpoint started in background.")
    try:
        # Heartbeat loop: also keep process alive and update DB meta
        while True:
            db.set_meta("last_heartbeat", str(int(time.time())))
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, stopping updater...")
        updater.stop()
        updater.is_idle = False
    except Exception:
        logger.exception("Main loop exception; shutting down updater.")
        try:
            updater.stop()
        except:
            pass

if __name__ == "__main__":
    start_polling_and_heartbeat()
