# main.py
import os
import logging
import threading
import time
import json
from typing import Optional, Dict, Any

from flask import Flask, jsonify
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler

# local modules (assumed to exist)
import db
import worker
from validator import validate_and_parse  # your validator (must follow requested contract)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# --- Config from env ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not set. Exiting.")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

# numeric owner(s)
SUDO_USERS = os.environ.get("SUDO_USERS", "")
SUDO_USERS_SET = set()
for x in [s.strip() for s in SUDO_USERS.split(",") if s.strip()]:
    try:
        SUDO_USERS_SET.add(int(x))
    except Exception:
        pass

# TARGET_CHATS format: Name:id,Name2:id2
TARGET_CHATS_RAW = os.environ.get("TARGET_CHATS", "")
TARGET_CHATS: Dict[str, int] = {}
for pair in [p.strip() for p in TARGET_CHATS_RAW.split(",") if p.strip()]:
    if ":" in pair:
        name, cid = pair.split(":", 1)
        name = name.strip()
        try:
            TARGET_CHATS[name] = int(cid.strip())
        except Exception:
            logger.warning("Invalid TARGET_CHATS id for %s -> %s", name, cid)

# timing and retry config
POLL_DELAY_SHORT = float(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = float(os.environ.get("POLL_DELAY_LONG", "2"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))

# initialize db (assumes db.init_db exists)
db.init_db()

# Flask health app
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
    return int(user_id) in SUDO_USERS_SET

# /start
def start_cmd(update: Update, context: CallbackContext):
    uid = update.effective_user.id if update.effective_user else None
    if uid and is_owner(uid):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")

def help_cmd(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Send formatted quiz text (owner only).")

# Helper to build inline keyboards
def kb_choose_anonymous(job_id: str):
    buttons = [
        InlineKeyboardButton("Anonymous ✅", callback_data=json.dumps({"act": "choose_type", "job": job_id, "anon": True})),
        InlineKeyboardButton("Public ✅", callback_data=json.dumps({"act": "choose_type", "job": job_id, "anon": False}))
    ]
    return InlineKeyboardMarkup([buttons])

def kb_choose_chat(job_id: str):
    # arrange one button per row with chat name as label and callback_data containing chat id
    rows = []
    for name, cid in TARGET_CHATS.items():
        rows.append([InlineKeyboardButton(name, callback_data=json.dumps({"act": "choose_chat", "job": job_id, "chat_id": int(cid)}))])
    return InlineKeyboardMarkup(rows)

# When owner sends formatted text
def owner_message_handler(update: Update, context: CallbackContext):
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

    # Validate input (validator should produce sequence ideally)
    res = validate_and_parse(text)
    if not isinstance(res, dict):
        context.bot.send_message(uid, "Validator returned invalid structure. See logs.")
        logger.error("Validator returned non-dict: %r", res)
        return

    if not res.get("ok", False):
        errs = "\n".join(res.get("errors", []))
        warns = "\n".join(res.get("warnings", []))
        reply = f"⚠️ Format errors:\n{errs}"
        if warns:
            reply += f"\n⚠️ Warnings:\n{warns}"
        context.bot.send_message(chat_id=uid, text=reply)
        return

    # Build a canonical sequence list that preserves DES positions if validator provided sequence.
    sequence = res.get("sequence")
    if not sequence:
        # fallback: put des (if any) before questions — validator ideally should return sequence to preserve positions
        sequence = []
        des_text = res.get("des")
        if des_text:
            sequence.append({"type": "des", "text": des_text})
        for q in res.get("questions", []):
            sequence.append({"type": "question", **q})

    # Save job in bot_data store with a job_id
    job_id = f"job:{int(time.time()*1000)}:{uid}"
    pending_jobs = context.bot_data.setdefault("pending_jobs", {})
    pending_jobs[job_id] = {"owner_id": uid, "sequence": sequence}
    # Ask user to choose Anonymous/Public (no Cancel)
    context.bot.send_message(uid, "Choose poll type:", reply_markup=kb_choose_anonymous(job_id))

# Callback handler for both type and chat choice
def callback_query_router(update: Update, context: CallbackContext):
    q = update.callback_query
    if not q or not q.data:
        return
    uid = q.from_user.id if q.from_user else None
    try:
        payload = json.loads(q.data)
    except Exception:
        q.answer("Invalid action")
        return

    act = payload.get("act")
    job_id = payload.get("job")
    if act == "choose_type":
        # owner chose anonymous/public: show chat choices
        if not job_id or job_id not in context.bot_data.get("pending_jobs", {}):
            q.answer("Job not found or expired.")
            return
        is_anon = bool(payload.get("anon"))
        # store choice
        choices = context.bot_data.setdefault("pending_choices", {})
        choices[job_id] = {"is_anonymous": is_anon}
        # show chat selection keyboard (no Cancel)
        q.answer()  # toast
        q.edit_message_text("Choose where to post the quiz:")
        q.edit_message_reply_markup(reply_markup=kb_choose_chat(job_id))
        return

    if act == "choose_chat":
        # owner chose chat — start posting (background thread)
        if not job_id or job_id not in context.bot_data.get("pending_jobs", {}):
            q.answer("Job not found or expired.")
            return
        chat_id = payload.get("chat_id")
        if chat_id is None:
            q.answer("Invalid chat")
            return

        # check and fetch job
        job = context.bot_data["pending_jobs"].pop(job_id, None)
        # merge choices
        choice = context.bot_data.get("pending_choices", {}).pop(job_id, {})
        if not job:
            q.answer("Job not found or expired.")
            return
        owner_id = job.get("owner_id")
        is_anonymous = choice.get("is_anonymous", False)

        # Acknowledge without posting a visible 'Queued' message
        q.answer("Starting posting…")

        # guard against double start for same job
        running = context.bot_data.setdefault("running_jobs", set())
        if job_id in running:
            q.answer("Job already running.")
            return
        running.add(job_id)

        # determine chat_name from TARGET_CHATS mapping (reverse lookup)
        chat_name = None
        for name, cid in TARGET_CHATS.items():
            if int(cid) == int(chat_id):
                chat_name = name
                break

        # launch background worker
        def run_job():
            try:
                sent = worker.post_sequence(
                    bot=context.bot,
                    chat_id=int(chat_id),
                    sequence=job.get("sequence", []),
                    is_anonymous=is_anonymous,
                    owner_id=owner_id,
                    chat_name=chat_name,
                    poll_delay_short=POLL_DELAY_SHORT,
                    poll_delay_long=POLL_DELAY_LONG,
                    max_retries=MAX_RETRIES,
                    sequential_fail_abort=SEQUENTIAL_FAIL_ABORT,
                    job_id=job_id
                )
                # send final owner confirmation (worker will also attempt; this is safe guard)
                if owner_id and sent:
                    try:
                        name = chat_name if chat_name else str(chat_id)
                        context.bot.send_message(owner_id, f"✅ {sent} quiz(es) sent successfully to {name} 🎉")
                    except Exception:
                        logger.exception("Failed to send owner confirmation from main thread.")
            except Exception:
                logger.exception("Uncaught error in run_job thread")
            finally:
                # remove running guard
                try:
                    context.bot_data.get("running_jobs", set()).discard(job_id)
                except Exception:
                    pass

        threading.Thread(target=run_job, daemon=True).start()
        # update message text to reflect started (only edit message)
        q.edit_message_text("Posting started. You will receive a confirmation when done.")
        return

    # unknown action
    q.answer("Unknown action")

def main():
    # run flask health server in background so main thread handles polling
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("help", help_cmd))

    # Owner message handler: messages that contain 'Q:' go to owner handler
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS_SET)) & Filters.regex(r'Q:'), owner_message_handler))
    # generic owner message fallback (if they reply with something else)
    dp.add_handler(MessageHandler(Filters.text & Filters.user(user_id=list(SUDO_USERS_SET)), lambda u,c: c.bot.send_message(u.effective_chat.id, "Send formatted quiz text (must contain Q:)")))

    dp.add_handler(CallbackQueryHandler(callback_query_router))

    # Start polling in main thread (so updater.idle won't raise signal error)
    logger.info("Starting polling (main thread)...")
    updater.start_polling(poll_interval=1.0, timeout=20)
    try:
        # heartbeat meta update
        while True:
            ts = str(int(time.time()))
            try:
                db.set_meta("last_heartbeat", ts)
            except Exception:
                logger.exception("Failed to set heartbeat.")
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; shutting down.")
    finally:
        updater.stop()
        logger.info("Bot stopped.")

if __name__ == "__main__":
    main()
