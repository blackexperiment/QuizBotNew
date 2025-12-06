import os
import uuid
import json
import threading
import time
from flask import Flask, jsonify
from telegram import Bot, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler, CallbackContext

from validator import parse_quiz_text
import db
from worker import post_job

# ---------- CONFIG ----------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Set TELEGRAM_BOT_TOKEN env var")

SUDO_USERS = os.environ.get("SUDO_USERS", "")
SUDO_USERS = [int(x.strip()) for x in SUDO_USERS.split(",") if x.strip()]  # numeric IDs

# TARGET_CHATS format: Name:ID;Name2:ID2
TARGET_CHATS_RAW = os.environ.get("TARGET_CHATS", "")
def parse_targets(raw: str):
    out = []
    if not raw:
        return out
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    for p in parts:
        if ":" in p:
            name, idpart = p.split(":", 1)
            try:
                cid = int(idpart.strip())
            except:
                continue
            out.append({"name": name.strip(), "id": cid})
    return out

TARGET_CHATS = parse_targets(TARGET_CHATS_RAW)

DB_PATH = os.environ.get("DB_PATH", "quizbot.db")
PORT = int(os.environ.get("PORT", "8080"))

# initialize DB
db.init_db()

bot = Bot(token=TELEGRAM_BOT_TOKEN)
updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
dispatcher = updater.dispatcher

# ---------- FLASK health ----------
app = Flask(__name__)
@app.route("/")
def index():
    return jsonify({"status": "ok", "app": "quizbot-simple"})

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# ---------- Helpers ----------
def is_sudo(user_id: int) -> bool:
    return user_id in SUDO_USERS

def build_targets_keyboard():
    buttons = []
    for t in TARGET_CHATS:
        buttons.append([InlineKeyboardButton(f"📢 {t['name']}", callback_data=f"select_target:{t['id']}")])
    return InlineKeyboardMarkup(buttons) if buttons else None

# ---------- Handlers ----------
def start(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if not is_sudo(uid):
        update.message.reply_text("🚫 This is a private bot.")
        return
    text = (
        "🛡️ BLACK RHINO CONTROL PANEL\n\n"
        "🚀 Send a pre-formatted quiz message and I'll post it as polls.\n\n"
        "Format must start with `DES:` and end with `DES: ... COMPLETED ✅`.\n"
        "Use:\nA: (A) option text\nB: (B) option text\n...\nANS: B\n\n"
        "Example: DES: Test\nQ: ...\nA: (A) ...\nANS: A\nDES: Test COMPLETED ✅"
    )
    update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# store pending jobs in memory mapping job_id -> parsed content (persisted in sqlite as well)
_pending_jobs = {}

def handle_message(update: Update, context: CallbackContext):
    if update.message is None or update.message.text is None:
        return
    uid = update.effective_user.id
    if not is_sudo(uid):
        return
    raw = update.message.text
    try:
        title, questions = parse_quiz_text(raw)
    except Exception as e:
        update.message.reply_text(f"⚠️ Format error: {e}")
        return

    job_id = str(uuid.uuid4())[:12]
    total = len(questions)
    # Save to DB
    db.save_job(job_id, title, uid, 0, total)  # target set after selection
    for idx, q in enumerate(questions, start=1):
        # options dict - keep letter->text
        options_json = json.dumps(q["options"])
        db.save_post(job_id, idx, q["q"], options_json, q["ans"])
    # keep in memory for callback access (lightweight)
    _pending_jobs[job_id] = {"title": title, "owner": uid, "total": total}

    # ask owner to select target
    kb = build_targets_keyboard()
    if not kb:
        update.message.reply_text("⚠️ No configured target channels/groups. Add TARGET_CHATS env var.")
        return
    update.message.reply_text("📌 Select where to send this quiz:\nChoose a channel or group below ⬇️", reply_markup=kb)

    # also store mapping from owner -> job_id so callback knows which job to use
    # we simply set last pending for that owner
    context.user_data["last_job_id"] = job_id

def callback_query_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    uid = query.from_user.id
    if not is_sudo(uid):
        query.answer("Not authorized")
        return
    data = query.data
    if data.startswith("select_target:"):
        target_id = int(data.split(":",1)[1])
        # find job id from user_data
        job_id = context.user_data.get("last_job_id")
        if not job_id or job_id not in _pending_jobs:
            query.answer("No pending job found for you. Send the formatted quiz again.")
            return
        # assign target in DB
        job = _pending_jobs[job_id]
        db.save_job(job_id, job["title"], job["owner"], target_id, job["total"])
        # start posting immediately (no confirm)
        query.answer("Selected. Posting will start now.")
        # send starting message to owner and start background posting thread
        context.bot.send_message(chat_id=uid, text=f"🚀 Posting your quiz '{job['title']}' to the selected chat now.")
        # start worker thread
        thr = threading.Thread(target=post_job, args=(job_id, uid), daemon=True)
        thr.start()
    else:
        query.answer("Unknown action")

# unknown commands
def unknown(update: Update, context: CallbackContext):
    pass

# register handlers
dispatcher.add_handler(CommandHandler("start", start))
dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_message))
dispatcher.add_handler(CallbackQueryHandler(callback_query_handler))
dispatcher.add_handler(MessageHandler(Filters.command, unknown))

# ---------- Start ----------
if __name__ == "__main__":
    # start flask thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    print("Flask started on port", PORT)
    # start polling
    print("Bot starting polling...")
    updater.start_polling()
    updater.idle()
