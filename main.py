# main.py
import os
import logging
import time
import threading
import json
from typing import Dict, List, Any

from flask import Flask, jsonify
from telegram import Bot, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError, Conflict
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler

import db  # your db interface (init_db(), set_meta/get_meta)
from validator import validate_and_parse

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("quizbot")

# Config
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN missing in env")
    raise SystemExit("TELEGRAM_BOT_TOKEN required")

SUDO_USERS = os.environ.get("SUDO_USERS", "")
SUDO_USERS_SET = set()
for s in filter(None, [x.strip() for x in SUDO_USERS.split(",")]):
    try:
        SUDO_USERS_SET.add(int(s))
    except Exception:
        pass

TARGET_CHATS_RAW = os.environ.get("TARGET_CHATS", "")  # e.g. "StudyGroup:-1001234,Announcements:-1009876"
TARGET_CHATS: Dict[str, int] = {}
for pair in filter(None, [x.strip() for x in TARGET_CHATS_RAW.split(",")]):
    if ":" in pair:
        name, cid = pair.split(":", 1)
        try:
            TARGET_CHATS[name.strip()] = int(cid.strip())
        except Exception:
            continue

# delays and retry config
POLL_DELAY_SHORT = int(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = int(os.environ.get("POLL_DELAY_LONG", "2"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))

# Init DB
db.init_db()

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

bot = Bot(token=TOKEN)

def is_sudo(uid: int) -> bool:
    return uid in SUDO_USERS_SET

# store ephemeral jobs in memory (simple)
PENDING_JOBS: Dict[str, Dict] = {}

def build_chat_selection_keyboard(poll_mode: str, job_id: str):
    # poll_mode: "anon" or "public"
    buttons = []
    for name, cid in TARGET_CHATS.items():
        buttons.append([InlineKeyboardButton(f"{name}", callback_data=json.dumps({"act":"post","job":job_id,"chat":cid,"mode":poll_mode}))])
    # add cancel
    buttons.append([InlineKeyboardButton("Cancel", callback_data=json.dumps({"act":"cancel","job":job_id}))])
    return InlineKeyboardMarkup(buttons)

def build_mode_keyboard(job_id: str):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Anonymous", callback_data=json.dumps({"act":"mode","job":job_id,"mode":"anon"})),
         InlineKeyboardButton("Public", callback_data=json.dumps({"act":"mode","job":job_id,"mode":"public"}))],
        [InlineKeyboardButton("Cancel", callback_data=json.dumps({"act":"cancel","job":job_id}))]
    ])
    return kb

def send_text_safe(chat_id: int, text: str):
    try:
        bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    except Exception:
        logger.exception("Failed to send message to %s", chat_id)

def post_sequence(chat_id: int, parsed: Dict[str, Any], mode: str, owner_id: int):
    """
    mode: 'anon' => anonymous polls; 'public' => normal polls (non-anonymous)
    Use parsed['des_entries'] to send DES messages before/after questions
    """
    questions = parsed.get("questions", [])
    des_entries = parsed.get("des_entries", [])
    # Organize DESs by position q_index and pos
    des_by_before = {}
    des_by_after = {}
    for d in des_entries:
        qidx = d.get("q_index", 0)
        if d.get("pos") == "before":
            des_by_before.setdefault(qidx, []).append(d["text"])
        else:
            des_by_after.setdefault(qidx, []).append(d["text"])

    # choose delay based on count
    delay = POLL_DELAY_SHORT if len(questions) <= 50 else POLL_DELAY_LONG

    sequential_failures = 0

    for idx, q in enumerate(questions):
        # send DES before if present
        if des_by_before.get(idx):
            for txt in des_by_before[idx]:
                try:
                    send_text_safe(chat_id, txt)
                except Exception:
                    logger.exception("Failed DES-before send")

        poll_question = q.get("raw_question") or ""
        options_map = q.get("options", {})
        # keep alphabetical order A,B,C...
        labels = sorted(options_map.keys())
        options_list = []
        for l in labels:
            # show label like "(A) text" if not already present
            txt = options_map.get(l, "")
            options_list.append(txt)

        # map ANS letter to index
        ans_letter = (q.get("ans") or "").upper()
        try:
            correct_index = labels.index(ans_letter)
        except Exception:
            correct_index = None

        # Try posting with retries
        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                is_anonymous = True if mode == "anon" else False
                # send poll (quiz)
                # python-telegram-bot send_poll supports: question, options, is_anonymous, type='quiz', correct_option_id, explanation
                kwargs = {
                    "chat_id": chat_id,
                    "question": poll_question,
                    "options": options_list,
                    "is_anonymous": is_anonymous,
                    "type": "quiz"
                }
                if correct_index is not None:
                    kwargs["correct_option_id"] = int(correct_index)
                if q.get("exp"):
                    kwargs["explanation"] = q.get("exp")
                bot.send_poll(**kwargs)
                sequential_failures = 0
                break
            except RetryAfter as e:
                # honor RetryAfter
                wait = int(getattr(e, "retry_after", 1))
                logger.warning("RetryAfter received. Waiting %s seconds", wait)
                time.sleep(wait)
                attempt += 1
                continue
            except Conflict as e:
                # only happens if multiple instances active -> notify owner and abort
                logger.error("Conflict error while posting: %s", str(e))
                send_text_safe(owner_id, "⚠️ Conflict error: another bot instance is running. Aborting job.")
                return False
            except (TimedOut, NetworkError) as e:
                logger.warning("Network/Timeout posting poll, attempt %s/%s", attempt+1, MAX_RETRIES)
                attempt += 1
                time.sleep(1 + attempt)
                continue
            except TelegramError as e:
                logger.exception("TelegramError while posting poll: %s", e)
                attempt += 1
                time.sleep(1)
                continue
            except Exception:
                logger.exception("Unexpected error posting poll")
                attempt += 1
                time.sleep(1)
                continue

        if attempt >= MAX_RETRIES:
            sequential_failures += 1
            send_text_safe(owner_id, f"❌ Failed posting Question {idx+1} after {MAX_RETRIES} attempts. Aborting job.")
            if sequential_failures >= SEQUENTIAL_FAIL_ABORT:
                send_text_safe(owner_id, f"❌ Repeated failures ({sequential_failures}). Aborting further posting.")
                return False
        # send DES after if present
        if des_by_after.get(idx):
            for txt in des_by_after[idx]:
                try:
                    send_text_safe(chat_id, txt)
                except Exception:
                    logger.exception("Failed DES-after send")

        time.sleep(delay)

    # finished
    send_text_safe(owner_id, f"✅ {len(questions)} quiz(es) sent successfully to {chat_id} 🎉")
    return True

# Handlers
def start(update, context):
    uid = update.effective_user.id if update.effective_user else None
    if uid and is_sudo(uid):
        msg = "🛡️ BLACK RHINO CONTROL PANEL\n🚀 Send a pre-formatted quiz message and I'll post it as polls."
        context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
    else:
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text="🚫 This is a private bot. This bot is restricted and can be used only by the authorized owner.")

def handle_quiz_text(update, context):
    uid = update.effective_user.id if update.effective_user else None
    if not uid or not is_sudo(uid):
        return
    text = update.effective_message.text or ""
    parsed = validate_and_parse(text)
    if not parsed.get("ok"):
        errs = "\n".join(parsed.get("errors", []))
        warns = "\n".join(parsed.get("warnings", []))
        reply = "⚠️ Format errors:\n" + errs
        if warns:
            reply += "\n⚠️ Warnings:\n" + warns
        context.bot.send_message(chat_id=uid, text=reply)
        return

    # Save job in memory with id
    job_id = str(int(time.time()*1000))
    PENDING_JOBS[job_id] = {
        "parsed": parsed,
        "owner": uid,
        "raw": text
    }
    # Ask owner to choose mode (anonymous/public)
    context.bot.send_message(chat_id=uid, text="Choose poll mode:", reply_markup=build_mode_keyboard(job_id))

def callback_query_handler(update, context):
    query = update.callback_query
    data_raw = query.data
    try:
        payload = json.loads(data_raw)
    except Exception:
        query.answer("Invalid data")
        return
    act = payload.get("act")
    job_id = payload.get("job")
    if act == "mode":
        mode = payload.get("mode")
        # present chat choices
        if job_id not in PENDING_JOBS:
            query.edit_message_text("Job not found or expired.")
            return
        query.edit_message_text(f"Selected mode: {'Anonymous' if mode=='anon' else 'Public'}\nChoose target chat:", reply_markup=build_chat_selection_keyboard(mode, job_id))
        return
    if act == "post":
        if job_id not in PENDING_JOBS:
            query.answer("Job expired")
            return
        chat_id = payload.get("chat")
        mode = payload.get("mode")
        job = PENDING_JOBS.pop(job_id, None)
        if not job:
            query.edit_message_text("Job expired.")
            return
        # start posting in background thread
        owner = job.get("owner")
        parsed = job.get("parsed")
        context.bot.send_message(chat_id=owner, text=f"✅ Posting to selected chat now...")
        threading.Thread(target=lambda: post_sequence(chat_id, parsed, mode, owner), daemon=True).start()
        query.edit_message_text("Posting started. You will be notified on completion.")
        return
    if act == "cancel":
        PENDING_JOBS.pop(job_id, None)
        query.edit_message_text("Cancelled.")
        return

def main():
    # start flask on daemon thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    updater = Updater(token=TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", start))
    # messages containing 'Q:' considered as quiz text (from sudo users)
    dp.add_handler(MessageHandler(Filters.text & Filters.regex(r'Q:'), handle_quiz_text))
    dp.add_handler(CallbackQueryHandler(callback_query_handler))

    # start polling in MAIN THREAD (so signal works)
    logger.info("Starting polling (main thread)...")
    updater.start_polling()
    # heartbeat loop
    try:
        while True:
            db.set_meta("last_heartbeat", str(int(time.time())))
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt -> stopping")
        updater.stop()
    except Exception:
        logger.exception("Main loop exception")
        updater.stop()

if __name__ == "__main__":
    main()
