# app.py  (aiogram v3 + aiohttp webserver for Render web service)
import os
import logging
import asyncio
import re
import json
from dotenv import load_dotenv

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

# keep your project imports
from parser import parse_bulk, ParseError
from sender import send_actions_to_chat
from db import init_db, get_session, User, Chat, Job, Action, JobTarget
from sqlalchemy.orm import Session

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Config
PORT = int(os.environ.get("PORT", 8000))
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
THROTTLE = float(os.getenv("THROTTLE_SECONDS", "2"))

if not BOT_TOKEN:
    raise SystemExit("Missing BOT_TOKEN env var")

# Bot + Dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# init DB
init_db()

# helper functions (unchanged)
def is_owner(telegram_id: int) -> bool:
    return telegram_id == OWNER_ID

def ensure_user(session: Session, telegram_id: int, username: str = None, role: str = "teacher"):
    user = session.query(User).filter_by(telegram_id=telegram_id).first()
    if not user:
        user = User(telegram_id=telegram_id, username=username, role=role)
        session.add(user)
        session.commit()
    return user

def store_chat(session: Session, chat_name: str, chat_id: int, owner_telegram_id: int = None, is_global=False):
    existing = session.query(Chat).filter_by(chat_id=chat_id).first()
    if existing:
        return existing
    c = Chat(chat_id=chat_id, name=chat_name, owner_telegram_id=owner_telegram_id, is_global=is_global)
    session.add(c)
    session.commit()
    return c

def get_chats_for_user(session: Session, telegram_id: int):
    user_chats = session.query(Chat).filter((Chat.owner_telegram_id == telegram_id) | (Chat.is_global==True)).all()
    return user_chats

def save_job_and_actions(session: Session, telegram_id: int, raw_text: str, actions: list):
    job = Job(created_by=telegram_id, raw_text=raw_text, status="pending", total_actions=len(actions))
    session.add(job)
    session.commit()
    for i, act in enumerate(actions, start=1):
        a = Action(job_id=job.id, seq=i, type=act["type"], payload=act)
        session.add(a)
    session.commit()
    return job

# Messages
WELCOME_TEACHER = "👋 Hi! Paste your questions here — I’ll turn them into polls for your classes. Fast & fun! ✨"
WELCOME_OWNER = "👑 Welcome, Owner! Manage teachers and target chats from here. Ready when you are. 🎯"
ANALYZING = "⏳ Got it — analyzing your text now..."
PARSE_SUCCESS = "✅ Parsed: {total} items — {polls} polls, {msgs} messages.\nPreview (first 3):\n{preview}"
PARSE_WARN = "⚠️ Warning: {msg}"
PARSE_ERROR = "❌ Parse Error: {msg}"
SEND_STARTED = "🚀 Sending started — Job #{job_id}. Targets: {targets}. I’ll update you when done."
SEND_DONE = "🎉 Done! Job #{job_id} completed. Delivered: {sent} items. Failures: {failed}."
SEND_FAIL = "❗ Sending paused — Action #{idx} failed. Error: {err}"

# Inline keyboard helper
def chats_selection_kb(chats, job_id):
    kb = InlineKeyboardMarkup(row_width=2)
    for c in chats:
        kb.insert(InlineKeyboardButton(text=c.name, callback_data=f"toggle_chat:{job_id}:{c.chat_id}"))
    kb.add(InlineKeyboardButton(text="✅ Confirm Send", callback_data=f"confirm_send:{job_id}"))
    kb.add(InlineKeyboardButton(text="🧪 Test Send", callback_data=f"test_send:{job_id}"))
    kb.add(InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel_job:{job_id}"))
    return kb

# in-memory selection state
JOB_SELECTIONS = {}  # job_id -> set(chat_id)

# Handlers (no decorators). We'll register them in on_startup in correct order.

async def cmd_start(message: Message):
    sess = get_session()
    if is_owner(message.from_user.id):
        ensure_user(sess, message.from_user.id, message.from_user.username, role="owner")
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➕ Add Chat (manual)", callback_data="owner_add_chat"))
        kb.add(InlineKeyboardButton("📒 List Chats", callback_data="owner_list_chats"))
        await message.reply(WELCOME_OWNER, reply_markup=kb)
    else:
        ensure_user(sess, message.from_user.id, message.from_user.username, role="teacher")
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➕ Add Chat (manual)", callback_data="teacher_add_chat"))
        kb.add(InlineKeyboardButton("📒 My Chats", callback_data="teacher_list_chats"))
        await message.reply(WELCOME_TEACHER, reply_markup=kb)

async def owner_add_chat_cb(message: Message, cb_request=False):
    # This function can be called when callback triggers; in callback-router we will send this as a reply
    await message.reply("Send chat in format `Name:chat_id` (example: Class9:-10012345)")

async def teacher_add_chat_cb(message: Message):
    await message.reply("Send chat in format `Name:chat_id` (example: Class9:-10012345). You must be admin in that group for auto-detection.")

async def manual_add_chat(message: Message):
    text = message.text or ""
    if not re.match(r'^[^:]+:-?\d+$', text.strip()):
        return  # not a manual-add message
    sess = get_session()
    parts = text.split(':', 1)
    name = parts[0].strip()
    try:
        chat_id = int(parts[1].strip())
    except:
        await message.reply("Invalid chat_id. Use numeric chat id like -100123456789.")
        return
    owner_id = message.from_user.id if not is_owner(message.from_user.id) else None
    is_global = False
    if is_owner(message.from_user.id):
        is_global = True
    store_chat(sess, name, chat_id, owner_telegram_id=owner_id, is_global=is_global)
    await message.reply(f"Added chat: {name} ({chat_id})")

async def bulk_text_handler(message: Message):
    text = (message.text or "") + (message.caption or "")
    if not text:
        return
    if '#Q' not in text.upper() and '#MSG' not in text.upper():
        return
    sess = get_session()
    user = ensure_user(sess, message.from_user.id, message.from_user.username)
    raw = text
    await message.reply(ANALYZING)
    try:
        actions = parse_bulk(raw)
    except ParseError as e:
        await message.reply(PARSE_ERROR.format(msg=str(e)))
        return
    polls = sum(1 for a in actions if a['type']=='POLL')
    msgs = sum(1 for a in actions if a['type']=='MSG')
    preview_lines = []
    for a in actions[:3]:
        if a['type']=='MSG':
            preview_lines.append(f"[MSG] {a.get('text','')[:60]}")
        else:
            preview_lines.append(f"[POLL] {a.get('question','')[:60]} ({len(a.get('options',[]))} opts)"+(" - Quiz" if a.get("answer_index") is not None else ""))
    preview = "\n".join(preview_lines)
    reply_text = PARSE_SUCCESS.format(total=len(actions), polls=polls, msgs=msgs, preview=preview)
    job = save_job_and_actions(sess, message.from_user.id, raw, actions)
    chats = get_chats_for_user(sess, message.from_user.id)
    if not chats:
        await message.reply("No chats found. Add a chat first using `Name:chat_id` or add via the group and use /start.", parse_mode="Markdown")
        return
    kb = chats_selection_kb(chats, job.id)
    JOB_SELECTIONS[job.id] = set()
    await message.reply(reply_text, reply_markup=kb)

# callback router - single entrypoint for callback_query, dispatch internally
async def callback_router(callback: CallbackQuery):
    data = callback.data or ""
    # answer quickly
    await callback.answer()
    if data.startswith('owner_add_chat'):
        await callback.message.reply("Send chat in format `Name:chat_id` (example: Class9:-10012345)")
        return
    if data.startswith('teacher_add_chat'):
        await callback.message.reply("Send chat in format `Name:chat_id` (example: Class9:-10012345). You must be admin in that group for auto-detection.")
        return
    if data.startswith('toggle_chat:'):
        try:
            _, job_id_str, chat_id_str = data.split(':')
            job_id = int(job_id_str); chat_id = int(chat_id_str)
        except:
            await callback.message.reply("Invalid toggle payload.")
            return
        sel = JOB_SELECTIONS.get(job_id, set())
        if chat_id in sel:
            sel.remove(chat_id)
            await callback.answer("Removed")
        else:
            sel.add(chat_id)
            await callback.answer("Selected")
        JOB_SELECTIONS[job_id] = sel
        return
    if data.startswith('confirm_send:'):
        try:
            _, job_id_str = data.split(':')
            job_id = int(job_id_str)
        except:
            await callback.message.reply("Invalid confirm payload.")
            return
        sel = JOB_SELECTIONS.get(job_id)
        if not sel:
            await callback.answer("No chat selected. Please select at least one chat.")
            return
        await callback.answer("Sending queued.")
        sess = get_session()
        job = sess.query(Job).filter_by(id=job_id).first()
        if not job:
            await callback.message.reply("Job not found.")
            return
        job.status = 'queued'
        sess.commit()
        for chat in sel:
            jt = JobTarget(job_id=job.id, chat_id=chat, status='pending')
            sess.add(jt)
        sess.commit()
        # start background sending
        asyncio.create_task(run_job(job.id, callback.from_user.id))
        await callback.message.reply(SEND_STARTED.format(job_id=job.id, targets=", ".join([str(c) for c in sel])))
        return
    if data.startswith('test_send:'):
        try:
            _, job_id_str = data.split(':')
            job_id = int(job_id_str)
        except:
            await callback.message.reply("Invalid test payload.")
            return
        sel = JOB_SELECTIONS.get(job_id)
        if not sel:
            await callback.answer("No chat selected.")
            return
        chat_id = list(sel)[0]
        await callback.answer("Test sending first 2 items...")
        sess = get_session()
        acts = sess.query(Action).filter_by(job_id=job_id).order_by(Action.seq).limit(2).all()
        actlist = [a.payload for a in acts]
        result = await send_actions_to_chat(bot, chat_id, actlist, job_id=job_id)
        await callback.message.reply(f"🧪 Test send result: Sent {result['sent']} Failed {result['failed']}")
        return
    if data.startswith('cancel_job:'):
        try:
            _, job_id_str = data.split(':')
            job_id = int(job_id_str)
        except:
            await callback.message.reply("Invalid cancel payload.")
            return
        sess = get_session()
        job = sess.query(Job).filter_by(id=job_id).first()
        if job:
            job.status = 'cancelled'
            sess.commit()
        await callback.message.reply(f"❌ Job #{job_id} cancelled.")
        return
    # unknown callback
    await callback.message.reply("Unknown action.")

# Background job runner (kept same logic)
async def run_job(job_id: int, trigger_user_id: int):
    sess = get_session()
    job = sess.query(Job).filter_by(id=job_id).first()
    if not job:
        return
    job.status = 'running'
    sess.commit()
    targets = sess.query(JobTarget).filter_by(job_id=job_id).all()
    actions_db = sess.query(Action).filter_by(job_id=job_id).order_by(Action.seq).all()
    actions = [json.loads(json.dumps(a.payload)) for a in actions_db]
    overall_sent = 0
    overall_failed = 0
    for t in targets:
        t.status = 'running'
        sess.commit()
        res = await send_actions_to_chat(bot, t.chat_id, actions, job_id=job_id)
        if res['failed'] > 0:
            t.status = 'failed'
            job.status = 'failed'
            sess.commit()
            await bot.send_message(trigger_user_id, SEND_FAIL.format(idx=0, err=res['errors'][0]['error'] if res['errors'] else 'Unknown'))
            return
        else:
            t.status = 'completed'
            overall_sent += res['sent']
            sess.commit()
    job.status = 'completed'
    sess.commit()
    await bot.send_message(trigger_user_id, SEND_DONE.format(job_id=job.id, sent=overall_sent, failed=overall_failed))

# aiohttp health endpoint & startup/cleanup lifecycle
async def handle_root(request):
    return web.Response(text="OK - bot running")

async def on_startup(app):
    # Register handlers in order (more specific first)
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(manual_add_chat)   # regex-based inside
    dp.message.register(bulk_text_handler) # detects #Q / #MSG inside
    # callback router for all callback queries
    dp.callback_query.register(callback_router)

    # start polling as a background task
    app["poller"] = asyncio.create_task(dp.start_polling(bot, shutdown_timeout=5.0))
    logging.info("Started polling background task")

async def on_cleanup(app):
    # cancel polling task
    poller = app.get("poller")
    if poller:
        poller.cancel()
        try:
            await poller
        except asyncio.CancelledError:
            pass
    # close bot session
    await bot.session.close()
    logging.info("Cleaned up polling and bot session")

def create_app():
    app = web.Application()
    app.add_routes([web.get("/", handle_root)])
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

if __name__ == "__main__":
    # Run aiohttp webserver (binds PORT so Render sees open port)
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
