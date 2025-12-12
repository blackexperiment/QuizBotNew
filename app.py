# app.py
import os
import logging
import asyncio
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from parser import parse_bulk, ParseError
from sender import send_actions_to_chat
from db import init_db, get_session, User, Chat, Job, Action, JobTarget
import json
from sqlalchemy.orm import Session

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
THROTTLE = float(os.getenv("THROTTLE_SECONDS", "2"))

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# initialize db
init_db()

# helper functions
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
    # user sees their own chats and owner global chats
    user_chats = session.query(Chat).filter((Chat.owner_telegram_id == telegram_id) | (Chat.is_global==True)).all()
    return user_chats

def save_job_and_actions(session: Session, telegram_id: int, raw_text: str, actions: list):
    job = Job(created_by=telegram_id, raw_text=raw_text, status="pending", total_actions=len(actions))
    session.add(job)
    session.commit()
    # save actions
    for i, act in enumerate(actions, start=1):
        a = Action(job_id=job.id, seq=i, type=act["type"], payload=act)
        session.add(a)
    session.commit()
    return job

# Messages (English short + emoji)
WELCOME_TEACHER = "👋 Hi! Paste your questions here — I’ll turn them into polls for your classes. Fast & fun! ✨"
WELCOME_OWNER = "👑 Welcome, Owner! Manage teachers and target chats from here. Ready when you are. 🎯"
ANALYZING = "⏳ Got it — analyzing your text now..."
PARSE_SUCCESS = "✅ Parsed: {total} items — {polls} polls, {msgs} messages.\nPreview (first 3):\n{preview}"
PARSE_WARN = "⚠️ Warning: {msg}"
PARSE_ERROR = "❌ Parse Error: {msg}"
SEND_STARTED = "🚀 Sending started — Job #{job_id}. Targets: {targets}. I’ll update you when done."
SEND_DONE = "🎉 Done! Job #{job_id} completed. Delivered: {sent} items. Failures: {failed}."
SEND_FAIL = "❗ Sending paused — Action #{idx} failed. Error: {err}"

# Inline keyboards helpers
def chats_selection_kb(chats, job_id):
    kb = InlineKeyboardMarkup(row_width=2)
    for c in chats:
        kb.insert(InlineKeyboardButton(text=c.name, callback_data=f"toggle_chat:{job_id}:{c.chat_id}"))
    kb.add(InlineKeyboardButton(text="✅ Confirm Send", callback_data=f"confirm_send:{job_id}"))
    kb.add(InlineKeyboardButton(text="🧪 Test Send", callback_data=f"test_send:{job_id}"))
    kb.add(InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel_job:{job_id}"))
    return kb

# state: keep track of selected chat ids per job in-memory for simplicity
JOB_SELECTIONS = {}  # job_id -> set(chat_id)

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
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

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('owner_add_chat'))
async def owner_add_chat_cb(cb: types.CallbackQuery):
    await cb.answer()
    await cb.message.reply("Send chat in format `Name:chat_id` (example: Class9:-10012345)")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('teacher_add_chat'))
async def teacher_add_chat_cb(cb: types.CallbackQuery):
    await cb.answer()
    await cb.message.reply("Send chat in format `Name:chat_id` (example: Class9:-10012345). You must be admin in that group for auto-detection.")

@dp.message_handler(regexp=r'^[^:]+:-?\d+$')
async def manual_add_chat(message: types.Message):
    # matches lines like Name:-12345
    sess = get_session()
    parts = message.text.split(':', 1)
    name = parts[0].strip()
    try:
        chat_id = int(parts[1].strip())
    except:
        await message.reply("Invalid chat_id. Use numeric chat id like -100123456789.")
        return
    owner_id = message.from_user.id if not is_owner(message.from_user.id) else None
    # if owner adds via their account and owner wants global, set is_global True
    is_global = False
    if is_owner(message.from_user.id):
        # make owner-added chats global by default
        is_global = True
    store_chat(sess, name, chat_id, owner_telegram_id=owner_id, is_global=is_global)
    await message.reply(f"Added chat: {name} ({chat_id})")

@dp.message_handler(lambda m: ('#Q' in m.text.upper() or '#MSG' in m.text.upper()))
async def bulk_text_handler(message: types.Message):
    """
    Auto-detect bulk text paste. Accepts owner and teachers.
    """
    sess = get_session()
    user = ensure_user(sess, message.from_user.id, message.from_user.username)
    raw = message.text
    await message.reply(ANALYZING)
    try:
        actions = parse_bulk(raw)
    except ParseError as e:
        await message.reply(PARSE_ERROR.format(msg=str(e)))
        return
    # count
    polls = sum(1 for a in actions if a['type']=='POLL')
    msgs = sum(1 for a in actions if a['type']=='MSG')
    preview_lines = []
    for a in actions[:3]:
        if a['type']=='MSG':
            preview_lines.append(f"[MSG] {a['text'][:60]}")
        else:
            preview_lines.append(f"[POLL] {a['question'][:60]} ({len(a['options'])} opts)"+(" - Quiz" if a.get("answer_index") is not None else ""))
    preview = "\n".join(preview_lines)
    reply_text = PARSE_SUCCESS.format(total=len(actions), polls=polls, msgs=msgs, preview=preview)
    # Save job and actions in DB
    job = save_job_and_actions(sess, message.from_user.id, raw, actions)
    # prepare selection keyboard - fetch user's chats
    chats = get_chats_for_user(sess, message.from_user.id)
    if not chats:
        await message.reply("No chats found. Add a chat first using `Name:chat_id` or add via the group and use /start.", parse_mode="Markdown")
        return
    kb = chats_selection_kb(chats, job.id)
    JOB_SELECTIONS[job.id] = set()
    await message.reply(reply_text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('toggle_chat:'))
async def toggle_chat_cb(cb: types.CallbackQuery):
    # format: toggle_chat:{job_id}:{chat_id}
    _, job_id_str, chat_id_str = cb.data.split(':')
    job_id = int(job_id_str)
    chat_id = int(chat_id_str)
    sel = JOB_SELECTIONS.get(job_id, set())
    if chat_id in sel:
        sel.remove(chat_id)
        await cb.answer("Removed")
    else:
        sel.add(chat_id)
        await cb.answer("Selected")
    JOB_SELECTIONS[job_id] = sel

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('confirm_send:'))
async def confirm_send_cb(cb: types.CallbackQuery):
    _, job_id_str = cb.data.split(':')
    job_id = int(job_id_str)
    sel = JOB_SELECTIONS.get(job_id)
    if not sel:
        await cb.answer("No chat selected. Please select at least one chat.")
        return
    await cb.answer("Sending queued.")
    sess = get_session()
    job = sess.query(Job).filter_by(id=job_id).first()
    if not job:
        await cb.message.reply("Job not found.")
        return
    job.status = 'queued'
    sess.commit()
    # create job_targets
    for chat in sel:
        jt = JobTarget(job_id=job.id, chat_id=chat, status='pending')
        sess.add(jt)
    sess.commit()
    # start background sending task
    asyncio.create_task(run_job(job.id, cb.from_user.id))
    await cb.message.reply(SEND_STARTED.format(job_id=job.id, targets=", ".join([str(c) for c in sel])))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('test_send:'))
async def test_send_cb(cb: types.CallbackQuery):
    _, job_id_str = cb.data.split(':')
    job_id = int(job_id_str)
    sel = JOB_SELECTIONS.get(job_id)
    if not sel:
        await cb.answer("No chat selected.")
        return
    # pick first chat
    chat_id = list(sel)[0]
    await cb.answer("Test sending first 2 items...")
    sess = get_session()
    # load actions for job
    acts = sess.query(Action).filter_by(job_id=job_id).order_by(Action.seq).limit(2).all()
    # build simple actions list
    actlist = [a.payload for a in acts]
    result = await send_actions_to_chat(bot, chat_id, actlist, job_id=job_id)
    await cb.message.reply(f"🧪 Test send result: Sent {result['sent']} Failed {result['failed']}")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('cancel_job:'))
async def cancel_job_cb(cb: types.CallbackQuery):
    _, job_id_str = cb.data.split(':')
    job_id = int(job_id_str)
    sess = get_session()
    job = sess.query(Job).filter_by(id=job_id).first()
    if job:
        job.status = 'cancelled'
        sess.commit()
    await cb.message.reply(f"❌ Job #{job_id} cancelled.")

async def run_job(job_id: int, trigger_user_id: int):
    sess = get_session()
    job = sess.query(Job).filter_by(id=job_id).first()
    if not job:
        return
    job.status = 'running'
    sess.commit()
    # fetch targets
    targets = sess.query(JobTarget).filter_by(job_id=job_id).all()
    # fetch all actions for job
    actions_db = sess.query(Action).filter_by(job_id=job_id).order_by(Action.seq).all()
    actions = [json.loads(json.dumps(a.payload)) for a in actions_db]  # normalize dict
    overall_sent = 0
    overall_failed = 0
    for t in targets:
        t.status = 'running'
        sess.commit()
        res = await send_actions_to_chat(bot, t.chat_id, actions, job_id=job_id)
        if res['failed'] > 0:
            # abort on failure (per selected rule)
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

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
