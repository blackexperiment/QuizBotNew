# worker.py
import time
import logging
from typing import Dict, List, Any, Optional
import os

logger = logging.getLogger("quizbot.worker")

# config from env (fall back to defaults)
POLL_DELAY_SHORT = float(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = float(os.environ.get("POLL_DELAY_LONG", "2"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))

# TARGET_CHATS env format: Name:chatid,Name2:chatid2
def parse_target_chats(env: Optional[str]=None) -> Dict[str, int]:
    env = env or os.environ.get("TARGET_CHATS", "")
    out = {}
    if not env:
        return out
    parts = [p.strip() for p in env.split(",") if p.strip()]
    for p in parts:
        if ":" in p:
            name, cid = p.split(":", 1)
            try:
                out[name.strip()] = int(cid.strip())
            except Exception:
                # ignore bad
                continue
    return out

TARGET_CHATS_MAP = parse_target_chats()

def _send_des(bot, chat_id: int, text: str):
    try:
        bot.send_message(chat_id=chat_id, text=text)
        return True
    except Exception as e:
        logger.exception("Failed to send DES to %s: %s", chat_id, e)
        return False

def _send_poll(bot, chat_id: int, q: Dict[str, Any], anonymous: bool):
    """
    q: {"raw_question":..., "options": {A: text,...}, "ans":"A", "exp": optional}
    returns True/False
    """
    question = q.get("raw_question")
    options_map = q.get("options", {})
    # Order options by letter A,B,C...
    labels = sorted(options_map.keys())
    opts = [options_map[k] for k in labels]
    # correct index
    try:
        correct_idx = labels.index(q.get("ans").upper())
    except Exception:
        correct_idx = None

    # make retries
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            # send_poll: use send_poll for quizzes
            # python-telegram-bot v13: bot.send_poll(chat_id, question, options,
            # options as list, is_anonymous=anonymous, type='quiz', correct_option_id=correct_idx, explanation=exp)
            exp = q.get("exp")
            if correct_idx is None:
                # fallback: send a normal poll (non-quiz)
                bot.send_poll(chat_id=chat_id, question=question, options=opts, is_anonymous=anonymous)
            else:
                # include explanation if provided
                kwargs = {"is_anonymous": anonymous, "type": "quiz", "correct_option_id": correct_idx}
                if exp:
                    # explanation supported in modern PTB; include but safe-guard with try/except
                    kwargs["explanation"] = exp
                bot.send_poll(chat_id=chat_id, question=question, options=opts, **kwargs)
            return True
        except Exception as e:
            attempt += 1
            logger.exception("Failed to send poll (attempt %d/%d) to %s: %s", attempt, MAX_RETRIES, chat_id, e)
            # exponential backoff
            time.sleep(min(2 ** attempt, 8))
    return False

def post_sequence(bot, chat_id: int, sequence: List[Dict[str, Any]], anonymous: bool, owner_id: Optional[int]=None, chat_name: Optional[str]=None) -> bool:
    """
    Post sequence (list of DES and question items) to chat_id in order.
    Returns True on full success (every question posted), False if aborted.
    """
    if chat_name is None:
        # try find name
        for k, v in TARGET_CHATS_MAP.items():
            if v == chat_id:
                chat_name = k
                break
    chat_name = chat_name or str(chat_id)

    fail_count = 0
    sent_questions = 0

    for item in sequence:
        if item.get("type") == "des":
            ok = _send_des(bot, chat_id, item.get("text",""))
            # DES shouldn't affect fail_count if it fails; we log
            if not ok:
                logger.warning("DES send failed for chat %s", chat_id)
            # small gap
            time.sleep(0.2)
            continue
        if item.get("type") == "question":
            ok = _send_poll(bot, chat_id, item, anonymous)
            if not ok:
                fail_count += 1
                logger.warning("Question post failed (count %d) for chat %s", fail_count, chat_id)
                if fail_count >= SEQUENTIAL_FAIL_ABORT:
                    # abort
                    if owner_id:
                        try:
                            bot.send_message(owner_id, f"❌ Aborting job: multiple failures posting to {chat_name}.")
                        except Exception:
                            logger.exception("Failed to notify owner of abort.")
                    return False
            else:
                sent_questions += 1
                # reset consecutive failures on success
                fail_count = 0
            # apply delay based on number of questions (simple rule)
            # if total > 50 use long delay
            delay = POLL_DELAY_SHORT if sent_questions <= 50 else POLL_DELAY_LONG
            time.sleep(delay)
            continue
        # unknown type: ignore
    # done
    # notify owner
    if owner_id:
        try:
            bot.send_message(owner_id, f"✅ {sent_questions} quiz(es) sent successfully to {chat_name} 🎉")
        except Exception:
            logger.exception("Failed to notify owner on success.")
    return True
