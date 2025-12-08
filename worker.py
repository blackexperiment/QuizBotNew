# worker.py
import os
import time
import logging
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError

logger = logging.getLogger("quizbot.worker")

SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))
POLL_DELAY_SHORT = float(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = float(os.environ.get("POLL_DELAY_LONG", "2"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))

def post_quiz_questions_background(bot, chat_id: int, parsed: dict, is_anon: bool, owner_id: int=None) -> bool:
    des = parsed.get("des")
    des_pos = parsed.get("des_pos")  # 'before'|'after'|None
    questions = parsed.get("questions", [])
    n = len(questions)
    delay = POLL_DELAY_SHORT if n <= 50 else POLL_DELAY_LONG

    # send DES before if requested
    try:
        if des and des_pos == "before":
            bot.send_message(chat_id=chat_id, text=des)
    except Exception:
        logger.exception("Failed to send DES (before). Continuing.")

    sequential_failures = 0
    for idx, q in enumerate(questions, start=1):
        question_text = q["raw_question"]
        labels = sorted(q["options"].keys())
        options_list = [ q["options"][lbl] for lbl in labels ]
        try:
            correct_idx = labels.index(q["ans"])
        except Exception:
            if owner_id:
                try:
                    bot.send_message(owner_id, f"❌ Invalid ANS for question {idx}. Aborting.")
                except Exception:
                    logger.exception("Failed to notify owner")
            return False

        attempt = 0
        success = False
        while attempt < MAX_RETRIES and not success:
            attempt += 1
            try:
                # send_poll for quiz
                bot.send_poll(chat_id=chat_id,
                              question=question_text,
                              options=options_list,
                              type='quiz',
                              correct_option_id=correct_idx,
                              is_anonymous=is_anon,
                              explanation=q.get("exp") or None)
                success = True
                sequential_failures = 0
                break
            except RetryAfter as ra:
                wait = int(getattr(ra, "retry_after", 5))
                logger.warning("RetryAfter=%s while posting Q%d. Sleeping %s", wait, idx, wait)
                time.sleep(wait)
                continue
            except (TimedOut, NetworkError) as ne:
                logger.warning("Network issue posting Q%d attempt %d: %s", idx, attempt, ne)
                time.sleep(2 ** attempt)
                continue
            except TelegramError as te:
                logger.exception("TelegramError posting Q%d: %s", idx, te)
                sequential_failures += 1
                if sequential_failures >= SEQUENTIAL_FAIL_ABORT:
                    if owner_id:
                        try:
                            bot.send_message(owner_id, f"❌ Multiple failures detected. Aborting quiz job. Failed at question {idx}.")
                        except Exception:
                            logger.exception("Failed to notify owner")
                    return False
                # small backoff then continue
                time.sleep(1)
                break
            except Exception:
                logger.exception("Unexpected error posting Q%d", idx)
                time.sleep(1)
                continue

        if not success:
            if attempt >= MAX_RETRIES:
                if owner_id:
                    try:
                        bot.send_message(owner_id, f"❌ Could not post question {idx} after {MAX_RETRIES} tries. Aborting.")
                    except Exception:
                        logger.exception("Failed to notify owner on exhaustion.")
                return False

        time.sleep(delay)

    # DES after polls if requested
    try:
        if des and des_pos == "after":
            bot.send_message(chat_id=chat_id, text=des)
    except Exception:
        logger.exception("Failed to send DES (after).")

    if owner_id:
        try:
            bot.send_message(owner_id, f"✅ {len(questions)} quiz(es) sent successfully to {chat_id} 🎉")
        except Exception:
            logger.exception("Failed to notify owner on success.")

    return True
