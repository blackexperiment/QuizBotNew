# worker.py - responsible for posting polls (quiz) given parsed questions
import time
import os
import logging
from typing import List, Dict, Any, Optional
import db
from telegram import Bot
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError

logger = logging.getLogger("quizbot.worker")

# Config from env
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))
POLL_DELAY_SHORT = float(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = float(os.environ.get("POLL_DELAY_LONG", "2"))

def post_quiz_questions(bot: Bot, target_chat: int, title: Optional[str], questions: List[Dict[str, Any]], owner_id: Optional[int]=None) -> bool:
    """
    Post all questions as quiz polls to target_chat.
    Returns True if finished (even if some questions failed), False if aborted due to too many sequential failures.
    """
    total = len(questions)
    if total == 0:
        return True
    delay = POLL_DELAY_SHORT if total <= 50 else POLL_DELAY_LONG
    sequential_failures = 0
    sent_count = 0

    for idx, q in enumerate(questions, start=1):
        q_text = q.get("raw_question") or ""
        opts_map = q.get("options", {})
        # ensure sorted by label A,B,C...
        labels = sorted(opts_map.keys())
        options = [opts_map[lbl] for lbl in labels]
        ans_label = q.get("ans", "").upper()
        try:
            correct_index = labels.index(ans_label)
        except ValueError:
            # invalid ans — skip and notify
            logger.error("Invalid ANS for question %d: %s", idx, ans_label)
            if owner_id:
                try:
                    bot.send_message(owner_id, f"⚠️ Skipping question {idx} — invalid correct option {ans_label}.")
                except Exception:
                    logger.exception("Failed to notify owner about invalid ans.")
            continue

        exp = q.get("exp")
        attempts = 0
        while attempts <= MAX_RETRIES:
            try:
                # send poll as quiz
                bot.send_poll(chat_id=target_chat,
                              question=q_text,
                              options=options,
                              is_anonymous=False,
                              type='quiz',
                              correct_option_id=correct_index,
                              explanation=exp if exp else None)
                sent_count += 1
                sequential_failures = 0
                break
            except RetryAfter as e:
                wait = getattr(e, 'retry_after', None) or 1
                logger.warning("RetryAfter from Telegram API. Waiting %s seconds.", wait)
                time.sleep(wait)
                attempts += 1
                continue
            except (TimedOut, NetworkError) as e:
                logger.warning("Network error posting Q%d: %s — attempt %d/%d", idx, e, attempts+1, MAX_RETRIES)
                attempts += 1
                time.sleep(1 + attempts)
                continue
            except TelegramError as e:
                logger.exception("Telegram API error posting Q%d: %s", idx, e)
                attempts += 1
                time.sleep(1)
                continue
            except Exception as e:
                logger.exception("Unexpected error posting Q%d: %s", idx, e)
                attempts += 1
                time.sleep(1)
                continue

        if attempts > MAX_RETRIES:
            sequential_failures += 1
            logger.error("Exceeded MAX_RETRIES for question %d", idx)
            if owner_id:
                try:
                    bot.send_message(owner_id, f"❌ Failed to post question {idx} after {MAX_RETRIES} attempts.")
                except Exception:
                    logger.exception("Failed to notify owner on per-question failure.")
        # after each question wait appropriate delay
        time.sleep(delay)

        # if consecutive failures exceed threshold abort whole job
        if sequential_failures >= SEQUENTIAL_FAIL_ABORT:
            logger.error("Too many sequential failures (%d). Aborting job.", sequential_failures)
            if owner_id:
                try:
                    bot.send_message(owner_id, f"❌ Multiple failures detected. Aborting quiz job.")
                except Exception:
                    logger.exception("Failed to notify owner on abort.")
            return False

    # done
    if owner_id:
        try:
            # send short completion message as requested:
            # "<n> quiz(es) sent successfully in (chat name) ... short + emoji"
            bot.send_message(owner_id, f"✅ {sent_count} quiz sent successfully. 🎉")
        except Exception:
            logger.exception("Failed to send owner completion message.")
    return True
