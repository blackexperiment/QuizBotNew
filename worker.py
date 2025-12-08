# worker.py
import time
import logging
import os
import traceback
from typing import List, Dict, Any, Optional

import telegram
from telegram import Bot

import db

logger = logging.getLogger("quizbot.worker")

# Config from env
POLL_DELAY_SHORT = int(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = int(os.environ.get("POLL_DELAY_LONG", "2"))
SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))

def post_quiz_questions(bot: Bot, chat_id: int, title: Optional[str], questions: List[Dict[str,Any]],
                        owner_id: Optional[int]=None, mode: str="public") -> bool:
    """
    posts questions sequentially.
    questions: list of {"raw_question":..., "options": {"A":"..","B":".."}, "ans": "A", "exp": "..."}
    mode: "public" or "anonymous"
    Returns True on success (all posted), False on abort.
    """

    try:
        total = len(questions)
        delay = POLL_DELAY_SHORT if total <= 50 else POLL_DELAY_LONG

        # Build a mapping of DES messages positioned: the validator provides des_list in payload.
        payload = None
        # If caller passed questions only, we expect they packed des_list into a special key:
        # Some callers may pass the parsed payload as 'title' and 'questions' etc. If not present skip.
        # But we design worker to accept optional payload in questions param by having caller include 'des_list' at top level.
        # We'll try to check if 'des_list' exists in questions (caller can pass a wrapper dict instead).
        if isinstance(questions, dict) and "questions" in questions:
            wrapper = questions
            des_list = wrapper.get("des_list", [])
            questions_list = wrapper["questions"]
        else:
            # try to read des_list from first question meta (if present)
            des_list = []
            questions_list = questions

        # Normalize options order and convert to list for send_poll
        # For each question, prepare question_text, options_list, correct_index, exp_text
        q_prepared = []
        for q in questions_list:
            # maintain sorted labels A,B,C.. order
            labels = sorted(q.get("options", {}).keys())
            options_list = [q["options"][lbl] for lbl in labels]
            # find index of ans in labels
            ans_letter = q.get("ans")
            correct_index = None
            if ans_letter and ans_letter in labels:
                correct_index = labels.index(ans_letter)
            else:
                # fallback: try first option
                correct_index = 0
            q_prepared.append({
                "question_text": q.get("raw_question"),
                "options_list": options_list,
                "correct_index": correct_index,
                "exp": q.get("exp")
            })

        # Build a mapping des_map: pos -> list of messages
        des_map = {}
        if isinstance(questions, dict) and "des_list" in questions:
            for d in questions["des_list"]:
                pos = d.get("pos", 0)
                des_map.setdefault(pos, []).append(d.get("text", ""))
        # Else attempt to get des_list via wrapper: already handled above

        sequential_failures = 0
        for idx, q in enumerate(q_prepared):
            # send any DES messages that belong before this question (pos == idx)
            if des_map.get(idx):
                for text in des_map[idx]:
                    try:
                        bot.send_message(chat_id=chat_id, text=text)
                    except telegram.error.RetryAfter as ra:
                        logger.warning("RetryAfter while sending DES: sleeping %s", ra.retry_after)
                        time.sleep(ra.retry_after)
                        bot.send_message(chat_id=chat_id, text=text)
                    except Exception:
                        logger.exception("Failed to send DES message; continuing.")

            # prepare to send poll
            question_text = q["question_text"] or "Question"
            options_list = q["options_list"]
            correct_index = q["correct_index"]
            exp_text = q.get("exp")

            # try to post with retries
            attempt = 0
            posted = False
            while attempt < MAX_RETRIES and not posted:
                try:
                    is_anonymous = (mode == "anonymous")
                    # send_poll parameters: type='quiz', correct_option_id=...
                    bot.send_poll(
                        chat_id=chat_id,
                        question=question_text,
                        options=options_list,
                        is_anonymous=is_anonymous,
                        type='quiz',
                        correct_option_id=correct_index,
                        explanation=exp_text or None
                    )
                    posted = True
                    sequential_failures = 0
                    # small delay between polls
                    time.sleep(delay)
                except telegram.error.RetryAfter as ra:
                    # honor Retry-After (HTTP 429)
                    sleep_for = int(getattr(ra, 'retry_after', 1))
                    logger.warning("RetryAfter received. Sleeping for %s seconds", sleep_for)
                    time.sleep(sleep_for)
                except (telegram.error.TimedOut, telegram.error.NetworkError) as neterr:
                    attempt += 1
                    logger.warning("Network error posting poll (attempt %s/%s): %s", attempt, MAX_RETRIES, neterr)
                    time.sleep(1 + attempt)
                except telegram.error.Conflict as ce:
                    # conflict usually means getUpdates vs webhook or multiple instances
                    logger.error("Conflict error posting poll: %s", ce)
                    # Cannot continue; abort
                    if owner_id:
                        try:
                            bot.send_message(owner_id, f"❌ Posting aborted due to Conflict error: {ce}")
                        except Exception:
                            logger.exception("Failed to notify owner")
                    return False
                except Exception as e:
                    attempt += 1
                    logger.exception("Unexpected error posting poll (attempt %s/%s): %s", attempt, MAX_RETRIES, e)
                    time.sleep(1 + attempt)
            if not posted:
                sequential_failures += 1
                logger.error("Failed to post question %s after %s attempts. sequential_failures=%s", idx+1, MAX_RETRIES, sequential_failures)
                # on repeated sequential failure abort and notify owner
                if sequential_failures >= SEQUENTIAL_FAIL_ABORT:
                    if owner_id:
                        try:
                            bot.send_message(owner_id, f"❌ Multiple failures detected. Aborting quiz job. Failed at question {idx+1}.")
                        except Exception:
                            logger.exception("Failed to notify owner about abort")
                    return False
                # else continue to next question (or you may choose to abort)
        # After loop, send any DES messages that belong after all polls (pos == total)
        if des_map.get(len(q_prepared)):
            for text in des_map[len(q_prepared)]:
                try:
                    bot.send_message(chat_id=chat_id, text=text)
                except telegram.error.RetryAfter as ra:
                    time.sleep(getattr(ra, 'retry_after', 1))
                    bot.send_message(chat_id=chat_id, text=text)
                except Exception:
                    logger.exception("Failed to send DES after polls.")

        # notify owner success
        if owner_id:
            try:
                bot.send_message(owner_id, f"✅ {len(q_prepared)} quiz(es) sent successfully to {chat_id} 🎉")
            except Exception:
                logger.exception("Failed to notify owner on success.")

        return True
    except Exception as e:
        logger.exception("Uncaught exception in post_quiz_questions: %s", e)
        if owner_id:
            try:
                bot.send_message(owner_id, f"❌ Quiz job crashed: {e}")
            except Exception:
                pass
        return False
