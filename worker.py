# worker.py
import time
import uuid
import json
import logging
import threading
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

# default safe delays
DEFAULT_DELAY_LESS_EQUAL_50 = 1.0
DEFAULT_DELAY_MORE_THAN_50 = 2.0

def safe_sleep(seconds: float):
    try:
        time.sleep(seconds)
    except Exception:
        pass

def post_quiz_questions(bot, chat_id: int, title: Optional[str], questions: List[Dict[str, Any]], owner_id: int):
    """
    Post a sequence of polls (quiz type) to the chat_id.
    Each question item expected:
    {
      "raw_question": "...",
      "options": {"A": "text", "B": "text", ...},
      "ans": "B",
      "exp": "optional explanation"
    }
    """
    job_id = str(uuid.uuid4())
    logger.info("Starting quiz job %s -> chat %s (%d questions)", job_id, chat_id, len(questions))
    # send header if title
    try:
        if title:
            bot.send_message(chat_id, title)
    except Exception as e:
        logger.exception("Failed to post quiz header: %s", e)

    # choose per-question delay
    delay = DEFAULT_DELAY_LESS_EQUAL_50 if len(questions) <= 50 else DEFAULT_DELAY_MORE_THAN_50

    for idx, q in enumerate(questions, start=1):
        qtext = q.get("raw_question") or f"Question {idx}"
        opts_map = q.get("options", {})
        # options must be ordered alphabetically by label
        labels = sorted(opts_map.keys())
        options = [opts_map[lbl] or "" for lbl in labels]
        # find index of correct option
        ans_letter = (q.get("ans") or "").upper()
        try:
            correct_index = labels.index(ans_letter)
        except ValueError:
            # fallback: 0
            correct_index = 0

        # prepare poll; using bot.send_poll (telegram.Bot)
        attempts = 0
        max_attempts = 4
        backoff = 2
        while attempts < max_attempts:
            try:
                # note: telegram.Bot.send_poll supports 'is_anonymous' and 'type' = 'quiz'
                # For explanation, python-telegram-bot v13.15 supports 'explanation' argument when creating polls.
                kwargs = dict(
                    chat_id=chat_id,
                    question=qtext,
                    options=options,
                    is_anonymous=False,  # default; owner can choose differently in UI beforehand
                    type='quiz',
                    correct_option_id=correct_index,
                )
                exp_text = q.get("exp")
                if exp_text:
                    # send as explanation parameter if supported
                    kwargs['explanation'] = exp_text

                bot.send_poll(**kwargs)
                logger.info("Posted Q%d successfully", idx)
                break
            except Exception as exc:
                attempts += 1
                # inspect for RetryAfter (telegram.error.RetryAfter)
                errstr = str(exc)
                logger.warning("Network issue posting Q%d. Attempt %d/%d. Error: %s", idx, attempts, max_attempts, errstr)
                # try to parse RetryAfter numeric seconds in message (common pattern)
                retry_seconds = None
                try:
                    # telegram.exceptions provide RetryAfter with .retry_after in some libs, but we use generic parse
                    import re
                    m = re.search(r'RetryAfter\((\d+)\)', errstr)
                    if m:
                        retry_seconds = int(m.group(1))
                except Exception:
                    retry_seconds = None

                if retry_seconds:
                    logger.info("Honoring RetryAfter for %ds", retry_seconds)
                    safe_sleep(retry_seconds + 0.5)
                else:
                    # exponential backoff
                    safe_sleep(backoff)
                    backoff *= 2

                if attempts >= max_attempts:
                    # notify owner and abort
                    try:
                        bot.send_message(owner_id, f"❌ Network issue posting Q{idx}. Aborting job {job_id} after {attempts} attempts.")
                    except Exception:
                        logger.exception("Failed to notify owner about abort.")
                    logger.error("Aborting quiz job %s after repeated failures", job_id)
                    return False
        # after success, safe delay between polls
        safe_sleep(delay)
    # Success
    try:
        bot.send_message(owner_id, f"✅ Quiz posted successfully in chat {chat_id}. Questions: {len(questions)}")
    except Exception:
        logger.exception("Failed to send completion message to owner.")
    logger.info("Finished quiz job %s", job_id)
    return True
