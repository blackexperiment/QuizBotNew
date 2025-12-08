# worker.py
import time
import logging
from typing import List, Dict, Any, Optional

from telegram import Bot
from telegram.error import RetryAfter, TelegramError

# if you have db.mark_job_done, try to use it for idempotency; otherwise we'll use in-memory guard
import db

logger = logging.getLogger("quizbot.worker")

# in-process notified guard (avoid duplicate final confirmations)
_notified_jobs = set()

def _letter_to_index(letter: str, ordered_labels: List[str]) -> Optional[int]:
    """
    Map option letter like 'A' to index in ordered_labels list (ordered_labels contains labels like ['A','B','C'...'])
    """
    letter_u = (letter or "").strip().upper()
    if not letter_u:
        return None
    try:
        return ordered_labels.index(letter_u)
    except ValueError:
        return None

def post_sequence(
    bot: Bot,
    chat_id: int,
    sequence: List[Dict[str, Any]],
    is_anonymous: bool = False,
    owner_id: Optional[int] = None,
    chat_name: Optional[str] = None,
    poll_delay_short: float = 1.0,
    poll_delay_long: float = 2.0,
    max_retries: int = 4,
    sequential_fail_abort: int = 3,
    job_id: Optional[str] = None
) -> int:
    """
    Post the sequence (list of dicts) into chat_id.
    sequence items:
     - {"type":"des", "text": "..."}
     - {"type":"question", "raw_question": "...", "options": {"A":"...","B":"..."}, "ans":"A", "exp":"..."}
    Returns number of polls successfully sent (int) or 0 on failure.
    """
    sent_count = 0
    consecutive_failures = 0

    for idx, item in enumerate(sequence, start=1):
        try:
            if item.get("type") == "des":
                # send DES as a normal message in-order
                try:
                    bot.send_message(chat_id=chat_id, text=item.get("text", ""))
                except Exception:
                    logger.exception("Failed to send DES at index %s", idx)
                # continue to next element
                continue

            if item.get("type") == "question":
                qtext = item.get("raw_question") or item.get("question") or ""
                options_map = item.get("options", {}) or {}
                # Order options by sorted label A,B,C... to maintain consistency
                ordered_labels = sorted(options_map.keys())
                # Build list of option texts preserving alphabetical label order
                option_texts = [options_map.get(lbl, "") for lbl in ordered_labels]
                # map ANS letter to index in that list
                ans_letter = (item.get("ans") or "").strip().upper()
                correct_index = _letter_to_index(ans_letter, ordered_labels)
                # validation: require at least 2 options and correct_index valid
                if len(option_texts) < 2 or correct_index is None:
                    logger.error("Invalid question format at seq index %s: options=%s ans=%s", idx, ordered_labels, ans_letter)
                    # treat as failure and continue or abort according to policy
                    consecutive_failures += 1
                    if consecutive_failures >= sequential_fail_abort:
                        # abort job and notify owner
                        try:
                            if owner_id:
                                bot.send_message(owner_id, f"❌ Aborting: too many sequential failures (question index {idx}).")
                        except Exception:
                            logger.exception("Failed to notify owner about abort.")
                        return sent_count
                    # skip this question
                    continue

                # Prepare explanation (EXP)
                explanation = item.get("exp") or None

                # Try sending poll with retries and handling RetryAfter
                attempt = 0
                while attempt < max_retries:
                    try:
                        # send_poll uses correct_option_id index
                        # python-telegram-bot 13's send_poll may accept 'explanation' param via Bot API though PTB v13 sometimes
                        # We'll call bot.send_poll and include explanation if API supports it. If not supported, fallback to sending separate msg.
                        poll = bot.send_poll(
                            chat_id=chat_id,
                            question=qtext,
                            options=option_texts,
                            is_anonymous=is_anonymous,
                            type="quiz",
                            correct_option_id=correct_index,
                            explanation=explanation if explanation else None
                        )
                        sent_count += 1
                        consecutive_failures = 0
                        # after posting poll, sleep appropriate delay
                        # small heuristics: if many questions, use long delay
                        if len(sequence) <= 50:
                            time.sleep(poll_delay_short)
                        else:
                            time.sleep(poll_delay_long)
                        break  # success -> leave retry loop
                    except RetryAfter as ra:
                        # if API asks to wait, honor it
                        wait = getattr(ra, "retry_after", None) or 1
                        logger.warning("RetryAfter(%s) for question index %s — sleeping %s s", ra, idx, wait)
                        time.sleep(wait)
                        attempt += 1
                        continue
                    except TelegramError as te:
                        # network / api errors; retry with backoff
                        attempt += 1
                        backoff = min(2 ** attempt, 30)
                        logger.exception("TelegramError posting question idx %s attempt %s: %s, retrying in %s s", idx, attempt, te, backoff)
                        time.sleep(backoff)
                        continue
                    except Exception as exc:
                        attempt += 1
                        backoff = min(2 ** attempt, 30)
                        logger.exception("Unexpected error posting question idx %s attempt %s: %s", idx, attempt, exc)
                        time.sleep(backoff)
                        continue
                else:
                    # exhausted retries for this question
                    consecutive_failures += 1
                    logger.error("Exhausted retries for question index %s. Skipping.", idx)
                    if owner_id:
                        try:
                            bot.send_message(owner_id, f"❌ Failed to post question #{idx}. Aborting job.")
                        except Exception:
                            logger.exception("Failed to notify owner about question failure.")
                    if consecutive_failures >= sequential_fail_abort:
                        if owner_id:
                            try:
                                bot.send_message(owner_id, f"❌ Aborted job due to repeated failures at question #{idx}.")
                            except Exception:
                                logger.exception("Failed to notify owner on abort.")
                        return sent_count
                    continue

            else:
                logger.warning("Unknown sequence item at index %s: %s", idx, item)
                continue

        except Exception:
            logger.exception("Unhandled exception while processing sequence index %s", idx)
            consecutive_failures += 1
            if consecutive_failures >= sequential_fail_abort:
                if owner_id:
                    try:
                        bot.send_message(owner_id, f"❌ Aborted job due to repeated unhandled exceptions at index {idx}.")
                    except Exception:
                        logger.exception("Failed to notify owner on abort.")
                return sent_count
            continue

    # job done — final single confirmation (idempotent)
    try:
        notified = False
        # try DB-level idempotency if available
        if job_id:
            try:
                # db.mark_job_done should return True if it marked now, False if already marked earlier.
                if hasattr(db, "mark_job_done"):
                    notified = not db.mark_job_done(job_id)  # mark_job_done returns False if already done, True if newly marked
                    # We want notified == False to mean NOT notified yet. So invert logic.
                    # If db.mark_job_done returns True (newly marked) -> we should notify now (so notified False)
                    # If returns False (already done) -> notified True (so skip)
                    notified = False if db.mark_job_done(job_id) else True
                else:
                    notified = False
            except Exception:
                logger.exception("db.mark_job_done() failed; falling back to in-memory guard.")
                notified = False

        # in-memory fallback guard
        if not notified:
            if job_id:
                key = job_id
            else:
                key = f"{chat_id}:{owner_id}:{sent_count}"
            if key in _notified_jobs:
                notified = True
            else:
                _notified_jobs.add(key)
                notified = False

        if not notified and owner_id:
            name = chat_name if chat_name else str(chat_id)
            bot.send_message(owner_id, f"✅ {sent_count} quiz(es) sent successfully to {name} 🎉")
    except Exception:
        logger.exception("Failed to send final owner confirmation.")

    return sent_count
