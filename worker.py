# worker.py
import os
import time
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger("quizbot.worker")
logger.setLevel(logging.INFO)

# Configs (environment)
POLL_DELAY_SHORT = float(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = float(os.environ.get("POLL_DELAY_LONG", "2"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))

# Helper to map option labels to index (A->0, B->1 ...)
def label_to_index(label: str) -> int:
    return ord(label.upper()) - ord('A')

def post_quiz_questions(bot, chat_id: int, title: Optional[str], questions: List[Dict[str, Any]], owner_id: Optional[int] = None, anonymous: bool = False) -> bool:
    """
    Post a list of questions (each: raw_question, options dict {label: text}, ans, exp optional).
    - Honors MAX_RETRIES on transient network failures.
    - If API returns RetryAfter (429) will honor it.
    - On repeated failures (SEQUENTIAL_FAIL_ABORT) aborts and notifies owner.
    - Applies delay per your rules: <=50 -> POLL_DELAY_SHORT else POLL_DELAY_LONG
    """
    q_count = len(questions)
    delay = POLL_DELAY_SHORT if q_count <= 50 else POLL_DELAY_LONG

    # Send title/des if provided as a plain message before starting (caller may already handle DES; this is safe extra)
    if title:
        try:
            bot.send_message(chat_id=chat_id, text=title)
        except Exception:
            logger.exception("Failed to send title DES before polls.")

    seq_fail = 0
    for idx, q in enumerate(questions, start=1):
        # Build ordered options (A,B,C..)
        labels = sorted(q["options"].keys())
        options = [q["options"][lab] for lab in labels]
        # find correct index
        try:
            correct_index = labels.index(q["ans"])
        except Exception:
            # invalid ANS -> notify owner and abort
            msg = f"❌ Invalid ANS for question {idx}. Aborting job."
            logger.error(msg)
            if owner_id:
                try:
                    bot.send_message(owner_id, msg)
                except Exception:
                    logger.exception("Failed to notify owner about invalid ANS.")
            return False

        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            try:
                # create poll of type quiz
                sent = bot.send_poll(chat_id=chat_id,
                                     question=q["raw_question"],
                                     options=options,
                                     is_anonymous=anonymous,
                                     type="quiz",
                                     correct_option_id=correct_index)
                # send EXP as a follow-up message if provided
                if q.get("exp"):
                    try:
                        bot.send_message(chat_id=chat_id, text=f"📝 Explanation: {q['exp']}", reply_to_message_id=sent.message_id)
                    except Exception:
                        logger.exception("Failed to send EXP for Q%s", idx)
                # success -> reset seq_fail and break
                seq_fail = 0
                break
            except Exception as e:
                # check for RetryAfter (requests/telegram throws a TelegramError with retry_after attribute sometimes)
                # We do conservative handling: if 'RetryAfter' substring or attribute present, honor it
                retry_after = None
                try:
                    # some exceptions set .retry_after
                    retry_after = getattr(e, "retry_after", None)
                except Exception:
                    retry_after = None

                # if exception contains 'Too Many Requests' or '429' check for retry_after in message
                msg = str(e)
                if retry_after is None:
                    # try to parse numeric seconds from message
                    import re
                    m = re.search(r"retry after (\d+)", msg, re.IGNORECASE)
                    if m:
                        retry_after = int(m.group(1))

                if retry_after:
                    wait = int(retry_after) + 1
                    logger.warning("API asked to retry after %s seconds (Q%s). Sleeping %s", retry_after, idx, wait)
                    time.sleep(wait)
                    continue

                logger.warning("Network issue posting Q%s attempt %s/%s: %s", idx, attempt, MAX_RETRIES, e)
                time.sleep(1 + attempt)  # small backoff between attempts
                # if exceeded attempts, increment seq_fail
                if attempt >= MAX_RETRIES:
                    seq_fail += 1
                    # notify owner about this failure
                    if owner_id:
                        try:
                            bot.send_message(owner_id, f"⚠️ Failed to post Q{idx} after {MAX_RETRIES} attempts. Error: {e}")
                        except Exception:
                            logger.exception("Failed to notify owner about posting failure.")
            # end attempts loop

        # if after attempts we still didn't succeed (attempt >= MAX_RETRIES and didn't break)
        if attempt >= MAX_RETRIES and seq_fail > 0:
            logger.error("Q%s failed and seq_fail=%s", idx, seq_fail)
            if seq_fail >= SEQUENTIAL_FAIL_ABORT:
                # abort
                if owner_id:
                    try:
                        bot.send_message(owner_id, f"❌ Multiple failures posting questions. Aborting at Q{idx}.")
                    except Exception:
                        logger.exception("Failed to notify owner about abort.")
                return False

        # safe delay between polls
        time.sleep(delay)

    # all done
    if owner_id:
        try:
            # try to map chat id to friendly name if TARGET_CHATS env provided
            target_map = {}
            tc_env = os.environ.get("TARGET_CHATS", "")
            for pair in [p.strip() for p in tc_env.split(",") if p.strip()]:
                if ":" in pair:
                    nm, cid = pair.split(":", 1)
                    try:
                        target_map[int(cid.strip())] = nm.strip()
                    except Exception:
                        continue
            friendly = target_map.get(chat_id, None)
            dest_name = friendly if friendly else str(chat_id)
            bot.send_message(owner_id, f"✅ {len(questions)} quiz(es) sent successfully to {dest_name} 🎉")
        except Exception:
            logger.exception("Failed to send success message to owner.")
    return True
