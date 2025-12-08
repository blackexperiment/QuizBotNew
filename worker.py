# worker.py
import time
import logging
import re
from typing import List, Dict, Any, Optional

logger = logging.getLogger("quizbot.worker")


def _send_with_retries(bot, method_name: str, max_retries: int = 4, *args, **kwargs):
    """
    Generic helper to call bot methods with retry handling for RetryAfter (429) and transient errors.
    Returns the method result if successful, else raises the last exception.
    """
    attempt = 0
    backoff = 1.0
    while True:
        attempt += 1
        try:
            method = getattr(bot, method_name)
            return method(*args, **kwargs)
        except Exception as e:
            # Try to detect RetryAfter
            retry_after = getattr(e, "retry_after", None)
            if retry_after is None:
                m = re.search(r"retry after (\d+)", str(e), flags=re.IGNORECASE)
                if m:
                    try:
                        retry_after = int(m.group(1))
                    except Exception:
                        retry_after = None

            if retry_after:
                logger.warning("API asked to retry after %s seconds (attempt %s/%s). Sleeping...", retry_after, attempt, max_retries)
                time.sleep(int(retry_after) + 1)
            else:
                logger.exception("Transient error calling %s (attempt %s/%s): %s", method_name, attempt, max_retries, e)
                if attempt >= max_retries:
                    logger.error("Exceeded retries for %s", method_name)
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)


def _options_list_from_question(qobj: Dict[str, Any]) -> List[str]:
    labels = sorted(qobj.get("options", {}).keys())
    return [qobj["options"][lbl] for lbl in labels if lbl in qobj["options"]]


def _correct_option_index(qobj: Dict[str, Any]) -> int:
    ans = qobj.get("ans")
    if not ans:
        return 0
    labels = sorted(qobj.get("options", {}).keys())
    try:
        return labels.index(ans.upper())
    except Exception:
        return 0


def post_quiz_sequence(
    bot,
    chat_id: int,
    sequence: List[Dict[str, Any]],
    anonymous: bool,
    owner_id: Optional[int] = None,
    chat_name: Optional[str] = None,
    poll_delay_short: int = 1,
    poll_delay_long: int = 2,
    max_retries: int = 4,
    sequential_fail_abort: int = 3,
) -> bool:
    """
    Posts a parsed sequence (validator-produced) into target chat.
    sequence: list of items with type 'des' or 'question'
    Returns True on success, False on abort/failure.
    """
    questions_sent = 0
    consecutive_failures = 0
    total_questions = len([x for x in sequence if x.get("type") == "question"])

    for idx, item in enumerate(sequence, start=1):
        try:
            if item.get("type") == "des":
                text = item.get("text", "")
                if text:
                    logger.info("Sending DES (at sequence pos %s) to %s", idx, chat_id)
                    _send_with_retries(bot, "send_message", max_retries=max_retries, chat_id=chat_id, text=text)
                continue

            if item.get("type") == "question":
                q = item.get("question", {})
                options = _options_list_from_question(q)
                if not options:
                    logger.warning("Skipping question with no options: %s", q.get("raw_question"))
                    continue

                correct_idx = _correct_option_index(q)
                question_text = q.get("raw_question") or q.get("question") or "Question"

                logger.info("Posting poll to %s: %s", chat_id, question_text[:60])
                # send_poll -> returns Message object
                poll_message = _send_with_retries(
                    bot,
                    "send_poll",
                    max_retries=max_retries,
                    chat_id=chat_id,
                    question=question_text,
                    options=options,
                    is_anonymous=bool(anonymous),
                    type="quiz",
                    correct_option_id=int(correct_idx),
                    disable_notification=False,
                )

                questions_sent += 1
                consecutive_failures = 0

                # If EXP present, send as follow-up message (reply to poll) so explanation is available.
                exp_text = q.get("exp")
                if exp_text:
                    try:
                        reply_to = None
                        # poll_message may be None or a Message; try to get message_id to reply to
                        if hasattr(poll_message, "message_id"):
                            reply_to = getattr(poll_message, "message_id")
                        _send_with_retries(
                            bot,
                            "send_message",
                            max_retries=max_retries,
                            chat_id=chat_id,
                            text=f"💡 Explanation: {exp_text}",
                            reply_to_message_id=reply_to,
                        )
                    except Exception:
                        logger.exception("Failed to send EXP follow-up for question %s", question_text[:40])

                # apply delay rule
                if total_questions <= 50:
                    time.sleep(poll_delay_short)
                else:
                    time.sleep(poll_delay_long)
                continue

            # unknown item type -> log and continue
            logger.warning("Unknown sequence item (skipped): %s", item)
        except Exception as e:
            logger.exception("Failed to post sequence item at index %s: %s", idx, e)
            consecutive_failures += 1
            if consecutive_failures >= sequential_fail_abort:
                logger.error("Consecutive failures >= %s ; aborting job", sequential_fail_abort)
                # notify owner
                if owner_id:
                    try:
                        bot.send_message(owner_id, f"❌ Multiple failures detected. Aborting quiz job for {chat_name or chat_id}.")
                    except Exception:
                        logger.exception("Failed to notify owner after abort")
                return False
            # small backoff before continuing
            time.sleep(min(2 ** consecutive_failures, 30))

    # finished all items
    if owner_id:
        try:
            bot.send_message(owner_id, f"✅ {questions_sent} quiz(es) sent successfully to {chat_name or chat_id} 🎉")
        except Exception:
            logger.exception("Failed to notify owner on success.")
    return True
