# worker.py
import time
import logging
import requests
from typing import List, Dict, Any

logger = logging.getLogger("quizbot.worker")

def _send_with_retries_send(bot, method_name: str, *args, max_retries: int = 4, **kwargs):
    """
    Generic helper to call bot methods with retry handling for RetryAfter (429) and transient errors.
    method_name is attribute on bot (like 'send_message' or 'send_poll').
    Returns the method's result or raises after exhausting retries.
    """
    attempt = 0
    backoff = 1.0
    while True:
        attempt += 1
        try:
            method = getattr(bot, method_name)
            return method(*args, **kwargs)
        except Exception as e:
            # look for RetryAfter as attribute on exception (telegram raises RetryAfter with .retry_after)
            # some libs embed headers in requests.exceptions.HTTPError - try to detect RetryAfter
            retry_after = None
            # common python-telegram-bot RetryAfter
            try:
                retry_after = getattr(e, "retry_after", None)
            except Exception:
                retry_after = None

            # Sometimes the exception message contains "Too Many Requests: retry after X"
            if retry_after is None:
                msg = str(e)
                import re
                m = re.search(r"retry after (\d+)", msg, flags=re.IGNORECASE)
                if m:
                    try:
                        retry_after = int(m.group(1))
                    except Exception:
                        retry_after = None

            if retry_after:
                logger.warning("Got RetryAfter=%s from API, sleeping then retrying (attempt %s/%s)",
                               retry_after, attempt, max_retries)
                time.sleep(int(retry_after) + 1)
            else:
                logger.exception("Transient error calling %s (attempt %s/%s): %s", method_name, attempt, max_retries, e)
                if attempt >= max_retries:
                    logger.error("Exceeded retries for %s", method_name)
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

def _options_list_from_question(qobj: Dict[str, Any]) -> List[str]:
    # options are keys like "A","B",... preserve alphabetical order
    labels = sorted(qobj["options"].keys())
    return [qobj["options"][lbl] for lbl in labels]

def _correct_option_index(qobj: Dict[str, Any]) -> int:
    # returns index (0-based) of correct option or 0 as safe fallback
    ans = qobj.get("ans")
    if not ans:
        return 0
    labels = sorted(qobj["options"].keys())
    try:
        return labels.index(ans.upper())
    except Exception:
        return 0

def post_quiz_sequence(bot, chat_id: int, sequence: List[Dict[str, Any]], anonymous: bool,
                       owner_id: Optional[int] = None, chat_name: Optional[str] = None,
                       poll_delay_short: int = 1, poll_delay_long: int = 2,
                       max_retries: int = 4, sequential_fail_abort: int = 3) -> bool:
    """
    Posts a parsed sequence (from validator.validate_and_parse) into target chat.
    sequence: list of {"type":"des","text":...} or {"type":"question","question":{...}}
    anonymous: boolean for polls' is_anonymous param
    Returns True on success, False on failure.
    """
    questions_sent = 0
    consecutive_failures = 0
    total_questions = len([x for x in sequence if x.get("type") == "question"])

    for item in sequence:
        try:
            if item.get("type") == "des":
                text = item.get("text", "")
                # send the DES as normal message to chat_id
                _send_with_retries_send(bot, "send_message", chat_id=chat_id, text=text, max_retries=max_retries)
                continue

            if item.get("type") == "question":
                q = item.get("question", {})
                opts = _options_list_from_question(q)
                correct_idx = _correct_option_index(q)
                # Telegram requires options as list of strings
                # create_poll arguments: chat_id, question, options, is_anonymous, allows_multiple_answers (False), type='quiz' etc
                # python-telegram-bot's send_poll uses: send_poll(chat_id, question, options, **kwargs)
                # We will use send_poll with type='quiz' and correct_option_id
                question_text = q.get("raw_question") or "Question"
                # send poll with retries
                _send_with_retries_send(
                    bot,
                    "send_poll",
                    chat_id=chat_id,
                    question=question_text,
                    options=opts,
                    is_anonymous=bool(anonymous),
                    type="quiz",
                    correct_option_id=int(correct_idx),
                    disable_notification=False,
                    max_retries=max_retries
                )
                questions_sent += 1
                consecutive_failures = 0
                # apply delay logic
                if total_questions <= 50:
                    time.sleep(poll_delay_short)
                else:
                    time.sleep(poll_delay_long)
                continue

            # unknown item type -> just skip
            logger.warning("Unknown sequence item type: %s", item)
        except Exception as e:
            logger.exception("Failed to post item: %s", e)
            consecutive_failures += 1
            # On repeated failures abort and notify owner (if provided)
            if consecutive_failures >= sequential_fail_abort:
                if owner_id:
                    try:
                        bot.send_message(owner_id, f"❌ Multiple failures detected while posting to {chat_name or chat_id}. Aborting job.")
                    except Exception:
                        logger.exception("Failed to notify owner about abort")
                return False
            # otherwise continue loop (we've already counted failure)
            # small backoff
            time.sleep(2 ** consecutive_failures)

    # Completed all items
    if owner_id:
        try:
            bot.send_message(owner_id, f"✅ {questions_sent} quiz(es) sent successfully to {chat_name or chat_id} 🎉")
        except Exception:
            logger.exception("Failed to notify owner on success.")

    return True
