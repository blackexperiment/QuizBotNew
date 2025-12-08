# worker.py
import time
import logging
from typing import List, Dict, Optional

from telegram import Bot
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError

logger = logging.getLogger("quizbot.worker")

def _safe_send_poll(bot: Bot, chat_id: int, question: str, options: List[str], correct_option_id: int, explanation: Optional[str]=None, is_anonymous: bool=True, quiz: bool=True):
    """
    send_poll with retry-on-RetryAfter and basic backoff.
    correct_option_id is index (0..)
    """
    max_attempts = 4
    attempt = 0
    while attempt < max_attempts:
        try:
            if explanation:
                # python-telegram-bot up to v13 doesn't have explanation arg for send_poll,
                # so we send poll and then edit... but many bots simply send poll; Telegram UI shows explanation in quiz mode if provided via sendPoll's "explanation" field (API). PTB may not expose in older versions.
                # We'll try to pass explanation (kwargs) safely; if PTB ignores it, fine.
                poll = bot.send_poll(chat_id=chat_id, question=question, options=options, type='quiz' if quiz else 'regular',
                                     correct_option_id=correct_option_id, is_anonymous=is_anonymous, explanation=explanation)
            else:
                poll = bot.send_poll(chat_id=chat_id, question=question, options=options, type='quiz' if quiz else 'regular',
                                     correct_option_id=correct_option_id, is_anonymous=is_anonymous)
            return True
        except RetryAfter as e:
            retry = int(getattr(e, "retry_after", 1))
            logger.warning("RetryAfter received; sleeping %s seconds (attempt %d/%d)", retry, attempt+1, max_attempts)
            time.sleep(retry)
            attempt += 1
            continue
        except (TimedOut, NetworkError) as e:
            # transient network error — exponential backoff
            sleep_for = min(2 ** attempt, 30)
            logger.warning("Network error sending poll: %s. Backing off %s s", e, sleep_for)
            time.sleep(sleep_for)
            attempt += 1
            continue
        except TelegramError as e:
            # non-retryable or unknown Telegram error
            logger.exception("TelegramError sending poll: %s", e)
            return False
        except Exception as e:
            logger.exception("Unexpected error sending poll: %s", e)
            return False
    logger.error("Exceeded max attempts sending poll.")
    return False


def post_quiz_questions(bot: Bot, target_chat: int, title: Optional[str], questions: List[Dict], notify_owner_id: Optional[int] = None) -> bool:
    """
    posts DES (title) if present, then posts questions as polls.
    Applies safe delay: <=50 questions -> 1s, >50 -> 2s
    Returns True on success, False on failure.
    Notifies owner on repeated failures.
    """
    try:
        if title:
            try:
                bot.send_message(chat_id=target_chat, text=title)
            except Exception:
                logger.exception("Failed to post DES/title to target chat.")

        total_q = len(questions)
        delay = 1 if total_q <= 50 else 2

        failed_questions = []
        for idx, q in enumerate(questions, start=1):
            q_text = q["raw_question"]
            # options are dict {label: text} — order by label
            opts = [q["options"][lbl] for lbl in sorted(q["options"].keys())]
            ans_letter = q.get("ans")
            # convert ans_letter A/B/C... to index 0-based according to sorted labels
            labels_sorted = sorted(q["options"].keys())
            try:
                correct_index = labels_sorted.index(ans_letter)
            except ValueError:
                correct_index = 0  # fallback to 0
            exp = q.get("exp")

            # Attempt send poll
            ok = _safe_send_poll(bot, target_chat, q_text, opts, correct_index, explanation=exp, is_anonymous=False, quiz=True)
            if not ok:
                failed_questions.append((idx, q_text))
                # retry logic: try a couple of times per question with exponential backoff (handled by _safe_send_poll)
            time.sleep(delay)

        if failed_questions:
            # notify owner
            msg_lines = [f"❌ Multiple failures detected while posting quiz to {target_chat}:"]
            for fi in failed_questions[:10]:
                msg_lines.append(f"- Q#{fi[0]} failed")
            body = "\n".join(msg_lines)
            logger.error(body)
            if notify_owner_id:
                try:
                    bot.send_message(notify_owner_id, body)
                except Exception:
                    logger.exception("Failed to notify owner about failed questions.")
            return False

        # success notify owner
        if notify_owner_id:
            try:
                bot.send_message(notify_owner_id, f"✅ {total_q} quiz(es) sent successfully to {target_chat} 🎉")
            except Exception:
                logger.exception("Failed to notify owner about success.")
        return True
    except Exception:
        logger.exception("Uncaught error in post_quiz_questions")
        if notify_owner_id:
            try:
                bot.send_message(notify_owner_id, "❌ Job failed due to internal error. See logs.")
            except Exception:
                pass
        return False
