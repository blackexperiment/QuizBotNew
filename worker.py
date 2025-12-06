import time
import json
import math
import os
import traceback
from telegram import Bot
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest
from db import get_next_pending_post, mark_post_posted, increment_attempt, mark_post_failed, get_job, set_job_status, get_progress
from typing import Optional

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
bot = Bot(token=TELEGRAM_BOT_TOKEN)

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))  # additional retries (total attempts = 1 + MAX_RETRIES)
SEQUENTIAL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))
POLL_DELAY_SHORT = float(os.environ.get("POLL_DELAY_SHORT", "1.0"))
POLL_DELAY_LONG = float(os.environ.get("POLL_DELAY_LONG", "2.0"))
PROGRESS_STEP = int(os.environ.get("PROGRESS_STEP", "10"))

def compute_delay(total_questions: int) -> float:
    return POLL_DELAY_SHORT if total_questions <= 50 else POLL_DELAY_LONG

def post_job(job_id: str, owner_id: int):
    """
    Process job until completion or abort.
    """
    job = get_job(job_id)
    if not job:
        return
    total = job["total_questions"]
    target = job["target_chat"]
    set_job_status(job_id, "running")
    delay = compute_delay(total)
    consecutive_failures = 0
    posted_count = 0
    # initial notify
    bot.send_message(chat_id=owner_id, text=f"🚀 Posting quiz '{job['title']}' to the selected chat. Total questions: {total}")

    while True:
        row = get_next_pending_post(job_id)
        if not row:
            break
        q_index = row["q_index"]
        question = row["question"]
        options = json.loads(row["options"])
        correct_letter = row["correct_letter"]
        letters = sorted(options.keys())
        opts_list = [options[l] for l in letters]
        try:
            # attempts logic
            attempt = row["attempts"] + 1
            # determine correct index
            correct_index = letters.index(correct_letter)
            resp = bot.send_poll(chat_id=target,
                                 question=question,
                                 options=opts_list,
                                 is_anonymous=False,
                                 type="quiz",
                                 correct_option_id=correct_index)
            # success
            mark_post_posted(job_id, q_index, resp.message_id)
            consecutive_failures = 0
            posted_count += 1
            # after posting, optional progress update
            if posted_count % PROGRESS_STEP == 0 or posted_count == total:
                bot.send_message(chat_id=owner_id, text=f"⏳ Progress: {posted_count}/{total} sent...")
            time.sleep(delay)
        except RetryAfter as e:
            # honor RetryAfter
            wait = int(math.ceil(e.retry_after))
            bot.send_message(chat_id=owner_id, text=f"⚠️ Rate limited by Telegram. Waiting {wait}s before retry...")
            time.sleep(wait)
            increment_attempt(job_id, q_index, last_error=f"RetryAfter:{wait}")
            consecutive_failures += 1
        except (TimedOut, NetworkError) as e:
            # transient network
            increment_attempt(job_id, q_index, last_error=str(e))
            attempt_count = row["attempts"] + 1
            backoff = min(60, 2 ** attempt_count)
            bot.send_message(chat_id=owner_id, text=f"⚠️ Network issue posting Q{q_index}. Retrying in {backoff}s (attempt {attempt_count}/{MAX_RETRIES+1})")
            time.sleep(backoff)
            consecutive_failures += 1
        except BadRequest as e:
            # permanent content error (invalid poll)
            errtxt = f"BadRequest: {e}"
            mark_post_failed(job_id, q_index, errtxt)
            bot.send_message(chat_id=owner_id, text=f"❌ Failed to post Q{q_index}: {e}. Aborting job.")
            set_job_status(job_id, "aborted")
            return
        except Exception as e:
            # unknown
            tb = traceback.format_exc()
            increment_attempt(job_id, q_index, last_error=str(e))
            attempt_count = row["attempts"] + 1
            if attempt_count > MAX_RETRIES:
                mark_post_failed(job_id, q_index, str(e))
                bot.send_message(chat_id=owner_id, text=f"❌ Q{q_index} failed after {attempt_count} attempts. Aborting.")
                set_job_status(job_id, "aborted")
                return
            backoff = min(60, 2 ** attempt_count)
            bot.send_message(chat_id=owner_id, text=f"⚠️ Error posting Q{q_index}. Retrying in {backoff}s (attempt {attempt_count}/{MAX_RETRIES+1})")
            time.sleep(backoff)
            consecutive_failures += 1

        # abort if too many consecutive failures
        if consecutive_failures >= SEQUENTIAL_ABORT:
            bot.send_message(chat_id=owner_id, text=f"❌ Multiple failures detected. Aborting quiz job {job_id}.")
            set_job_status(job_id, "aborted")
            return

    set_job_status(job_id, "completed")
    done, total = get_progress(job_id)
    # final summary message to owner
    bot.send_message(chat_id=owner_id, text=f"🎉 Quiz posted! All {done} questions were sent successfully to the selected chat. Send more questions in the same format anytime. ✨")
