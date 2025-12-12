# sender.py
import asyncio
import os
import time
from typing import List, Dict
THROTTLE = float(os.getenv("THROTTLE_SECONDS", "2"))

async def send_actions_to_chat(bot, chat_id: int, actions: List[Dict], job_id: int = None):
    """
    actions: list of action dicts (same format from parse_bulk)
    Returns dict summary: {'sent': n, 'failed': n, 'errors': [...]}
    """
    sent = 0
    failed = 0
    errors = []
    for idx, action in enumerate(actions, start=1):
        attempts = 0
        success = False
        while attempts < 3 and not success:
            try:
                if action["type"] == "MSG":
                    text = action["text"]
                    await bot.send_message(chat_id, text)
                elif action["type"] == "POLL":
                    q = action["question"]
                    opts = action["options"]
                    # if answer_index present -> quiz
                    answer_index = action.get("answer_index")
                    explanation = action.get("explanation")
                    if answer_index is not None:
                        # quiz mode
                        await bot.send_poll(chat_id, q, opts, is_anonymous=False, type='quiz', correct_option_id=answer_index, explanation=explanation or "")
                    else:
                        await bot.send_poll(chat_id, q, opts, is_anonymous=False)
                else:
                    # unknown type - skip
                    break
                success = True
                sent += 1
            except Exception as e:
                attempts += 1
                last_err = str(e)
                await asyncio.sleep(2 ** attempts)  # exponential backoff
        if not success:
            failed += 1
            errors.append({"index": idx, "error": last_err})
            # Per chosen rule: abort job on failure
            return {"sent": sent, "failed": failed, "errors": errors}
        # throttle
        await asyncio.sleep(THROTTLE)
    return {"sent": sent, "failed": failed, "errors": errors}
