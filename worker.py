# worker.py
import os
import time
import logging
import json
import uuid

logger = logging.getLogger("quizbot.worker")

# envs and defaults
POLL_DELAY_SHORT = float(os.environ.get("POLL_DELAY_SHORT", "1"))
POLL_DELAY_LONG = float(os.environ.get("POLL_DELAY_LONG", "2"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
SEQUENTIAL_FAIL_ABORT = int(os.environ.get("SEQUENTIAL_FAIL_ABORT", "3"))

# TARGET_CHATS format: Name:id,Name2:id2
def parse_target_chats_env():
    raw = os.environ.get("TARGET_CHATS", "")
    result = []
    if not raw:
        return result
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for p in parts:
        if ":" in p:
            name, _, idstr = p.partition(":")
            name = name.strip()
            try:
                cid = int(idstr.strip())
            except Exception:
                continue
            result.append({"name": name, "id": cid})
    return result

TARGET_CHATS = parse_target_chats_env()

def post_quiz_events(bot, chat_id: int, events: list, is_anonymous: bool, owner_id: int=None, chat_name: str=None):
    """
    Posts events in the order they appear.
    events: list of {"type":"des", "text":...} or {"type":"question", ...}
    Returns True on overall success, False otherwise.
    """
    sent_count = 0
    sequential_failures = 0

    for ev in events:
        if ev["type"] == "des":
            # send as plain message
            text = ev.get("text", "")
            if text is None:
                text = ""
            tries = 0
            while True:
                try:
                    bot.send_message(chat_id=chat_id, text=text)
                    break
                except Exception as e:
                    tries += 1
                    logger.exception("Failed sending DES to %s (try %d): %s", chat_id, tries, e)
                    if tries >= MAX_RETRIES:
                        sequential_failures += 1
                        break
                    time.sleep(POLL_DELAY_SHORT)
            if sequential_failures >= SEQUENTIAL_FAIL_ABORT:
                logger.error("Too many sequential failures, aborting posting.")
                return False
            # small delay
            time.sleep(POLL_DELAY_SHORT)
            continue

        if ev["type"] == "question":
            q = ev
            question_text = q.get("raw_question", "") or ""
            options_map = q.get("options", {})
            # Create list of options in alphabetical order
            labels = sorted(options_map.keys())
            # Ensure deterministic ordering by sorting labels A,B,C...
            opts_list = []
            label_to_index = {}
            for i, lab in enumerate(labels):
                opts_list.append(options_map[lab])
                label_to_index[lab] = i

            ans_label = q.get("ans")
            correct_option_id = None
            if ans_label and ans_label in label_to_index:
                correct_option_id = label_to_index[ans_label]

            explanation = q.get("exp") or None

            # retries
            tries = 0
            posted = False
            while tries < MAX_RETRIES and not posted:
                try:
                    # send_poll: using send_poll with type='quiz' sets correct_option_id
                    # If explanation present, use explanation argument.
                    # PTB v13 supports send_poll(explanation=...) in newer versions; guard with kwargs.
                    kwargs = {
                        "chat_id": chat_id,
                        "question": question_text,
                        "options": opts_list,
                        "is_anonymous": bool(is_anonymous),
                        "type": "quiz"
                    }
                    if correct_option_id is not None:
                        kwargs["correct_option_id"] = correct_option_id
                    if explanation:
                        # some PTB versions accept 'explanation' name; otherwise put into 'explanation' if supported.
                        kwargs["explanation"] = explanation

                    bot.send_poll(**kwargs)
                    posted = True
                    sent_count += 1
                except TypeError as te:
                    # maybe this PTB doesn't accept 'explanation' kw; retry without explanation but try to send explanation as message after poll
                    logger.warning("send_poll TypeError (maybe explanation unsupported): %s", te)
                    try:
                        # remove explanation and retry
                        _kwargs = dict(kwargs)
                        _kwargs.pop("explanation", None)
                        bot.send_poll(**_kwargs)
                        posted = True
                        sent_count += 1
                        # send explanation as a follow-up message
                        if explanation:
                            bot.send_message(chat_id=chat_id, text=f"💡 Explanation: {explanation}")
                    except Exception as e2:
                        tries += 1
                        logger.exception("Failed to send poll (try %d): %s", tries, e2)
                        time.sleep(POLL_DELAY_SHORT)
                except Exception as e:
                    tries += 1
                    logger.exception("Failed to send poll (try %d): %s", tries, e)
                    time.sleep(POLL_DELAY_SHORT)
            if not posted:
                sequential_failures += 1
                logger.error("Failed to post question after %d attempts.", MAX_RETRIES)
            else:
                sequential_failures = 0

            if sequential_failures >= SEQUENTIAL_FAIL_ABORT:
                logger.error("Too many sequential failures during posting, aborting.")
                return False

            # spacing delay after each poll
            time.sleep(POLL_DELAY_SHORT)

    # All events processed; notify owner with one confirmation message (done by caller normally)
    return True


# Utility to pretty map chat id to name using TARGET_CHATS env helper
def find_chat_name(chat_id: int):
    for c in TARGET_CHATS:
        if c["id"] == chat_id:
            return c["name"]
    return str(chat_id)
