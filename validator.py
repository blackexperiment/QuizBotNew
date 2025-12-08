# validator.py
import re
from typing import List, Dict, Any, Tuple

EG_PATTERN = re.compile(r'^\s*Eg\(', re.IGNORECASE)
DES_PATTERN = re.compile(r'^\s*DES\s*:\s*(.*)', re.IGNORECASE)
Q_PATTERN = re.compile(r'^\s*Q\s*:\s*(.*)', re.IGNORECASE)
OPT_PATTERN = re.compile(r'^\s*([A-Z])\s*:\s*(.*)')  # A: text
ANS_PATTERN = re.compile(r'^\s*ANS\s*:\s*([A-Z])\s*$', re.IGNORECASE)
EXP_PATTERN = re.compile(r'^\s*EXP\s*:\s*(.*)', re.IGNORECASE)

def _strip_trailing_colon_text(s: str) -> str:
    return s.strip()

def validate_and_parse(text: str) -> Dict[str, Any]:
    """
    Very simple validator/parser following the rules:
    1) DES allowed anywhere (but not inside a question block); record its position relative to Q blocks.
    2) Ignore any line starting with Eg(
    3) Q blocks must have at least A-D
    4) ANS must be one of present options
    5) EXP optional
    6) Keep checks minimal - no heavy validation of option contents
    """
    lines = text.splitlines()
    errors: List[str] = []
    warnings: List[str] = []
    des_entries: List[Dict[str, Any]] = []
    questions: List[Dict[str, Any]] = []

    current_q = None
    current_opts = {}
    current_ans = None
    current_exp = None
    last_des_text = None
    last_was_des = False

    def finalize_question():
        nonlocal current_q, current_opts, current_ans, current_exp
        if current_q is None:
            return
        # minimal checks
        missing_opts = [o for o in ("A", "B", "C", "D") if o not in current_opts]
        if missing_opts:
            errors.append(f"⚠️ Question {len(questions)+1} missing options: {', '.join(missing_opts)}")
        if not current_ans:
            errors.append(f"⚠️ Missing ANS in Question {len(questions)+1}")
        else:
            if current_ans not in current_opts:
                errors.append(f'⚠️ ANS "{current_ans}" does not match any option in Question {len(questions)+1}')
        questions.append({
            "raw_question": current_q,
            "options": dict(current_opts),
            "ans": current_ans,
            "exp": current_exp
        })
        current_q = None
        current_opts = {}
        current_ans = None
        current_exp = None

    # We'll track DES positions relative to question index.
    # If a DES appears and there's no current question started yet, it's a "before question 0" (global before)
    # If DES appears immediately before a Q line -> mark as before that question index.
    # If DES appears after finishing a question and before next Q -> mark as after that question index.

    q_index = 0
    des_buffer = []  # track DES texts until assigned
    in_question_block = False

    for lineno, raw in enumerate(lines, start=1):
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        if EG_PATTERN.match(line):
            # ignore anything starting with Eg(
            continue
        m_des = DES_PATTERN.match(line)
        if m_des:
            txt = m_des.group(1).strip()
            # record raw DES with lineno, we'll assign position later
            des_buffer.append((lineno, txt))
            last_was_des = True
            continue

        m_q = Q_PATTERN.match(line)
        if m_q:
            # If there was an open question, finalize it before starting new
            if current_q is not None:
                # finalize previous question
                finalize_question()
                q_index += 1

            # assign any DESs that are in des_buffer to be "before" this new question
            for (dline, dtext) in des_buffer:
                des_entries.append({
                    "lineno": dline,
                    "text": dtext,
                    "pos": "before",
                    "q_index": len(questions)  # this question will be next
                })
            des_buffer = []
            current_q = m_q.group(1).strip()
            in_question_block = True
            last_was_des = False
            continue

        m_opt = OPT_PATTERN.match(line)
        if m_opt and in_question_block:
            label = m_opt.group(1).strip().upper()
            val = m_opt.group(2).strip()
            # Accept forms like "(A) text" or "A) text" inside val — do not over-normalize, keep as-is.
            # If duplicate label, warn + overwrite
            if label in current_opts:
                warnings.append(f"⚠️ Duplicate option {label} in Question {len(questions)+1} (line {lineno}). Overwriting.")
            current_opts[label] = val
            continue

        m_ans = ANS_PATTERN.match(line)
        if m_ans and in_question_block:
            current_ans = m_ans.group(1).strip().upper()
            continue

        m_exp = EXP_PATTERN.match(line)
        if m_exp and in_question_block:
            current_exp = m_exp.group(1).strip()
            continue

        # If we reached a DES-like line inside an open question block, that's invalid per your rule
        if DES_PATTERN.match(line) and in_question_block:
            errors.append(f"⚠️ DES found inside question block (Question {len(questions)+1}) at line {lineno}.")
            continue

        # If line doesn't match anything and we're not in a question, treat as stray text (could be extra DES-like)
        # We'll ignore stray lines that aren't DES/Q/A/ANS/EXP/Eg
        # But if in question and line is unrecognized, append to current question text (raw_question) to be lenient
        if in_question_block and current_q is not None:
            # append to question body for safety
            current_q = current_q + " " + line.strip()
            continue

        # else ignore
        continue

    # finalize last question
    if current_q is not None:
        finalize_question()
        q_index = len(questions)

    # Any remaining des_buffer -> assign as "after last question" (pos: after, q_index = last index-1)
    for (dline, dtext) in des_buffer:
        # if there are questions, attach as after the last question; else attach as global before (q_index 0 with pos before)
        if questions:
            des_entries.append({
                "lineno": dline,
                "text": dtext,
                "pos": "after",
                "q_index": len(questions)-1
            })
        else:
            des_entries.append({
                "lineno": dline,
                "text": dtext,
                "pos": "before",
                "q_index": 0
            })

    # Build combined des (all DES concatenated) for backward compatibility
    combined_des = None
    if des_entries:
        combined_des = "\n".join([d["text"] for d in des_entries])

    ok = len(errors) == 0

    return {
        "ok": ok,
        "errors": errors,
        "warnings": warnings,
        "des_entries": des_entries,
        "des": combined_des,
        "questions": questions
    }
