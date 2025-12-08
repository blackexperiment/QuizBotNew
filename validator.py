# validator.py
import re
from typing import Dict, Any, List

def validate_and_parse(text: str) -> Dict[str, Any]:
    """
    Returns:
    {
      "ok": bool,
      "errors": [...],
      "warnings": [...],
      "des": "string or None",
      "questions": [
        {"raw_question": "...", "options": {"A":"...","B":"..."}, "ans":"A", "exp":"..."},
        ...
      ]
    }
    Rules enforced (simple):
    1. DES allowed anywhere (but NOT inside a Q block)
    2. Eg(...) lines ignored entirely
    3. Q must have A-D
    4. ANS must be one of A-D
    5. EXP optional (must come after ANS if present)
    6. No extra strict checks
    """
    errors: List[str] = []
    warnings: List[str] = []
    des_text = None
    questions = []

    lines = text.splitlines()
    # Normalize trailing spaces
    lines = [ln.rstrip() for ln in lines]

    in_q = False
    q_lines = []
    current_q_line_no = 0

    # Helper to finish a question block
    def finish_q(block_lines, line_no_start):
        if not block_lines:
            return
        # block_lines include Q: ... then option lines and maybe ANS: and EXP:
        qraw = []
        options = {}
        ans = None
        exp = None
        qtitle = ""
        for ln in block_lines:
            if ln.strip().startswith("Q:"):
                qtitle = ln.split("Q:",1)[1].strip()
                qraw.append(ln)
            elif ln.strip().startswith("A:"):
                options['A'] = ln.split("A:",1)[1].strip()
            elif ln.strip().startswith("B:"):
                options['B'] = ln.split("B:",1)[1].strip()
            elif ln.strip().startswith("C:"):
                options['C'] = ln.split("C:",1)[1].strip()
            elif ln.strip().startswith("D:"):
                options['D'] = ln.split("D:",1)[1].strip()
            elif ln.strip().startswith("ANS:"):
                ans = ln.split("ANS:",1)[1].strip()
                if ans:
                    ans = ans.split()[0]  # take first token if extra text
            elif ln.strip().startswith("EXP:"):
                exp = ln.split("EXP:",1)[1].strip()
            else:
                # additional lines appended to question text or ignored
                if qtitle == "":
                    qtitle = ln.strip()
                else:
                    qtitle += "\n" + ln.strip()
            qraw.append(ln)
        # Basic validation
        qindex = len(questions) + 1
        if 'A' not in options or 'B' not in options or 'C' not in options or 'D' not in options:
            errors.append(f"⚠️ Question {qindex} missing required options A-D (line ~{line_no_start}).")
            return
        if not ans:
            errors.append(f"⚠️ Missing ANS in Question {qindex}.")
            # still add partial question (so user can see what's wrong)
            questions.append({"raw_question": qtitle, "options": options, "ans": None, "exp": exp})
            return
        ans_up = ans.strip().upper()
        if ans_up not in options:
            errors.append(f"⚠️ ANS \"{ans}\" does not match options A-D in Question {qindex}.")
            questions.append({"raw_question": qtitle, "options": options, "ans": ans_up, "exp": exp})
            return
        # all good
        questions.append({"raw_question": qtitle, "options": options, "ans": ans_up, "exp": exp})

    # Parse lines
    for idx, ln in enumerate(lines, start=1):
        s = ln.strip()
        if not s:
            # blank line ends a Q block
            if in_q:
                finish_q(q_lines, current_q_line_no)
                q_lines = []
                in_q = False
            continue

        # Ignore Eg( anywhere
        if "Eg(" in s or s.startswith("Eg("):
            continue

        # DES detection: line that starts with 'DES:'
        if s.upper().startswith("DES:"):
            # If currently inside a Q block -> error
            if in_q:
                # DES inside question block: error
                errors.append(f"⚠️ DES found inside question block (Question {len(questions)+1}) at line {idx}.")
            else:
                # if des_text is not set, set des_text (first occurrence)
                if des_text is None:
                    des_text = s.split("DES:",1)[1].strip()
                else:
                    # additional DES occurrences: append
                    des_text += "\n" + s.split("DES:",1)[1].strip()
            continue

        # Q start
        if s.startswith("Q:"):
            # If already in q, finish previous
            if in_q and q_lines:
                finish_q(q_lines, current_q_line_no)
                q_lines = []
            in_q = True
            current_q_line_no = idx
            q_lines.append(ln)
            continue

        # If in question capture lines
        if in_q:
            q_lines.append(ln)
            continue

        # Any other top-level text: could be stray text; treat as DES if no DES present
        if des_text is None:
            # treat as ad-hoc DES line
            des_text = s
        else:
            # extra text outside Q blocks—ignored
            continue

    # At end, if in_q still open, finish
    if in_q and q_lines:
        finish_q(q_lines, current_q_line_no)

    ok = len(errors) == 0
    return {"ok": ok, "errors": errors, "warnings": warnings, "des": des_text, "questions": questions}
