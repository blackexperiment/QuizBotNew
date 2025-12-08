# validator.py
import re
from typing import Dict, Any, List, Optional

Q_RE = re.compile(r'^\s*Q\s*:\s*(.+)', re.IGNORECASE)
OPT_RE = re.compile(r'^\s*([A-L])\s*:\s*(.+)', re.IGNORECASE)  # allow A-L, but A-D are required
ANS_RE = re.compile(r'^\s*ANS\s*:\s*([A-L])\s*$', re.IGNORECASE)
EXP_RE = re.compile(r'^\s*EXP\s*:\s*(.+)', re.IGNORECASE)
DES_RE = re.compile(r'^\s*DES\s*:\s*(.+)', re.IGNORECASE)
EG_RE = re.compile(r'^\s*Eg\(', re.IGNORECASE)

def validate_and_parse(text: str) -> Dict[str, Any]:
    """
    Very permissive parser per your rules:
    1) DES allowed anywhere but not inside a Q block (we error if found inside)
    2) Eg(...) lines ignored
    3) Q must have at least A-D
    4) ANS must be one of provided options
    5) EXP optional
    6) No strict checking of text content inside blocks
    Returns:
    {
      "ok": bool,
      "errors": [...],
      "warnings": [...],
      "des": str|None,
      "des_pos": "before"|"after"|None,
      "questions": [ { "raw_question": str, "options": {"A":...}, "ans": "A", "exp": str|None }, ... ]
    }
    """
    lines = text.splitlines()
    # strip and remove Eg(...) lines
    filtered = []
    for ln in lines:
        if EG_RE.search(ln):
            continue
        filtered.append(ln.rstrip())

    lines = filtered

    errors: List[str] = []
    warnings: List[str] = []
    des_lines = []  # tuples (line_no, text)
    qblocks = []
    current = None
    line_no = 0

    for ln in lines:
        line_no += 1
        if not ln.strip():
            continue

        m = DES_RE.match(ln)
        if m:
            des_lines.append((line_no, m.group(1).strip()))
            continue

        m = Q_RE.match(ln)
        if m:
            # start new question block
            if current:
                qblocks.append(current)
            current = {
                "start_line": line_no,
                "raw_question": m.group(1).strip(),
                "options": {},
                "ans": None,
                "exp": None,
                "lines": [ln]
            }
            continue

        if current is None:
            # text outside Q and DES ignored
            continue

        # inside a question block
        m = OPT_RE.match(ln)
        if m:
            label = m.group(1).upper()
            txt = m.group(2).strip()
            # warn if duplicate
            if label in current["options"]:
                warnings.append(f"⚠️ Duplicate option {label} in Question starting line {current['start_line']}. Overwriting.")
            current["options"][label] = txt
            continue

        m = ANS_RE.match(ln)
        if m:
            current["ans"] = m.group(1).upper()
            continue

        m = EXP_RE.match(ln)
        if m:
            current["exp"] = m.group(1).strip()
            continue

        # treat as continuation: attach to last option if exists else to question
        if current:
            if current["options"]:
                # append to last option (sorted by label order)
                last_label = sorted(current["options"].keys())[-1]
                current["options"][last_label] = current["options"][last_label] + " " + ln.strip()
            else:
                current["raw_question"] = current["raw_question"] + " " + ln.strip()
            continue

    if current:
        qblocks.append(current)

    # detect DES position:
    des: Optional[str] = None
    des_pos: Optional[str] = None
    if des_lines:
        first_des_line, first_des_text = des_lines[0]
        last_des_line, last_des_text = des_lines[-1]
        first_q_line = qblocks[0]["start_line"] if qblocks else float('inf')
        last_q_line = qblocks[-1]["start_line"] if qblocks else -1
        if first_des_line < first_q_line:
            des = first_des_text
            des_pos = "before"
        elif last_des_line > last_q_line:
            des = last_des_text
            des_pos = "after"
        else:
            # DES found inside/question area -> error
            errors.append(f"⚠️ DES found inside question block (line {first_des_line})")
            des = first_des_text
            des_pos = None

    parsed_questions = []
    q_index = 0
    for q in qblocks:
        q_index += 1
        opts = q["options"]
        # require A-D
        for label in ("A","B","C","D"):
            if label not in opts:
                errors.append(f"⚠️ Question {q_index} missing option {label}")
        if not q["ans"]:
            errors.append(f"⚠️ Missing ANS in Question {q_index}")
        else:
            if q["ans"] not in opts:
                errors.append(f"⚠️ ANS \"{q['ans']}\" does not match options in Question {q_index}")
        parsed_questions.append({
            "raw_question": q["raw_question"],
            "options": opts,
            "ans": q["ans"],
            "exp": q.get("exp")
        })

    ok = (len(errors) == 0)
    return {
        "ok": ok,
        "errors": errors,
        "warnings": warnings,
        "des": des,
        "des_pos": des_pos,
        "questions": parsed_questions
    }
