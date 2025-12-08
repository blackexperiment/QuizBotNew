# validator.py
import re
from typing import List, Dict, Any

_DES_RE = re.compile(r'^\s*DES:\s*(.*)$', re.IGNORECASE)
_Q_RE = re.compile(r'^\s*Q:\s*(.*)$', re.IGNORECASE)
_OPT_RE = re.compile(r'^\s*([A-Z]):\s*(.*)$', re.IGNORECASE)
_ANS_RE = re.compile(r'^\s*ANS:\s*([A-Z])\s*$', re.IGNORECASE)
_EXP_RE = re.compile(r'^\s*EXP:\s*(.*)$', re.IGNORECASE)
_EG_RE = re.compile(r'(^|\s)Eg\(', re.IGNORECASE)  # contains Eg( -> ignore line

def validate_and_parse(text: str) -> Dict[str, Any]:
    """
    Parse incoming formatted quiz text.

    Rules (minimal):
      - Lines containing Eg( are ignored.
      - DES: allowed anywhere except inside a question block.
        Each DES is recorded and associated to the "next question index" (or after last if no next).
      - Q: begins a question block.
      - Options must include at least A,B,C,D each with some text.
      - ANS: must be present and one of the option labels provided.
      - EXP: optional, must appear after ANS within the same question block.
    Returns:
      {
        "ok": bool,
        "errors": [...],
        "warnings": [...],
        "des": first_des_or_None,
        "des_list": [{"text": "...", "question_index": int, "line": lineno}, ...],
        "questions": [
            {"raw_question": "...", "options": {"A":"...","B":"...","C":"...","D":"..."}, "ans":"A", "exp":"..."},
            ...
        ]
      }
    """
    lines = text.splitlines()
    errors: List[str] = []
    warnings: List[str] = []
    des_list: List[Dict[str, Any]] = []

    questions: List[Dict[str, Any]] = []
    in_q = False
    current_q = None
    line_no = 0

    for raw in lines:
        line_no += 1
        line = raw.rstrip("\n")
        if not line.strip():
            # blank lines are allowed, treat as separator
            # but if we were inside a question and didn't see ANS, we keep waiting
            continue

        # ignore Eg( anywhere in line
        if _EG_RE.search(line):
            continue

        m_des = _DES_RE.match(line)
        if m_des:
            des_text = m_des.group(1).strip()
            if in_q:
                # DES inside Q block -> error
                errors.append(f"⚠️ DES found inside question block (Question {len(questions)+1}) at line {line_no}.")
            else:
                # record DES to be sent before the next question: question_index = len(questions)
                des_list.append({"text": des_text, "question_index": len(questions), "line": line_no})
            continue

        m_q = _Q_RE.match(line)
        if m_q:
            # start a new question block
            if in_q:
                # previous question open but missing ANS => error
                errors.append(f"⚠️ Missing ANS in Question {len(questions)+1} (line ~{line_no}).")
                # finalize previous with what we have (but mark invalid)
                # do not append incomplete question
            in_q = True
            current_q = {
                "raw_question": m_q.group(1).strip(),
                "options": {},
                "ans": None,
                "exp": None,
                "line": line_no
            }
            continue

        # if not in question and line doesn't match Q or DES, treat as extra text - ignore
        if not in_q:
            # ignore stray lines (they may be just text like meta)
            continue

        # inside question block parsing
        m_opt = _OPT_RE.match(line)
        if m_opt:
            label = m_opt.group(1).strip().upper()
            text_opt = m_opt.group(2).strip()
            # Accept only single-letter labels (A-Z)
            if not text_opt:
                errors.append(f"⚠️ Option {label} is empty (Question {len(questions)+1}) at line {line_no}.")
            # overwrite if duplicate but warn
            if label in current_q["options"]:
                warnings.append(f"⚠️ Duplicate option {label} in Question {len(questions)+1} (line {line_no}). Overwriting.")
            current_q["options"][label] = text_opt
            continue

        m_ans = _ANS_RE.match(line)
        if m_ans:
            ans_letter = m_ans.group(1).strip().upper()
            # ANS must be single letter
            if len(ans_letter) != 1:
                errors.append(f"⚠️ Invalid ANS format in Question {len(questions)+1} at line {line_no}.")
            current_q["ans"] = ans_letter
            continue

        m_exp = _EXP_RE.match(line)
        if m_exp:
            # EXP must come after ANS
            if current_q.get("ans") is None:
                errors.append(f"⚠️ EXP must come after ANS (Question {len(questions)+1}) at line {line_no}.")
            else:
                if current_q.get("exp") is not None:
                    errors.append(f"⚠️ Multiple EXP found in Question {len(questions)+1} at line {line_no}.")
                current_q["exp"] = m_exp.group(1).strip()
            continue

        # unknown line inside question -> treat as appended to raw_question (helps flexible formatting)
        # but keep it safe: append to raw question text
        current_q["raw_question"] += " " + line.strip()

    # after processing all lines: finalize any open question
    if in_q and current_q:
        # final checks for last question
        # must have ANS and at least A-D
        labels = set(k.upper() for k in current_q["options"].keys())
        required = {"A", "B", "C", "D"}
        missing_opts = required - labels
        q_index = len(questions) + 1
        if missing_opts:
            errors.append(f"⚠️ Question {q_index} missing options: {', '.join(sorted(missing_opts))}.")
        if not current_q.get("ans"):
            errors.append(f"⚠️ Missing ANS in Question {q_index}.")
        else:
            if current_q["ans"] not in labels:
                errors.append(f"⚠️ ANS \"{current_q['ans']}\" does not match any option in Question {q_index}.")
        # append only if structurally present (even with errors we keep it so owner can fix)
        questions.append({
            "raw_question": current_q["raw_question"],
            "options": {k.upper(): v for k, v in current_q["options"].items()},
            "ans": current_q.get("ans"),
            "exp": current_q.get("exp"),
            "line": current_q["line"]
        })
        in_q = False
        current_q = None

    # If there were earlier completed questions while parsing, we didn't append them mid-loop.
    # The logic above appended only the last question; we need to reconstruct full questions list.
    # To simplify and be robust: re-parse to capture all questions cleanly.

    # --- strict reparse to build question list + inject DES positions properly ---
    # We'll do a second pass which is simpler: scan lines and collect blocks.
    questions = []
    des_list2 = []
    block = None
    q_idx = 0
    line_no = 0
    for raw in lines:
        line_no += 1
        if _EG_RE.search(raw):
            continue
        if _DES_RE.match(raw):
            m = _DES_RE.match(raw)
            txt = m.group(1).strip()
            # if we are currently building a question block (block not None), DES inside -> error
            if block is not None and not block.get("_closed", False):
                errors.append(f"⚠️ DES found inside question block (Question {q_idx+1}) at line {line_no}.")
            else:
                # assign to next question index (q_idx)
                des_list2.append({"text": txt, "question_index": q_idx, "line": line_no})
            continue
        m_q = _Q_RE.match(raw)
        if m_q:
            # start new block
            if block is not None and not block.get("_closed", False):
                # previous block missing ANS -> error already caught earlier; close anyway
                block["_closed"] = True
                questions.append({
                    "raw_question": block.get("raw_question", ""),
                    "options": block.get("options", {}),
                    "ans": block.get("ans"),
                    "exp": block.get("exp")
                })
                q_idx += 1
            block = {"raw_question": m_q.group(1).strip(), "options": {}, "ans": None, "exp": None, "_closed": False}
            continue
        if block is None:
            continue
        m_opt = _OPT_RE.match(raw)
        if m_opt:
            label = m_opt.group(1).upper()
            block["options"][label] = m_opt.group(2).strip()
            continue
        m_ans = _ANS_RE.match(raw)
        if m_ans:
            block["ans"] = m_ans.group(1).upper()
            continue
        m_exp = _EXP_RE.match(raw)
        if m_exp:
            block["exp"] = m_exp.group(1).strip()
            continue
        # blank or other lines
        if raw.strip() == "":
            # ignore
            continue
        # unrecognized inside block -> append to question text
        block["raw_question"] += " " + raw.strip()

    # finalize last block if present
    if block is not None:
        questions.append({
            "raw_question": block.get("raw_question", ""),
            "options": {k.upper(): v for k, v in block.get("options", {}).items()},
            "ans": block.get("ans"),
            "exp": block.get("exp")
        })

    # final validation pass for each question
    for idx, q in enumerate(questions, start=1):
        labels = set(q["options"].keys())
        required = {"A", "B", "C", "D"}
        missing = required - labels
        if missing:
            errors.append(f"⚠️ Question {idx} missing options: {', '.join(sorted(missing))}.")
        if not q.get("ans"):
            errors.append(f"⚠️ Missing ANS in Question {idx}.")
        else:
            if q["ans"] not in labels:
                errors.append(f"⚠️ ANS \"{q['ans']}\" does not match any option in Question {idx}.")
        # EXP after ANS already enforced in first pass; but if exp exists but ans absent we added error earlier.

    result = {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "des": des_list2[0]["text"] if des_list2 else None,
        "des_list": des_list2,
        "questions": questions
    }
    return result
