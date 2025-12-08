# validator.py
import re
from typing import List, Dict, Any, Optional

LABELS_REQUIRED = ["A", "B", "C", "D"]
LABEL_REGEX = re.compile(r'^([A-D])\s*[:\)\.]?\s*(.*)$', re.IGNORECASE)
Q_PREFIX = "Q:"
DES_PREFIX = "DES:"
ANS_PREFIX = "ANS:"
EXP_PREFIX = "EXP:"
EG_PATTERN = re.compile(r'\bEg\(', re.IGNORECASE)

def _line_strip(l: str) -> str:
    return l.strip()

def validate_and_parse(text: str) -> Dict[str, Any]:
    """
    Parse and validate according to the user's simple rules:
    1) DES allowed anywhere (but NOT inside a question block). If present before questions, its text
       will be returned in 'des' and sent as a normal message.
    2) Lines containing Eg( are ignored.
    3) Each Q: must have options A:,B:,C:,D: (at least these).
    4) ANS: must be one of A-D and must be present for each question.
    5) EXP: optional and if present must come AFTER ANS for that question.
    6) No extra strict checks on inner text.
    Returns dict:
      {
        "ok": bool,
        "errors": [...],
        "warnings": [...],
        "des": "..." or None,
        "questions": [ { "raw_question": str,
                         "options": {"A": str, "B": str, ...},
                         "ans": "A",
                         "exp": "..." (optional)
                       }, ... ]
      }
    """
    errors: List[str] = []
    warnings: List[str] = []
    questions: List[Dict[str, Any]] = []
    des_lines: List[str] = []

    lines = text.splitlines()
    current_q: Optional[Dict[str, Any]] = None
    q_index = 0
    in_question = False
    ans_expected_for_current = False

    for ln_no, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            # skip blank lines
            continue

        # ignore Eg(...) lines anywhere
        if EG_PATTERN.search(line):
            continue

        # DES handling
        if line.upper().startswith(DES_PREFIX):
            # DES must not be inside an open question (i.e., between Q: and that Q's ANS:)
            if in_question and not current_q.get("ans"):
                errors.append(f"⚠️ DES found inside question block (Question {q_index}) at line {ln_no}.")
            else:
                # store DES text (everything after 'DES:')
                des_text = line[len(DES_PREFIX):].strip()
                if des_text:
                    des_lines.append(des_text)
                else:
                    # empty DES is allowed (no-op)
                    pass
            continue

        # Q: start a new question
        if line.upper().startswith(Q_PREFIX):
            # if previous question is open but missing ANS -> error
            if in_question and not current_q.get("ans"):
                errors.append(f"⚠️ Missing ANS in Question {q_index} (previous question did not finish before new Q:).")
                # close previous question anyway to continue parsing
            # start new question
            q_index += 1
            in_question = True
            current_q = {
                "raw_question": line[len(Q_PREFIX):].strip(),
                "options": {},
                "ans": None,
                "exp": None,
                "_line_no": ln_no
            }
            questions.append(current_q)
            continue

        # Option lines: A:, B:, C:, D:  (also accept A) or A.)
        m = LABEL_REGEX.match(line)
        if m and in_question and current_q is not None:
            label = m.group(1).upper()
            opt_text = m.group(2).strip()
            if not opt_text:
                errors.append(f"⚠️ Option {label} is empty (Question {q_index}) at line {ln_no}.")
            # normalize and store
            if label in current_q["options"]:
                warnings.append(f"⚠️ Duplicate option {label} in Question {q_index} (line {ln_no}). Overwriting.")
            current_q["options"][label] = opt_text
            continue

        # ANS:
        if line.upper().startswith(ANS_PREFIX) and in_question and current_q is not None:
            ans_value = line[len(ANS_PREFIX):].strip()
            if not ans_value:
                errors.append(f"⚠️ Missing ANS value in Question {q_index} at line {ln_no}.")
            else:
                # take first letter A-D if present
                candidate = ans_value.strip().upper()
                # allow forms like "ANS: A" or "ANS: (A)" or "ANS:A"
                c = None
                if candidate and candidate[0] in ("A", "B", "C", "D"):
                    c = candidate[0]
                else:
                    # try to extract letter from within parentheses
                    m2 = re.search(r'([A-D])', candidate, re.IGNORECASE)
                    if m2:
                        c = m2.group(1).upper()
                if not c:
                    errors.append(f"⚠️ ANS value invalid in Question {q_index} at line {ln_no}. Must be one of A-D.")
                else:
                    current_q["ans"] = c
                    # verify that option exists (we'll check at end if options missing)
                    if c not in current_q["options"]:
                        warnings.append(f"⚠️ ANS '{c}' in Question {q_index} does not match any parsed option yet.")
            continue

        # EXP:
        if line.upper().startswith(EXP_PREFIX) and in_question and current_q is not None:
            exp_text = line[len(EXP_PREFIX):].strip()
            # EXP must come after ANS
            if not current_q.get("ans"):
                errors.append(f"⚠️ EXP must come after ANS (Question {q_index}) at line {ln_no}.")
            else:
                if current_q.get("exp"):
                    errors.append(f"⚠️ Multiple EXP found in Question {q_index} at line {ln_no}.")
                else:
                    current_q["exp"] = exp_text
            continue

        # If we reach here and line looks like an option but no Q started
        if LABEL_REGEX.match(line) and not in_question:
            errors.append(f"⚠️ Option found before any Q: (line {ln_no}).")
            continue

        # Unknown/unexpected line — if it occurs while in_question, treat as continuation of question text
        if in_question and current_q is not None:
            # treat as continuation of question body if it doesn't match other keywords
            # append to raw_question (keep a space)
            current_q["raw_question"] = (current_q.get("raw_question") or "") + " " + line
            continue

        # If nothing matches and we are not in a question — ignore stray lines
        # (We intentionally do not block on arbitrary text.)
        continue

    # Post-parse checks
    # 1) If last question exists but missing ANS -> error
    for idx, q in enumerate(questions, start=1):
        # must have at least options A-D
        missing = [lbl for lbl in LABELS_REQUIRED if lbl not in q["options"]]
        if missing:
            errors.append(f"⚠️ Question {idx} missing options: {', '.join(missing)}.")
        # ANS must be present and one of A-D
        if not q.get("ans"):
            errors.append(f"⚠️ Missing ANS in Question {idx}.")
        else:
            if q["ans"] not in q["options"]:
                # it's possible owner wrote ANS before options; we already warned earlier — still mark error
                errors.append(f"⚠️ ANS '{q['ans']}' does not match any option in Question {idx}.")
        # EXP is optional; no further checks here

    # Build final des
    des_text = None
    if des_lines:
        des_text = "\n".join(des_lines)

    ok = len(errors) == 0
    result = {
        "ok": ok,
        "errors": errors,
        "warnings": warnings,
        "des": des_text,
        "questions": []
    }

    # normalize questions for output (only if parsed; even if warnings present, include parsed data)
    for q in questions:
        q_out = {
            "raw_question": (q.get("raw_question") or "").strip(),
            "options": q.get("options", {}),
            "ans": q.get("ans"),
        }
        if q.get("exp"):
            q_out["exp"] = q.get("exp")
        result["questions"].append(q_out)

    return result


# quick manual test when run as script (not required)
if __name__ == "__main__":
    sample = """
    DES: Sample Quiz
    Eg(this is meta)
    Q: What is 2+2?
    A: (A) 3
    B: (B) 4
    C: (C) 5
    D: (D) 22
    ANS: B
    EXP: Because 2+2=4
    """
    import json
    print(json.dumps(validate_and_parse(sample), indent=2, ensure_ascii=False))
