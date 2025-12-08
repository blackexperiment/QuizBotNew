# validator.py
import re
from typing import List, Dict, Any

def validate_and_parse(raw_text: str) -> Dict[str, Any]:
    """
    Parse a text payload into:
    {
      "ok": bool,
      "errors": [...],
      "warnings": [...],
      "des_list": [ {"pos": int, "text": str, "line": int}, ... ],
      "des": first_des_text_or_none,
      "questions": [
         {"raw_question": "Q text", "options": {"A": "...","B":"...","C":"...","D":"..."}, "ans":"A", "exp":"..."},
         ...
      ]
    }

    Rules implemented (simple):
    1) Lines starting with Eg( or containing Eg( are ignored completely.
    2) DES: allowed anywhere but must NOT be inside a Q-block (between Q: and ANS:).
       If found while parsing a question block -> it's an error.
    3) Q: starts a question block.
    4) A:, B:, C:, D: required (at least those four).
    5) ANS: required and must match one of the present options.
    6) EXP: optional (must appear after ANS).
    7) Minimal normalization: option labels are standardized to single uppercase letters A..Z (strip spaces).
    """

    errors: List[str] = []
    warnings: List[str] = []
    questions: List[Dict[str, Any]] = []
    des_list: List[Dict[str, Any]] = []

    lines = raw_text.splitlines()
    # normalize lines trimming only trailing \r and spaces on right but keep left spaces not needed
    # we'll operate on stripped left/right where appropriate
    in_q = False
    q = None
    q_start_line = None
    question_count = 0

    # helper regex
    re_label = re.compile(r'^([A-Za-z])\s*[:\)]\s*(.*)$')  # matches "A: text" or "A) text"
    # iterate lines
    for idx, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if line == "":
            # blank lines separate blocks; if inside question, allow blank
            continue

        # ignore Eg(...) anywhere
        if "Eg(" in line or line.startswith("Eg(") or line.startswith("eg("):
            continue

        # DES detection (must be line starting with DES:)
        if line.upper().startswith("DES:"):
            des_text = line[len("DES:"):].strip()
            # If we're currently inside a question block between Q: and before end (ANS processed)
            if in_q:
                # DES inside question block → error
                errors.append(f"⚠️ DES found inside question block (Question {question_count+1}) at line {idx}.")
                # still record, but keep it flagged; we won't stop parsing
                des_list.append({"pos": question_count, "text": des_text, "line": idx})
            else:
                # record DES positioned at current question_count (before next question)
                des_list.append({"pos": question_count, "text": des_text, "line": idx})
            continue

        # Question start
        if line.startswith("Q:"):
            # if previous question still open, finalize and validate
            if in_q and q:
                # finalize: check mandatory fields
                missing = []
                for label in ["A", "B", "C", "D"]:
                    if label not in q["options"]:
                        missing.append(label)
                if missing:
                    errors.append(f"⚠️ Question {question_count+1} missing options: {', '.join(missing)}")
                if "ans" not in q or not q["ans"]:
                    errors.append(f"⚠️ Missing ANS in Question {question_count+1}.")
                questions.append(q)
                question_count += 1
                q = None
                in_q = False

            # start new question
            q = {"raw_question": line[len("Q:"):].strip(), "options": {}, "ans": None, "exp": None, "line": idx}
            in_q = True
            q_start_line = idx
            continue

        # If inside question block, parse A/B/C.. / ANS / EXP
        if in_q:
            # ANS:
            if line.upper().startswith("ANS:"):
                ans_val = line[len("ANS:"):].strip()
                if len(ans_val) == 0:
                    errors.append(f"⚠️ Missing ANS value in Question {question_count+1}.")
                else:
                    # take first letter token
                    ans_letter = ans_val.strip().upper()[0]
                    q["ans"] = ans_letter
                continue

            # EXP:
            if line.upper().startswith("EXP:"):
                exp_val = line[len("EXP:"):].strip()
                # EXP must occur after ANS (rule). If ANS not yet present -> error
                if not q.get("ans"):
                    errors.append(f"⚠️ EXP must come after ANS (Question {question_count+1}) at line {idx}.")
                q["exp"] = exp_val
                continue

            # Option detection - prefer format "A: text" or "A) text" or "A: (A) text"
            m = re.match(r'^(A|B|C|D|E|F|G|H|I|J|K|L)\s*[:\)]\s*(.*)$', line, re.I)
            if m:
                label = m.group(1).upper()
                text_after = m.group(2).strip()
                # If the option text contains "(A)" prefix like "(A) option", strip the leading (X)
                if text_after.startswith("(") and ")" in text_after:
                    # remove first "(...)" only
                    first_close = text_after.find(")")
                    possible = text_after[:first_close+1]
                    # If possible is like "(A)" remove it
                    if re.match(r'^\([A-Za-z]\)$', possible):
                        text_after = text_after[first_close+1:].strip()
                # store
                if label in q["options"]:
                    warnings.append(f"⚠️ Duplicate option {label} in Question {question_count+1} (line {idx}). Overwriting.")
                q["options"][label] = text_after
                continue

            # Sometimes option given as "A: (A) text" or "A) (A) text" handled above; also handle "A: text" done.
            # If line begins with "A:" but not matched, try looser regex:
            m2 = re_label.match(line)
            if m2 and m2.group(1).upper() in ["A","B","C","D","E","F","G","H"]:
                label = m2.group(1).upper()
                text_after = m2.group(2).strip()
                if label in q["options"]:
                    warnings.append(f"⚠️ Duplicate option {label} in Question {question_count+1} (line {idx}). Overwriting.")
                q["options"][label] = text_after
                continue

            # If line doesn't match any expected inside a question, it's unexpected text; we treat it as continuation of last added field or add to question text
            # Append to raw_question if no options yet
            if not q["options"]:
                q["raw_question"] = q["raw_question"] + " " + line
            else:
                # append to last option text
                # find last inserted option
                last_label = None
                if q["options"]:
                    last_label = list(q["options"].keys())[-1]
                if last_label:
                    q["options"][last_label] = q["options"][last_label] + " " + line
            continue

        # If not in question and not DES/Eg, ignore stray lines but warn
        # But allow extra text before first Q (could be header). We'll ignore but not error.
        # To avoid noisy warnings, only warn if line looks like "ANS:" or "A:" outside question
        if line.upper().startswith(("ANS:", "A:", "B:", "C:", "D:", "EXP:")):
            warnings.append(f"⚠️ Found {line.split()[0]} outside any question block at line {idx}. Ignored.")
        # else ignore

    # finalize last question if open
    if in_q and q:
        missing = []
        for label in ["A", "B", "C", "D"]:
            if label not in q["options"]:
                missing.append(label)
        if missing:
            errors.append(f"⚠️ Question {question_count+1} missing options: {', '.join(missing)}")
        if "ans" not in q or not q["ans"]:
            errors.append(f"⚠️ Missing ANS in Question {question_count+1}.")
        questions.append(q)
        question_count += 1
        q = None
        in_q = False

    # Validate ANS matches an option
    for idx_q, qq in enumerate(questions, start=1):
        if qq.get("ans"):
            if qq["ans"] not in qq["options"]:
                errors.append(f'⚠️ ANS "{qq.get("ans")}" does not match any option in Question {idx_q}')
        else:
            errors.append(f"⚠️ Missing ANS in Question {idx_q}")

    ok = (len(errors) == 0)
    # pick first DES text (backwards compat)
    first_des = des_list[0]["text"] if des_list else None

    result = {
        "ok": ok,
        "errors": errors,
        "warnings": warnings,
        "des_list": des_list,
        "des": first_des,
        "questions": questions
    }
    return result
