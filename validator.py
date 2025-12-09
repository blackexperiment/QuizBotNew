# validator.py
import re
from typing import List, Dict, Any

LINE_DES = re.compile(r'^\s*DES\s*:\s*(.*)$', re.IGNORECASE)
LINE_Q = re.compile(r'^\s*Q\s*:\s*(.*)$', re.IGNORECASE)
LINE_OPT = re.compile(r'^\s*([A-Z])\s*:\s*(?:\([A-Z]\)\s*)?(.*)$')  # matches "A: (A) text" or "A: text"
LINE_ANS = re.compile(r'^\s*ANS\s*:\s*([A-Z])\s*$', re.IGNORECASE)
LINE_EXP = re.compile(r'^\s*EXP\s*:\s*(.*)$', re.IGNORECASE)
LINE_EG = re.compile(r'(^|\s)Eg\s*\(', re.IGNORECASE)

def validate_and_parse(text: str) -> Dict[str, Any]:
    """
    Permissive parser using user's requested rules:
    - Preserve order: any DES lines become sequence items at position
    - Q blocks parsed into question items (must have A-D and ANS)
    - EXP used only if after ANS for that question
    - Eg( ... ) lines ignored
    Returns dict with keys: ok, errors, warnings, sequence
    """
    lines = text.splitlines()
    sequence = []
    errors: List[str] = []
    warnings: List[str] = []

    cur_q = None  # dict with keys raw_question, options(dict), ans, exp
    line_no = 0

    def flush_question():
        nonlocal cur_q
        if cur_q is None:
            return
        # minimal checks
        opts = cur_q.get("options", {})
        if not opts:
            errors.append(f"⚠️ Question missing options: '{cur_q.get('raw_question','')[:40]}...'")
            cur_q = None
            return
        # require at least A-D
        required = ['A','B','C','D']
        missing = [r for r in required if r not in opts]
        if missing:
            errors.append(f"⚠️ Question missing options {missing} for question '{cur_q.get('raw_question','')[:40]}...'")
            cur_q = None
            return
        if not cur_q.get("ans"):
            errors.append(f"⚠️ Missing ANS in question: '{cur_q.get('raw_question','')[:40]}...'")
            cur_q = None
            return
        ans = cur_q["ans"].upper()
        if ans not in opts:
            errors.append(f"⚠️ ANS '{ans}' does not match available options in question '{cur_q.get('raw_question','')[:40]}...'")
            cur_q = None
            return
        # normalize options: ensure values trimmed
        cur_q["options"] = {k.strip(): v.strip() for k, v in cur_q["options"].items()}
        sequence.append({"type":"question",
                         "raw_question": cur_q.get("raw_question","").strip(),
                         "options": cur_q["options"],
                         "ans": cur_q["ans"].upper(),
                         "exp": cur_q.get("exp")})
        cur_q = None

    for raw in lines:
        line_no += 1
        if not raw or raw.strip() == "":
            # skip blank lines
            continue
        if LINE_EG.search(raw):
            # ignore Eg( ... ) anywhere
            continue
        m = LINE_DES.match(raw)
        if m:
            # flush ongoing question if completed (we don't force flush if incomplete)
            # we treat DES as a separate sequence item at this position
            # If a question is in progress but has ANS and options, flush it first
            if cur_q and cur_q.get("ans") and cur_q.get("options"):
                flush_question()
            # add DES
            text_val = m.group(1).strip()
            sequence.append({"type":"des","text": text_val})
            continue
        m = LINE_Q.match(raw)
        if m:
            # if currently building a question that hasn't been flushed,
            # flush it only if it already had ANS and options (to avoid mixing); else force error and flush previous incomplete if any
            if cur_q:
                # if incomplete
                if cur_q.get("ans") and cur_q.get("options"):
                    flush_question()
                else:
                    # incomplete previous question: push error and drop it
                    errors.append(f"⚠️ Incomplete previous question dropped before line {line_no}.")
                    cur_q = None
            cur_q = {"raw_question": m.group(1).strip(), "options": {}, "ans": None, "exp": None}
            continue
        m = LINE_ANS.match(raw)
        if m:
            if cur_q is None:
                errors.append(f"⚠️ ANS without a question at line {line_no}.")
                continue
            cur_q["ans"] = m.group(1).strip().upper()
            continue
        m = LINE_EXP.match(raw)
        if m:
            if cur_q is None:
                # EXP outside question -> treat as DES-like message? per user, EXP belongs to previous question only; otherwise ignore
                warnings.append(f"⚠️ EXP found outside question at line {line_no}; ignoring.")
                continue
            # EXP must be after ANS to be valid here. If ANS missing, record error.
            if not cur_q.get("ans"):
                errors.append(f"⚠️ EXP found before ANS (line {line_no}). Move EXP after ANS.")
                # still attach it but mark error
                cur_q["exp"] = m.group(1).strip()
            else:
                cur_q["exp"] = m.group(1).strip()
            continue
        m = LINE_OPT.match(raw)
        if m:
            # option like 'A: (A) text' or 'A: text'
            lab = m.group(1).strip().upper()
            txt = m.group(2).strip()
            if not cur_q:
                errors.append(f"⚠️ Option {lab} found outside a question at line {line_no}. Ignoring.")
                continue
            # if duplicate label warn and overwrite
            if lab in cur_q["options"]:
                warnings.append(f"⚠️ Duplicate option {lab} in question '{cur_q.get('raw_question','')[:40]}'. Overwriting.")
            cur_q["options"][lab] = txt
            continue
        # If line doesn't match any recognized token, treat as continuation of previous question text if inside question
        if cur_q:
            # append to raw_question or to last option if last option exists?
            # Simple behavior: append to raw_question if no options yet, else append to last option text.
            if not cur_q["options"]:
                cur_q["raw_question"] += " " + raw.strip()
            else:
                # append to last option added
                last_lab = sorted(cur_q["options"].keys())[-1]
                cur_q["options"][last_lab] += " " + raw.strip()
            continue
        # otherwise if free text outside anything, treat as DES (user wanted permissive behavior)
        sequence.append({"type":"des","text": raw.strip()})

    # end for lines
    # flush last question if present
    if cur_q:
        if cur_q.get("ans") and cur_q.get("options"):
            flush_question()
        else:
            errors.append("⚠️ Last question incomplete or missing ANS/options.")

    ok = len(errors) == 0
    return {"ok": ok, "errors": errors, "warnings": warnings, "sequence": sequence}
