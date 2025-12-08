# validator.py
"""
Simple, tolerant validator/parser for your quiz format.

Rules implemented (minimal / as requested):
- Lines starting with "Eg(" (case-insensitive) are ignored.
- "DES:" lines are accepted anywhere and returned as sequence events.
- Q blocks are accepted and must contain at minimum options A-D and an ANS: line.
- EXP: is optional and attached to the current question.
- Options may be provided as "A: ..." or "A) ..." or "A. ..." — normalized to label "A".
- No strict checks about option text content.
- Returns an ordered "sequence" list preserving the appearance order of DES and questions.
- Also returns a questions[] list for backwards compatibility.

Return schema:
{
  "ok": bool,
  "errors": [...],
  "warnings": [...],
  "sequence": [
     {"type":"des","text": "..."},
     {"type":"question", "question": { "raw_question": "...",
                                      "options": {"A":"...","B":"...",...},
                                      "ans":"A",
                                      "exp": "..." or None,
                                      "line": <line number where Q: was found>
                                    }
     },
     ...
  ],
  "questions": [ same question objects as in sequence but only questions ... ]
}
"""
import re

OPTION_RE = re.compile(r'^\s*([A-Da-d])\s*[:\)\.\-]\s*(.*)$')  # A: text or A) text or A. text
LABEL_RE = re.compile(r'^\s*([A-Za-z]+)\s*:\s*(.*)$')  # for Q:, DES:, ANS:, EXP:
EG_RE = re.compile(r'\bEg\(', re.IGNORECASE)

def _line_label(line):
    m = LABEL_RE.match(line)
    if not m:
        return None, line
    label = m.group(1).strip()
    rest = m.group(2).rstrip()
    return label.upper(), rest

def validate_and_parse(text: str):
    """
    Parse the given text and return the structured result described above.
    This function is intentionally permissive about DES placement.
    """
    errors = []
    warnings = []
    sequence = []
    questions = []

    if text is None:
        return {"ok": False, "errors": ["No content"], "warnings": [], "sequence": [], "questions": []}

    lines = text.splitlines()
    # Normalize line endings and keep track of line numbers (1-indexed)
    cur_q = None  # building question dict
    cur_q_line = None

    def flush_current_question():
        nonlocal cur_q, cur_q_line
        if cur_q is None:
            return
        # Validate basic requirements A-D and ANS
        missing_opts = [lbl for lbl in ["A", "B", "C", "D"] if lbl not in cur_q["options"]]
        if missing_opts:
            errors.append(f"⚠️ Question starting at line {cur_q_line}: missing options {', '.join(missing_opts)}")
        if not cur_q.get("ans"):
            errors.append(f"⚠️ Missing ANS in question starting at line {cur_q_line}")
        else:
            if cur_q["ans"].upper() not in cur_q["options"]:
                errors.append(f"⚠️ ANS \"{cur_q['ans']}\" does not match any option in question at line {cur_q_line}")
        # normalize ans uppercase or None
        if cur_q.get("ans"):
            cur_q["ans"] = cur_q["ans"].upper()
        # append
        qobj = {
            "raw_question": cur_q.get("raw_question", "").strip(),
            "options": cur_q["options"].copy(),
            "ans": cur_q.get("ans"),
            "exp": cur_q.get("exp"),
            "line": cur_q_line
        }
        sequence.append({"type": "question", "question": qobj})
        questions.append(qobj)
        cur_q = None
        cur_q_line = None

    # iterate lines sequentially, build sequence of DES and questions in exact order seen
    for idx, raw in enumerate(lines, start=1):
        line = raw.rstrip()
        if not line.strip():
            # blank line - ignore but keep position
            continue

        # Ignore Eg( ... ) anywhere in the line
        if EG_RE.search(line):
            # skip entire line
            continue

        # If line looks like a labeled line (DES:, Q:, ANS:, EXP:, etc.)
        label, content = _line_label(line)
        if label is not None:
            if label == "DES":
                # Immediately record DES event at current linear position
                sequence.append({"type": "des", "text": content.strip(), "line": idx})
                continue

            if label == "Q":
                # If we were building a previous question, flush it first
                if cur_q is not None:
                    # flush previous question before starting new one
                    flush_current_question()
                # start new question
                cur_q_line = idx
                cur_q = {"raw_question": content.strip(), "options": {}, "ans": None, "exp": None}
                continue

            if label == "ANS":
                if cur_q is None:
                    errors.append(f"⚠️ ANS found outside a question block at line {idx}")
                    # still capture nothing
                else:
                    # accept single letter answer (first non-space char)
                    ans_text = content.strip()
                    if ans_text:
                        # sometimes people write "ANS: A" or "ANS: (A)"
                        m = re.search(r'([A-Da-d])', ans_text)
                        if m:
                            cur_q["ans"] = m.group(1).upper()
                        else:
                            cur_q["ans"] = ans_text.strip().upper()
                    else:
                        errors.append(f"⚠️ ANS line empty at line {idx}")
                continue

            if label == "EXP":
                if cur_q is None:
                    # attach EXP as standalone? We'll treat as warning and add a des message (non-blocking)
                    warnings.append(f"⚠️ EXP found outside question at line {idx}; treating as DES message.")
                    sequence.append({"type": "des", "text": "Explanation: " + content.strip(), "line": idx})
                else:
                    # attach as explanation
                    cur_q["exp"] = content.strip()
                continue

            # Unknown label — could be "A:", "B:" etc but those matched earlier via OPTION_RE?
            # we'll fallthrough to option detection below

        # Option detection: A: B: etc (allow variations A) A. A:)
        m_opt = OPTION_RE.match(line)
        if m_opt:
            label_char = m_opt.group(1).upper()
            opt_text = m_opt.group(2).strip()
            if cur_q is None:
                # Option found before a question start — treat as format error but continue by creating a question placeholder
                warnings.append(f"⚠️ Option {label_char} found before any Q: at line {idx}. Creating implicit question.")
                cur_q_line = idx
                cur_q = {"raw_question": "", "options": {}, "ans": None, "exp": None}
            # If duplicate option label, overwrite and warn
            if label_char in cur_q["options"]:
                warnings.append(f"⚠️ Duplicate option {label_char} in question starting at line {cur_q_line}. Overwriting.")
            cur_q["options"][label_char] = opt_text
            continue

        # If none of above matched, treat as continuation text:
        # - if inside question and raw_question is not empty, append to raw_question
        # - else if inside question and no options yet, append to raw_question
        # - else treat as free text -> if it starts with something like "A)" not matched above, try to salvage
        # simple heuristics:
        if cur_q is not None and cur_q.get("raw_question") is not None and not cur_q["options"]:
            # append to question text (multiline Q)
            cur_q["raw_question"] = (cur_q.get("raw_question", "") + " " + line).strip()
            continue

        # Not an option, not a labeled line, not part of a question — but could be stray text or stray "DES" without colon
        # If line begins with "DES" without colon, handle it
        if line.strip().upper().startswith("DES"):
            # try to extract after DES
            rest = line.partition(':')[2].strip()
            sequence.append({"type": "des", "text": rest, "line": idx})
            continue

        # If we reach here and there's unknown text, add as warning and ignore
        warnings.append(f"⚠️ Unrecognized or stray text at line {idx}: {line[:80]!r}")

    # end for lines
    # flush any last question
    if cur_q is not None:
        flush_current_question()

    ok = len(errors) == 0

    return {
        "ok": ok,
        "errors": errors,
        "warnings": warnings,
        "sequence": sequence,
        "questions": questions,
    }


# Quick local test helper (not executed at import)
if __name__ == "__main__":
    sample = """DES: Hello before
Q: Sample?
A: (A) One
B: (B) Two
DES: mid-des
C: (C) Three
D: (D) Four
ANS: B
EXP: Because...
DES: after all"""
    import json
    print(json.dumps(validate_and_parse(sample), indent=2, ensure_ascii=False))
