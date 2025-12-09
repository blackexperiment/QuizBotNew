# validator.py
"""
Lenient validator/parser per your spec.

Input text -> sequence of events:
[
  {"type":"des","text":"..."},
  {"type":"question",
   "raw_question": "...",
   "options": {"A":"...","B":"...", ...},
   "ans": "A",
   "exp": "..." or None
  },
  ...
]

Return structure:
{
 "ok": bool,
 "errors": [...],
 "warnings": [...],
 "events": [...],
}
"""

import re

_option_label_re = re.compile(r'^\s*([A-Za-z])\s*[:)\-\.]\s*(.*)$')
# Accept lines starting "A:" or "A)" or "A-" etc when parsing options.

def _strip_prefix(line, prefix):
    if line.startswith(prefix):
        return line[len(prefix):].strip()
    return None

def validate_and_parse(text: str):
    lines = text.splitlines()
    errors = []
    warnings = []
    events = []

    idx = 0
    # We'll parse sequentially; DES lines may appear anywhere.
    # Q blocks start with "Q:" and run until an ANS: line or next Q: or end.
    while idx < len(lines):
        raw = lines[idx].rstrip("\n")
        line = raw.strip()

        # Ignore empty lines
        if line == "":
            idx += 1
            continue

        # Ignore Eg( ... ) lines entirely
        if line.startswith("Eg(") or "Eg(" in line:
            idx += 1
            continue

        # DES line (can be anywhere)
        if line.startswith("DES:"):
            des_text = raw.partition("DES:")[2].strip()
            events.append({"type": "des", "text": des_text})
            idx += 1
            continue

        # Q block start
        if line.startswith("Q:"):
            q_text = raw.partition("Q:")[2].strip()
            idx += 1
            # collect option lines until ANS: or next Q: or DES:
            options = {}
            ans = None
            exp = None
            raw_question = q_text

            # Read following lines that belong to this question block
            while idx < len(lines):
                l_raw = lines[idx].rstrip("\n")
                l = l_raw.strip()
                # If next question starts or DES or another top-level marker -> stop
                if l.startswith("Q:") or l.startswith("DES:"):
                    break
                if l.startswith("Eg(") or "Eg(" in l:
                    idx += 1
                    continue
                # ANS:
                if l.upper().startswith("ANS:"):
                    val = l_raw.partition(":")[2].strip()
                    if val == "":
                        errors.append(f"Missing ANS value at line {idx+1}")
                    else:
                        # Normalize to single letter (first non-space character)
                        m = re.search(r'([A-Za-z])', val)
                        if m:
                            ans = m.group(1).upper()
                        else:
                            errors.append(f"Invalid ANS value at line {idx+1}: {val!r}")
                    idx += 1
                    continue
                # EXP:
                if l.upper().startswith("EXP:"):
                    exp_text = l_raw.partition(":")[2].strip()
                    exp = exp_text
                    idx += 1
                    continue
                # Option candidate: lines starting with A: or A) etc or lines beginning with 'A: (' pattern used in your example
                opt_match = None
                # direct "A: (A) text" or "A: (A)text" or "A: text"
                if re.match(r'^[A-Za-z]\s*[:)\-\.]', l_raw):
                    # try label split
                    m = re.match(r'^\s*([A-Za-z])\s*[:)\-\.]\s*(.*)$', l_raw)
                    if m:
                        label = m.group(1).upper()
                        text_opt = m.group(2).strip()
                        options[label] = text_opt
                        idx += 1
                        continue
                # also accept lines that start with "(A)" or "(A) text"
                m2 = re.match(r'^\s*\(?([A-Za-z])\)?\s*(?:[:)\-\.])?\s*(.*)$', l_raw)
                if m2 and l_raw.strip().startswith("("):
                    label = m2.group(1).upper()
                    options[label] = m2.group(2).strip()
                    idx += 1
                    continue

                # If line contains "A: (A) text" style where label repeated inside parentheses,
                if ":" in l_raw:
                    left, _, right = l_raw.partition(":")
                    left = left.strip()
                    if len(left) == 1 and left.isalpha():
                        options[left.upper()] = right.strip()
                        idx += 1
                        continue

                # Otherwise it's likely extra text or part of question continuation
                # If options empty yet, append to question text (multi-line question)
                if not options and ans is None and exp is None:
                    raw_question += " " + l_raw.strip()
                    idx += 1
                    continue

                # If we reached here, treat as unknown line inside question -> append as warning and skip
                warnings.append(f"Ignored line inside question at {idx+1}: {l_raw}")
                idx += 1

            # End of inner loop for Q block

            # Minimal checks: must have at least A-D and ans must be one of present options
            # We require at least A,B,C,D labels present, but be lenient: if user provided less it's an error.
            required_labels = ["A", "B", "C", "D"]
            for rlbl in required_labels:
                if rlbl not in options:
                    errors.append(f"Option {rlbl} missing for question starting at line {idx+1 - len(options) - 1}")
            if ans is None:
                errors.append(f"Missing ANS for question starting with Q: {raw_question[:50]!r}")
            else:
                if ans not in options:
                    errors.append(f'ANS "{ans}" does not match any option for question: {raw_question[:40]!r}')

            events.append({
                "type": "question",
                "raw_question": raw_question,
                "options": options,
                "ans": ans,
                "exp": exp
            })
            continue

        # Any other line that is not DES or Q or Eg() -> treat as stray text (we'll make it a DES)
        # This keeps behavior lenient: stray text is treated as DES message.
        events.append({"type": "des", "text": raw})
        idx += 1

    ok = len(errors) == 0
    return {"ok": ok, "errors": errors, "warnings": warnings, "events": events}
