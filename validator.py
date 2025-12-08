# validator.py
# Very simple, robust parser that never rejects DES inside question blocks.
# It produces a clean "sequence" list preserving exact input order.

import re

def validate_and_parse(text: str):
    lines = text.splitlines()
    sequence = []
    errors = []
    warnings = []

    current_q = None  # None OR dict of an active question block
    waiting_options = False  # True once Q: has been seen

    def flush_question():
        """Push current_q into sequence if valid."""
        if not current_q:
            return

        # Must have A–D
        opts = current_q.get("options", {})
        for letter in ["A", "B", "C", "D"]:
            if letter not in opts:
                errors.append(f"Missing option {letter} for question: {current_q['raw_question']}")
                return

        # Must have ANS and it must exist among options
        ans = current_q.get("ans")
        if not ans:
            errors.append(f"Missing ANS for question: {current_q['raw_question']}")
            return
        if ans not in opts:
            errors.append(f"ANS {ans} does not match any option in question: {current_q['raw_question']}")
            return

        # Valid – add to sequence
        sequence.append(current_q.copy())

    # ---------------------------------------------------------
    # MAIN LOOP
    # ---------------------------------------------------------
    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        # Eg() → ignore
        if line.lower().startswith("eg("):
            continue

        # -----------------------------
        # DES line
        # -----------------------------
        if line.lower().startswith("des:"):
            des_text = line[4:].strip()
            # If in middle of question block: still allowed (your rule)
            # Push DES as its own sequence item
            sequence.append({"type": "des", "text": des_text})
            continue

        # -----------------------------
        # Q: new question starts
        # -----------------------------
        if line.lower().startswith("q:"):
            # If previous question exists → flush it
            if current_q:
                flush_question()
            # Start new question
            question_text = line[2:].strip()
            current_q = {
                "type": "question",
                "raw_question": question_text,
                "options": {},
                "ans": None,
                "exp": None,
            }
            waiting_options = True
            continue

        # If inside question block:
        if current_q:
            # -----------------------------
            # A:, B:, C:, ... any option
            # -----------------------------
            m_opt = re.match(r"([A-Z]):\s*(.*)", line)
            if m_opt:
                label = m_opt.group(1).upper()
                text_val = m_opt.group(2).strip()
                # Save option
                current_q["options"][label] = text_val
                continue

            # -----------------------------
            # ANS:
            # -----------------------------
            if line.lower().startswith("ans:"):
                ans_letter = line[4:].strip().upper()
                current_q["ans"] = ans_letter
                continue

            # -----------------------------
            # EXP:
            # -----------------------------
            if line.lower().startswith("exp:"):
                exp_text = line[4:].strip()
                current_q["exp"] = exp_text
                continue

            # If a line appears here that does not match anything:
            # You said extra text should NOT break anything → ignore it.
            continue

        # -----------------------------
        # Any other text outside Q block → ignore
        # -----------------------------
        continue

    # Final flush
    if current_q:
        flush_question()

    # Done
    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "sequence": sequence
    }
