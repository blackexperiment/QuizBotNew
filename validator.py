import re
from typing import Tuple, List, Dict

RE_DES = re.compile(r"^DES:\s*(.+)", re.IGNORECASE)
RE_DES_COMPLETED = re.compile(r"^DES:\s*(.+)\s+COMPLETED\s*✅\s*$", re.IGNORECASE)
RE_Q = re.compile(r"^Q:\s*(.+)", re.IGNORECASE)
RE_OPTION = re.compile(r"^([A-L]):\s*\(\1\)\s*(.+)", re.IGNORECASE)
RE_ANS = re.compile(r"^ANS:\s*([A-L])\s*$", re.IGNORECASE)
RE_EXP = re.compile(r"^EXP:\s*(.+)", re.IGNORECASE)

def parse_quiz_text(text: str) -> Tuple[str, List[Dict]]:
    """
    Returns title, list of questions: each {'q':..., 'options': {'A':..}, 'ans': 'A', 'exp': optional}
    Raises ValueError on format issues.
    """
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln != ""]
    if not lines:
        raise ValueError("Empty message")

    # First line DES:
    m = RE_DES.match(lines[0])
    if not m:
        raise ValueError("First line must start with: DES: <title>")
    title = m.group(1).strip()

    # Last line must be DES: ... COMPLETED ✅
    if not RE_DES_COMPLETED.match(lines[-1]):
        raise ValueError("Last line must be: DES: <title> COMPLETED ✅")

    # Parse blocks between first and last
    body = lines[1:-1]
    questions = []
    cur = None
    q_index = 0
    for ln in body:
        if RE_Q.match(ln):
            if cur:
                # finalize previous
                if "ans" not in cur:
                    raise ValueError(f"ANS missing for question: {cur.get('q','(unknown)')}")
                questions.append(cur)
            cur = {"q": RE_Q.match(ln).group(1).strip(), "options": {}}
            q_index += 1
            continue
        m_opt = RE_OPTION.match(ln)
        if m_opt and cur is not None:
            label = m_opt.group(1).upper()
            textopt = m_opt.group(2).strip()
            cur["options"][label] = textopt
            continue
        m_ans = RE_ANS.match(ln)
        if m_ans and cur is not None:
            cur["ans"] = m_ans.group(1).upper()
            continue
        m_exp = RE_EXP.match(ln)
        if m_exp and cur is not None:
            cur["exp"] = m_exp.group(1).strip()
            continue
        # allow Eg(...) meta lines to be ignored
        if ln.startswith("Eg(") and ln.endswith(")"):
            continue
        # else unknown line
        raise ValueError(f"Unexpected line: {ln}")

    if cur:
        if "ans" not in cur:
            raise ValueError(f"ANS missing for final question: {cur.get('q','(unknown)')}")
        questions.append(cur)

    if not questions:
        raise ValueError("No questions found")

    # Validate each question minimal
    for idx, q in enumerate(questions, start=1):
        if len(q["options"]) < 2:
            raise ValueError(f"Question {idx} has less than 2 options")
        if q["ans"] not in q["options"]:
            raise ValueError(f"ANS invalid for question {idx}")

    return title, questions
