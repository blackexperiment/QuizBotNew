# parser.py
import re
from typing import List, Dict, Tuple, Optional

# Return structure:
# {"type":"MSG", "text": "..."}
# {"type":"POLL", "question":"...", "options":[...], "answer_index":int|None, "explanation":str|None}

BLOCK_START_RE = re.compile(r'^\s*#\s*(MSG|Q\d*|Q)\s*$', re.IGNORECASE)
OPTION_RE = re.compile(r'^\s*([A-Za-z0-9]+)\)\s*(.+\S)\s*$', re.MULTILINE)
ANSWER_RE = re.compile(r'^\s*#\s*(ANS|ANSWER)\s*:\s*([A-Za-z0-9]+)\s*$', re.IGNORECASE)
EXP_RE = re.compile(r'^\s*#\s*(EXP|EXPLANATION)\s*:\s*(.+)$', re.IGNORECASE)

class ParseError(Exception):
    def __init__(self, message, block_index=None, block_text=None):
        self.block_index = block_index
        self.block_text = block_text
        super().__init__(message)

def normalize_label_to_index(label: str, labels: List[str]) -> Optional[int]:
    label = label.strip()
    if not label:
        return None
    # Map numeric "1" or "1" to 0 etc.
    if re.fullmatch(r'\d+', label):
        num = int(label)
        if 1 <= num <= len(labels):
            return num - 1
    # Map letter like A or a
    lab = label.upper()
    if len(lab) == 1 and 'A' <= lab <= 'Z':
        idx = ord(lab) - ord('A')
        if 0 <= idx < len(labels):
            return idx
    # If label matches an option label (in original option labels)
    for i, l in enumerate(labels):
        if l.split()[0].strip(').:').upper() == label.upper():
            return i
    return None

def split_blocks(text: str) -> List[Tuple[str, List[str]]]:
    """Return list of (block_type, block_lines). block_type is 'MSG' or 'Q' """
    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    blocks = []
    i = 0
    current = None
    while i < len(lines):
        line = lines[i]
        m = BLOCK_START_RE.match(line)
        if m:
            tag = m.group(1).upper()
            block_type = 'MSG' if tag.startswith('MSG') else 'Q'
            # gather following lines until next block or EOF
            j = i + 1
            content_lines = []
            while j < len(lines):
                if BLOCK_START_RE.match(lines[j]):
                    break
                content_lines.append(lines[j])
                j += 1
            blocks.append((block_type, content_lines))
            i = j
        else:
            # ignore loose lines that are not under any block
            i += 1
    return blocks

def parse_bulk(text: str) -> List[Dict]:
    """
    Parse bulk text into ordered actions.
    Raises ParseError if major problem (as per chosen rules).
    """
    blocks = split_blocks(text)
    if not blocks:
        raise ParseError("No blocks found. Use #MSG and #Q blocks.")
    actions = []
    for idx, (btype, lines) in enumerate(blocks, start=1):
        if btype == 'MSG':
            # collect non-empty lines
            joined = '\n'.join([ln.strip() for ln in lines if ln.strip()])
            if not joined:
                # per choice: skip empty message block
                continue
            actions.append({"type":"MSG", "text": joined})
        else:  # Q block
            # find question lines until an option line
            q_lines = []
            option_lines = []
            answer = None
            explanation = None
            i = 0
            while i < len(lines):
                ln = lines[i]
                if OPTION_RE.match(ln):
                    break
                if ln.strip():
                    q_lines.append(ln.strip())
                i += 1
            # collect options
            while i < len(lines):
                ln = lines[i]
                mopt = OPTION_RE.match(ln)
                if mopt:
                    # take text part only after label
                    label = mopt.group(1)
                    opt_text = mopt.group(2).strip()
                    option_lines.append(opt_text)
                    i += 1
                    continue
                # check for answer or exp tags anywhere after options
                mans = ANSWER_RE.match(ln)
                if mans:
                    answer = mans.group(2).strip()
                    i += 1
                    continue
                mexp = EXP_RE.match(ln)
                if mexp:
                    # explanation could be long - take rest of this line and subsequent lines until end
                    exp_text = mexp.group(2).strip()
                    # also absorb following non-tag lines as part of explanation
                    j = i + 1
                    extra = []
                    while j < len(lines):
                        if ANSWER_RE.match(lines[j]) or EXP_RE.match(lines[j]) or OPTION_RE.match(lines[j]):
                            break
                        extra.append(lines[j])
                        j += 1
                    if extra:
                        exp_text = exp_text + "\n" + "\n".join([e for e in extra if e.strip()])
                    explanation = exp_text.strip()
                    i = j
                    continue
                # if unknown line, skip
                i += 1
            question_text = ' '.join(q_lines).strip()
            if not question_text:
                raise ParseError(f"Missing question text in Q-block #{idx}", block_index=idx, block_text="\n".join(lines))
            if len(option_lines) < 2:
                raise ParseError(f"Question in block #{idx} has less than 2 options.", block_index=idx, block_text="\n".join(lines))
            # map answer to index if present
            answer_index = None
            if answer:
                ai = normalize_label_to_index(answer, option_lines)
                if ai is None:
                    raise ParseError(f"Invalid ANSWER in block #{idx}: '{answer}' does not match options.", block_index=idx, block_text="\n".join(lines))
                answer_index = ai
            else:
                # As per chosen rule: if EXP present without ANS -> error
                if explanation:
                    raise ParseError(f"Explanation provided but no ANSWER in block #{idx}. Explanation requires an ANSWER.", block_index=idx, block_text="\n".join(lines))
            actions.append({
                "type":"POLL",
                "question": question_text,
                "options": option_lines,
                "answer_index": answer_index,
                "explanation": explanation
            })
    return actions

if __name__ == "__main__":
    # quick manual test
    sample = """
#MSG
Maths

#Q1
2+2 = ?
A) 3
B) 4
C) 5
#ANS: B
#EXP: Because 2+2 equals 4

#MSG
Done
"""
    try:
        res = parse_bulk(sample)
        import json
        print(json.dumps(res, indent=2, ensure_ascii=False))
    except ParseError as e:
        print("ParseError:", e)
