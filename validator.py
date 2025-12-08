# validator.py
# Simple Validator for DES / Q / A / B / C / D / ... / ANS / EXP blocks
# - Minimal rules as requested: structural validation only.
# - Ignores lines containing Eg(...
# - DES optional. If present before first Q:, extracted and returned.
# - Q blocks must follow: Q: -> A:, B:, C:, D: (E-L optional) -> ANS: -> EXP: (optional, after ANS)
# - ANS must be a single letter matching one of the present option labels.
# - Provides errors/warnings in a short, clear format.

import re
from typing import List, Dict, Any, Tuple, Optional


class ValidationError(Exception):
    pass


def _strip_eg_lines(lines: List[str]) -> List[str]:
    """Remove any lines that contain 'Eg(' anywhere (case-sensitive as requested)."""
    return [ln for ln in lines if 'Eg(' not in ln]


_option_label_re = re.compile(r'^([A-Z]):\s*(?:\((?:[A-Z])\))?\s*(.*)$')  # matches "A: (A) text" or "A: text"
_ans_re = re.compile(r'^ANS:\s*(\S+)\s*$')
_q_re = re.compile(r'^Q:\s*(.*)$')
_des_re = re.compile(r'^DES:\s*(.*)$')
_exp_re = re.compile(r'^EXP:\s*(.*)$')


def validate_and_parse(raw_text: str) -> Dict[str, Any]:
    """
    Validate raw_text and parse into structured form.

    Returns dict:
    {
      "ok": bool,
      "errors": [str],
      "warnings": [str],
      "des": Optional[str],
      "questions": [
         {
           "index": 1,
           "raw_question": "...",
           "options": {"A": "text", "B": "text", ...},
           "ans": "B",
           "exp": "explanation or None",
           "errors": [...],   # per-question errors if any
           "warnings": [...],
         },
         ...
      ]
    }
    """
    # Normalize lines
    lines = raw_text.splitlines()
    lines = [ln.rstrip() for ln in lines]
    # remove Eg(...) lines entirely
    lines = _strip_eg_lines(lines)

    result = {"ok": True, "errors": [], "warnings": [], "des": None, "questions": []}

    i = 0
    n = len(lines)
    seen_q = False
    des_found = False

    # If a DES appears before first Q, capture it. If appears later inside a Q block, that's an error.
    # We'll parse sequentially.
    while i < n:
        ln = lines[i].strip()
        if ln == "":
            i += 1
            continue

        # DES handling (only valid if before first Q)
        m_des = _des_re.match(ln)
        if m_des and not seen_q:
            result["des"] = m_des.group(1).strip()
            des_found = True
            i += 1
            continue
        elif m_des and seen_q:
            # DES inside question area -> error
            result["errors"].append(f"⚠️ DES found inside question block (line {i+1})")
            result["ok"] = False
            i += 1
            continue

        # Q handling
        m_q = _q_re.match(ln)
        if m_q:
            seen_q = True
            q_text = m_q.group(1).strip()
            q_obj = {
                "index": len(result["questions"]) + 1,
                "raw_question": q_text,
                "options": {},
                "ans": None,
                "exp": None,
                "errors": [],
                "warnings": [],
            }
            i += 1
            # parse option lines until we hit ANS:, EXP:, next Q: or EOF
            # options must have at least A,B,C,D (we'll enforce minimal count later)
            while i < n:
                cur = lines[i].strip()
                if cur == "":
                    i += 1
                    continue
                # If new question starts, break (we will continue outer loop)
                if _q_re.match(cur):
                    break
                # If DES appears here -> error (DES must not be inside Q block)
                if _des_re.match(cur):
                    q_obj["errors"].append(f"⚠️ DES found inside question block (Question {q_obj['index']})")
                    result["ok"] = False
                    i += 1
                    continue
                # ANS?
                m_ans = _ans_re.match(cur)
                if m_ans:
                    ans_val = m_ans.group(1).strip()
                    # Accept single letter or single-letter followed by punctuation (but we insist on one char letter)
                    if len(ans_val) != 1 or not ans_val.isalpha():
                        q_obj["errors"].append(f'⚠️ ANS must be a single letter (Question {q_obj["index"]})')
                        result["ok"] = False
                        q_obj["ans"] = ans_val  # still store raw for debugging
                    else:
                        q_obj["ans"] = ans_val.upper()
                    i += 1
                    continue
                # EXP?
                m_exp = _exp_re.match(cur)
                if m_exp:
                    # EXP allowed only after ANS (per your rule)
                    if q_obj["ans"] is None:
                        q_obj["errors"].append(f'⚠️ EXP must come after ANS (Question {q_obj["index"]})')
                        result["ok"] = False
                        # still capture exp text
                        q_obj["exp"] = m_exp.group(1).strip()
                    else:
                        q_obj["exp"] = m_exp.group(1).strip()
                    i += 1
                    continue
                # Option?
                m_opt = _option_label_re.match(cur)
                if m_opt:
                    label = m_opt.group(1).upper()
                    text_after = m_opt.group(2).strip()
                    # store option even if text empty (we'll warn)
                    q_obj["options"][label] = text_after
                    i += 1
                    continue
                # Unknown line inside Q block -- treat as trailing text; stop parsing options and consider as potential error
                # But because you requested minimal rules, we will tolerate extra lines between blocks:
                # If it doesn't match anything, we treat it as continuation of previous option if any, else as warning.
                # Simple approach: if options exist, append this line to the last option's text.
                if q_obj["options"]:
                    # append to last option text (preserves content)
                    last_label = sorted(q_obj["options"].keys())[-1]
                    if q_obj["options"][last_label]:
                        q_obj["options"][last_label] += " " + cur
                    else:
                        q_obj["options"][last_label] = cur
                    i += 1
                    continue
                else:
                    # no options yet and line unknown => it's unexpected before options; make a warning and skip
                    q_obj["warnings"].append(f"⚠️ Unexpected line inside question block (Question {q_obj['index']}, line {i+1})")
                    i += 1
                    continue

            # After finishing question block parsing, run simple checks:
            # Must have at least options A,B,C,D
            required_labels = ["A", "B", "C", "D"]
            for lbl in required_labels:
                if lbl not in q_obj["options"]:
                    q_obj["errors"].append(f"⚠️ Question {q_obj['index']} missing option {lbl}")
                    result["ok"] = False
            # ANS must be present and must match one of the option labels
            if q_obj["ans"] is None:
                q_obj["errors"].append(f"⚠️ Missing ANS in Question {q_obj['index']}")
                result["ok"] = False
            else:
                if q_obj["ans"] not in q_obj["options"]:
                    q_obj["errors"].append(f'⚠️ ANS "{q_obj["ans"]}" does not match any option in Question {q_obj["index"]}')
                    result["ok"] = False
            # EXP already handled above (position). No further checks required.

            result["questions"].append(q_obj)
            continue  # continue outer while (we did not increment i here because inner loop did)
        else:
            # Line outside Q or DES (before first Q or between blocks). If it's non-empty and not Eg, allow (we don't strictly forbid).
            # However if it looks like ANS/EXP outside a Q, warn.
            if _ans_re.match(ln):
                result["warnings"].append(f"⚠️ ANS found outside any question block (line {i+1})")
            elif _exp_re.match(ln):
                result["warnings"].append(f"⚠️ EXP found outside any question block (line {i+1})")
            # ignore otherwise
            i += 1
            continue

    # Post-parse: if no questions found -> error
    if len(result["questions"]) == 0:
        result["errors"].append("⚠️ No questions found (no Q: blocks).")
        result["ok"] = False

    return result


# Small CLI for quick testing
def _cli():
    import argparse, sys
    parser = argparse.ArgumentParser(description="Validate quiz text (DES/Q/A/B/C/D/ANS/EXP).")
    parser.add_argument("file", nargs="?", help="Input text file (if omitted, reads stdin).")
    args = parser.parse_args()
    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            text = f.read()
    else:
        text = sys.stdin.read()
    out = validate_and_parse(text)
    # print summary
    print("OK:" if out["ok"] else "NOT OK:")
    if out.get("des"):
        print(f"DES: {out['des']}")
    print(f"Questions parsed: {len(out['questions'])}")
    for q in out["questions"]:
        print(f"\nQuestion {q['index']}:")
        print(f"  Q: {q['raw_question']!s}")
        for lbl, txt in sorted(q["options"].items()):
            print(f"  {lbl}: {txt!s}")
        print(f"  ANS: {q['ans']!s}")
        print(f"  EXP: {q['exp']!s}")
        if q["errors"]:
            print("  ERRORS:")
            for e in q["errors"]:
                print("   -", e)
        if q["warnings"]:
            print("  WARNINGS:")
            for w in q["warnings"]:
                print("   -", w)
    if out["warnings"]:
        print("\nGlobal warnings:")
        for w in out["warnings"]:
            print(" -", w)
    if out["errors"]:
        print("\nGlobal errors:")
        for e in out["errors"]:
            print(" -", e)
    # exit code 0 if ok, 1 if not
    sys.exit(0 if out["ok"] else 1)


if __name__ == "__main__":
    _cli()
