# tests/test_parser.py
from parser import parse_bulk, ParseError

def test_example1():
    text = """
#MSG
Maths

#Q1
2+2 = ?
A) 3
B) 4
C) 5
#ANS: B
#EXP: Explanation text...

#MSG
Maths Completed
"""
    actions = parse_bulk(text)
    assert actions[0]['type'] == 'MSG'
    assert actions[1]['type'] == 'POLL'
    assert actions[1]['question'].startswith('2+2')
    assert actions[1]['options'][1] == '4'
    assert actions[1]['answer_index'] == 1
    assert actions[2]['type'] == 'MSG'

def test_example2():
    text = """
#Q1
2+2 = ?
A) 3
B) 4
C) 5
#ANS: B
#EXP: Explanation...

#MSG
Maths Completed

#MSG
Best of luck
"""
    actions = parse_bulk(text)
    assert actions[0]['type'] == 'POLL'
    assert actions[1]['type'] == 'MSG'
    assert actions[2]['type'] == 'MSG'

def test_invalid_no_options():
    text = """
#Q1
Question with no options

#MSG
Hi
"""
    try:
        parse_bulk(text)
        assert False, "Should have raised ParseError"
    except ParseError:
        assert True

def test_exp_without_answer():
    text = """
#Q1
Sample?
A) 1
B) 2
#EXP: explanation but no ans
"""
    try:
        parse_bulk(text)
        assert False, "Should error when EXP without ANS"
    except ParseError:
        assert True
