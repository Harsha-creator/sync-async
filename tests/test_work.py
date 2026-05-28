from app.work import MAX_COMPLEXITY, run_work


def test_determinism_same_input_same_output():
    a = run_work({"text": "hello world", "complexity": 1})
    b = run_work({"text": "hello world", "complexity": 1})
    assert a == b


def test_different_text_different_hash():
    a = run_work({"text": "hello", "complexity": 1})
    b = run_work({"text": "world", "complexity": 1})
    assert a["sha256"] != b["sha256"]


def test_complexity_changes_hash_chain():
    a = run_work({"text": "hello", "complexity": 1})
    b = run_work({"text": "hello", "complexity": 2})
    assert a["sha256"] != b["sha256"]
    assert b["iterations"] > a["iterations"]


def test_complexity_clamped():
    out = run_work({"text": "x", "complexity": 9999})
    assert out["complexity"] == MAX_COMPLEXITY


def test_complexity_lower_bound():
    out = run_work({"text": "x", "complexity": 0})
    assert out["complexity"] == 1


def test_counts_match():
    out = run_work({"text": "one two three", "complexity": 1})
    assert out["word_count"] == 3
    assert out["char_count"] == len("one two three")


def test_invalid_text_type():
    import pytest

    with pytest.raises(ValueError):
        run_work({"text": 123, "complexity": 1})
