import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ADAPTER_WORK))

import adapter  # noqa: E402


def test_prepare_reads_twenty_positions():
    specs = adapter.prepare()
    assert len(specs) == 20


def test_parse_returns_an_answer_or_a_classified_failure():
    assert adapter.parse("MATCH (n) RETURN n").status == "answered"
    assert adapter.parse("").status == "format_error"
