import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ADAPTER_WORK))

from common.loader import load  # noqa: E402

adapter = load(Path(__file__).resolve().parents[1] / "adapter.py")


def test_prepare_reads_twenty_positions():
    specs = adapter.prepare()
    assert len(specs) == 20
    assert sum(1 for s in specs if s["full_flow_five"]) == 5
    assert all(s["messages"] for s in specs)


def test_parse_classifies_replies():
    assert adapter.parse("MATCH (n) RETURN n").status == "answered"
    assert adapter.parse("").status == "format_error"
    assert adapter.parse("I cannot help").status == "refused"
