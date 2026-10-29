import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ADAPTER_WORK))

import adapter  # noqa: E402


def test_prepare_reads_twenty_positions():
    specs = adapter.prepare()
    assert len(specs) == 20
    assert sum(1 for s in specs if s.full_flow_five) == 5
    assert all(s.schema.entities for s in specs)
    assert all(s.messages for s in specs)


def test_parse_reads_only_the_answer_block():
    assert adapter.parse("<answer>MATCH (n) RETURN n</answer>").status == "ready"
    assert adapter.parse("no answer").status == "format_error"
    assert adapter.parse("MATCH (n) RETURN n", http_status=504).status == "unknown"
