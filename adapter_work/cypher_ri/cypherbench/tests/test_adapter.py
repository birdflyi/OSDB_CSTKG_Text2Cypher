"""Offline checks for M07-B01. Fake runtime and scripted replies only."""

import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import adapter  # noqa: E402
from adapter import (  # noqa: E402
    Prediction,
    consume,
    next_request,
    parse,
    prepare,
)

POSITIONS = Path(
    r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\cypher-ri__CypherBench.positions.json"
)


class FakeRuntime:
    def __init__(self):
        self.calls = []
        self.answers = {}

    def save_answer(self, position_id, cypher, raw):
        self.calls.append(("save", position_id))
        self.answers[position_id] = cypher

    def graph_lease(self, graph):
        self.calls.append(("lease", graph))
        return "lease:" + graph

    def db_client(self, lease, cypher):
        self.calls.append(("db", lease))
        return [{"n": 1}]

    def scorer(self, task_id, cypher, records):
        self.calls.append(("score", task_id))
        return 1.0


def test_prepare_parses_schema_and_hides_properties():
    specs = prepare(POSITIONS)
    assert len(specs) == 20
    assert sum(1 for s in specs if s.full_flow_five) == 5
    first = specs[0]
    assert first.turn == 1
    assert first.schema.labels and first.schema.relationships
    prompt = first.messages[0]["content"]
    assert "Labels:" in prompt and "Question:" in prompt
    properties = [p for values in first.schema.properties.values() for p in values]
    assert properties and not any(p in prompt for p in properties)


def test_prepare_rejects_changed_input(tmp_path):
    document = json.loads(POSITIONS.read_text(encoding="utf-8-sig"))
    document["positions"][0]["public_input_sha256"] = "0" * 64
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(adapter.IntegrityError):
        prepare(broken)


def test_next_request_expands_selected_schema():
    spec = prepare(POSITIONS, limit=1)[0]
    label = spec.schema.labels[0]
    relation = spec.schema.relationships[0]
    reply = 'thinking <json>{"labels":["%s"],"relationships":["%s"]}</json>' % (label, relation)
    turn2, accepted = next_request(spec, reply)
    assert accepted is True
    assert turn2 is not None and turn2.turn == 2
    text = turn2.messages[0]["content"]
    assert "Full schema:" in text
    assert label in text


def test_bad_selection_falls_back_to_full_schema():
    spec = prepare(POSITIONS, limit=1)[0]
    turn2, accepted = next_request(spec, "no json here")
    assert accepted is False
    assert turn2 is not None
    assert "Full schema:" in turn2.messages[0]["content"]


def test_parse_reads_only_answer_block():
    assert parse("ok <answer>MATCH (n) RETURN n</answer>").cypher == "MATCH (n) RETURN n"
    assert parse("no answer block").status == "format_error"
    assert parse("MATCH (n) RETURN n", http_status=500).status == "unknown"
    assert parse("<answer>MATCH (n) RETURN n</answer>", accepted=False).fallback is True


def test_consume_counts_fallbacks_and_keeps_denominator():
    specs = prepare(POSITIONS)
    predictions = {
        s.position_id: Prediction("answered", "MATCH (n) RETURN n", "raw", fallback=(i % 2 == 0))
        for i, s in enumerate(specs[:15])
    }
    runtime = FakeRuntime()
    result = consume(specs, predictions, runtime)
    assert result.positions == 20
    assert result.answered == 15
    assert result.not_dispatched == 5
    assert result.fallbacks == 8
    kinds = [c[0] for c in runtime.calls]
    assert kinds.index("save") < kinds.index("db") < kinds.index("score")
