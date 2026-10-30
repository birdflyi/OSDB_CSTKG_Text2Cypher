"""Offline checks for M07-B01 under CONTRACT_V2. Scripted replies only."""

import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.loader import load  # noqa: E402

adapter = load(Path(__file__).resolve().parents[1] / "adapter.py")
Prediction = adapter.Prediction
consume = adapter.consume
next_request = adapter.next_request
parse = adapter.parse
parse_selection = adapter.parse_selection
prepare = adapter.prepare

POSITIONS = Path(
    r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\cypher-ri__CypherBench.positions.json"
)


class Receipt:
    def __init__(self, path):
        self.path = path


class FakeRuntime:
    def __init__(self):
        self.calls = []
        self.finals = {}

    def save_prediction(self, position, prediction):
        self.calls.append(("save_prediction", position.position_id))
        return Receipt("pred")

    def graph_lease(self, graph):
        self.calls.append(("lease", graph))
        return "lease:" + graph

    def execute(self, lease, position, prediction):
        self.calls.append(("execute", lease))
        return {"rows": [{"n": 1}]}

    def save_final_answer(self, position, execution):
        self.calls.append(("save_final_answer", position.position_id))
        self.finals[position.position_id] = execution
        return Receipt("final")

    def replay_controls(self, position, saved):
        self.calls.append(("control", position.position_id))
        return "control_ok"

    def score_saved_answer(self, position, saved):
        self.calls.append(("score", position.task_id))
        return type("Score", (), {"status": "scored", "value": 1.0, "error": None})()


def test_schema_keeps_types_and_endpoints():
    spec = prepare(POSITIONS, limit=1)[0]
    assert spec.schema.entities and spec.schema.relations
    assert any(p.type for e in spec.schema.entities for p in e.properties)
    assert all(r.subject and r.object for r in spec.schema.relations)


def test_compact_prompt_hides_properties():
    spec = prepare(POSITIONS, limit=1)[0]
    prompt = spec.messages[0]["content"]
    properties = [p.name for e in spec.schema.entities for p in e.properties]
    assert properties and not any(name in prompt for name in properties)
    assert "Labels:" in prompt and "Question:" in prompt


def test_selection_accepts_paper_and_public_shape():
    spec = prepare(POSITIONS, limit=1)[0]
    entity = spec.schema.entities[0].label
    relation = spec.schema.relations[0].label
    paper = '<json>{"name":"%s","entities":["%s"],"relations":["%s"]}</json>' % (
        spec.schema.graph,
        entity,
        relation,
    )
    public = '<json>{"labels":["%s"],"relationships":["%s"]}</json>' % (entity, relation)
    assert parse_selection(paper, spec.schema).accepted
    assert parse_selection(public, spec.schema).accepted


def test_selection_rejections_are_bounded():
    spec = prepare(POSITIONS, limit=1)[0]
    for bad in (
        '<json>["Entity"]</json>',
        '<json>{"labels":[{"label":"X"}],"relationships":[]}</json>',
        '<json>{"labels":["Nope"],"relationships":[]}</json>',
        '<json>{broken</json>',
        "no json at all",
    ):
        selection = parse_selection(bad, spec.schema)
        assert selection.accepted is False
        assert selection.reason


def test_empty_selection_is_explicit_not_silent_full():
    spec = prepare(POSITIONS, limit=1)[0]
    selection = parse_selection('<json>{"labels":[],"relationships":[]}</json>', spec.schema)
    assert selection.accepted is False
    assert selection.reason == "empty_selection"
    step = next_request(spec, '<json>{"labels":[],"relationships":[]}</json>', generated_tokens=12)
    assert step.action == "request"
    assert step.state["selection_note"] == "explicit_empty_selection"
    assert step.request.protocol == "completion_prefix"


def test_stage2_continues_the_same_prefix():
    spec = prepare(POSITIONS, limit=1)[0]
    entity = spec.schema.entities[0].label
    relation = spec.schema.relations[0].label
    reply = 'thinking <json>{"labels":["%s"],"relationships":["%s"]}</json>' % (entity, relation)
    step = next_request(spec, reply, generated_tokens=12)
    assert step.action == "request"
    prefix = step.request.messages[0]["content"]
    assert prefix.startswith(spec.messages[0]["content"])
    assert reply in prefix
    assert "<schema>" in prefix and "</schema>" in prefix
    assert step.request.state["generation_stage"] if "generation_stage" in step.request.state else True


def test_missing_stop_and_transport_unknown_do_not_continue():
    spec = prepare(POSITIONS, limit=1)[0]
    assert next_request(spec, "<json>{\"labels\": [").action == "failed"
    assert next_request(spec, "plain refusal").reason == "missing_json_block"
    assert next_request(spec, "<json>[]</json>", transport_unknown=True).reason == "transport_unknown"


def test_parse_answer_and_consume():
    spec = prepare(POSITIONS, limit=1)[0]
    prediction = parse("<answer>MATCH (n) RETURN n</answer>", fallback=True)
    assert prediction.status == "ready" and prediction.fallback is True
    runtime = FakeRuntime()
    result = consume([spec], {spec.position_id: prediction}, runtime)
    assert result.executed == 1 and result.scored == 1
    kinds = [c[0] for c in runtime.calls]
    assert kinds.index("save_prediction") < kinds.index("save_final_answer") < kinds.index("score")
