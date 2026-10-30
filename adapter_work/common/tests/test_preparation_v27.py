"""Regression checks against frozen inputs and real scheduler contracts; no network."""
import asyncio
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest

from common import author, controls, direct, feedback, loader, ours, ri, stages
from common.dispatch import generate_path
from common.errors import IntegrityError
from common.prompts import compose_ours, DEFAULT_PROFILES
from engine.config import canonical
from engine.fake import FakeTransport
from engine.tests.test_scheduler import make_scheduler
from engine.transport import classify

ROOT = Path(r"D:\ch7\reports\t1_preparation_20261029")
BENCHMARKS = ("CypherBench", "Text2GQL", "MindTheQuery", "ZOGRASCOPE", "OSDB-STKG")


def positions(method, benchmark):
    return ROOT / "adapter_positions_v22" / "cells" / f"{method}__{benchmark}.positions.json"


@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_all_frozen_prompts_and_shared_initials(benchmark):
    rows = json.loads(positions("direct", benchmark).read_text(encoding="utf-8-sig"))["positions"]
    for row in rows:
        payload = json.loads(Path(row["public_input_path"]).read_text(encoding="utf-8-sig"))
        for method, compose in (("direct", direct.compose), ("ours", compose_ours)):
            frozen = ROOT / "delegation_bc_v23/prepared_messages" / method / (row["position_id"] + ".messages.json")
            assert list(compose(benchmark, payload)) == json.loads(frozen.read_text(encoding="utf-8-sig"))["messages"]
    direct_messages = direct.compose(benchmark, payload)
    assert feedback.prepare(positions("direct-feedback", benchmark), benchmark, "test")[-1].messages == direct_messages
    for method in ("cyphersmith", "cyquark"):
        assert author.prepare(positions(method, benchmark), benchmark, "test", method)[-1]["messages"] == direct_messages


def test_prompt_missing_or_changed_never_falls_back(tmp_path):
    with pytest.raises(FileNotFoundError):
        compose_ours("CypherBench", {}, tmp_path / "absent")
    profiles = json.loads(DEFAULT_PROFILES.read_text(encoding="utf-8"))
    profiles["CypherBench"]["prefix_messages"][0]["content"] = "tampered"
    path = tmp_path / "changed.json"
    path.write_text(json.dumps(profiles))
    with pytest.raises(IntegrityError):
        compose_ours("CypherBench", {}, path)


def test_ri_preserves_real_schema_property_scopes():
    mtq = ri.prepare(positions("cypher-ri", "MindTheQuery"), "MindTheQuery", "test")[0]
    assert any(p.name == "lastUsed" for r in mtq.schema.relations for p in r.properties)
    zog = ri.prepare(positions("cypher-ri", "ZOGRASCOPE"), "ZOGRASCOPE", "test")[0]
    assert len(zog.schema.global_properties) == 21
    selection = ri.Selection((mtq.schema.entities[0].label,), (), True, "ok")
    assert all(r.label + " " not in mtq.schema.expanded_text(selection) for r in mtq.schema.relations)
    assert "lastUsed" not in mtq.schema.compact_text()
    assert '"age"' in zog.schema.expanded_text(ri.Selection((), (), False, "empty"))


def test_ri_stop_usage_and_budget_are_required():
    spec = ri.prepare(positions("cypher-ri", "CypherBench"), "CypherBench", "test", limit=1, total_tokens=40)[0]
    raw = '<json>' + json.dumps(dict(name=spec.schema.graph, entities=[spec.schema.entities[0].label], relations=[]))
    assert ri.next_request(spec, raw, generated_tokens=5).reason == "truncated_before_stop"
    assert ri.next_request(spec, raw + '</json>').reason == "missing_generated_token_usage"
    step = ri.next_request(spec, raw, generated_tokens=12, finish_reason="stop", matched_stop="</json>")
    assert step.request.state["token_budget_remaining"] == 28
    assert step.request.as_dict()["body"]["max_tokens"] == 28
    assert step.request.messages[0]["content"].startswith(spec.messages[0]["content"] + raw + '</json>')
    assert ri.next_request(spec, raw + '</json>', generated_tokens=40).reason == "token_budget_exhausted"
    assert ri.next_request(spec, raw, generated_tokens=12, finish_reason="length", matched_stop="</json>").action == "failed"
    with pytest.raises(stages.StageError):
        stages.check(spec, stages.NextStep("request", spec), previous_stage="schema_select", position_id=spec.position_id)


def test_osdb_json_is_unwrapped_and_declines_stay_in_denominator():
    p = direct.parse('{"status":"ANSWER","query":"RETURN 1"}')
    assert p.cypher == "RETURN 1"
    assert direct.parse('{"status":"UNSUPPORTED","reason":"outside schema"}').status == "unsupported"
    assert direct.parse('{"status":"INSUFFICIENT_EVIDENCE","reason":"history absent"}').status == "refused"


@pytest.mark.parametrize("error", [PermissionError("disk"), FileNotFoundError("receipt"), controls.EvidenceConflict("hash")])
def test_control_storage_and_identity_errors_propagate(error):
    runtime = SimpleNamespace(verify_source=lambda _: (_ for _ in ()).throw(error))
    with pytest.raises(type(error)):
        controls.replay({"position_id": "p"}, runtime, control_id="correct-detail")


def test_multi_query_plan_reaches_execute_plan_unflattened():
    from common import consumption
    compiled = {"queries": ["RETURN 1", "RETURN 2"], "combine": {"op": "union"}}
    seen = []
    runtime = SimpleNamespace(
        compile=lambda *a, **k: compiled, save_prediction=lambda *a: None,
        graph_lease=lambda g: g, execute=lambda *a: pytest.fail("flattened plan"),
        execute_plan=lambda lease, pos, plan: seen.append(plan) or {"rows": [[1], [2]]},
        save_final_answer=lambda *a: None, score_saved_answer=lambda *a: 1.0,
    )
    plan = {"status":"ANSWER", "calls":[{"nodes":[{"id":"n","label":"X"}], "returns":[{"field":"n","alias":"x"}]}], "combine":{"op":"single"}}
    result = ours.consume([dict(position_id="p", graph="g", benchmark="CypherBench")],
                          {"p": consumption.Prediction("ready", raw=json.dumps(plan))}, runtime)
    assert result.executed == 1 and seen == [compiled]


def test_completion_response_is_not_mistaken_for_chat():
    body = dict(model="m", choices=[dict(text='<json>{}', finish_reason="stop", stop_reason="</json>")], usage={"completion_tokens":5})
    assert classify(dict(http_status=200, body=body), "m")["status"] == "unknown"
    result = classify(dict(http_status=200, body=body), "m", completion=True)
    assert result["status"] == "ok" and result["matched_stop"] == "</json>"


def test_ri_two_stages_use_existing_scheduler_and_separate_billing(tmp_path):
    spec = ri.prepare(positions("cypher-ri", "CypherBench"), "CypherBench", "test", limit=1, total_tokens=40)[0]
    wires = []
    def response(call, wire, endpoint):
        body = json.loads(wire); wires.append(body)
        text = '<json>' + json.dumps(dict(name=spec.schema.graph, entities=[], relations=[])) if call == 1 else '<answer>RETURN 1</answer>'
        return dict(status="ok", content=text, usage={"completion_tokens":12}, finish_reason="stop",
                    matched_stop="</json>" if call == 1 else None, http_status=200)
    async def scenario():
        scheduler, _, transport, ledger = await make_scheduler(tmp_path, cap=2, transport=FakeTransport(0, response))
        from engine.config import Endpoint
        await scheduler.pool.update([Endpoint("a", "http://127.0.0.1/not-called", "m", "test", 2,
                                             wire_params=("model", "prompt", "max_tokens", "stop"))])
        try:
            result = await generate_path(ri, spec, scheduler, model="m", sampling={})
            assert result["status"] == "ready" and transport.calls == 2
            assert len(ledger.records) == 2 and not ledger.summary()["pending"]
            assert wires[1]["max_tokens"] == 28
            assert all("messages" not in body for body in wires)
        finally:
            scheduler.http_workers.shutdown(); scheduler.db_workers.shutdown()
    asyncio.run(scenario())
