import json
from pathlib import Path

import pytest

from adapter_work.cohort_preparation import WORK, build_inventory, public_payload
from adapter_work.common.tests.test_cohort import make_bundle, pin


def profiles():
    return {method: pin(WORK / "prompts" / (method + ".json")) for method in ("direct", "ours")}


def test_real_adapters_prepare_new_complete_inventory_and_request_loader(tmp_path):
    frozen = make_bundle(tmp_path / "cohort", repeated_task=True, count=3)
    methods = ("direct", "ours", "direct-feedback", "correct-detail", "simple-correct", "native-set-count")
    result = build_inventory([frozen], methods, tmp_path / "inventory", profiles=profiles(), bindings={})
    assert result["counts"]["A_positions"] == 18
    assert result["counts"]["initial_requests"] == 6
    assert {c["cell_id"] for c in result["cells"]} == {f"M{i:02d}-B01" for i in range(1, 7)}
    for cell in result["cells"]:
        assert len(cell["positions"]) == 3
        assert all(p["full_flow_five"] is False for p in cell["positions"])
        assert all(p["cohort_sha256"] == frozen["sha256"] for p in cell["positions"])
        if cell["method"] in {"direct", "ours"}:
            for p in cell["positions"]:
                template = json.loads(Path(p["request_template"]["path"]).read_bytes())
                assert "DO_NOT_READ_OR_SEND" not in json.dumps(template)
                assert template["task_id"] == p["task_id"]
                assert len(template["body"]["messages"]) >= 2
        else:
            assert all("source" in p for p in cell["positions"])
    # Current request loader accepts the new legacy-compatible shape. Repeated
    # task IDs deliberately require the new runtime's execution_task_id branch.
    from adapter_work.request_pipeline import load_jobs
    bindings = {m: {"model": "fixture", "ready": True, "sampling": {}} for m in ("direct", "ours")}
    with pytest.raises(Exception, match="Duplicate prepared path identity"):
        load_jobs(result, bindings, simulated=True)


def test_unique_tasks_load_with_existing_request_path(tmp_path):
    from adapter_work.request_pipeline import load_jobs
    frozen = make_bundle(tmp_path / "cohort", count=3)
    result = build_inventory([frozen], ("direct", "ours"), tmp_path / "inventory", profiles=profiles(), bindings={})
    bindings = {m: {"model": "fixture", "ready": True, "sampling": {}} for m in ("direct", "ours")}
    jobs, positions = load_jobs(result, bindings, simulated=True)
    assert len(jobs) == len(positions) == 6


def test_ours_explicit_v3_profile_matches_generation_helper(tmp_path):
    from common.cohort_specs import prepare_cell_specs
    frozen = make_bundle(tmp_path / "cohort")
    current_profiles = profiles()
    current_profiles["ours"] = pin(WORK / "prompts/ours_contract_v3.json")
    result = build_inventory([frozen], ("ours",), tmp_path / "inventory", profiles=current_profiles, bindings={})
    cell = result["cells"][0]
    specs = prepare_cell_specs(cell)
    assert cell["prompt_profile"]["sha256"] == current_profiles["ours"]["sha256"]
    for position, spec in zip(cell["positions"], specs):
        template = json.loads(Path(position["request_template"]["path"]).read_bytes())
        assert template["body"] == spec.as_dict()["body"]
    assert current_profiles["ours"]["sha256"] != profiles()["ours"]["sha256"]


@pytest.mark.parametrize("benchmark", ["text2gql", "mindthequery", "zograscope", "osdb_stkg"])
def test_other_benchmark_adapters_accept_new_positions(tmp_path, benchmark):
    frozen = make_bundle(tmp_path / "cohort", benchmark=benchmark)
    result = build_inventory([frozen], ("direct", "ours"), tmp_path / "inventory", profiles=profiles(), bindings={})
    assert result["counts"]["initial_requests"] == 4


def test_no_overwrite_or_unbound_dependency(tmp_path):
    frozen = make_bundle(tmp_path / "cohort")
    with pytest.raises(ValueError, match="requires ours"):
        build_inventory([frozen], ("correct-detail",), tmp_path / "inventory", profiles=profiles(), bindings={})
    build_inventory([frozen], ("direct",), tmp_path / "inventory", profiles=profiles(), bindings={})
    with pytest.raises(FileExistsError):
        build_inventory([frozen], ("direct",), tmp_path / "inventory", profiles=profiles(), bindings={})


def test_schema_and_context_resolved_without_dropping_dialect_or_candidates(tmp_path):
    schema = tmp_path / "schema.json"
    schema.write_text('{"name":"g"}')
    position = {"source_position_id": "p", "graph": "g", "public_payload": {
        "graph": "g", "question": "Q?", "schema_binding": pin(schema), "entity_candidates": [{"name": "X"}], "database_dialect": "Native"}}
    result = public_payload(position, "Text2GQL", {})
    body = json.loads(result["messages"][0]["content"])
    assert body["schema"] == {"name": "g"}
    assert body["entity_candidates"] == [{"name": "X"}]
    assert body["database_dialect"] == "Native"
    position["public_payload"]["reference_query"] = "RETURN 1"
    with pytest.raises(ValueError, match="Unexpected fields"):
        public_payload(position, "Text2GQL", {})


def test_old_preparation_still_requires_twenty_and_five(tmp_path):
    from adapter_work.preparation import build
    source = tmp_path / "materials/adapter_positions_v22/cells"
    source.mkdir(parents=True)
    (source / "direct__CypherBench.positions.json").write_text('{"positions":[]}')
    with pytest.raises(ValueError, match="frozen 20 positions"):
        build(tmp_path / "materials", tmp_path / "old", methods=("direct",))
