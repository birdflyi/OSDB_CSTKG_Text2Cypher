import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from adapter_work.common.tests.test_cohort import make_bundle
from adapter_work.common.tests.test_cohort_preparation import profiles
from adapter_work.cohort_preparation import build_inventory
from adapter_work import cohort_pipeline as pipeline
from adapter_work import request_pipeline, offline_pipeline
from engine.errors import IntegrityError


def fixture(tmp_path):
    cohort = make_bundle(tmp_path / "cohort", count=3, repeated_task=True)
    methods = ("direct", "ours", "direct-feedback", "correct-detail", "simple-correct", "native-set-count")
    inventory = build_inventory([cohort], methods, tmp_path / "inventory", profiles=profiles(), bindings={})
    inventory_pin = pipeline.save(tmp_path / "inventory.json", inventory)
    output = tmp_path / "run"
    generation = dict(source=request_pipeline.source_manifest(), inventory=inventory_pin,
                      bindings={m: dict(model="fixture", ready=True, sampling={}) for m in ("direct", "ours", "direct-feedback")},
                      name="cohort-test", run_dir=str(output / "generation"),
                      ledger_dir=str(tmp_path / "ledger"), approval_path=str(tmp_path / "approval.json"),
                      budget_scope="fixture", simulation_cap=2, physical_attempt_cap=30)
    consumer = dict(source=offline_pipeline.source_manifest(), inventory=inventory_pin,
                    selection=dict(mode="cohort", cohorts=[cohort]), source_finals=[],
                    output=str(output / "consumption"), backends={"CypherBench": {}}, execute_authorized=True)
    return dict(schema_version="cohort_pipeline_v1", output=str(output), physical_attempt_cap=30,
                request_config=pipeline.save(tmp_path / "request.json", generation),
                offline_config=pipeline.save(tmp_path / "offline.json", consumer))


class Backend:
    def execute(self, row, source, directory):
        return dict(status="EXECUTED", complete=True, columns=["n"], rows=[[1]])

    control = execute

    def score(self, row, answer, receipt):
        assert json.loads(Path(receipt["path"]).read_bytes()) == answer
        return dict(status="scored", value=True, method=row["method"], partition="C")


@contextmanager
def backend(*args):
    yield Backend()


def test_generate_and_consume_are_independent_complete_cohort_programs(tmp_path):
    config = fixture(tmp_path)
    assert pipeline.verify(config)["initial_requests"] == 6
    generation_only = {k: v for k, v in config.items() if k != "offline_config"}
    receipt = pipeline.generate(generation_only, simulated=True)
    assert len(pipeline.read_pin(receipt)["source_finals"]) == 6
    assert not (Path(config["output"]) / "consumption").exists()
    final = pipeline.consume(config, simulated=True, backend_factory=backend)
    assert final["planned"] == 18 and len(final["results"]) == 18
    assert final["counts"] == {"EXECUTED": 18}
    assert len({r["position_id"] for r in final["results"]}) == 3
    assert all(r["cohort_sha256"] for r in final["results"])
    with pytest.raises(FileExistsError):
        pipeline.generate(generation_only, simulated=True)


def test_complete_cohort_selector_does_not_accept_old_smoke_limit(tmp_path):
    config = fixture(tmp_path)
    consumer = pipeline.read_pin(config["offline_config"])
    consumer["per_cell"] = 5
    with pytest.raises(IntegrityError):
        offline_pipeline.load_selection(consumer)


def test_failed_case_enqueues_one_feedback_and_other_cells_continue(tmp_path):
    config = fixture(tmp_path)
    class Retry(Backend):
        def execute(self, row, source, directory):
            if row["method"] == "direct":
                return dict(status="execution_error", complete=False, error_type="SyntaxError",
                            error="bad query", database_code="Neo.ClientError.Statement.SyntaxError")
            return super().execute(row, source, directory)
    @contextmanager
    def factory(*args):
        yield Retry()
    pipeline.generate(config, simulated=True)
    final = pipeline.consume(config, simulated=True, backend_factory=factory)
    assert final["planned"] == 18 and final["feedback_positions"] == 3
    repaired = [r for r in final["results"] if r["method"] == "direct-feedback"]
    assert all(r["status"] == "EXECUTED" and r["feedback_revision"] for r in repaired)
    assert len([r for r in final["results"] if r["method"] == "direct" and r["status"] == "execution_error"]) == 3
