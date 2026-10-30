import json
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common import feedback
from feedback_queue import operational_error
from offline_pipeline import save
import request_pipeline


def test_transport_failure_and_correct_answer_never_request_repair():
    assert operational_error(dict(status="EXECUTED", complete=True)) is None
    assert operational_error(dict(status="INFRASTRUCTURE_ERROR")) is None
    assert (
        operational_error(dict(status="execution_error", error_type="TimeoutError"))
        is None
    )
    error = operational_error(
        dict(
            database_code="Neo.ClientError.Statement.SyntaxError",
            error="invalid syntax",
        )
    )
    assert error.type == "ClientError"


def test_feedback_job_preserves_identity_and_uses_central_stage(tmp_path, monkeypatch):
    spec = feedback.RequestSpec(
        "M03",
        "P1",
        "q1",
        "CypherBench",
        "direct-feedback",
        "g",
        None,
        1,
        True,
        "initial",
        "chat",
        ({"role": "user", "content": "question"},),
        str(tmp_path / "input.json"),
        "a" * 64,
        dict(stage="initial"),
    )
    adapter = SimpleNamespace(
        prepare=lambda path: [spec],
        next_request=feedback.next_request,
        parse=feedback.parse,
    )
    monkeypatch.setattr(request_pipeline.loader, "load", lambda path: adapter)
    cell = dict(
        cell_id="M03",
        method="direct-feedback",
        adapter=save(tmp_path / "adapter.json", {}),
        positions_source=save(tmp_path / "positions.json", {}),
    )
    entry = dict(
        position=dict(
            cell_id="M03",
            position_id="P1",
            task_id="q1",
            graph="g",
            benchmark="CypherBench",
        ),
        source=save(
            tmp_path / "source.json",
            dict(
                method="direct",
                benchmark="CypherBench",
                position_id="P1",
                final=dict(raw_prediction="MATCH bad"),
            ),
        ),
        database_final=save(
            tmp_path / "db.json", dict(status="EXECUTION_ERROR", error="invalid syntax")
        ),
    )
    queue = save(tmp_path / "queue.json", [save(tmp_path / "entry.json", entry)])
    jobs, positions = request_pipeline.load_feedback_jobs(
        dict(cells=[cell]), dict(direct=dict(model="m")), queue
    )
    assert len(jobs) == 1 and jobs[0].item.phase == "feedback"
    assert jobs[0].item.method == "direct-feedback"
    body = json.loads(jobs[0].item.wire)
    assert body["messages"][-2]["content"] == "MATCH bad"
    assert "invalid syntax" in body["messages"][-1]["content"]
    assert (
        feedback.next_request(
            jobs[0].spec,
            "again",
            db_error=operational_error({"status": "EXECUTION_ERROR"}),
        ).action
        == "complete"
    )
