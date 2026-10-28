import asyncio
import json

from engine.config import canonical, digest
from engine.matrix import audit_matrix, run_matrix
from engine.matrix_wall import snapshot


def test_matrix_failure_and_missing_adapter_do_not_prevent_next_cell(tmp_path):
    config = tmp_path / "config.json"
    config.write_bytes(canonical(dict(test_fixture=True)))
    cells = [
        dict(
            name=n,
            method=n,
            benchmark="fixture",
            pipeline_config=str(config),
            config_sha256=digest(config.read_bytes()),
        )
        for n in ("bad", "good")
    ]
    cells.insert(
        1,
        dict(
            name="missing",
            method="author-method",
            benchmark="fixture",
            missing=["author inference assets not available"],
        ),
    )
    plan = tmp_path / "matrix.json"
    plan.write_bytes(canonical(dict(mode="smoke", cells=cells)))
    audit = audit_matrix(plan, tmp_path / "audit.json")
    assert audit["ready"] == 2 and audit["blocked"] == 1
    seen = []

    async def fake_pipeline(path):
        seen.append(path)
        if len(seen) == 1:
            raise ConnectionError("local fixture runtime failed")
        return dict(planned=2, completed=2, blocked=0, incomplete=[])

    root = tmp_path / "run"
    result = asyncio.run(run_matrix(plan, root, run_pipeline=fake_pipeline))
    assert len(seen) == 2 and result["completed"] == 1 and result["blocked"] == 1
    assert result["incomplete"] == ["bad"] and result["dispatched_blocks"] == 2
    wall = snapshot(plan, root)
    assert [r["smoke"] for r in wall["cells"]] == [False, False, True]
    asyncio.run(run_matrix(plan, root, run_pipeline=fake_pipeline, resume=True))
    assert len(seen) == 2  # receipts prevent a paid rerun on a terminal failure


def test_shared_feedback_view_reuses_source_without_dispatch(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}")
    plan = tmp_path / "matrix.json"
    plan.write_bytes(
        canonical(
            dict(
                mode="smoke",
                cells=[
                    dict(
                        name="direct",
                        method="Direct",
                        benchmark="fixture",
                        pipeline_config=str(config),
                        config_sha256=digest(config.read_bytes()),
                    ),
                    dict(
                        name="feedback",
                        method="Direct+feedback",
                        benchmark="fixture",
                        shared_with="direct",
                    ),
                ],
            )
        )
    )
    calls = []

    async def pipeline(path):
        calls.append(path)
        return dict(planned=1, completed=1)

    result = asyncio.run(run_matrix(plan, tmp_path / "run", run_pipeline=pipeline))
    assert len(calls) == 1 and result["completed"] == 2
    assert result["blocks"][1]["result"]["additional_model_calls"] == 0


def test_changed_config_is_not_dispatched(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}")
    plan = tmp_path / "matrix.json"
    plan.write_bytes(
        canonical(
            dict(
                mode="smoke",
                cells=[
                    dict(
                        name="changed",
                        method="Direct",
                        benchmark="fixture",
                        pipeline_config=str(config),
                        config_sha256="wrong",
                    )
                ],
            )
        )
    )

    async def forbidden(path):
        raise AssertionError("must not dispatch")

    result = asyncio.run(run_matrix(plan, tmp_path / "run", run_pipeline=forbidden))
    assert result["blocked"] == 1 and result["dispatched_blocks"] == 0


def test_matrix_uses_one_shared_dispatch_pipeline(tmp_path):
    root = tmp_path / "run"
    config = tmp_path / "config.json"
    config.write_bytes(
        canonical(
            dict(
                overlap_initial_generation=True,
                run_dir=str(root),
                stages=[
                    dict(name="a", method="Direct", benchmark="fixture"),
                    dict(name="b", method="Ours", benchmark="fixture"),
                ],
            )
        )
    )
    sha = digest(config.read_bytes())
    plan = tmp_path / "matrix.json"
    plan.write_bytes(
        canonical(
            dict(
                mode="smoke",
                dispatch_pipeline=dict(path=str(config), sha256=sha),
                cells=[
                    dict(
                        name=name,
                        method=method,
                        benchmark="fixture",
                        pipeline_config=str(config),
                        config_sha256=sha,
                    )
                    for name, method in [("a", "Direct"), ("b", "Ours")]
                ],
            )
        )
    )
    calls = []

    async def pipeline(path, resume=False):
        calls.append(path)
        return dict(completed=2)

    result = asyncio.run(run_matrix(plan, root, run_pipeline=pipeline))
    assert calls == [config] and result["completed"] == 2
