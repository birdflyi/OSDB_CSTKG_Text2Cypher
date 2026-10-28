from __future__ import annotations

import asyncio
from dataclasses import replace
import pytest

from engine.config import Endpoint, Sampling
from engine.pipeline import run_stages
from engine.errors import IntegrityError
from engine.tests.test_scheduler import make_scheduler


def test_serial_blocks_continue_without_another_user_prompt(tmp_path):
    async def scenario():
        order = []

        async def block(stage):
            order.append(("start", stage["name"]))
            await asyncio.sleep(0.01)
            order.append(("end", stage["name"]))
            return dict(stop_reason=None, budget=dict(pending=[]))

        stages = [
            dict(name="a"),
            dict(name="unavailable", blocked_reason="reference missing"),
            dict(name="b"),
        ]
        result = await run_stages(tmp_path / "pipeline", stages, block)
        assert order == [("start", "a"), ("end", "a"), ("start", "b"), ("end", "b")]
        assert result["completed"] == 2 and result["blocked"] == 1
        await run_stages(tmp_path / "pipeline", stages, block, resume=True)
        assert len(order) == 4

    asyncio.run(scenario())


def test_interrupted_block_is_never_implicitly_resent(tmp_path):
    async def scenario():
        async def fail(stage):
            raise IntegrityError("interrupted")

        stages = [dict(name="a"), dict(name="b")]
        with pytest.raises(IntegrityError):
            await run_stages(tmp_path / "p", stages, fail)
        calls = []

        async def should_not_call(stage):
            calls.append(stage)
            return dict(budget=dict(pending=[]))

        result = await run_stages(tmp_path / "p", stages, should_not_call, resume=True)
        assert calls == [stages[1]]
        assert result["incomplete"] == ["a"] and result["completed"] == 1

    asyncio.run(scenario())


def test_separate_models_share_global_pool_without_identity_collision(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, cap=12, count=16)
        scheduler.spec = replace(scheduler.spec, models=("m", "other"))
        await scheduler.pool.update(
            [
                Endpoint("a", "http://localhost/a", "m", "test", 8),
                Endpoint("b", "http://localhost/b", "other", "test", 4),
            ]
        )
        original = list(items)
        items += [
            replace(i, model="other", wire=i.wire.replace(b'"m"', b'"other"')) for i in original
        ]
        assert len({i.key for i in items}) == 32
        paths = await scheduler.run(items)
        assert len(paths) == 32 and {p["model"] for p in paths} == {"m", "other"}
        assert scheduler.pool.peak <= 12 and len(ledger.records) == 32

    asyncio.run(scenario())


def test_deepseek_wire_keeps_common_sampling_and_explicit_reasoning_budget():
    body = Sampling("deepseek", reasoning_format="chat_template").body([])
    assert body["temperature"] == 0.2 and body["top_p"] == 0.95 and body["max_tokens"] == 8192
    assert body["chat_template_kwargs"] == dict(thinking=True, reasoning_effort=50)
    assert "reasoning_effort" not in body


def test_non_global_block_stop_records_failure_and_runs_next(tmp_path):
    async def scenario():
        calls = []

        async def block(stage):
            calls.append(stage["name"])
            return dict(stop_reason="block_timeout" if stage["name"] == "a" else None)

        result = await run_stages(tmp_path / "run", [dict(name="a"), dict(name="b")], block)
        assert calls == ["a", "b"] and result["incomplete"] == ["a"]
        assert (tmp_path / "run/blocks/a/incomplete.json").exists()

    asyncio.run(scenario())


def test_operator_stop_preserves_receipts_for_unrun_blocks(tmp_path):
    async def scenario():
        async def block(stage):
            return dict(stop_reason="operator_drain")

        result = await run_stages(tmp_path / "run", [dict(name="a"), dict(name="b")], block)
        assert result["incomplete"] == ["a"] and result["skipped"] == 1
        assert (tmp_path / "run/blocks/b/skipped.json").exists()

    asyncio.run(scenario())


def test_known_removed_retry_fields_migrate_but_typos_do_not():
    from engine.config import retry_policy

    with pytest.warns(UserWarning, match="Removed retry settings"):
        policy = retry_policy(
            dict(max_attempts=3, wait_seconds=10, recovery_cap=16, recovery_min_cap=4)
        )
    assert policy.max_attempts == 3
    with pytest.raises(ValueError, match="Unknown retry policy fields"):
        retry_policy(dict(max_attemps=3))


def test_admitted_smoke_purpose_reaches_route_validation(tmp_path, monkeypatch):
    import json
    from engine import pipeline
    from engine.config import canonical, digest
    from engine.campaign import code_manifest

    config = dict(admission_path=str(tmp_path / "admission.json"), registry_path="fixture")
    config_path = tmp_path / "config.json"
    config_path.write_bytes(canonical(config))
    (tmp_path / "admission.json").write_bytes(
        canonical(
            dict(
                approved=True,
                purpose="smoke_T1",
                code_files=code_manifest(),
                plan_sha256=digest(canonical(config)),
            )
        )
    )

    def route_validation(path):
        raise RuntimeError("route validation reached")

    monkeypatch.setattr(pipeline, "registry_from_file", route_validation)
    with pytest.raises(RuntimeError, match="route validation reached"):
        asyncio.run(pipeline.live_pipeline(config_path))
