from __future__ import annotations

import asyncio
from dataclasses import replace
import pytest

from engine.config import Endpoint, Sampling
from engine.pipeline import run_stages
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
            raise RuntimeError("interrupted")

        stages = [dict(name="a"), dict(name="b")]
        with pytest.raises(RuntimeError):
            await run_stages(tmp_path / "p", stages, fail)
        calls = []

        async def should_not_call(stage):
            calls.append(stage)

        with pytest.raises(ValueError, match="Interrupted"):
            await run_stages(tmp_path / "p", stages, should_not_call, resume=True)
        assert not calls

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
