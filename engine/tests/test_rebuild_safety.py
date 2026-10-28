"""Offline regressions for audited pool failures; no external API calls."""

import asyncio
from dataclasses import replace

import pytest

from engine.config import Endpoint
from engine.pool import DynamicPool
from engine.transport import classify, endpoint_health
from engine.tests.test_scheduler import make_scheduler


def endpoint(**kwargs):
    values = dict(
        name="a", url="http://localhost/unused", model="m", key_ref="test", cap=32
    )
    values.update(kwargs)
    return Endpoint(**values)


@pytest.mark.parametrize("body", [None, [], {}, {"model": None}, {"model": ""}])
def test_malformed_200_never_claims_different_model(body):
    value = classify(dict(http_status=200, body=body), "m")
    assert value["protocol_error"] == "invalid_response_envelope"
    assert endpoint_health(value) == "neutral"


def test_quarantine_expires_and_disabled_capacity_is_zero():
    async def scenario():
        now = [100.0]
        pool = DynamicPool(32, clock=lambda: now[0])
        await pool.update([endpoint(), endpoint(name="disabled", enabled=False)])
        permit = await pool.acquire("m", 1)
        change = await pool.record(permit, "model_mismatch")
        await permit.release()
        assert change["transition"] == "quarantined"
        assert [e["effective_cap"] for e in pool.snapshot()["endpoints"]] == [0, 0]
        now[0] += 61
        permit = await pool.acquire("m", 1)
        await permit.release()
        assert pool.snapshot()["endpoints"][0]["effective_cap"] == 32

    asyncio.run(scenario())


def test_cancelled_release_returns_every_permit():
    async def scenario():
        pool = DynamicPool(32)
        await pool.update([endpoint()])
        for _ in range(100):
            permit = await pool.acquire("m", 1)
            async with pool.condition:
                task = asyncio.create_task(permit.release())
                await asyncio.sleep(0)
                task.cancel()
                await asyncio.sleep(0)
                assert pool.inflight == 1
            with pytest.raises(asyncio.CancelledError):
                await task
            assert permit.released and pool.inflight == 0

    asyncio.run(scenario())


def test_queue_total_deadline_is_not_renewed(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path, count=1)
        scheduler.spec = replace(
            scheduler.spec,
            admission_timeout_seconds=0.01,
            max_queue_wait_seconds=0.07,
            queue_wait_until_available=True,
        )
        await scheduler.pool.update([])
        paths = await asyncio.wait_for(scheduler.run(items), 1)
        assert paths[0]["final"]["reason"] == "queue_exhausted"
        assert transport.calls == 0 and not ledger.records
        assert scheduler.pool.inflight == scheduler.pool.waiting == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("code", [401, 403, 404, 405])
def test_auth_errors_are_explicit_and_do_not_retry(code):
    value = classify(dict(http_status=code, write_started=True), "m")
    assert value["status"] == endpoint_health(value) == "auth_error"
    assert value["generation_outcome_unknown"] is False


def test_local_resource_errors_do_not_reduce_remote_capacity():
    assert (
        endpoint_health(dict(status="local_resource_error", transport_error="OSError"))
        == "neutral"
    )


@pytest.mark.parametrize("retry_after, expected", [(None, 5), (12, 12), (99999, 300)])
def test_throttling_has_bounded_backoff(retry_after, expected):
    value = dict(http_status=429)
    if retry_after is not None:
        value["retry_after_seconds"] = retry_after
    assert classify(value, "m")["retry_after_seconds"] == expected


@pytest.mark.parametrize("kwargs", [dict(enabled="false"), dict(cap=0)])
def test_invalid_endpoint_limits_are_rejected(kwargs):
    with pytest.raises(ValueError):
        endpoint(**kwargs)
