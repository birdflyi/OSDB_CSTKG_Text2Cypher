"""Per-model capacity: caps, hot add/remove, and the unavailable-model error."""

import asyncio

import pytest

from engine.config import Endpoint
from engine.pool import AdmissionTimeout, DynamicPool, ModelUnavailable


def endpoint(name, cap, model="m"):
    return Endpoint(name, "http://localhost/" + name, model, "fixture", cap)


def run(scenario):
    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_model_cap_bounds_the_sum_of_endpoints():
    async def scenario():
        pool = DynamicPool(64)
        await pool.update([endpoint("a", 16), endpoint("b", 16)],
                          per_model_cap={"m": 20})
        assert pool.model_capacity("m") == 20

        held = [await pool.acquire("m", 1) for _ in range(20)]
        assert pool.model_inflight["m"] == 20
        with pytest.raises(AdmissionTimeout):
            await pool.acquire("m", 0.05)
        for permit in held:
            await permit.release()
        assert pool.model_inflight["m"] == 0

    run(scenario)


def test_endpoint_sum_wins_when_the_model_cap_is_larger():
    async def scenario():
        pool = DynamicPool(64)
        await pool.update([endpoint("a", 16), endpoint("b", 16)],
                          per_model_cap={"m": 40})
        assert pool.model_capacity("m") == 32

    run(scenario)


def test_adding_an_endpoint_raises_capacity_immediately():
    async def scenario():
        pool = DynamicPool(64)
        await pool.update([endpoint("a", 4)], per_model_cap={"m": 32})
        assert pool.model_capacity("m") == 4

        waiters = [asyncio.create_task(pool.acquire("m", 2)) for _ in range(4)]
        await asyncio.sleep(0.02)
        await pool.update([endpoint("a", 4), endpoint("b", 4)])
        permits = await asyncio.gather(*waiters)
        assert pool.model_capacity("m") == 8
        for permit in permits:
            await permit.release()

    run(scenario)


def test_removed_endpoint_drains_before_it_disappears():
    async def scenario():
        pool = DynamicPool(8)
        await pool.update([endpoint("a", 2), endpoint("b", 2)], per_model_cap={"m": 8})
        held = [await pool.acquire("m", 1) for _ in range(2)]

        await pool.update([endpoint("b", 2)])
        rows = {row["name"]: row for row in pool.snapshot()["endpoints"]}
        assert rows["a"]["state"] == "draining"
        assert rows["a"]["inflight"] >= 1
        assert pool.model_capacity("m") == 2

        for permit in held:
            await permit.release()
        await pool.update([endpoint("b", 2)])
        assert "a" not in {row["name"] for row in pool.snapshot()["endpoints"]}

    run(scenario)


def test_model_without_endpoints_fails_as_unavailable():
    async def scenario():
        pool = DynamicPool(8)
        await pool.update([endpoint("a", 2)], per_model_cap={"m": 2})
        await pool.update([])
        with pytest.raises(ModelUnavailable) as error:
            await pool.acquire("m", 0.05)
        assert error.value.model == "m"

    run(scenario)


def test_bad_model_caps_are_rejected():
    async def scenario():
        pool = DynamicPool(8)
        with pytest.raises(ValueError, match="undeclared model"):
            await pool.update([endpoint("a", 2)], per_model_cap={"other": 2})
        with pytest.raises(ValueError, match="positive integer"):
            await pool.update([endpoint("a", 2)], per_model_cap={"m": 0})

    run(scenario)


def test_snapshot_reports_models_and_endpoint_state():
    async def scenario():
        pool = DynamicPool(8)
        await pool.update([endpoint("a", 2), Endpoint("b", "http://localhost/b", "n", "fixture", 0, enabled=False)],
                          per_model_cap={"m": 2})
        snapshot = pool.snapshot()
        rows = {row["model"]: row for row in snapshot["models"]}
        assert rows["m"]["cap"] == 2 and rows["m"]["capacity"] == 2
        assert rows["n"]["unavailable"] is True
        assert {row["state"] for row in snapshot["endpoints"]} == {"active", "disabled"}

    run(scenario)


def test_endpoint_changes_are_recorded_as_transitions():
    async def scenario():
        pool = DynamicPool(8)
        await pool.update([endpoint("a", 2)], per_model_cap={"m": 2})
        await pool.update([endpoint("a", 3)])
        await pool.update([])
        kinds = [row["kind"] for row in pool.transitions]
        assert "endpoint_added" in kinds
        assert "endpoint_cap_changed" in kinds
        assert "endpoint_removed" in kinds
        changed = next(
            row for row in pool.transitions if row["kind"] == "endpoint_cap_changed"
        )
        assert (changed["previous_cap"], changed["cap"]) == (2, 3)

    run(scenario)
