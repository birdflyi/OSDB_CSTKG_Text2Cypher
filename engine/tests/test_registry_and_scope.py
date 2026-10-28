"""Dynamic registry and counter regressions; no external model calls."""

import asyncio
from dataclasses import asdict, replace
import json

import pytest

from engine.config import Endpoint, canonical, registry_from_file
from engine.pool import DynamicPool
from engine.transport import HTTPTransport
from engine.fake import FakeTransport
from engine.tests.test_scheduler import make_scheduler


def endpoint(name="a", cap=32):
    return Endpoint(name, "http://localhost/unused", "m", "test", cap)


@pytest.mark.parametrize(
    "extra",
    [
        dict(unknown=1),
        dict(global_cap=True),
        dict(global_cap=0),
        dict(reload_seconds=float("nan")),
        dict(reload_seconds=0),
    ],
)
def test_registry_rejects_invalid_top_level_fields(tmp_path, extra):
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(dict(endpoints=[], **extra)), encoding="utf-8")
    with pytest.raises(ValueError):
        registry_from_file(path)


def test_registry_reload_applies_global_cap_and_keeps_previous_on_invalid_revision(tmp_path):
    async def scenario():
        path = tmp_path / "registry.json"

        def write(cap):
            path.write_bytes(
                canonical(
                    dict(endpoints=[asdict(endpoint(cap=4))], global_cap=cap, reload_seconds=0.01)
                )
            )

        pool = DynamicPool(2, ceiling=4)
        await pool.update([endpoint(cap=4)])
        stop = asyncio.Event()
        write(3)
        task = asyncio.create_task(pool.watch(path, stop, interval=0.01))
        for _ in range(100):
            if pool.global_cap == 3:
                break
            await asyncio.sleep(0.005)
        assert pool.global_cap == 3
        write(4)
        for _ in range(100):
            if pool.global_cap == 4:
                break
            await asyncio.sleep(0.005)
        assert pool.global_cap == 4
        write(5)
        await asyncio.sleep(0.04)
        assert pool.global_cap == 4 and pool.issues[-1]["kind"] == "invalid_registry_keep_previous"
        stop.set()
        await task

    asyncio.run(scenario())


def test_disabled_capacity_and_configured_sum_are_not_global_reservations():
    async def scenario():
        pool = DynamicPool(32)
        await pool.update(
            [endpoint(cap=32), endpoint("b", 16), replace(endpoint("disabled", 16), enabled=False)]
        )
        value = pool.snapshot()
        assert value["enabled_capacity_sum"] == 48 and value["global_cap"] == 32
        assert value["endpoints"][2]["effective_cap"] == 0

    asyncio.run(scenario())


def test_campaign_network_peak_does_not_inherit_an_earlier_campaign(tmp_path):
    transport = HTTPTransport(tmp_path)
    for _ in range(3):
        transport.enter_request("e", "A")
    for _ in range(3):
        transport.leave_request("e", "A")
    transport.enter_request("e", "B")
    transport.leave_request("e", "B")
    assert transport.snapshot()["network_peak"] == 3
    assert transport.snapshot(scope="A")["network_peak"] == 3
    assert transport.snapshot(scope="B")["network_peak"] == 1
    assert transport.snapshot(scope="B")["peak_scope"] == "campaign"
    assert transport.snapshot(scope="unused")["network_peak"] == 0
    assert transport.snapshot()["network_inflight"] == 0


def test_empty_key_fails_preflight_without_exposing_its_contents(tmp_path):
    transport = HTTPTransport(tmp_path)
    (tmp_path / "test.txt").write_text("  ", encoding="utf-8")
    with pytest.raises(ValueError, match="test.txt"):
        transport.validate_keys([endpoint()])
    transport.validate_keys([replace(endpoint(), enabled=False)])


def test_runtime_bad_key_does_not_drain_other_work(tmp_path):
    class Transport(FakeTransport):
        preparations = 0

        def prepare(self, endpoint):
            self.preparations += 1
            if self.preparations == 1:
                raise ValueError("Synthetic empty key")
            return super().prepare(endpoint)

    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, count=4, transport=Transport(0)
        )
        paths = await scheduler.run(items)
        assert len(paths) == 4 and transport.calls == len(ledger.terminals) == 3
        assert not scheduler.pool.closed and scheduler.pool.inflight == 0
        assert sum(e["status"] == "local_configuration_error" for e in scheduler.events) == 1

    asyncio.run(scenario())


def test_three_failures_degrade_three_successes_restore_and_ttl_is_bounded():
    from engine.config import EndpointPolicy

    async def scenario():
        now = [100.0]
        pool = DynamicPool(32, clock=lambda: now[0], policy=EndpointPolicy(degraded_ttl_seconds=10))
        await pool.update([endpoint()])
        for n in range(3):
            permit = await pool.acquire("m", 1)
            change = await pool.record(permit, "infra")
            await permit.release()
            assert pool.capacity(permit.state) == (32 if n < 2 else 16)
        assert change["transition"] == "degraded"
        for _ in range(3):
            permit = await pool.acquire("m", 1)
            assert permit.timeout_multiplier == 2
            change = await pool.record(permit, "ok")
            await permit.release()
        assert change["transition"] == "restored" and pool.capacity(permit.state) == 32
        for _ in range(3):
            permit = await pool.acquire("m", 1)
            await pool.record(permit, "infra")
            await permit.release()
        now[0] += 11
        assert pool.capacity(permit.state) == 32
        assert pool.transitions[-1] == dict(
            endpoint="a", transition="restored_by_ttl", certified_by_successes=False
        )
        # TTL changes only admission state. No model request is generated here.
        assert pool.inflight == 0

    asyncio.run(scenario())


def test_jittering_health_does_not_permanently_reduce_capacity_and_reset_is_explicit():
    async def scenario():
        pool = DynamicPool(32)
        await pool.update([endpoint()])
        for _ in range(30):
            for status in ["ok", "ok", "infra"]:
                permit = await pool.acquire("m", 1)
                await pool.record(permit, status)
                await permit.release()
        assert pool.capacity(permit.state) == 32
        permit = await pool.acquire("m", 1)
        await pool.record(permit, "auth_error")
        await permit.release()
        await pool.reset("a")
        assert pool.snapshot()["endpoints"][0]["effective_cap"] == 32
        assert pool.transitions[-1]["transition"] == "operator_reset"

    asyncio.run(scenario())


def test_throttle_does_not_degrade_capacity():
    async def scenario():
        pool = DynamicPool(32)
        await pool.update([endpoint()])
        permit = await pool.acquire("m", 1)
        await pool.record(permit, "throttled", retry_after=5)
        await permit.release()
        assert not permit.state.degraded and pool.capacity(permit.state) == 32
        assert permit.state.cooldown_until > pool.clock()

    asyncio.run(scenario())


def test_run_settings_reject_unknown_keys_instead_of_ignoring_them():
    from engine.config import validate_run_fields

    validate_run_fields(dict(global_cap=32, endpoint_policy=dict(failures_to_degrade=3)))
    with pytest.raises(ValueError, match="quarantine_seconds"):
        validate_run_fields(dict(global_cap=32, quarantine_seconds=60))
    with pytest.raises(ValueError, match="globla_cap"):
        validate_run_fields(dict(globla_cap=32))


def test_real_loopback_posts_keep_separate_campaign_peaks(tmp_path):
    import http.server
    import threading
    from concurrent.futures import ThreadPoolExecutor

    barrier = threading.Barrier(3, timeout=10)

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        count = 0
        lock = threading.Lock()

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            with self.lock:
                Handler.count += 1
                index = Handler.count
            if index <= 3:
                barrier.wait()
            raw = canonical(
                dict(model="m", choices=[dict(finish_reason="stop", message=dict(content="ok"))])
            )
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *unused):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    (tmp_path / "test.txt").write_text("synthetic-not-a-provider-key", encoding="utf-8")
    transport = HTTPTransport(tmp_path)
    ep = replace(endpoint(), url="http://127.0.0.1:%d/test" % server.server_port)

    def send(scope):
        prepared = transport.prepare(ep)
        prepared.scope = scope
        try:
            assert prepared.send(b"{}", 10)["status"] == "ok"
        finally:
            prepared.close()

    try:
        with ThreadPoolExecutor(max_workers=3) as workers:
            list(workers.map(send, ["A"] * 3))
        send("B")
        assert transport.snapshot()["network_peak"] == 3
        assert transport.snapshot(scope="A")["network_peak"] == 3
        assert transport.snapshot(scope="B")["network_peak"] == 1
        assert transport.snapshot(scope="B")["network_started"] == 1
        assert transport.snapshot()["network_inflight"] == 0
    finally:
        transport.close()
        server.shutdown()
        server.server_close()


def test_invalid_key_revision_does_not_replace_previous_pool_configuration(tmp_path):
    async def scenario():
        pool = DynamicPool(32)
        await pool.update([endpoint("known")])
        path = tmp_path / "registry.json"
        path.write_bytes(canonical(dict(endpoints=[asdict(endpoint("bad"))], reload_seconds=0.01)))

        def validate(rows):
            raise ValueError("Synthetic invalid key")

        stop = asyncio.Event()
        task = asyncio.create_task(pool.watch(path, stop, interval=0.01, validate=validate))
        await asyncio.sleep(0.04)
        stop.set()
        await task
        assert pool.snapshot()["endpoints"][0]["name"] == "known"
        assert pool.issues[-1]["kind"] == "invalid_registry_keep_previous"

    asyncio.run(scenario())


def test_single_route_auth_failure_does_not_keep_other_items_waiting_or_billing(tmp_path):
    async def scenario():
        transport = FakeTransport(0, lambda *_: dict(status="auth_error", http_status=401))
        scheduler, items, _, ledger = await make_scheduler(
            tmp_path, cap=1, count=8, transport=transport
        )
        scheduler.spec = replace(scheduler.spec, queue_wait_until_available=True)
        paths = await asyncio.wait_for(scheduler.run(items), 2)
        assert transport.calls == len(ledger.terminals) == 1
        assert len(paths) == 8 and not scheduler.pool.closed
        assert sum(e.get("undispatched_reason") == "auth_error" for e in scheduler.events) == 7

    asyncio.run(scenario())
