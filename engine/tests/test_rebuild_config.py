"""Product config and retry decisions: deterministic and entirely offline."""
import asyncio
from dataclasses import replace

import pytest

from engine.config import DEFAULT_TIMEOUTS, RetryPolicy, Timeouts, timeouts_from
from engine.execution import TracedRpcExecutor, suggested_db_cap
from engine.fake import FakeTransport
from engine.tests.test_scheduler import make_scheduler
from engine.tests.test_worker_read_contract import Driver
from engine.transport import endpoint_health, retryable
from engine.workers.neo4j import ReadHandler


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True, "600"])
def test_nonfinite_or_non_numeric_deadlines_fail_before_running(value):
    with pytest.raises(ValueError, match="positive and finite"):
        Timeouts(queue_total=value)


def test_timeout_schema_rejects_unknown_keys_and_short_rpc():
    with pytest.raises(ValueError, match="Unknown timeout"):
        timeouts_from(dict(qurey=10))
    with pytest.raises(ValueError, match="RPC deadline"):
        Timeouts(rpc=80)


def test_legacy_deadlines_translate_without_editing_input_config(tmp_path):
    async def scenario():
        scheduler, _, _, _ = await make_scheduler(tmp_path, count=1)
        try:
            configured = replace(scheduler.spec, timeouts=dict(admission=12, queue_total=30))
            assert configured.timeouts.admission == 12
            assert configured.timeouts.queue_total == 30
            assert configured.timeouts.model == DEFAULT_TIMEOUTS.model
            legacy = replace(configured, admission_timeout_seconds=9, max_queue_wait_seconds=18)
            assert legacy.timeouts.admission == 9 and legacy.timeouts.queue_total == 18
        finally:
            scheduler.http_workers.shutdown()
            scheduler.db_workers.shutdown()
    asyncio.run(scenario())


def test_custom_db_and_rpc_deadlines_reach_real_execution_calls(tmp_path):
    limits = Timeouts(explain=3, query=7, rpc=12)
    driver = Driver()
    handler = ReadHandler(driver, None, lambda q: q, lambda x: x,
                          lambda text, timeout: (text, timeout), timeouts=limits)
    deadlines = []
    class Client:
        def request(self, request, timeout):
            deadlines.append(timeout)
            return handler(request)
    executor = TracedRpcExecutor(Client(), tmp_path, graph_sha256="fixture",
                                 worker_version="fixture", timeouts=limits)
    result = executor.execute("RETURN 1", {})
    assert result["rows"] == [[1], [2]]
    assert deadlines == [12]
    assert [q[0][1] for q in driver.queries] == [3, 7]


def test_db_capacity_uses_measured_rpc_work_instead_of_a_fixed_floor():
    assert suggested_db_cap(128, 41.5, 0.66) == 4
    assert suggested_db_cap(128, 41.5, 0.66, rpc_per_case=2) == 7
    assert suggested_db_cap(32, 40, 0, rpc_per_case=2) == 1
    with pytest.raises(ValueError):
        suggested_db_cap(32, 40, 1, rpc_per_case=0)


@pytest.mark.parametrize("value,health,may_retry", [
    (dict(status="ok"), "ok", False),
    (dict(status="length"), "length", False),
    (dict(status="refused"), "refused", False),
    (dict(status="empty"), "empty", False),
    (dict(status="unknown", http_status=500, charged=True), "infra", True),
    (dict(status="unknown", http_status=408, charged=True), "infra", True),
    (dict(status="unknown", http_status=409, charged=True), "infra", True),
    (dict(status="unknown", http_status=400, charged=True), "neutral", False),
    (dict(status="auth_error", http_status=401, charged=True), "auth_error", False),
    (dict(status="local_resource_error", transport_error="OSError"), "neutral", False),
    (dict(status="unknown", protocol_error="model_mismatch", charged=True), "model_mismatch", False),
    (dict(status="unknown", protocol_error="invalid_response_envelope", charged=True), "neutral", True),
    (dict(status="unknown", transport_error="timeout", charged=None), "infra", False),
    (dict(status="throttled", charged=True), "throttled", True),
])
def test_one_failure_classification_drives_health_and_authorized_retry(value, health, may_retry):
    policy = RetryPolicy(max_attempts=3, before_write_only=False, retry_unknown=True)
    assert endpoint_health(value) == health
    assert retryable(value, policy) is may_retry


def test_protocol_retries_are_bounded_keep_every_bill_and_do_not_degrade_endpoint(tmp_path):
    async def scenario():
        transport = FakeTransport(0, lambda n, *_: dict(
            status="ok" if n == 3 else "unknown", write_started=True,
            **({} if n == 3 else dict(protocol_error="invalid_response_envelope"))))
        scheduler, items, _, ledger = await make_scheduler(tmp_path, count=1, transport=transport)
        scheduler.spec = replace(scheduler.spec, retry=RetryPolicy(
            max_attempts=3, before_write_only=False, retry_unknown=True))
        async def no_sleep(unused):
            pass
        scheduler.retry_sleep = no_sleep
        paths = await scheduler.run(items)
        assert transport.calls == len(ledger.terminals) == 3
        assert [e["status"] for e in scheduler.events] == ["unknown", "unknown", "ok"]
        assert paths[0]["final"]["status"] == "ok"
        assert not any(s.degraded for s in scheduler.pool.states.values())
        assert scheduler.pool.inflight == 0
    asyncio.run(scenario())


def test_ssh_liveness_is_product_behavior_for_every_worker_command():
    from engine.execution import worker_command
    original = [r"C:\Windows\System32\OpenSSH\ssh.exe", "-p", "3107", "root@owned", "python -m worker"]
    limits = Timeouts(ssh_connect=11, ssh_keepalive_interval=17, ssh_keepalive_count=5)
    command = worker_command(original, limits)
    assert command[1:9] == ["-o", "BatchMode=yes", "-o", "ConnectTimeout=11",
                            "-o", "ServerAliveInterval=17", "-o", "ServerAliveCountMax=5"]
    assert command[9:] == original[1:]
    fractional = worker_command(original, Timeouts(ssh_connect=0.1, ssh_keepalive_interval=0.1))
    assert "ConnectTimeout=1" in fractional and "ServerAliveInterval=1" in fractional
    assert worker_command(["python", "local_worker.py"], limits) == ["python", "local_worker.py"]
    with pytest.raises(ValueError):
        worker_command([])


def test_custom_query_limits_require_a_matching_ready_contract():
    from engine.execution import verify_worker_timeouts
    from engine.errors import IntegrityError
    verify_worker_timeouts({}, DEFAULT_TIMEOUTS)
    limits = Timeouts(explain=3, query=7, rpc=12)
    with pytest.raises(IntegrityError):
        verify_worker_timeouts({}, limits)
    with pytest.raises(IntegrityError):
        verify_worker_timeouts(dict(timeouts=dict(explain=3, query=8)), limits)
    verify_worker_timeouts(dict(timeouts=dict(explain=3, query=7)), limits)
