from __future__ import annotations

import asyncio
from dataclasses import replace
import socket
import sys
import time

import pytest

from engine.config import RetryPolicy, digest
from engine.fake import FakeTransport
from engine.retries import RecoveryGate
from engine.tests.test_scheduler import make_scheduler
from engine.workers.neo4j import check_bolt_available


def test_three_campaign_recovery_keeps_one_ledger_and_never_rerolls_success(tmp_path):
    import json
    from pathlib import Path
    from engine.config import canonical
    from engine.journal import Journal
    from engine.ledger import Ledger, LegacySnapshot
    from engine.recovery import saved_replies
    from engine.scheduler import Scheduler

    async def scenario():
        first, items, _, _ = await make_scheduler(tmp_path, count=1)
        approval = tmp_path / "approval.json"
        approval.write_bytes(
            canonical(
                dict(
                    approved=True,
                    active=True,
                    cumulative_cap=3,
                    scope_caps={"synthetic": 3},
                    ledger_dir=str(tmp_path / "ledger"),
                )
            )
        )

        async def sleep(unused):
            pass

        with Ledger(
            tmp_path / "ledger",
            approval,
            LegacySnapshot(0, {}, (), {}, ""),
            "synthetic",
        ) as ledger:
            for n in range(1, 5):
                transport = FakeTransport(
                    0,
                    lambda *_: dict(
                        status="unknown" if n < 3 else "ok",
                        write_started=True,
                        transport_error="RemoteDisconnected",
                    ),
                )
                spec = replace(
                    first.spec,
                    name="recovery-" + str(n),
                    run_dir=tmp_path / str(n),
                    retry=RetryPolicy(
                        max_attempts=min(n, 3),
                        before_write_only=False,
                        retry_unknown=True,
                    ),
                )
                scheduler = Scheduler(
                    spec,
                    first.pool,
                    ledger,
                    Journal(spec.run_dir / "journal.jsonl"),
                    transport,
                )
                scheduler.retry_sleep = sleep
                manifest = tmp_path / ("cache-" + str(n) + ".json")
                entries = []
                for row in ledger.records.values():
                    terminal = Path(row["request_path"]).parent / "terminal.json"
                    entries.append(
                        dict(
                            request_id=row["request_id"],
                            request_sha256=row["request_sha256"],
                            terminal_sha256=digest(terminal.read_bytes()),
                        )
                    )
                manifest.write_bytes(canonical(entries))
                scheduler.saved_replies = saved_replies(
                    manifest, digest(manifest.read_bytes()), ledger
                )
                paths = await scheduler.run(items)
                assert transport.calls == (1 if n <= 3 else 0)
                assert len(ledger.records) == min(n, 3)
                if n >= 3:
                    assert paths[0]["final"]["status"] == "ok"
            assert [
                json.loads((Path(r["request_path"]).parent / "terminal.json").read_text())["status"]
                for r in ledger.records.values()
            ] == ["unknown", "unknown", "ok"]
            manifest.write_bytes(canonical([]))
            with pytest.raises(ValueError, match="every charged attempt"):
                saved_replies(manifest, digest(manifest.read_bytes()), ledger)

    asyncio.run(scenario())


def test_unknown_retry_is_explicit_bounded_and_all_attempts_preserved(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path,
            count=1,
            transport=FakeTransport(
                0,
                lambda *_: dict(
                    status="unknown",
                    http_status=None,
                    write_started=True,
                    transport_error="RemoteDisconnected",
                ),
            ),
        )
        scheduler.spec = replace(
            scheduler.spec,
            retry=RetryPolicy(max_attempts=3, before_write_only=False, retry_unknown=True),
        )
        waits = []

        async def sleep(seconds):
            waits.append(seconds)

        scheduler.retry_sleep = sleep
        paths = await scheduler.run(items)
        assert transport.calls == 3 and len(ledger.terminals) == 3
        assert len(scheduler.events) == 3 and paths[0]["final"]["status"] == "unknown"
        assert len(waits) == 2 and all(9.5 < value <= 10 for value in waits)

    asyncio.run(scenario())


def test_sealed_prior_attempts_count_toward_limit_and_success_is_not_rerolled(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, count=1, transport=FakeTransport(0)
        )
        scheduler.spec = replace(
            scheduler.spec,
            retry=RetryPolicy(max_attempts=3, before_write_only=False, retry_unknown=True),
        )
        response = dict(
            status="unknown",
            charged=True,
            transport_error="RemoteDisconnected",
            ended_epoch=time.time() - 60,
        )
        history = [
            dict(
                response=response,
                source_terminal="old",
                source_terminal_sha256="sealed",
            )
        ] * 2
        scheduler.saved_replies[items[0].key] = dict(
            response=response, wire_sha256=digest(items[0].wire), history=history
        )
        await scheduler.run(items)
        assert transport.calls == 1 and scheduler.events[-1]["attempt_no"] == 3
        assert scheduler.events[-1]["status"] == "ok"

    asyncio.run(scenario())


@pytest.mark.parametrize("status", ["ok", "length", "empty", "refused", "model_mismatch"])
def test_quality_outcomes_are_not_transport_retried(tmp_path, status):
    async def scenario():
        scheduler, items, transport, _ = await make_scheduler(
            tmp_path,
            count=1,
            transport=FakeTransport(0, lambda *_: dict(status=status)),
        )
        scheduler.spec = replace(
            scheduler.spec,
            retry=RetryPolicy(max_attempts=3, before_write_only=False, retry_unknown=True),
        )
        await scheduler.run(items)
        assert transport.calls == 1

    asyncio.run(scenario())


def test_failure_window_shrinks_retry_gate_and_releases_all_slots():
    async def scenario():
        gate = RecoveryGate(16, 4)
        for _ in range(16):
            async with gate.slot():
                await gate.record("infra")
        assert gate.cap == 4 and gate.active == 0
        for _ in range(8):
            await gate.record("ok")
        assert gate.cap == 4 and len(gate.changes) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("failures", [2, 3])
def test_local_preparation_retry_does_not_reset_or_consume_physical_ordinal(tmp_path, failures):
    import errno

    class Transport(FakeTransport):
        preparations = 0

        def prepare(self, endpoint):
            self.preparations += 1
            if self.preparations <= failures:
                raise OSError(errno.ENOBUFS, "Synthetic local preparation failure")
            return super().prepare(endpoint)

    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, count=1, transport=Transport(0)
        )

        async def sleep(unused):
            pass

        scheduler.retry_sleep = sleep
        paths = await scheduler.run(items)
        assert transport.preparations == 3
        assert transport.calls == (1 if failures == 2 else 0)
        assert len(ledger.terminals) == transport.calls
        assert scheduler.events[0]["errno"] == errno.ENOBUFS
        if failures == 2:
            assert paths[0]["final"]["attempt_no"] == 1
            assert not any("undispatched_reason" in e for e in scheduler.events)
        else:
            assert paths[0]["final"]["undispatched_reason"] == "local_resource"

    asyncio.run(scenario())


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Owned Neo4j worker runs on Linux; Windows SO_REUSEADDR permits duplicate bind",
)
def test_bolt_preflight_rejects_a_live_listener_but_allows_time_wait():
    # Use a real server: only SO_REUSEADDR, never SO_REUSEPORT.
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    port = server.getsockname()[1]
    server.listen()
    with pytest.raises(OSError):
        check_bolt_available(port)
    client = socket.create_connection(("127.0.0.1", port))
    accepted, _ = server.accept()
    accepted.close()
    client.recv(1)
    client.close()
    server.close()
    check_bolt_available(port)
