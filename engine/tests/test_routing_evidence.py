"""No sockets: routed request evidence, accounting and cached reuse agree."""

import asyncio
from dataclasses import replace
import json

import pytest

from engine.config import Endpoint, canonical, digest
from engine.fake import FakeTransport
from engine.journal import Journal, claim_owner, owner_alive
from engine.ledger import Ledger, LegacySnapshot
from engine.pool import DynamicPool, serves
from engine.recovery import checkpoint_replies
from engine.scheduler import Scheduler
from engine.tests.test_scheduler import make_scheduler


def alias():
    return Endpoint("replica", "http://localhost/not-used", "served", "fixture", 1,
                    serves=("m",), alias_evidence_sha256="a" * 64)


def test_family_or_unverified_alias_does_not_establish_equivalence():
    endpoint = Endpoint("a", "http://localhost/a", "revision-1", "fixture", 1,
                        family="revision", serves=("revision-2",))
    assert serves(endpoint, "revision-1")
    assert not serves(endpoint, "revision-2")
    assert not serves(endpoint, "revision-3")


def test_alias_permit_returns_requested_model_slot_after_reload():
    async def scenario():
        pool = DynamicPool(2)
        await pool.update([alias()], per_model_cap={"m": 1})
        held = await pool.acquire("m", 1)
        await pool.update([replace(alias(), cap=2)])
        await held.release()
        assert pool.model_inflight == {"m": 0}
        following = await pool.acquire("m", 1)
        await following.release()
        assert pool.inflight == 0
    asyncio.run(scenario())


def test_routed_wire_is_exactly_persisted_and_cached_without_resend(tmp_path):
    async def scenario():
        sent = []

        def response(call, wire, endpoint):
            sent.append(wire)
            return dict(status="ok", content="RETURN 1")

        first, items, transport, _ = await make_scheduler(
            tmp_path, count=1, transport=FakeTransport(0, response))
        await first.pool.update([alias()], per_model_cap={"m": 1})
        approval = tmp_path / "approval.json"
        approval.write_bytes(canonical(dict(approved=True, active=True, simulated=True,
            cumulative_cap=3, scope_caps={"synthetic": 3}, ledger_dir=str(tmp_path / "ledger"))))
        with Ledger(tmp_path / "ledger", approval, LegacySnapshot(0, {}, (), {}, ""), "synthetic") as ledger:
            first.ledger = ledger
            await first.run(items)
            record = next(iter(ledger.records.values()))
            from pathlib import Path
            request = Path(record["request_path"])
            terminal = json.loads(request.with_name("terminal.json").read_bytes())
            assert sent == [request.read_bytes()]
            assert json.loads(sent[0])["model"] == "served"
            assert terminal["model"] == "served" and terminal["requested_model"] == "m"
            assert terminal["logical_request_sha256"] == digest(items[0].wire)
            manifest = tmp_path / "cached.json"
            replies = checkpoint_replies(manifest, ledger)
            spec = replace(first.spec, name="continued", run_dir=tmp_path / "continued")
            unused = FakeTransport(0)
            second = Scheduler(spec, first.pool, ledger, Journal(spec.run_dir / "journal.jsonl"), unused)
            second.saved_replies = replies
            await second.run(items)
            assert unused.calls == 0 and len(ledger.records) == 1
    asyncio.run(scenario())


def test_unknown_owner_is_preserved_and_local_dead_owner_can_be_reclaimed(tmp_path):
    import socket
    lock = tmp_path / "owner"
    lock.write_text("broken", encoding="utf-8")
    assert owner_alive(lock)
    with pytest.raises(FileExistsError):
        claim_owner(lock, canonical(dict(pid=1, hostname=socket.gethostname())))
    lock.write_bytes(canonical(dict(pid=2147483647, hostname=socket.gethostname())))
    claim_owner(lock, canonical(dict(pid=1, hostname=socket.gethostname())))
