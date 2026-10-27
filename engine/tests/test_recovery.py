from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

from engine.config import canonical, digest
from engine.fake import FakeTransport
from engine.journal import Journal
from engine.ledger import Ledger, LegacySnapshot
from engine.recovery import saved_replies
from engine.scheduler import Scheduler
from engine.tests.test_scheduler import make_scheduler


def test_sealed_charged_unknown_is_reused_never_resent_and_new_work_continues(tmp_path):
    async def scenario():
        first, items, _, _ = await make_scheduler(
            tmp_path,
            count=1,
            transport=FakeTransport(
                0, lambda *_: dict(status="unknown", http_status=None, write_started=True)
            ),
        )
        approval = tmp_path / "approval.json"
        approval.write_bytes(
            canonical(
                dict(
                    approved=True,
                    active=True,
                    simulated=True,
                    cumulative_cap=3,
                    scope_caps={"synthetic": 3},
                    ledger_dir=str(tmp_path / "ledger"),
                )
            )
        )
        with Ledger(
            tmp_path / "ledger", approval, LegacySnapshot(0, {}, (), {}, ""), "synthetic"
        ) as ledger:
            first.ledger = ledger
            await first.run(items)
            record = next(iter(ledger.records.values()))
            terminal = Path(record["request_path"]).parent / "terminal.json"
            before = digest(terminal.read_bytes())
            manifest = tmp_path / "recovery.json"
            manifest.write_bytes(
                canonical(
                    [
                        dict(
                            request_id=record["request_id"],
                            request_sha256=record["request_sha256"],
                            terminal_sha256=before,
                        )
                    ]
                )
            )
            transport = FakeTransport(0)
            spec = replace(first.spec, name="explicit-continuation", run_dir=tmp_path / "continued")
            continued = Scheduler(
                spec, first.pool, ledger, Journal(spec.run_dir / "journal.jsonl"), transport
            )
            continued.saved_replies = saved_replies(manifest, digest(manifest.read_bytes()), ledger)
            paths = await continued.run([items[0], replace(items[0], task_id="new")])
            assert len(paths) == 2 and transport.calls == 1 and len(ledger.records) == 2
            old = next(p for p in paths if p["task_id"] == items[0].task_id)
            assert old["final"]["status"] == "unknown" and old["model_attempts"][0]["reused"]
            assert digest(terminal.read_bytes()) == before

    asyncio.run(scenario())
