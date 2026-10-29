import asyncio
from dataclasses import replace
import json
import threading

from engine.config import canonical
from engine.fake import FakeTransport
from engine.probe import execute_probe
from engine.ledger import Ledger, LegacySnapshot
from engine.tests.test_ledger import approval
from engine.tests.test_scheduler import make_scheduler


def test_next_cell_dispatches_before_previous_long_tail_and_seals_progress(tmp_path):
    later_cell_started = threading.Event()
    def reply(call, wire, endpoint):
        task = json.loads(wire)["messages"][0]["content"]
        if task == "0":
            assert later_cell_started.wait(10), "Cell B was blocked by cell A's long tail"
        if task == "2":
            later_cell_started.set()
        return dict(status="ok", content="RETURN 1", http_status=200)

    async def scenario():
        transport = FakeTransport(0, reply)
        scheduler, items, _, ledger = await make_scheduler(tmp_path, cap=2, count=5, transport=transport)
        items = [replace(i, method="direct" if n < 2 else "ours", purpose="stability_probe",
                         probe_run_id="probe-1", wire=canonical(dict(model="m", messages=[
                             dict(role="user", content=str(n))]))) for n, i in enumerate(items)]
        spec = replace(scheduler.spec, budget_scope="test", approval_path=approval(tmp_path, cap=10, scope_cap=10))
        with Ledger(spec.ledger_dir, spec.approval_path, LegacySnapshot(0, {}, (), {}, ""), "test") as ledger:
            ledger.limit_run(spec.run_dir, 5)
            result = await execute_probe(spec, items, scheduler.pool, ledger,
                                         transport, simulated=True)
        assert result["complete"] and [c["api_ok"] for c in result["cells"]] == [2, 3]
        assert later_cell_started.is_set() and transport.peak == 2
        manifest = json.loads((scheduler.spec.run_dir / "artifact_manifest.json").read_text())
        from engine.config import digest
        assert manifest["cell_progress.json"] == digest((scheduler.spec.run_dir / "cell_progress.json").read_bytes())
        terminals = list(scheduler.spec.run_dir.rglob("terminal.json"))
        assert len(terminals) == 5
        assert all(json.loads(p.read_text())["purpose"] == "stability_probe" for p in terminals)
    asyncio.run(scenario())


def test_finished_bad_responses_are_not_reported_as_passed_cells(tmp_path):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(
            tmp_path, count=2, transport=FakeTransport(0, lambda *a: dict(status="unknown", http_status=504)))
        items = [replace(i, purpose="stability_probe", probe_run_id="bad") for i in items]
        result = await execute_probe(scheduler.spec, items, scheduler.pool, ledger, transport, simulated=True)
        assert result["cells"][0]["finished"] == 2
        assert result["cells"][0]["unknown"] == 2
        assert not result["complete"] and not result["all_api_paths_ok"]
    asyncio.run(scenario())
