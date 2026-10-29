from dataclasses import replace
import pytest

from engine.config import canonical
from engine.scheduler import Item
from engine.journal import Journal
from engine.ledger import Ledger, LegacySnapshot, BudgetError
from engine.tests.test_ledger import approval


def test_probe_run_does_not_collide_with_answered_experiment_or_another_probe():
    original = Item("b", "q", "direct", "g", b"{}", "m")
    first = replace(original, purpose="stability_probe", probe_run_id="run1")
    second = replace(first, probe_run_id="run2")
    assert len({original.key, first.key, second.key}) == 3
    assert replace(first).key == first.key
    assert first.wire == original.wire and first.task_id == original.task_id
    with pytest.raises(ValueError):
        replace(original, purpose="stability_probe")


def test_one_run_limit_covers_cells_and_retries_and_survives_restart(tmp_path):
    path = approval(tmp_path, cap=100, scope_cap=100)
    def reserve(ledger, cell, number):
        run = tmp_path / "pipeline" / cell
        rid = cell + str(number)
        slot = ledger.reserve(rid, rid, number, b"{}", run / rid / "request.json", run,
                              Journal(run / "journal.jsonl"))
        ledger.finish(rid, dict(budget_slot=slot["slot"], status="unknown", http_status=504))
    with Ledger(tmp_path / "ledger", path, LegacySnapshot(0, {}, (), {}, ""), "test") as ledger:
        ledger.limit_run(tmp_path / "pipeline", 2)
        reserve(ledger, "A", 1)
        reserve(ledger, "B", 1)
        with pytest.raises(BudgetError, match="physical-attempt"):
            reserve(ledger, "A", 2)
    with Ledger(tmp_path / "ledger", path, LegacySnapshot(0, {}, (), {}, ""), "test") as ledger:
        ledger.limit_run(tmp_path / "pipeline", 2)
        with pytest.raises(BudgetError, match="physical-attempt"):
            reserve(ledger, "B", 2)
        assert len(ledger.records) == 2
