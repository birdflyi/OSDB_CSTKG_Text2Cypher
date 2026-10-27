from __future__ import annotations

import json
from pathlib import Path
import pytest

from engine.cli import verify
from engine.config import canonical
from engine.journal import Journal, write_new
from engine.ledger import BudgetError, Ledger, LegacySnapshot, scan_legacy


def approval(root, cap=5, scope_cap=5):
    path = root / "approval.json"
    path.write_bytes(
        canonical(
            dict(
                approved=True,
                active=True,
                cumulative_cap=cap,
                scope_caps={"test": scope_cap},
                simulated=True,
                ledger_dir=str(root / "ledger"),
            )
        )
    )
    return path


def test_all_three_legacy_formats_and_special_terminal_rewrite(tmp_path):
    ledger, migrated = tmp_path / "old", tmp_path / "migrated"
    ledger.mkdir()
    migrated.mkdir()
    for n, kind in enumerate(("legacy_b", "standard", "scoped"), 1):
        folder = migrated / str(n)
        folder.mkdir()
        request = folder / ("connectivity_probe_01_request.json" if n == 1 else "request.json")
        request.write_bytes(b"{}")
        terminal = request.with_name("connectivity_probe_01.json" if n == 1 else "terminal.json")
        terminal.write_bytes(b'{"http_status":200}')
        value = dict(request_path="C:\\legacy\\" + str(n) + "\\" + request.name, pid=1)
        if n > 1:
            from engine.config import digest

            value.update(
                request_sha256=digest(b"{}"),
                purpose="test",
                approval_sha256="unknown",
                approved_cap_at_reservation=10,
            )
        if n == 3:
            value.update(scope_id="OSDB", request_id="x", plan_manifest_sha256="x")
        (ledger / (str(n) + ".json")).write_bytes(canonical(value))
    before = {p.name: p.read_bytes() for p in ledger.iterdir()}
    result = scan_legacy(ledger, [("C:\\legacy", migrated)])
    assert result.charged == 3 and not result.pending
    assert result.formats == dict(legacy_b=1, standard=1, scoped=1)
    assert before == {p.name: p.read_bytes() for p in ledger.iterdir()}


def test_idempotency_body_conflict_and_physical_claim_once(tmp_path):
    old = LegacySnapshot(2, {"test": 2}, (), {}, "")
    with Ledger(tmp_path / "ledger", approval(tmp_path), old, "test") as ledger:
        journal = Journal(tmp_path / "run/journal.jsonl")
        args = ("a__a1", "a", 1, b"{}", tmp_path / "run/a/request.json", tmp_path / "run", journal)
        one = ledger.reserve(*args)
        assert ledger.reserve(*args) == one
        with pytest.raises(BudgetError):
            ledger.reserve(*args[:3], b'{"different":1}', *args[4:])
        ledger.claim("a__a1")
        with pytest.raises(FileExistsError):
            ledger.claim("a__a1")
        ledger.finish("a__a1", dict(budget_slot=1, http_status=200))
        assert ledger.summary()["total_charged"] == 3
    assert not (tmp_path / "ledger/.writer.lock").exists()


def test_scope_cap_and_inactive_approval_fail_closed(tmp_path):
    path = approval(tmp_path, cap=100, scope_cap=1)
    with Ledger(
        tmp_path / "ledger", path, LegacySnapshot(1, {"test": 1}, (), {}, ""), "test"
    ) as ledger:
        with pytest.raises(BudgetError):
            ledger.reserve(
                "x__a1",
                "x",
                1,
                b"{}",
                tmp_path / "run/request.json",
                tmp_path / "run",
                Journal(tmp_path / "journal"),
            )
    path.write_bytes(b'{"active":false,"approved":false}')
    with pytest.raises(BudgetError):
        with Ledger(tmp_path / "no-directory", path, LegacySnapshot(0, {}, (), {}, ""), "test"):
            pass
    assert not (tmp_path / "no-directory").exists()


def test_interruption_retains_charge_and_verify_never_resends(tmp_path):
    with Ledger(
        tmp_path / "ledger", approval(tmp_path), LegacySnapshot(0, {}, (), {}, ""), "test"
    ) as ledger:
        ledger.reserve(
            "x__a1",
            "x",
            1,
            b"{}",
            tmp_path / "run/request.json",
            tmp_path / "run",
            Journal(tmp_path / "run/journal"),
        )
        ledger.claim("x__a1")
    assert (tmp_path / "ledger/.writer.lock").exists()
    audit = verify(tmp_path / "ledger")
    assert audit["unknown"] == ["x__a1"] and audit["automatically_resent"] == 0


def test_partial_journal_is_not_silently_accepted(tmp_path):
    path = tmp_path / "journal.jsonl"
    path.write_bytes(b'{"partial":')
    with pytest.raises(ValueError):
        Journal(path).read()


def test_one_approved_ledger_cannot_be_duplicated_to_reset_global_usage(tmp_path):
    path = approval(tmp_path)
    with pytest.raises(BudgetError):
        with Ledger(tmp_path / "other-ledger", path, LegacySnapshot(0, {}, (), {}, ""), "test"):
            pass
    assert not (tmp_path / "other-ledger").exists()


def test_multiple_scopes_share_the_same_total_and_no_borrowing(tmp_path):
    path = approval(tmp_path, cap=3)
    value = json.loads(path.read_text())
    value["scope_caps"] = {"test": 1, "other": 2}
    path.write_bytes(canonical(value))
    with Ledger(tmp_path / "ledger", path, LegacySnapshot(0, {}, (), {}, ""), "test") as ledger:
        journal = Journal(tmp_path / "run/journal")
        for rid, scope in [("one", "test"), ("two", "other"), ("three", "other")]:
            slot = ledger.reserve(
                rid,
                rid,
                1,
                b"{}",
                tmp_path / "run" / rid / "request.json",
                tmp_path / "run",
                journal,
                scope=scope,
            )
            ledger.claim(rid)
            ledger.finish(rid, dict(budget_slot=slot["slot"], http_status=200))
        assert ledger.summary()["total_charged"] == 3
        with pytest.raises(BudgetError):
            ledger.reserve(
                "four",
                "four",
                1,
                b"{}",
                tmp_path / "run/four/request.json",
                tmp_path / "run",
                journal,
                scope="other",
            )
