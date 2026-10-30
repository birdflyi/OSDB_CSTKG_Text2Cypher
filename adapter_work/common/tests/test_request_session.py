import asyncio
import json
from pathlib import Path
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import request_session
import request_pipeline
from request_session import RequestSession
from engine.config import canonical
from engine.errors import IntegrityError
from engine.journal import Journal
from engine.ledger import BudgetError


def config(root, cap=2):
    approval = root / "approval.json"
    approval.write_bytes(
        canonical(
            dict(
                approved=True,
                active=True,
                cumulative_cap=10,
                scope_caps={"fixture": 10},
                ledger_dir=str(root / "ledger"),
                simulated=True,
            )
        )
    )
    (root / "legacy").mkdir()
    return dict(
        physical_attempt_cap=cap,
        budget_scope="fixture",
        legacy_ledger=str(root / "legacy"),
        ledger_dir=str(root / "ledger"),
        approval_path=str(approval),
        binding="frozen",
    )


def batch(session, number):
    return dict(
        session.config,
        run_dir=str(session.root / str(number)),
        feedback_queue={"path": "frozen-queue", "sha256": "fixture"},
        physical_attempt_cap=session.cap - session.count(),
        recovery=session.recovery,
    )


def reserve(session, number, key=None, attempt=1, finish=True, status="ok"):
    folder = session.root / str(number)
    rid = "r" + str(number)
    key = key or rid
    slot = session.ledger.reserve(
        rid,
        key,
        attempt,
        b'{"model":"fixture"}',
        folder / "request.json",
        folder,
        Journal(folder / "journal.jsonl"),
    )
    session.ledger.claim(rid)
    if finish:
        session.ledger.finish(
            rid,
            dict(
                request_id=rid,
                budget_slot=slot["slot"],
                status=status,
                http_status=200 if status == "ok" else None,
            ),
        )
    return slot


def test_single_admission_and_incremental_history_with_fixed_parent_cap(tmp_path):
    import engine.ledger

    cfg = config(tmp_path)
    with patch.object(
        request_session, "scan_legacy", wraps=request_session.scan_legacy
    ) as scan, patch.object(
        engine.ledger, "audit_new", wraps=engine.ledger.audit_new
    ) as audit, patch.object(
        request_session, "checkpoint_replies", wraps=request_session.checkpoint_replies
    ) as checkpoint:
        with RequestSession(cfg, tmp_path / "phase") as session:
            session.validate(batch(session, 1))
            reserve(session, 1)
            assert session.replies_for(["r1"])["r1"]["response"]["status"] == "ok"
            session.validate(batch(session, 2))
            reserve(session, 2)
            assert len(session.replies_for(["r1", "r2"])) == 2
            assert session.count() == 2
            with pytest.raises(BudgetError, match="cap"):
                reserve(session, 3)
            assert (scan.call_count, audit.call_count, checkpoint.call_count) == (
                1,
                1,
                1,
            )
        assert (scan.call_count, audit.call_count, checkpoint.call_count) == (1, 2, 1)
    assert not (tmp_path / "ledger/.writer.lock").exists()


def test_pending_attempt_blocks_next_batch_and_close_retains_owner(tmp_path):
    session = RequestSession(config(tmp_path), tmp_path / "phase")
    session.__enter__()
    reserve(session, 1, finish=False)
    with pytest.raises(IntegrityError, match="unfinalized"):
        session.replies_for(["r1"])
    with pytest.raises(IntegrityError, match="unfinalized"):
        session.__exit__(None, None, None)
    assert (tmp_path / "ledger/.writer.lock").exists()


def test_unknown_terminal_is_charged_and_preserves_retry_history(tmp_path):
    with RequestSession(config(tmp_path), tmp_path / "phase") as session:
        reserve(session, 1, key="same", status="unknown")
        assert session.count() == 1
        assert session.replies_for(["same"])["same"]["response"]["status"] == "unknown"
        reserve(session, 2, key="same", attempt=2)
        saved = session.replies_for(["same"])["same"]
        assert len(saved["history"]) == 2 and saved["response"]["status"] == "ok"
        assert session.count() == 2


def test_cached_reply_is_rechecked_only_when_used(tmp_path):
    with RequestSession(config(tmp_path), tmp_path / "phase") as session:
        reserve(session, 1)
        session.replies_for(["r1"])
        terminal = session.root / "1/terminal.json"
        value = json.loads(terminal.read_bytes())
        value["text"] = "changed"
        terminal.write_bytes(canonical(value))
        assert session.replies_for(["other"]) == {}
        with pytest.raises(IntegrityError, match="Changed"):
            session.replies_for(["r1"])


def test_session_rejects_changed_identity_and_nonfeedback_entry(tmp_path):
    with RequestSession(config(tmp_path), tmp_path / "phase") as session:
        good = batch(session, 1)
        session.validate(good)
        for update in [
            dict(binding="changed"),
            dict(run_dir=str(tmp_path / "foreign")),
            dict(physical_attempt_cap=3),
            dict(recovery={}),
            dict(feedback_queue=None),
        ]:
            with pytest.raises(IntegrityError):
                session.validate(dict(good, **update))
        with pytest.raises(IntegrityError, match="Direct-feedback"):
            asyncio.run(
                request_pipeline.run(
                    dict(good, feedback_queue=None), admitted_session=session
                )
            )
    with pytest.raises(IntegrityError, match="active"):
        session.validate(good)


def test_final_audit_cannot_report_success_if_terminal_disappears(tmp_path):
    session = RequestSession(config(tmp_path), tmp_path / "phase")
    session.__enter__()
    reserve(session, 1)
    (session.root / "1/terminal.json").unlink()
    assert session.count() == 1  # In-memory count; final audit must still fail.
    with pytest.raises(IntegrityError, match="unfinalized"):
        session.__exit__(None, None, None)
    assert (tmp_path / "ledger/.writer.lock").exists()


def test_request_entry_reuses_one_session_without_scanning_per_batch(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace
    from engine.config import Endpoint

    cfg = dict(
        config(tmp_path),
        source={},
        inventory={},
        bindings={},
        name="fixture",
        registry_path="unused",
        key_dir="unused",
        execute_authorized=True,
    )
    endpoint = Endpoint(
        "fixture", "http://127.0.0.1/not-called", "fixture", "unused", 2
    )
    job = SimpleNamespace(
        item=SimpleNamespace(
            key="r1",
            model="fixture",
            method="direct-feedback",
            wire=b'{"model":"fixture","messages":[]}',
        )
    )
    monkeypatch.setattr(request_pipeline, "source_manifest", lambda: {})
    monkeypatch.setattr(request_pipeline, "verify", lambda pin: pin)
    monkeypatch.setattr(request_pipeline, "read", lambda pin: {})
    monkeypatch.setattr(
        request_pipeline, "load_feedback_jobs", lambda *args: ([job], [])
    )
    monkeypatch.setattr(
        request_pipeline,
        "registry_from_file",
        lambda path: dict(global_cap=2, per_model_cap={}, endpoints=[endpoint]),
    )
    monkeypatch.setattr(request_pipeline, "validate_paths", lambda *args: None)

    class Transport:
        def __init__(self, *args, **kwargs):
            pass

        def validate_keys(self, *args):
            pass

    monkeypatch.setattr(request_pipeline, "HTTPTransport", Transport)

    def forbidden(*args, **kwargs):
        raise AssertionError("Per-batch historical scan is forbidden")

    monkeypatch.setattr(request_pipeline, "scan_legacy", forbidden)
    monkeypatch.setattr(request_pipeline, "saved_replies", forbidden)

    async def execute(spec, jobs, positions, pool, ledger, transport, **kwargs):
        assert kwargs["stop_event"] is session.stop
        number = len(ledger.records) + 1
        reserve(session, number)
        return dict(stop_reason=None)

    monkeypatch.setattr(request_pipeline, "execute", execute)
    with RequestSession(cfg, tmp_path / "phase") as session:
        for number in (1, 2):
            asyncio.run(
                request_pipeline.run(batch(session, number), admitted_session=session)
            )
        assert session.count() == 2
