import copy
import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from batch_audit import audit


def pin(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")
    return dict(
        path=str(path.resolve()), sha256=hashlib.sha256(path.read_bytes()).hexdigest()
    )


def fixture(tmp_path):
    final = pin(tmp_path / "final.json", dict(status="EXECUTED", rows=[[1]]))
    row = dict(
        cell_id="M01-B01",
        position_id="P1",
        status="EXECUTED",
        final=final,
        score=dict(status="scored", value=0.0),
    )
    summary = pin(tmp_path / "summary.json", dict(results=[row]))
    before = pin(
        tmp_path / "before.json",
        dict(
            engine_charged=4,
            engine_head_sha256="head",
            timestamp="2026-10-30T12:00:00+08:00",
            scope="test",
        ),
    )
    after = pin(
        tmp_path / "after.json",
        dict(
            engine_slots=4,
            engine_head_sha256="head",
            timestamp="2026-10-30T12:05:00+08:00",
        ),
    )
    process = pin(
        tmp_path / "process.json",
        dict(
            timestamp="2026-10-30T12:05:00+08:00",
            active_owned_processes=[],
            remaining_owner_locks=[],
            remote_cleanup=dict(
                hostname="owned", active_owned_worker_pids=[], remaining_locks=[]
            ),
        ),
    )
    inventory = pin(
        tmp_path / "inventory.json",
        dict(
            cells=[
                dict(
                    cell_id="M01-B01",
                    positions=[dict(position_id="P1", full_flow_five=True)],
                )
            ]
        ),
    )
    config = pin(tmp_path / "config.json", dict(inventory=inventory, per_cell=1))
    return (
        dict(
            selection_source=dict(inventory=inventory, config=config),
            planned=[dict(cell_id="M01-B01", position_id="P1")],
            summaries=[summary],
            ledger=dict(before=before, after=after),
            process_check=process,
        ),
        row,
    )


def test_zero_delta_is_proved_and_zero_score_still_scored(tmp_path):
    manifest, _ = fixture(tmp_path)
    result = audit(manifest)
    assert result["planned"] == result["terminal"] == result["scoring"]["scored"] == 1
    assert result["ledger_delta"] == 0 and result["evidence_checks_passed"]
    assert result["process_check"]["status"] == "historical_receipt"
    assert result["process_check"]["live_process_inspection"] is False
    assert result["experiment_success"] is None and not result["scientific_admitted"]


def test_missing_checks_and_missing_position_never_pass(tmp_path):
    manifest, _ = fixture(tmp_path)
    del manifest["ledger"]
    del manifest["process_check"]
    del manifest["selection_source"]
    manifest["planned"].append(dict(cell_id="M02-B01", position_id="P2"))
    result = audit(manifest)
    assert result["ledger_delta"] is None and result["leftover_processes"] is None
    assert result["not_run"] == 1 and result["planned"] == 2 and result["terminal"] == 1
    assert result["by_cell"]["M02-B01"]["not_run"] == 1
    assert not result["evidence_checks_passed"]


def test_identical_results_merge_without_double_counting(tmp_path):
    manifest, row = fixture(tmp_path)
    manifest["summaries"].append(pin(tmp_path / "second.json", dict(results=[row])))
    result = audit(manifest)
    assert result["positions"] == 1 and result["identical_results_deduplicated"] == 1
    assert result["evidence_checks_passed"]


def test_conflicting_duplicate_is_not_best_score_selected(tmp_path):
    manifest, row = fixture(tmp_path)
    changed = copy.deepcopy(row)
    changed["score"]["value"] = 1
    manifest["summaries"].append(pin(tmp_path / "other.json", dict(results=[changed])))
    result = audit(manifest)
    assert result["statuses"] == {"conflict": 1} and result["terminal"] == 0
    assert result["issues"][0]["kind"] == "conflicting_result"
    assert not result["evidence_checks_passed"]


@pytest.mark.parametrize(
    "change",
    [
        "final_bytes",
        "missing_final",
        "unknown_status",
        "unknown_score",
        "score_value_missing",
    ],
)
def test_bad_evidence_does_not_pass(tmp_path, change):
    manifest, row = fixture(tmp_path)
    if change == "final_bytes":
        Path(row["final"]["path"]).write_text("{}")
    elif change == "missing_final":
        del row["final"]
    elif change == "unknown_status":
        row["status"] = "MAGIC_SUCCESS"
    elif change == "unknown_score":
        row["score"] = {"status": "maybe"}
    else:
        row["score"]["value"] = None
    manifest["summaries"] = [pin(tmp_path / "summary.json", dict(results=[row]))]
    result = audit(manifest)
    assert not result["evidence_checks_passed"]
    assert result["hash_mismatch"] or result["issues"]


def test_reference_error_needs_explicit_evidence_and_keeps_scoring_failure(tmp_path):
    manifest, row = fixture(tmp_path)
    row["score"] = dict(status="execution_error", error="bad reference query")
    manifest["summaries"] = [pin(tmp_path / "summary.json", dict(results=[row]))]
    manifest["reference_anomalies"] = [
        dict(
            cell_id="M01-B01",
            position_id="P1",
            kind="reference_error",
            evidence=pin(
                tmp_path / "reference_audit.json", dict(reason="invalid original query")
            ),
        )
    ]
    result = audit(manifest)
    assert result["terminal"] == 1 and result["scoring"]["not_scored"] == 1
    assert (
        result["scoring"]["reference_error"] == result["scoring"]["scoring_error"] == 1
    )
    assert not result["all_positions_executed_and_scored"]


def test_changed_chain_without_slot_evidence_is_unknown(tmp_path):
    manifest, _ = fixture(tmp_path)
    manifest["ledger"]["after"] = pin(
        tmp_path / "after.json", dict(engine_slots=5, engine_head_sha256="new")
    )
    assert audit(manifest)["ledger_delta"] is None
    manifest["ledger"]["after"] = pin(
        tmp_path / "after.json",
        dict(engine_slots=5, engine_head_sha256="new", new_request_ids=["request-5"]),
    )
    assert audit(manifest)["ledger_delta"] == 1


def test_seal_escape_is_rejected_without_reading_outside_root(tmp_path):
    manifest, _ = fixture(tmp_path)
    root = tmp_path / "sealed"
    root.mkdir()
    manifest["seals"] = [
        dict(
            pin(tmp_path / "seal.json", dict(files={"../outside": "abc"})),
            root=str(root),
        )
    ]
    result = audit(manifest)
    assert any("escapes" in e["error"] for e in result["hash_mismatch"])
    assert not result["evidence_checks_passed"]


@pytest.mark.parametrize("change", ["omitted", "extra"])
def test_independent_selection_rejects_manually_changed_denominator(tmp_path, change):
    manifest, _ = fixture(tmp_path)
    if change == "omitted":
        manifest["planned"] = []
    else:
        manifest["planned"].append(dict(cell_id="M02-B01", position_id="extra"))
    result = audit(manifest)
    assert result["planned"] == 1 and result["plan_status"] == "mismatch"
    assert not result["evidence_checks_passed"]
    assert any(i["kind"] == "planned_selection_mismatch" for i in result["issues"])


@pytest.mark.parametrize(
    "change", ["no_source", "forged_sha", "wrong_binding", "unsupported_full"]
)
def test_unverifiable_selection_cannot_pass(tmp_path, change):
    manifest, _ = fixture(tmp_path)
    source = manifest["selection_source"]
    if change == "no_source":
        del manifest["selection_source"]
    elif change == "forged_sha":
        source["inventory"]["sha256"] = "0" * 64
    elif change == "wrong_binding":
        source["config"] = pin(
            tmp_path / "config.json",
            dict(inventory=dict(path=str(tmp_path / "other"), sha256="a"), per_cell=1),
        )
    else:
        source["config"] = pin(
            tmp_path / "config.json", dict(inventory=source["inventory"], per_cell=2348)
        )
    result = audit(manifest)
    assert result["plan_status"] == (
        "declared_only" if change == "no_source" else "not_verified"
    )
    assert not result["evidence_checks_passed"]


@pytest.mark.parametrize("change", ["identity", "status", "unknown_format"])
def test_final_identity_and_status_are_checked(tmp_path, change):
    manifest, row = fixture(tmp_path)
    value = {"status": "EXECUTED"}
    if change == "identity":
        value["cell_id"] = "OTHER"
    elif change == "status":
        value["status"] = "refused"
    else:
        value["status"] = {"unexpected": "object"}
    row["final"] = pin(tmp_path / "final.json", value)
    manifest["summaries"] = [pin(tmp_path / "summary.json", dict(results=[row]))]
    result = audit(manifest)
    assert not result["evidence_checks_passed"]
    assert any(
        i["kind"]
        in ("final_identity_mismatch", "final_status_mismatch", "unknown_final_status")
        for i in result["issues"]
    )


def test_selected_positions_derives_exact_frozen_subset(tmp_path):
    manifest, _ = fixture(tmp_path)
    source = manifest["selection_source"]
    source["config"] = pin(
        tmp_path / "config.json",
        dict(
            inventory=source["inventory"],
            per_cell=1,
            selected_positions={"M01-B01": ["P1"]},
        ),
    )
    assert audit(manifest)["plan_status"] == "verified"
    source["config"] = pin(
        tmp_path / "config.json",
        dict(
            inventory=source["inventory"],
            per_cell=1,
            selected_positions={"M01-B01": ["outside"]},
        ),
    )
    assert audit(manifest)["plan_status"] == "not_verified"


def test_cli_writes_failure_report_but_exits_nonzero(tmp_path):
    import subprocess
    import batch_audit

    manifest, _ = fixture(tmp_path)
    manifest["planned"] = []
    source = tmp_path / "input.json"
    source.write_text(json.dumps(manifest), encoding="utf-8")
    output = tmp_path / "report.json"
    result = subprocess.run(
        [
            sys.executable,
            str(Path(batch_audit.__file__)),
            str(source),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2 and output.exists()
    assert json.loads(output.read_text())["plan_status"] == "mismatch"
