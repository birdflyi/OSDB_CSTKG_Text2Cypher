"""Read explicitly pinned batch evidence; never run a model, database or scanner.

Manifest: planned=[{cell_id, position_id}], summaries=[{path,sha256}],
selection_source={inventory:pin, config:pin} verifies the independent denominator;
without it the declared plan is not verified. Only the existing one/five-position
smoke selector is supported; full-selection formats require a separate contract.
seals=[{path,sha256,root}], optional ledger={before:pin,after:pin},
process_check=pin. Multiple summaries can cover disjoint or identical positions;
conflicting duplicates are errors, never resolved by choosing a better result.
The process receipt is a historical observation, not a live process inspection.
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

NORMAL = {
    "executed": "EXECUTED",
    "execution_error": "execution_error",
    "unsupported": "unsupported",
    "not_applicable": "not_applicable",
    "refused": "refused",
    "not_run": "not_run",
    "not_dispatched": "not_run",
    "blocked": "not_run",
    "backend_error": "backend_error",
    "interrupted": "interrupted",
    "interrupted_after_final": "interrupted_after_final",
    "feedback_pending": "feedback_pending",
    "dependency_pending": "dependency_pending",
    "dependency_failed": "dependency_failed",
}
TERMINAL = {
    "EXECUTED",
    "execution_error",
    "unsupported",
    "not_applicable",
    "refused",
    "backend_error",
    "dependency_failed",
}
SCORED = {"scored", "scored_development", "scored_auxiliary"}
REFERENCE = {"reference_missing", "reference_error"}
SCORE_STATUSES = SCORED | REFERENCE | {"execution_error", "not_scored"}


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def identity(row):
    return row["cell_id"], row["position_id"]


def read_pin(pin, errors, checks):
    label = str(pin.get("path", "<missing path>"))
    try:
        path = Path(pin["path"])
        if not path.is_absolute():
            raise ValueError("Evidence paths must be absolute")
        if not pin.get("sha256"):
            raise ValueError("Missing expected SHA256")
        if sha(path) != pin["sha256"]:
            raise ValueError("SHA256 mismatch")
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        checks.append(label)
        return value
    except (OSError, ValueError, KeyError, TypeError) as error:
        errors.append(dict(path=label, error=str(error)))
        return None


def selection_expected(source, errors, checks):
    """Mirror the existing offline smoke selector, without loading any backend."""
    if not source:
        return None, "declared_only"
    try:
        inventory = read_pin(source["inventory"], errors, checks)
        config = read_pin(source["config"], errors, checks)
        if inventory is None or config is None:
            return None, "not_verified"
        binding = config["inventory"]
        if (
            Path(binding["path"]).resolve()
            != Path(source["inventory"]["path"]).resolve()
            or binding["sha256"] != source["inventory"]["sha256"]
        ):
            raise ValueError("Selection config binds another inventory")
        limit = config["per_cell"]
        if type(limit) is not int or limit not in (1, 5):
            raise ValueError(
                "Only existing one/five-position smoke selection is supported"
            )
        selection = config.get("selected_positions")
        expected, cell_ids = set(), set()
        for cell in inventory["cells"]:
            cid = cell["cell_id"]
            if cid in cell_ids:
                raise ValueError("Duplicate inventory cell")
            cell_ids.add(cid)
            positions = cell["positions"]
            if len({p["position_id"] for p in positions}) != len(positions):
                raise ValueError("Duplicate inventory position")
            if any(type(p.get("full_flow_five")) is not bool for p in positions):
                raise ValueError("Missing or invalid frozen subset marker")
            chosen = [p["position_id"] for p in positions if p["full_flow_five"]][
                :limit
            ]
            if selection is not None:
                wanted = selection.get(cid, [])
                if (
                    not isinstance(wanted, list)
                    or len(set(wanted)) != len(wanted)
                    or not set(wanted) <= set(chosen)
                ):
                    raise ValueError("Selected positions differ from frozen subset")
                chosen = [p for p in chosen if p in wanted]
            elif len(chosen) != limit:
                raise ValueError("Incomplete frozen subset")
            expected.update((cid, p) for p in chosen)
        if selection is not None and set(selection) - cell_ids:
            raise ValueError("Selection names an unknown cell")
        return expected, "verified"
    except (KeyError, TypeError, ValueError) as error:
        errors.append(dict(path=None, error="Selection not verified: " + str(error)))
        return None, "not_verified"


def check_seals(seals, errors, checks):
    for seal in seals:
        value = read_pin(seal, errors, checks)
        if value is None:
            continue
        root = Path(seal["root"]).resolve()
        for relative, expected in value["files"].items():
            path = (root / relative).resolve()
            try:
                if not path.is_relative_to(root):
                    raise ValueError("Seal path escapes declared root")
                if sha(path) != expected:
                    raise ValueError("SHA256 mismatch")
                checks.append(str(path))
            except (OSError, ValueError) as error:
                errors.append(dict(path=str(path), error=str(error)))


def ledger_check(spec, errors, checks):
    if not spec:
        return dict(status="unknown", delta=None, reason="No before/after evidence")
    before = read_pin(spec["before"], errors, checks)
    after = read_pin(spec["after"], errors, checks)
    if before is None or after is None:
        return dict(status="unknown", delta=None, reason="Invalid snapshot evidence")
    n0 = before.get("engine_charged", before.get("engine_slots"))
    n1 = after.get("engine_charged", after.get("engine_slots"))
    head0, head1 = before.get("engine_head_sha256"), after.get("engine_head_sha256")
    if type(n0) is not int or type(n1) is not int or n1 < n0 or not head0 or not head1:
        return dict(
            status="unknown", delta=None, reason="Missing/invalid counts or chain heads"
        )
    # Positive changes require explicit slot identity evidence to merge safely.
    slots = after.get("new_request_ids")
    delta = n1 - n0
    if (delta == 0 and head0 != head1) or (
        delta > 0
        and (
            head0 == head1
            or not isinstance(slots, list)
            or len(set(slots)) != delta
            or len(slots) != delta
        )
    ):
        return dict(
            status="unknown",
            delta=None,
            reason="Chain changed without matching new slot evidence",
        )
    return dict(
        status="checked_snapshot",
        delta=delta,
        before=n0,
        after=n1,
        before_head=head0,
        after_head=head1,
        new_request_ids=slots if delta else [],
        scope=before.get("scope"),
        before_time=before.get("timestamp"),
        after_time=after.get("timestamp"),
        scope_delta=None,
        note="Ledger-wide delta; per-cell/scope attribution is not inferred",
    )


def process_check(pin, errors, checks):
    if not pin:
        return dict(status="not_checked", leftover_processes=None)
    receipt = read_pin(pin, errors, checks)
    required = (
        "timestamp",
        "active_owned_processes",
        "remaining_owner_locks",
        "remote_cleanup",
    )
    if receipt is None or any(k not in receipt for k in required):
        return dict(status="not_checked", leftover_processes=None)
    remote = receipt["remote_cleanup"]
    if not all(
        k in remote for k in ("hostname", "active_owned_worker_pids", "remaining_locks")
    ):
        return dict(status="not_checked", leftover_processes=None)
    processes = receipt["active_owned_processes"] + remote["active_owned_worker_pids"]
    locks = receipt["remaining_owner_locks"] + remote["remaining_locks"]
    return dict(
        status="historical_receipt",
        checked_at=receipt["timestamp"],
        remote_hostname=remote["hostname"],
        leftover_processes=processes,
        leftover_locks=locks,
        live_process_inspection=False,
    )


def counts(rows):
    return dict(
        planned=len(rows),
        terminal=sum(r["terminal"] for r in rows),
        not_run=sum(r["status"] == "not_run" for r in rows),
        incomplete=sum(not r["terminal"] for r in rows),
        raw_statuses=dict(Counter(r["raw_status"] for r in rows)),
        statuses=dict(Counter(r["status"] for r in rows)),
        scoring=dict(
            scored=sum(r["scored"] for r in rows),
            not_scored=sum(not r["scored"] for r in rows),
            reference_missing=sum(
                r["reference_status"] == "reference_missing" for r in rows
            ),
            reference_error=sum(
                r["reference_status"] == "reference_error" for r in rows
            ),
            scoring_error=sum(r["raw_score_status"] == "execution_error" for r in rows),
        ),
        raw_scoring=dict(Counter(r["raw_score_status"] for r in rows)),
        final_hash_checked=sum(r["final_hash_checked"] for r in rows),
        final_hash_not_checked=sum(not r["final_hash_checked"] for r in rows),
        ledger_delta=None,
        ledger_attribution="not_provided",
    )


def audit(manifest):
    errors, checks, issues = [], [], []
    for source in manifest.get("plan_sources", []):
        read_pin(source, errors, checks)
    planned = set()
    for row in manifest["planned"]:
        key = identity(row)
        if key in planned:
            issues.append(dict(kind="duplicate_planned", cell=key[0], position=key[1]))
        planned.add(key)
    declared_count = len(planned)
    expected, plan_status = selection_expected(
        manifest.get("selection_source"), errors, checks
    )
    if expected is not None:
        if expected != planned:
            issues.append(
                dict(
                    kind="planned_selection_mismatch",
                    omitted=[list(k) for k in sorted(expected - planned)],
                    extra=[list(k) for k in sorted(planned - expected)],
                )
            )
            plan_status = "mismatch"
        planned = expected
    if not planned:
        issues.append(dict(kind="empty_plan"))
    results, duplicates, conflicts = {}, 0, set()
    for pin in manifest["summaries"]:
        value = read_pin(pin, errors, checks)
        if value is None:
            continue
        for row in value["results"]:
            key = identity(row)
            if key not in planned:
                issues.append(
                    dict(kind="unexpected_result", cell=key[0], position=key[1])
                )
                continue
            if key in results:
                if results[key] == row:
                    duplicates += 1
                else:
                    conflicts.add(key)
                    issues.append(
                        dict(kind="conflicting_result", cell=key[0], position=key[1])
                    )
                continue
            results[key] = row
    anomalies = {}
    for entry in manifest.get("reference_anomalies", []):
        if read_pin(entry["evidence"], errors, checks) is not None:
            anomalies[identity(entry)] = entry.get("kind", "reference_error")
    records = []
    for cell, position in sorted(planned):
        row = results.get((cell, position))
        raw = row.get("status", "missing_status") if row else "missing_result"
        status = NORMAL.get(str(raw).lower(), "unknown") if row else "not_run"
        if (cell, position) in conflicts:
            status = "conflict"
        if status == "unknown":
            issues.append(
                dict(kind="unknown_status", cell=cell, position=position, raw=raw)
            )
        score = row.get("score", {}) if row else {}
        raw_score = score.get("status", "missing")
        if row and raw_score not in SCORE_STATUSES:
            issues.append(
                dict(
                    kind="unknown_score_status",
                    cell=cell,
                    position=position,
                    raw=raw_score,
                )
            )
        is_scored = raw_score in SCORED and score.get("value") is not None
        if raw_score in SCORED and not is_scored:
            issues.append(
                dict(kind="missing_score_value", cell=cell, position=position)
            )
        reference = (
            "reference_missing"
            if raw_score == "reference_missing"
            else "reference_error" if raw_score in REFERENCE else None
        )
        reference = anomalies.get((cell, position), reference)
        final = row.get("final") if row else None
        hash_ok = False
        if final:
            value = read_pin(final, errors, checks)
            hash_ok = value is not None
            if value is not None:
                final_status = value.get("status") if isinstance(value, dict) else None
                if (
                    not isinstance(final_status, str)
                    or final_status.lower() not in NORMAL
                ):
                    issues.append(
                        dict(kind="unknown_final_status", cell=cell, position=position)
                    )
                elif (
                    NORMAL[final_status.lower()] != status
                    and status != "interrupted_after_final"
                ):
                    issues.append(
                        dict(
                            kind="final_status_mismatch",
                            cell=cell,
                            position=position,
                            final=final_status,
                            summary=raw,
                        )
                    )
                if isinstance(value, dict):
                    for field in (
                        "cell_id",
                        "position_id",
                        "task_id",
                        "method",
                        "benchmark",
                        "graph",
                    ):
                        if field in value and value[field] != row.get(field):
                            issues.append(
                                dict(
                                    kind="final_identity_mismatch",
                                    field=field,
                                    cell=cell,
                                    position=position,
                                )
                            )
        elif row:
            errors.append(
                dict(
                    path=None,
                    cell=cell,
                    position=position,
                    error="Missing final receipt",
                )
            )
        records.append(
            dict(
                cell_id=cell,
                position_id=position,
                raw_status=raw,
                status=status,
                terminal=status in TERMINAL,
                raw_score_status=raw_score,
                scored=is_scored,
                reference_status=reference,
                final_hash_checked=hash_ok,
            )
        )
    check_seals(manifest.get("seals", []), errors, checks)
    ledger = ledger_check(manifest.get("ledger"), errors, checks)
    processes = process_check(manifest.get("process_check"), errors, checks)
    by_cell = defaultdict(list)
    for row in records:
        by_cell[row["cell_id"]].append(row)
    report = dict(
        version=1,
        audited_at=datetime.now(timezone.utc).isoformat(),
        cells=len(by_cell),
        positions=len(planned),
        declared_positions=declared_count,
        plan_status=plan_status,
        **counts(records),
        by_cell={cell: counts(rows) for cell, rows in sorted(by_cell.items())},
        records=records,
        identical_results_deduplicated=duplicates,
        issues=issues,
        hash_mismatch=errors,
        checked_files=len(set(checks)),
        ledger=ledger,
        process_check=processes,
        leftover_processes=processes["leftover_processes"],
        paper_result=False
    )
    report["ledger_delta"] = ledger["delta"]
    report["evidence_checks_passed"] = (
        bool(planned)
        and plan_status == "verified"
        and not (errors or issues)
        and all(r["terminal"] and r["final_hash_checked"] for r in records)
        and ledger["status"] == "checked_snapshot"
        and processes["status"] == "historical_receipt"
        and not processes["leftover_processes"]
        and not processes["leftover_locks"]
    )
    report["experiment_success"] = None
    report["scientific_admitted"] = False
    report["all_positions_executed_and_scored"] = all(
        r["status"] == "EXECUTED" and r["scored"] for r in records
    ) and bool(records)
    report["note"] = (
        "Terminal means recorded outcome, not correct answer. Process checks are dated evidence; missing checks remain unknown."
    )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8-sig"))
    result = audit(manifest)
    result["audit_tool"] = dict(
        path=str(Path(__file__).resolve()), sha256=sha(__file__)
    )
    result["input_manifest"] = dict(
        path=str(args.manifest.resolve()), sha256=sha(args.manifest)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print(
        json.dumps(
            {
                k: result[k]
                for k in (
                    "cells",
                    "planned",
                    "terminal",
                    "not_run",
                    "statuses",
                    "scoring",
                    "ledger_delta",
                    "evidence_checks_passed",
                )
            },
            ensure_ascii=False,
        )
    )
    return 0 if result["evidence_checks_passed"] else 2


if __name__ == "__main__":
    sys.exit(main())
