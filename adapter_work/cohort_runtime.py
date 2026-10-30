"""Read and bind explicit cohorts without changing legacy smoke selection.

No model, database, or reference comparison belongs in this module.
"""
from dataclasses import is_dataclass, replace
import json
from pathlib import Path

from engine.config import digest
from engine.errors import IntegrityError


SCHEMA = "cohort_inventory_v1"


def is_cohort(inventory):
    return inventory.get("schema_version") == SCHEMA


def read_pin(pin):
    raw = Path(pin["path"]).read_bytes()
    if digest(raw) != pin["sha256"]:
        raise IntegrityError("Cohort artifact changed: " + pin["path"])
    return json.loads(raw)


def validate_inventory(inventory):
    """Verify each cell against its immutable, independently selected positions."""
    if not is_cohort(inventory):
        raise IntegrityError("Expected explicit cohort inventory")
    from common.cohort import load_cohort

    cohorts = {}
    for pin in inventory["cohorts"]:
        cohort = load_cohort(pin)
        if pin["sha256"] in cohorts:
            raise IntegrityError("Duplicate cohort binding")
        cohorts[pin["sha256"]] = cohort
    cells = set()
    for cell in inventory["cells"]:
        cid = cell["cell_id"]
        if cid in cells:
            raise IntegrityError("Duplicate cohort cell")
        cells.add(cid)
        binding = cell["cohort"]
        cohort = cohorts.get(binding["sha256"])
        if cohort is None:
            raise IntegrityError("Cell binds an undeclared cohort")
        original = {
            row.get("source_position_id", row.get("position_id")): row
            for row in cohort.positions
        }
        positions = cell["positions"]
        if len(original) != len(cohort.positions) or len(positions) != len(original):
            raise IntegrityError("Cell does not preserve cohort denominator")
        seen, execution_ids = set(), set()
        exported = read_pin(cell["positions_source"])["positions"]
        exports = {row["position_id"]: row for row in exported}
        if len(exports) != len(exported) or set(exports) != set(original):
            raise IntegrityError("Adapter positions differ from cohort")
        for row in positions:
            pid = row["position_id"]
            if pid in seen or pid not in original:
                raise IntegrityError("Duplicate or foreign cohort position")
            seen.add(pid)
            source = original[pid]
            for field in ("task_id", "graph"):
                if row[field] != source[field] or exports[pid][field] != source[field]:
                    raise IntegrityError("Cohort task/graph identity changed")
            if row.get("cohort_sha256") != binding["sha256"]:
                raise IntegrityError("Position binds another cohort")
            execution = row.get("execution_task_id")
            if not isinstance(execution, str) or not execution or execution in execution_ids:
                raise IntegrityError("Missing or duplicate execution identity")
            execution_ids.add(execution)
            if exports[pid].get("execution_task_id") != execution:
                raise IntegrityError("Adapter execution identity changed")
            public = row["public_input"]
            if exports[pid]["public_input_sha256"] != public["sha256"]:
                raise IntegrityError("Adapter public input binding changed")
            read_pin(public)
        if seen != set(original):
            raise IntegrityError("Missing cohort positions")
    return inventory


def selected_cells(inventory, selection):
    """Return an explicit execution subset; the inventory remains the denominator."""
    validate_inventory(inventory)
    if selection.get("mode") != "cohort":
        raise IntegrityError("Explicit inventory needs selection.mode=cohort")
    pins = selection.get("cohorts")
    if pins != inventory["cohorts"]:
        raise IntegrityError("Selection binds different cohorts")
    recovery = selection.get("selected_positions")
    if recovery is not None and (not isinstance(recovery, dict) or set(recovery) - {c["cell_id"] for c in inventory["cells"]}):
        raise IntegrityError("Recovery names an unknown cell")
    chosen = []
    for cell in inventory["cells"]:
        positions = cell["positions"]
        if recovery is not None:
            wanted = recovery.get(cell["cell_id"], [])
            if not isinstance(wanted, list) or len(wanted) != len(set(wanted)):
                raise IntegrityError("Recovery positions must be unique")
            available = {p["position_id"] for p in positions}
            if not set(wanted) <= available:
                raise IntegrityError("Recovery selection is outside frozen cohort")
            positions = [p for p in positions if p["position_id"] in set(wanted)]
        chosen.append(dict(cell, positions=positions))
    return chosen


def execution_spec(spec, position):
    """Give the scheduler a unique ID without changing a prompt or source task ID."""
    execution = position.get("execution_task_id")
    if execution is None:
        return spec
    if isinstance(spec, dict):
        return dict(spec, task_id=execution)
    if is_dataclass(spec):
        return replace(spec, task_id=execution)
    raise IntegrityError("Cohort adapter requires a dict or dataclass RequestSpec")


def provenance(position):
    return {name: position[name] for name in (
        "cohort_sha256", "source_position_id", "execution_task_id"
    ) if name in position}


def validate_source(row, result):
    if any(result.get(k) != row[k] for k in ("task_id", "graph", "benchmark", "position_id")):
        raise IntegrityError("Saved response identity differs from cohort position")
    if result.get("cohort_sha256") != row["cohort_sha256"]:
        raise IntegrityError("Saved response belongs to another cohort")
    if result.get("execution_task_id") != row["execution_task_id"]:
        raise IntegrityError("Saved response execution identity changed")
