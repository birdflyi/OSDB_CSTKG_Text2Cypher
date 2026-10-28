"""Table 1 inventory. This module never calls models or reads evaluator gold."""

from collections import Counter
from copy import deepcopy
import json
import math
from pathlib import Path

from .config import digest
from .journal import write_status

STATUS_MEASURED = "measured"
STATUS_PARTIAL = "partial"
STATUS_EMPTY = "empty"
STATUS_BLOCKED = "blocked"
STATUS_REPORTED = "reported"
STATUSES = {STATUS_MEASURED, STATUS_PARTIAL, STATUS_EMPTY, STATUS_BLOCKED, STATUS_REPORTED}
METHOD_KINDS = ("model", "replay", "author")


def validate(value):
    _validate_version(value)
    _validate_methods(value["methods"])
    _validate_benchmarks(value["benchmarks"])
    _validate_cells(value)
    return value


def _validate_version(value):
    if value.get("version") != 1:
        raise ValueError("Table version must be 1")


def _validate_methods(methods):
    ids = [row["id"] for row in methods]
    if len(ids) != len(set(ids)) or not all(isinstance(name, str) and name for name in ids):
        raise ValueError("Duplicate or empty method ID")
    known = set(ids)
    for method in methods:
        if method["kind"] not in METHOD_KINDS:
            raise ValueError("Unknown method kind")
        if method.get("shared_with") and method["shared_with"] not in known:
            raise ValueError("Unknown shared source method")


def _validate_benchmarks(benchmarks):
    ids = [row["id"] for row in benchmarks]
    if len(ids) != len(set(ids)) or not all(isinstance(name, str) and name for name in ids):
        raise ValueError("Duplicate or empty benchmark ID")
    for benchmark in benchmarks:
        names = benchmark["splits"]
        if (
            not names
            or not all(isinstance(name, str) and name for name in names)
            or len(names) != len(set(names))
        ):
            raise ValueError("Benchmark split names must be nonempty and unique")
        population = benchmark.get("population")
        if population is not None and (
            set(population) != set(names)
            or any(type(number) is not int or number <= 0 for number in population.values())
        ):
            raise ValueError("Benchmark population needs a positive integer for each split")


def _validate_cells(value):
    methods = {row["id"] for row in value["methods"]}
    benchmarks = {row["id"]: row for row in value["benchmarks"]}
    seen = set()
    for cell in value["cells"]:
        key = cell["method"], cell["benchmark"]
        if key in seen or key[0] not in methods or key[1] not in benchmarks:
            raise ValueError("Duplicate cell or unknown method/benchmark: " + str(key))
        seen.add(key)
        _validate_cell_status(cell, value, benchmarks[key[1]])


def _validate_cell_status(cell, value, benchmark):
    status = cell["status"]
    if status not in STATUSES:
        raise ValueError("Unknown cell status: " + str(status))
    if status == STATUS_REPORTED:
        _validate_reported(cell)
    if status == STATUS_BLOCKED and not cell.get("unblock", {}).get("missing"):
        raise ValueError("Blocked cell needs unblock.missing")
    if status == STATUS_MEASURED:
        _validate_measured(cell, value, benchmark)
    if cell.get("splits") and set(cell["splits"]) - set(benchmark["splits"]):
        raise ValueError("Cell references an unknown split")


def _validate_reported(cell):
    number = cell.get("value")
    if not cell.get("source") or type(number) not in (int, float) or not math.isfinite(number):
        raise ValueError("Reported result needs source and finite value")


def _validate_measured(cell, value, benchmark):
    result = cell.get("result", {})
    population = benchmark.get("population")
    score = result.get("score")
    if (
        not cell.get("splits")
        or not result.get("evidence")
        or result.get("stage") != "full"
        or result.get("complete") is not True
        or result.get("full_population") is not True
        or not population
        or cell["splits"] != population
        or result.get("model") != value.get("track", {}).get("model")
        or type(score) not in (int, float)
        or not math.isfinite(score)
        or not result.get("model")
    ):
        raise ValueError("Measured needs complete full-population results and evidence")
    for name, number in population.items():
        row = result.get("splits", {}).get(name, {})
        if row.get("terminal") != number or row.get("scored") != number:
            raise ValueError("Measured split denominator is not closed: " + name)


def load(path):
    return validate(json.loads(Path(path).read_text(encoding="utf-8-sig")))


def cells(table):
    """Missing grid entries stay explicit unconfigured demand, never executable."""
    declared = {(c["method"], c["benchmark"]): c for c in table["cells"]}
    return [
        declared.get(
            (m["id"], b["id"]),
            dict(method=m["id"], benchmark=b["id"], status="empty", unconfigured=True),
        )
        for m in table["methods"]
        for b in table["benchmarks"]
    ]


def empty_cells(table):
    return sorted(
        (c for c in cells(table) if c["status"] == "empty"),
        key=lambda c: (c.get("order", 999999), c["method"], c["benchmark"]),
    )


def plan(table):
    from .adapters import ADAPTERS

    rows = []
    for cell in cells(table):
        benchmark = next(b for b in table["benchmarks"] if b["id"] == cell["benchmark"])
        reasons = []
        if benchmark["adapter"] not in ADAPTERS:
            reasons.append("尚无可执行适配器")
        if not cell.get("execution"):
            reasons.append("尚未绑定冻结输入、执行配置及预算准入")
        rows.append(
            dict(
                method=cell["method"],
                benchmark=cell["benchmark"],
                status=cell["status"],
                splits=benchmark["splits"],
                preparation_missing=reasons,
                unblock=cell.get("unblock"),
            )
        )
    return dict(
        track=table.get("track"),
        total=len(rows),
        counts=dict(Counter(c["status"] for c in rows)),
        cells=rows,
        demand_is_not_dispatch_approval=True,
        new_model_calls=0,
    )


def report(table):
    rows, details = [], []
    for cell in cells(table):
        measured = cell.get("result", {}) if cell["status"] == "measured" else {}
        rows.append(
            dict(
                method=cell["method"],
                benchmark=cell["benchmark"],
                status=cell["status"],
                measured=measured.get("score"),
                reported=cell.get("value") if cell["status"] == "reported" else None,
                source=cell.get("source"),
                evidence=measured.get("evidence"),
            )
        )
        for name, n in cell.get("splits", {}).items():
            details.append(
                dict(
                    method=cell["method"],
                    benchmark=cell["benchmark"],
                    split=name,
                    planned=n,
                    result=cell.get("result", {}).get("splits", {}).get(name),
                )
            )
    return dict(
        track=table.get("track"),
        total=len(rows),
        rows=rows,
        split_details=details,
        measured_count=sum(r["status"] == "measured" for r in rows),
        historical_sources=table.get("historical_sources", []),
    )


def write_back(path, method, benchmark, result, *, expected_sha256):
    """One result writer; a second writer or stale table is rejected explicitly."""
    from .config import canonical
    from .journal import write_new
    import uuid

    path = Path(path)
    lock = path.with_name(path.name + ".writer.lock")
    claim = canonical(dict(expected_sha256=expected_sha256, owner=uuid.uuid4().hex))
    write_new(lock, claim)
    try:
        if digest(path.read_bytes()) != expected_sha256:
            raise ValueError("Table changed; reload before writing a result")
        table = deepcopy(load(path))
        cell = next(
            (
                c
                for c in table["cells"]
                if (c["method"], c["benchmark"]) == (method, benchmark)
            ),
            None,
        )
        if cell is None or cell["status"] == "reported":
            raise ValueError("Cannot replace a missing cell or a literature citation")
        complete = (
            result.get("stage") == "full"
            and result.get("complete") is True
            and result.get("full_population") is True
        )
        cell.update(
            status="measured" if complete else "partial",
            result=result,
            splits={
                name: row["planned"] for name, row in result.get("splits", {}).items()
            },
        )
        if complete:
            cell.pop("unblock", None)
        write_status(path, validate(table))
    finally:
        if lock.read_bytes() != claim:
            raise ValueError("Foreign table writer lock retained")
        lock.unlink()
