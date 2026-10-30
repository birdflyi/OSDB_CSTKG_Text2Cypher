"""Consume pinned request results without owning any model capacity.

Each backend owns its graph lease and reference comparator. This entry point
owns selection, durable answers and the complete denominator. Ordinary case
failures continue; evidence/identity failures remain hard errors.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "adapter_work")]
from engine.config import canonical, digest
from engine.errors import IntegrityError, PersistenceError
from engine.journal import write_new

HARD_ERRORS = (IntegrityError, PersistenceError, PermissionError, FileExistsError)
METHODS = (
    "direct",
    "ours",
    "direct-feedback",
    "correct-detail",
    "simple-correct",
    "native-set-count",
)


def read_pin(pin):
    raw = Path(pin["path"]).read_bytes()
    if digest(raw) != pin["sha256"]:
        raise IntegrityError("Pinned input changed: " + pin["path"])
    return json.loads(raw)


def save(path, value):
    write_new(path, canonical(value))
    return dict(path=str(path), sha256=digest(path.read_bytes()))


def source_manifest():
    folders = (
        "engine",
        "adapter_work",
        "operations/analysis",
        "experiment-harness/public_benchmark_v1",
    )
    return {
        p.relative_to(ROOT).as_posix(): digest(p.read_bytes())
        for folder in folders
        for p in sorted((ROOT / folder).rglob("*.py"))
        if "tests" not in p.parts and not p.name.startswith("test_")
    }


def load_selection(config):
    inventory = read_pin(config["inventory"])
    limit = config["per_cell"]
    if limit not in (1, 5):
        raise ValueError("Offline smoke selects one or five frozen positions")
    sources = {}
    for pin in config["source_finals"]:
        result = read_pin(pin)
        key = result["method"], result["benchmark"], result["position_id"]
        if key in sources:
            raise IntegrityError("Duplicate source final")
        sources[key] = dict(result=result, receipt=pin)
    groups = defaultdict(list)
    for cell in inventory["cells"]:
        if cell["method"] not in METHODS:
            raise ValueError("This consumer scope contains the six nonauthor methods")
        selected = [p for p in cell["positions"] if p["full_flow_five"]][:limit]
        if "selected_positions" in config:
            wanted = set(config["selected_positions"].get(cell["cell_id"], []))
            selected = [p for p in selected if p["position_id"] in wanted]
            if {p["position_id"] for p in selected} != wanted:
                raise IntegrityError("Recovery selection is outside the frozen subset")
            if not selected:
                continue
        elif len(selected) != limit:
            raise IntegrityError("Incomplete frozen five-position subset")
        for position in selected:
            read_pin(position["public_input"])
            row = dict(
                position,
                cell_id=cell["cell_id"],
                method=cell["method"],
                benchmark=cell["benchmark"],
            )
            groups[cell["benchmark"], position["graph"]].append(row)
    return groups, sources


def error_result(error):
    return dict(
        status="execution_error",
        complete=False,
        error_type=type(error).__name__,
        error=str(error)[:1000],
        database_code=getattr(error, "code", None),
        rows=None,
        columns=None,
    )


def run_group(rows, sources, backend, directory):
    results, method_answers, method_scores = [], {}, {}
    # Freeze a method's actual answer before its controls and evaluator.
    for row in sorted(
        rows, key=lambda r: (METHODS.index(r["method"]), r["position_id"])
    ):
        folder = directory / row["cell_id"] / row["position_id"]
        started = time.monotonic()
        source_method = (
            "direct"
            if row["method"] == "direct-feedback"
            else ("ours" if row["method"] in METHODS[3:] else row["method"])
        )
        feedback_source = sources.get(
            ("direct-feedback", row["benchmark"], row["position_id"])
        )
        if row["method"] == "direct-feedback" and feedback_source:
            source_method = "direct-feedback"
        source = sources.get((source_method, row["benchmark"], row["position_id"]))
        save(
            folder / "input.json",
            dict(position=row, source=source["receipt"] if source else None),
        )
        source_status = (
            source["result"]["final"]["status"] if source else "not_dispatched"
        )
        try:
            if source_status not in ("ready", "answered"):
                answer = dict(
                    status=source_status, complete=False, rows=None, columns=None
                )
            elif row["method"] in ("direct", "ours"):
                answer = backend.execute(row, source["result"], folder)
                method_answers[row["method"], row["position_id"]] = answer
            elif row["method"] == "direct-feedback":
                initial = method_answers.get(("direct", row["position_id"]))
                if feedback_source:
                    answer = backend.execute(row, source["result"], folder)
                    answer = dict(answer, feedback_used=True)
                elif initial is None:
                    answer = dict(status="dependency_missing", complete=False)
                elif (
                    initial.get("status") == "EXECUTED"
                    and initial.get("complete") is True
                ):
                    answer = dict(initial, shared_initial=True, feedback_required=False)
                else:
                    from feedback_queue import operational_error

                    error = operational_error(initial)
                    answer = dict(
                        status=(
                            "feedback_pending"
                            if error
                            else "initial_failed_no_model_repair"
                        ),
                        complete=False,
                        rows=None,
                        columns=None,
                        initial_status=initial.get("status"),
                        feedback_required=error is not None,
                    )
                    if error:
                        initial_row = next(
                            r
                            for r in rows
                            if r["method"] == "direct"
                            and r["position_id"] == row["position_id"]
                        )
                        initial_file = (
                            directory
                            / initial_row["cell_id"]
                            / row["position_id"]
                            / "final.json"
                        )
                        save(
                            folder / "feedback_request.json",
                            dict(
                                position=row,
                                source=source["receipt"],
                                database_final=dict(
                                    path=str(initial_file),
                                    sha256=digest(initial_file.read_bytes()),
                                ),
                            ),
                        )
            else:
                initial = method_answers.get(("ours", row["position_id"]))
                if initial is None:
                    answer = dict(
                        status="source_plan_not_executed",
                        complete=False,
                        rows=None,
                        columns=None,
                    )
                else:
                    answer = backend.control(row, source["result"], folder)
        except HARD_ERRORS:
            raise
        except Exception as error:
            answer = error_result(error)
            if row["method"] in ("direct", "ours"):
                method_answers[row["method"], row["position_id"]] = answer
        if answer.get("status") == "EXECUTED" and answer.get("complete") is not True:
            answer = dict(answer, status="incomplete_answer")
        receipt = save(folder / "final.json", answer)
        try:
            # The backend may read references only at this point.
            if answer.get("shared_initial"):
                score = dict(
                    method_scores["direct", row["position_id"]], shared_initial=True
                )
            else:
                score = (
                    backend.score(row, answer, receipt)
                    if answer.get("complete") is True
                    else dict(status="not_scored", value=None)
                )
        except HARD_ERRORS:
            raise
        except Exception as error:
            score = dict(
                status="scoring_error", value=None, error_type=type(error).__name__
            )
        if row["method"] in ("direct", "ours"):
            method_scores[row["method"], row["position_id"]] = score
        result = dict(
            cell_id=row["cell_id"],
            position_id=row["position_id"],
            method=row["method"],
            benchmark=row["benchmark"],
            graph=row["graph"],
            status=answer["status"],
            final=receipt,
            score=score,
            seconds=time.monotonic() - started,
            source_status=source_status,
            model_calls=0,
        )
        save(folder / "result.json", result)
        results.append(result)
    return results


def run(config, backend_factory=None):
    if config.get("execute_authorized") is not True:
        raise ValueError("Explicit offline smoke admission required")
    if backend_factory is None and config.get("source") != source_manifest():
        raise IntegrityError("Consumer source changed; freeze a new version")
    groups, sources = load_selection(config)
    output = Path(config["output"])
    output.mkdir(parents=True, exist_ok=False)
    save(output / "config.json", config)
    results = []
    if backend_factory is None:
        from offline_backends import open_backend

        backend_factory = open_backend
    for (benchmark, graph), rows in groups.items():
        settings = config["backends"].get(benchmark)
        try:
            with ExitStack() as stack:
                backend = stack.enter_context(
                    backend_factory(benchmark, graph, settings, output)
                )
                results.extend(run_group(rows, sources, backend, output))
        except HARD_ERRORS:
            raise
        except Exception as error:
            # Existing case receipts survive a later group/cleanup failure.
            done = {(r["cell_id"], r["position_id"]) for r in results}
            for row in rows:
                if (row["cell_id"], row["position_id"]) not in done:
                    results.append(
                        dict(
                            cell_id=row["cell_id"],
                            position_id=row["position_id"],
                            benchmark=benchmark,
                            graph=graph,
                            method=row["method"],
                            status="backend_error",
                            error_type=type(error).__name__,
                            error=str(error)[:1000],
                            model_calls=0,
                        )
                    )
            save(
                output
                / "group_errors"
                / (digest(canonical([benchmark, graph]))[:20] + ".json"),
                dict(benchmark=benchmark, graph=graph, **error_result(error)),
            )
    summary = dict(
        planned=sum(map(len, groups.values())),
        results=results,
        counts=dict(Counter(r["status"] for r in results)),
        model_calls=0,
        paper_result=False,
    )
    save(output / "summary.json", summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    summary = run(json.loads(args.config.read_text(encoding="utf-8-sig")))
    print(json.dumps(dict(planned=summary["planned"], counts=summary["counts"])))
