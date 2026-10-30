"""Consume pinned request results without owning any model capacity.

Each backend owns its graph lease and reference comparator. This entry point
owns selection, durable answers and the complete denominator. Ordinary case
failures continue; evidence/identity failures remain hard errors.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import ExitStack
import threading
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "adapter_work")]
from engine.config import canonical, digest
from engine.errors import IntegrityError, PersistenceError
from engine.journal import write_new

from common.methods import AUTHOR_METHODS, CONTROL_METHODS, METHODS
from common.author_assets import method_binding, validate_asset

HARD_ERRORS = (IntegrityError, PersistenceError, PermissionError, FileExistsError)


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


def validate_author_source(row, source, config):
    """A Cypher execution route does not itself admit an author's model assets."""
    if config.get("test_only") is True:
        return
    if source is None:
        return  # Retain the unrun position in the denominator.
    result = source["result"]
    if result["final"]["status"] not in ("ready", "answered"):
        return
    request_config = read_pin(config["author_request_config"])
    binding = method_binding(
        request_config["bindings"], row["method"], row["benchmark"], row["graph"]
    )
    if binding.get("ready") is not True:
        raise IntegrityError("Author inference assets remain resource_pending")
    validate_asset(
        binding, row["method"], row["benchmark"], row["graph"], read_pin=read_pin
    )
    # This is the original logical Item.model, not the endpoint's wire alias.
    if result.get("model") != binding["model"]:
        raise IntegrityError("Source response belongs to another model")
    if any(
        result.get(k) != row[k]
        for k in ("task_id", "graph", "method", "benchmark", "position_id")
    ):
        raise IntegrityError("Author source identity differs from position")


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
            raise ValueError("Unsupported method in offline consumer")
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
            if row["method"] in AUTHOR_METHODS:
                source = sources.get(
                    (row["method"], row["benchmark"], row["position_id"])
                )
                validate_author_source(row, source, config)
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


def run_group(
    rows, sources, backend, directory, *, on_feedback=None, on_result=None, stop=None
):
    results, method_answers, method_scores = [], {}, {}
    # Freeze a method's actual answer before its controls and evaluator.
    for row in sorted(
        rows, key=lambda r: (METHODS.index(r["method"]), r["position_id"])
    ):
        if stop is not None and stop.is_set():
            break
        try:
            folder = directory / row["cell_id"] / row["position_id"]
            started = time.monotonic()
            source_method = (
                "direct"
                if row["method"] == "direct-feedback"
                else ("ours" if row["method"] in CONTROL_METHODS else row["method"])
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
                elif row["method"] in ("direct", "ours") + AUTHOR_METHODS:
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
                        answer = dict(
                            initial, shared_initial=True, feedback_required=False
                        )
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
                            feedback_pin = save(
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
                            if on_feedback is not None:
                                try:
                                    on_feedback(feedback_pin)
                                except Exception as error:
                                    raise PersistenceError(
                                        "Feedback delivery callback failed"
                                    ) from error
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
            if (
                answer.get("status") == "EXECUTED"
                and answer.get("complete") is not True
            ):
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
            if on_result is not None:
                on_result(result)
        finally:
            finish = getattr(backend, "finish_case", None)
            if finish is not None:
                active_error = sys.exc_info()[0] is not None
                try:
                    finish(row)
                except Exception as error:
                    save(
                        directory
                        / row["cell_id"]
                        / row["position_id"]
                        / "cleanup_error.json",
                        error_result(error),
                    )
                    if not active_error:
                        raise
    return results


def run(config, backend_factory=None, *, runtime=None, on_feedback=None):
    if config.get("test_only") is True and backend_factory is None:
        raise IntegrityError("test_only requires an explicitly injected test backend")
    if config.get("execute_authorized") is not True:
        raise ValueError("Explicit offline smoke admission required")
    if backend_factory is None and config.get("source") != source_manifest():
        raise IntegrityError("Consumer source changed; freeze a new version")
    groups, sources = load_selection(config)
    output = Path(config["output"])
    output.mkdir(parents=True, exist_ok=False)
    save(output / "config.json", config)
    results, results_lock = [], threading.Lock()
    if backend_factory is None:
        from offline_backends import open_backend

        backend_factory = open_backend

    def record(result):
        with results_lock:
            results.append(result)

    def group_failure(benchmark, graph, rows, error):
        # Keep all saved results, including those produced before a cleanup error.
        with results_lock:
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

    parallel = config.get("parallel")
    if runtime is not None and parallel is not None:
        from offline_runtime import Limits

        if runtime.limits != Limits.parse(parallel):
            raise IntegrityError("Shared runtime capacity differs from frozen config")

    def consume_group(item):
        (benchmark, graph), rows = item
        settings = config["backends"].get(benchmark)
        try:
            if runtime is None:
                with backend_factory(benchmark, graph, settings, output) as backend:
                    run_group(
                        rows,
                        sources,
                        backend,
                        output,
                        on_feedback=on_feedback,
                        on_result=record,
                    )
            else:
                with runtime.pool.lease(
                    benchmark, graph, settings, runtime.stop
                ) as backend:
                    positions = defaultdict(list)
                    for row in rows:
                        positions[row["position_id"]].append(row)
                    runtime.run_positions(
                        lambda position: run_group(
                            position,
                            sources,
                            backend,
                            output,
                            on_feedback=on_feedback,
                            on_result=record,
                            stop=runtime.stop,
                        ),
                        positions.values(),
                        backend,
                        benchmark + "/" + graph,
                    )
        except HARD_ERRORS:
            if runtime is not None:
                runtime.stop.set()
            raise
        except Exception as error:
            if runtime is not None and runtime.stop.is_set():
                raise
            group_failure(benchmark, graph, rows, error)

    failure = None
    try:
        with ExitStack() as stack:
            if runtime is None and parallel is not None:
                from offline_runtime import OfflineRuntime

                runtime = stack.enter_context(
                    OfflineRuntime(output / "runtime", backend_factory, parallel)
                )
            if runtime is None:
                for item in groups.items():
                    consume_group(item)
            else:
                if runtime.stop.is_set():
                    raise IntegrityError("Shared runtime is already draining")
                for benchmark, graph in groups:
                    runtime.pool.resources(graph, config["backends"].get(benchmark))
                runtime.run_groups(consume_group, groups.items())
    except BaseException as error:
        failure = error

    # Build the entire denominator, even when admission or persistent IO halted work.
    done = {(r["cell_id"], r["position_id"]) for r in results}
    for rows in groups.values():
        for row in rows:
            if (row["cell_id"], row["position_id"]) not in done:
                final_file = output / row["cell_id"] / row["position_id"] / "final.json"
                receipt = (
                    dict(path=str(final_file), sha256=digest(final_file.read_bytes()))
                    if final_file.is_file()
                    else None
                )
                results.append(
                    dict(
                        cell_id=row["cell_id"],
                        position_id=row["position_id"],
                        method=row["method"],
                        benchmark=row["benchmark"],
                        graph=row["graph"],
                        status=(
                            "interrupted_after_final"
                            if receipt
                            else "not_run_hard_stop"
                        ),
                        final=receipt,
                        model_calls=0,
                    )
                )
    results.sort(key=lambda r: (r["cell_id"], r["position_id"]))
    summary = dict(
        planned=sum(map(len, groups.values())),
        results=results,
        counts=dict(Counter(r["status"] for r in results)),
        model_calls=0,
        paper_result=False,
        test_only=config.get("test_only") is True,
        runtime=runtime.snapshot() if runtime is not None else None,
        hard_stop=error_result(failure) if failure else None,
    )
    save(output / ("partial_summary.json" if failure else "summary.json"), summary)
    if failure is not None:
        raise failure
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    summary = run(json.loads(args.config.read_text(encoding="utf-8-sig")))
    print(json.dumps(dict(planned=summary["planned"], counts=summary["counts"])))
