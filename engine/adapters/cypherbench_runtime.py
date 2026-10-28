from __future__ import annotations

import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time

from .cypherbench import GRAPHS, ExecutedAnswerAdapter, feedback_item
from ..campaign import execute_campaign, finalize_campaign, generate_initial
from ..config import canonical, digest
from ..execution import JsonRpcClient, TracedRpcExecutor, verify_worker_timeouts, worker_command
from ..journal import Journal, write_new
from ..replay import frozen_modules
from ..scheduler import Scheduler
from ..transport import HTTPTransport
from ..errors import IntegrityError, PersistenceError
from ..ahead import local_budget


def traced_executor(client, directory, item, kind, worker, timeouts=None):
    identity = dict(
        logical_key=item.key,
        model=item.model,
        task_id=item.task_id,
        method=item.method,
        graph=item.graph,
        phase=kind,
    )
    # One bounded directory component, with the full identity retained inside.
    # This also leaves room for atomic temporary filenames on Windows.
    folder = Path(directory) / "db_traces" / digest(canonical(identity))[:32]
    return TracedRpcExecutor(
        client,
        folder,
        identity=identity,
        graph_sha256=worker["expected_ready"]["graph_sha256"],
        worker_version="engine-neo4j-v1",
        timeouts=timeouts,
    )


def _score_answer(qid, outcome, references, scoring, compare):
    try:
        reference = references[qid]
        status = outcome.get("legacy_status", outcome["status"])
        if status not in (
            "EXECUTED",
            "TRUNCATED",
            "UNSUPPORTED",
            "MODEL_ERROR",
            "EXECUTION_ERROR",
        ):
            status = "INFRASTRUCTURE_ERROR"
        score = scoring.score_executed_answer(
            status=status,
            predicted_rows=outcome.get("rows"),
            reference_rows=scoring.read_reference_answer(reference["answer_json"]),
            order_matters="order by" in reference["gold_cypher"].lower(),
            qid=qid,
            compare=compare,
        )
        return dict(status=status, score=score)
    except (PersistenceError, FileExistsError, IntegrityError):
        raise
    except Exception as exc:
        import traceback

        return dict(
            status="SCORING_ERROR",
            score=None,
            scoring_error=dict(error_type=type(exc).__name__, traceback=traceback.format_exc()),
        )


def score_paths(paths, references, scoring, compare):
    """Evaluator-only reference access, after final answers have been committed."""
    scores = []
    for path in paths:
        for label, outcome in [
            ("initial", path["views"][0]["outcome"]),
            ("final", path["final"]),
        ]:
            scores.append(
                dict(
                    model=path["model"],
                    method=path["method"],
                    qid=path["task_id"],
                    graph=path["graph"],
                    view=label,
                    **_score_answer(path["task_id"], outcome, references, scoring, compare),
                )
            )
    return scores


def score_graph(paths, controls, data, graph, scoring, compare):
    """Evaluator work runs in consumer threads, away from HTTP admission."""
    references = {
        r["qid"]: r
        for r in (
            json.loads(line)
            for line in (Path(data) / "evaluator/test.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        )
    }
    graph_scores = score_paths(paths, references, scoring, compare)
    control_scores = []
    for control in controls:
        if control["status"] != "shared_plan_control":
            continue
        scored = _score_answer(control["qid"], control["full_detail"], references, scoring, compare)
        control_scores.append(
            dict(
                qid=control["qid"],
                model=control["model"],
                graph=graph,
                full_detail_score=scored["score"],
                simple_correct_score=scored["score"],
                scoring_error=scored.get("scoring_error"),
                independent_model_method=False,
            )
        )
    return graph_scores, control_scores


async def run_graphs(
    stage,
    items,
    config,
    make_spec,
    pool,
    ledger,
    *,
    transport_factory=HTTPTransport,
    client_factory=JsonRpcClient,
    shared_transport=None,
    saved_replies=None,
    pacer=None,
    initial_handoff=None,
    consumer_executor=None,
):
    """One method/benchmark block; lifecycle-serial graphs, rolling model requests.

    The worker command must start our owned runtime and prove READY identity. No
    legacy daemon or benchmark runner is implicitly resumed. Other benchmarks
    need their own admitted adapter; they cannot be cast into CypherBench.
    """
    settings = stage["adapter"]
    try:
        _, methods, scoring = await asyncio.get_running_loop().run_in_executor(
            consumer_executor,
            frozen_modules,
            Path(settings["code_root"]),
            Path(settings["legacy_freeze"]),
        )
    except ValueError as exc:
        raise IntegrityError("Frozen method sources differ") from exc
    data = Path(settings["data"])
    compare = await asyncio.get_running_loop().run_in_executor(
        consumer_executor,
        scoring.load_official_comparator,
        Path(settings["official_source"]),
    )
    specs = make_spec(stage)
    directory = specs.run_dir
    directory.mkdir(parents=True, exist_ok=initial_handoff is not None)
    scores, summaries = [], []
    undispatched = Counter()
    case_failures = 0
    drain_paths = [
        Path(config.get("run_dir", directory.parent)) / "DRAIN",
        directory / "DRAIN",
    ]
    if any(i.graph not in GRAPHS for i in items):
        raise ValueError("Formal CypherBench requires the seven declared test graphs")
    if initial_handoff is None and config.get("prefetch_initial", False):
        transport = shared_transport or transport_factory(
            Path(config["key_dir"]), connect_timeout=specs.timeouts.http_connect
        )
        try:
            initial_handoff = await generate_initial(
                specs,
                items,
                pool,
                ledger,
                transport,
                registry_path=config["registry_path"],
                saved=saved_replies,
                pacer=pacer,
                drain_paths=drain_paths,
            )
        finally:
            if shared_transport is None:
                transport.close()
    if initial_handoff is not None:
        from ..recovery import saved_replies as load_replies

        path = Path(initial_handoff["path"])
        if digest(path.read_bytes()) != initial_handoff["sha256"]:
            raise IntegrityError("Changed generation handoff")
        value = json.loads(path.read_text(encoding="utf-8"))
        if value["stage"] != stage.get("name", specs.name) or set(value["keys"]) != {
            i.key for i in items
        }:
            raise IntegrityError("Generation handoff belongs to another cell")
        if "history" not in value:
            return dict(
                stop_reason=value["summary"].get("stop_reason"),
                case_failures=1,
                budget=local_budget(ledger, directory),
                undispatched_by_reason=value["summary"].get("undispatched_by_reason", {}),
            )
        cached = load_replies(
            value["history"],
            value["history_sha256"],
            ledger,
            logical_keys=set(value["keys"]),
        )
        saved_replies = dict(saved_replies or {}, **cached)
    for graph in GRAPHS:
        selected = [i for i in items if i.graph == graph]
        if not selected:
            # Formal population is checked before dispatch by live_pipeline.
            # Smoke/recovery inventories may explicitly contain fewer graphs.
            continue
        worker = settings["workers"][graph]
        if digest(canonical(worker)) != settings["worker_sha256"][graph]:
            raise IntegrityError("Changed graph runtime contract")
        schema = json.loads(
            (data / "upstream/graphs/schemas" / (graph + "_schema.json")).read_text(
                encoding="utf-8"
            )
        )
        stderr = (directory / (graph + "_worker_stderr.log")).open("x", encoding="utf-8")
        client = await asyncio.get_running_loop().run_in_executor(
            consumer_executor,
            lambda: client_factory(
                worker_command(worker["command"], specs.timeouts),
                expected_ready=worker["expected_ready"],
                stderr=stderr,
                ready_timeout=settings.get(
                    "worker_startup_timeout_seconds", specs.timeouts.worker_ready
                ),
                timeouts=specs.timeouts,
            ),
        )
        from dataclasses import replace

        spec = replace(specs, name=specs.name + "-" + graph, run_dir=directory / graph)

        def executor(item, kind=None):
            return traced_executor(
                client, spec.run_dir, item, kind or item.phase, worker, spec.timeouts
            )

        adapter = ExecutedAnswerAdapter(
            {graph: schema}, methods.interpret, executor, scoring.score_executed_answer
        )
        try:
            verify_worker_timeouts(client.ready_value, spec.timeouts)
            scheduler = Scheduler(
                spec,
                pool,
                ledger,
                Journal(spec.run_dir / "journal.jsonl"),
                (
                    shared_transport
                    if shared_transport is not None
                    else transport_factory(
                        Path(config["key_dir"]), connect_timeout=specs.timeouts.http_connect
                    )
                ),
                close_transport=shared_transport is None,
            )
            scheduler.saved_replies = saved_replies or {}
            scheduler.extra_drain_paths = drain_paths
            if pacer is not None:
                scheduler.pacer = pacer
            summary = await execute_campaign(
                scheduler,
                selected,
                registry_path=Path(config["registry_path"]),
                execute=adapter.execute,
                feedback=feedback_item,
                defer_seal=True,
                metadata=dict(
                    purpose="formal_T1",
                    worker_READY=client.ready_value,
                    benchmark="CypherBench",
                    method=stage["method"],
                ),
            )
            summaries.append(summary)
            undispatched.update(summary["undispatched_by_reason"])
            hard_stop = (
                summary["stop_reason"]
                or local_budget(ledger, directory)["pending"]
                or summary["undispatched_by_reason"].get("budget")
            )
            if hard_stop or (
                summary["undispatched_by_reason"]
                and not config.get("continue_on_path_failures", True)
            ):
                finalize_campaign(spec.run_dir, summary)
                return dict(
                    stop_reason=summary["stop_reason"]
                    or ("undispatched_paths" if summary["undispatched_by_reason"] else "pending"),
                    budget=local_budget(ledger, directory),
                )
            # execute_campaign has already persisted every method final answer.
            # Shared-planner controls are separate reads/costs, never model arms.
            by_key = {i.key: i for i in selected}
            controls = []

            def control(path):
                item = by_key[path["logical_key"]]
                plan = path["final"].get("plan")
                if item.method != "ours" or plan is None:
                    return dict(model=item.model, qid=item.task_id, status="not_applicable")
                started = time.monotonic()
                try:
                    result = methods.execute_plan(
                        plan,
                        schema,
                        executor(item, "full_detail_control").execute,
                        strategy="full_detail",
                    )
                except (
                    methods.UnsupportedPlan,
                    methods.IncompleteEvidence,
                    methods.ServiceExecutionError,
                    ValueError,
                    TypeError,
                    KeyError,
                ) as exc:
                    result = dict(status="EXECUTION_ERROR", rows=None, error=type(exc).__name__)
                return dict(
                    model=item.model,
                    qid=item.task_id,
                    status="shared_plan_control",
                    full_detail=result,
                    wall_seconds=time.monotonic() - started,
                    simple_correct=result,
                    simple_correct_equals_full_detail=True,
                    independent_model_method=False,
                )

            with ThreadPoolExecutor(max_workers=spec.db_cap) as workers:
                loop = asyncio.get_running_loop()
                controls = await asyncio.gather(
                    *[loop.run_in_executor(workers, control, p) for p in scheduler.paths]
                )
            write_new(spec.run_dir / "shared_controls.json", canonical(controls))
            (
                graph_scores,
                control_scores,
            ) = await asyncio.get_running_loop().run_in_executor(
                consumer_executor,
                score_graph,
                scheduler.paths,
                controls,
                data,
                graph,
                scoring,
                compare,
            )
            failures = {
                p["task_id"] for p in scheduler.paths if p["final"].get("status") == "case_error"
            }
            failures.update(
                r["qid"] for r in graph_scores + control_scores if r.get("scoring_error")
            )
            case_failures += len(failures)
            for row in graph_scores + control_scores:
                if row.get("scoring_error"):
                    scheduler.journal.append(
                        "scoring_error", qid=row["qid"], **row["scoring_error"]
                    )
            write_new(spec.run_dir / "shared_control_scores.json", canonical(control_scores))
            scores.extend(graph_scores)
            write_new(spec.run_dir / "scores.json", canonical(graph_scores))
            scheduler.journal.append("graph_scored", graph=graph, scored_views=len(graph_scores))
            finalize_campaign(spec.run_dir, summary)
        finally:
            await asyncio.get_running_loop().run_in_executor(consumer_executor, client.close)
            stderr.close()
    write_new(directory / "scores.json", canonical(scores))
    return dict(
        stop_reason=None,
        budget=local_budget(ledger, directory),
        graphs=summaries,
        scored_views=len(scores),
        denominators_by_model=dict(Counter(i.model for i in items)),
        undispatched_by_reason=dict(undispatched),
        case_failures=case_failures,
        result_kind="new_model_rerun_not_replacement_of_historical_results",
    )
