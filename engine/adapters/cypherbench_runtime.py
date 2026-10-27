from __future__ import annotations

import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time

from .cypherbench import GRAPHS, ExecutedAnswerAdapter, feedback_item
from ..campaign import execute_campaign
from ..config import canonical, digest
from ..execution import JsonRpcClient, TracedRpcExecutor
from ..ids import component
from ..journal import Journal, write_new
from ..replay import frozen_modules
from ..scheduler import Scheduler
from ..transport import HTTPTransport


def score_paths(paths, references, scoring, compare):
    """Evaluator-only reference access, after final answers have been committed."""
    scores = []
    for path in paths:
        reference = references[path["task_id"]]
        for label, outcome in [("initial", path["views"][0]["outcome"]), ("final", path["final"])]:
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
                qid=path["task_id"],
                compare=compare,
            )
            scores.append(
                dict(
                    model=path["model"],
                    method=path["method"],
                    qid=path["task_id"],
                    graph=path["graph"],
                    view=label,
                    status=status,
                    score=score,
                )
            )
    return scores


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
):
    """One method/benchmark block; lifecycle-serial graphs, rolling model requests.

    The worker command must start our owned runtime and prove READY identity. No
    legacy daemon or benchmark runner is implicitly resumed. Other benchmarks
    need their own admitted adapter; they cannot be cast into CypherBench.
    """
    settings = stage["adapter"]
    _, methods, scoring = frozen_modules(
        Path(settings["code_root"]), Path(settings["legacy_freeze"])
    )
    data = Path(settings["data"])
    compare = scoring.load_official_comparator(Path(settings["official_source"]))
    specs = make_spec(stage)
    directory = specs.run_dir
    directory.mkdir(parents=True, exist_ok=False)
    scores, summaries = [], []
    if any(i.graph not in GRAPHS for i in items):
        raise ValueError("Formal CypherBench requires the seven declared test graphs")
    for graph in GRAPHS:
        selected = [i for i in items if i.graph == graph]
        if not selected:
            raise ValueError("Missing graph in fixed formal denominator")
        worker = settings["workers"][graph]
        if digest(canonical(worker)) != settings["worker_sha256"][graph]:
            raise ValueError("Changed graph runtime contract")
        schema = json.loads(
            (data / "upstream/graphs/schemas" / (graph + "_schema.json")).read_text(
                encoding="utf-8"
            )
        )
        stderr = (directory / (graph + "_worker_stderr.log")).open("x", encoding="utf-8")
        client = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: client_factory(
                worker["command"], expected_ready=worker["expected_ready"], stderr=stderr
            ),
        )
        from dataclasses import replace

        spec = replace(specs, name=specs.name + "-" + graph, run_dir=directory / graph)

        def executor(item, kind=None):
            return TracedRpcExecutor(
                client,
                spec.run_dir
                / "db_traces"
                / component(item.model)
                / component(item.task_id)
                / (kind or item.phase),
                graph_sha256=worker["expected_ready"]["graph_sha256"],
                worker_version="engine-neo4j-v1",
            )

        adapter = ExecutedAnswerAdapter(
            {graph: schema}, methods.interpret, executor, scoring.score_executed_answer
        )
        try:
            scheduler = Scheduler(
                spec,
                pool,
                ledger,
                Journal(spec.run_dir / "journal.jsonl"),
                (
                    shared_transport
                    if shared_transport is not None
                    else transport_factory(Path(config["key_dir"]))
                ),
                close_transport=shared_transport is None,
            )
            summary = await execute_campaign(
                scheduler,
                selected,
                registry_path=Path(config["registry_path"]),
                execute=adapter.execute,
                feedback=feedback_item,
                metadata=dict(
                    purpose="formal_T1",
                    worker_READY=client.ready_value,
                    benchmark="CypherBench",
                    method=stage["method"],
                ),
            )
            summaries.append(summary)
            if (
                summary["stop_reason"]
                or summary["budget"]["pending"]
                or summary["undispatched_by_reason"]
            ):
                return dict(
                    stop_reason=summary["stop_reason"]
                    or ("undispatched_paths" if summary["undispatched_by_reason"] else "pending"),
                    budget=ledger.summary(),
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
            references = {
                r["qid"]: r
                for r in (
                    json.loads(line)
                    for line in (data / "evaluator/test.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                )
            }
            graph_scores = score_paths(scheduler.paths, references, scoring, compare)
            control_scores = []
            for control in controls:
                if control["status"] != "shared_plan_control":
                    continue
                reference = references[control["qid"]]
                outcome = control["full_detail"]
                score = scoring.score_executed_answer(
                    status=outcome["status"],
                    predicted_rows=outcome.get("rows"),
                    reference_rows=scoring.read_reference_answer(reference["answer_json"]),
                    order_matters="order by" in reference["gold_cypher"].lower(),
                    qid=control["qid"],
                    compare=compare,
                )
                control_scores.append(
                    dict(
                        qid=control["qid"],
                        model=control["model"],
                        graph=graph,
                        full_detail_score=score,
                        simple_correct_score=score,
                        independent_model_method=False,
                    )
                )
            write_new(spec.run_dir / "shared_control_scores.json", canonical(control_scores))
            scores.extend(graph_scores)
            write_new(spec.run_dir / "scores.json", canonical(graph_scores))
        finally:
            await asyncio.get_running_loop().run_in_executor(None, client.close)
            stderr.close()
    write_new(directory / "scores.json", canonical(scores))
    return dict(
        stop_reason=None,
        budget=ledger.summary(),
        graphs=summaries,
        scored_views=len(scores),
        denominators_by_model=dict(Counter(i.model for i in items)),
        result_kind="new_model_rerun_not_replacement_of_historical_results",
    )
