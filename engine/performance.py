"""Derived client-side timing; never infer backend capacity from a configured cap."""

from collections import Counter
import json
from pathlib import Path

from .campaign import distribution
from .journal import Journal, write_new
from .config import canonical


def concurrency(intervals):
    if not intervals:
        return dict(window_seconds=None, peak=0, mean=None)
    events = [(s, 1) for s, e in intervals] + [(e, -1) for s, e in intervals]
    active = peak = 0
    for stamp, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    span = max(e for s, e in intervals) - min(s for s, e in intervals)
    return dict(
        window_seconds=span,
        peak=peak,
        mean=sum(e - s for s, e in intervals) / span if span > 0 else None,
    )


def report_pipeline(root, stages, ledger, config):
    root = Path(root)
    with ledger.lock:
        records = list(ledger.records.values())
    reports = []
    for stage in stages:
        if stage.get("blocked_reason"):
            continue
        directory = root / "runs" / stage["name"]
        generation = directory / "_generation"
        if not (generation / "run_meta.json").exists():
            continue
        started = json.loads((generation / "run_meta.json").read_text(encoding="utf-8"))[
            "started_epoch"
        ]
        send_times = {}
        final_times, score_times = {}, {}
        for path in directory.glob("*/journal.jsonl"):
            for event in Journal(path).read():
                if event["kind"] == "send_started":
                    send_times[event["request_id"]] = event["t"]
                if path.parent.name != "_generation":
                    if event["kind"] == "path_final_saved":
                        final_times[event["logical_key"]] = event["t"]
                    elif event["kind"] == "graph_scored":
                        score_times[event["graph"]] = event["t"]
        terminals = []
        for row in records:
            if row["request_id"] not in send_times:
                continue
            terminal = Path(row["request_path"]).with_name("terminal.json")
            if terminal.is_file():
                terminals.append(json.loads(terminal.read_text(encoding="utf-8")))
        intervals = [(send_times[t["request_id"]], t["ended_epoch"]) for t in terminals]
        occupancy = concurrency(intervals)
        span = occupancy["window_seconds"]
        paths = [
            json.loads(p.read_text(encoding="utf-8"))
            for folder in directory.glob("*/paths")
            if folder.parent.name != "_generation"
            for p in folder.rglob("*.json")
        ]
        meta = json.loads((generation / "summary.json").read_text(encoding="utf-8"))
        initial_count = meta["counts"]["planned"]
        reports.append(
            dict(
                stage=stage["name"],
                method=stage["method"],
                benchmark=stage["benchmark"],
                planned_cases=initial_count,
                final_cases=len(paths),
                configured_global_cap=config["global_cap"],
                initial_demand_cap=min(config["global_cap"], initial_count),
                new_physical_calls=len(terminals),
                reused_initial_replies=meta["counts"]["reused_model_attempts"],
                physical_statuses=dict(Counter(t["status"] for t in terminals)),
                request_seconds=distribution(e - s for s, e in intervals),
                actual_HTTP=occupancy,
                attempt_qps=len(terminals) / span if span else None,
                normal_reply_qps=(
                    sum(t["status"] == "ok" for t in terminals) / span if span else None
                ),
                answer_completion_seconds=distribution(
                    (
                        final_times.get(p["logical_key"], started) - started
                        if p["logical_key"] in final_times
                        else None
                    )
                    for p in paths
                ),
                scored_completion_seconds_including_control_replay=distribution(
                    score_times[p["graph"]] - started if p["graph"] in score_times else None
                    for p in paths
                ),
                theoretical_backend_max_QPS=None,
                notes="Client timing only; short finite batches have a natural tail. Cached replies are not new traffic. Scored completion includes diagnostic control replay.",
            )
        )
    result = dict(
        cells=reports,
        all_physical_attempts_included=True,
        case_clock="cell initial generation started to final answer saved / graph score saved",
        monetary_cost=None,
        backend_capacity_measured=False,
    )
    write_new(root / "performance.json", canonical(result))
    return result
