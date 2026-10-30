"""Summarize observed offline intervals, keeping unfinished work explicit."""

from collections import defaultdict


def distribution(values):
    values = sorted(values)
    if not values:
        return dict(n=0, mean=None, p50=None, p90=None)

    def percentile(fraction):
        index = (len(values) - 1) * fraction
        lower = int(index)
        upper = min(lower + 1, len(values) - 1)
        return values[lower] + (values[upper] - values[lower]) * (index - lower)

    return dict(n=len(values), mean=sum(values) / len(values),
                p50=percentile(0.5), p90=percentile(0.9))


def intervals(events, start_kind, end_kind, identity):
    active, completed = {}, []
    for event in events:
        if event["kind"] == start_kind:
            key = event[identity]
            if key in active:
                raise ValueError("Duplicate active interval: " + key)
            active[key] = event["t"]
        elif event["kind"] == end_kind:
            key = event[identity]
            if key not in active:
                raise ValueError("Interval has no start: " + key)
            started = active.pop(key)
            if event["t"] < started:
                raise ValueError("Clock moved backwards; interval cannot be measured")
            completed.append((started, event["t"]))
    return completed, active


def utilization(completed, unfinished, start, end):
    points = []
    for left, right in completed + [(t, end) for t in unfinished.values()]:
        points.extend(((left, 1), (right, -1)))
    active = peak = 0
    for _, change in sorted(points):
        active += change
        peak = max(peak, active)
    duration = end - start
    area = sum(right - left for left, right in completed)
    area += sum(end - left for left in unfinished.values())
    return dict(peak=peak, mean=area / duration if duration else None,
                completed_intervals=len(completed), unfinished=list(unfinished))


def summarize(events, results):
    """Runtime-wide occupancy differs from query-only or model concurrency."""
    if not events:
        return dict(status="no_runtime_events", model_calls=0)
    start, end = min(e["t"] for e in events), max(e["t"] for e in events)
    cases, unfinished_cases = intervals(events, "position_started", "position_finished", "work_id")
    graphs, unfinished_graphs = intervals(events, "graph_ready", "graph_closed", "instance")
    cells = defaultdict(list)
    for row in results:
        cells[row["cell_id"]].append(row)
    return dict(
        runtime_observed_seconds=end - start,
        position_workers=utilization(cases, unfinished_cases, start, end),
        ready_graphs=utilization(graphs, unfinished_graphs, start, end),
        graph_preparation_seconds=distribution([e["seconds"] for e in events if e["kind"] == "graph_ready"]),
        admitted_slot_wait_seconds=distribution([e["wait_seconds"] for e in events if e["kind"] == "position_started"]),
        cells={cell: dict(planned=len(rows), timed=sum("seconds" in r for r in rows),
                         case_seconds=distribution([r["seconds"] for r in rows if "seconds" in r]))
               for cell, rows in sorted(cells.items())},
        model_calls=0,
        query_only_concurrency=None,
        note="Position intervals include method reads, controls, persistence and scoring. They are not HTTP or query-only concurrency. Partial timing keeps its own n.",
    )
