from __future__ import annotations

from collections import Counter, defaultdict
import json
from pathlib import Path
import random

from .config import canonical, digest
from .journal import write_new


def summarize(scores, groups):
    expected = {r["qid"]: r for r in groups}
    if len(expected) != len(groups) or not expected:
        raise ValueError("Unique frozen evaluator population required")
    models = sorted({r["model"] for r in scores})
    table = []
    indices = {}
    for model in models:
        for method, view, label in [
            ("direct", "initial", "Direct"),
            ("direct", "final", "Direct+feedback"),
            ("ours", "final", "Ours"),
        ]:
            selected = [
                r
                for r in scores
                if r["model"] == model and r["method"] == method and r["view"] == view
            ]
            rows = {r["qid"]: r for r in selected}
            if len(rows) != len(selected) or rows.keys() != expected.keys():
                raise ValueError(
                    "Incomplete/duplicate model-method population; do not shrink denominator"
                )
            if any(r["graph"] != expected[q]["graph"] for q, r in rows.items()):
                raise ValueError("Graph identity differs")
            n = len(rows)
            correct = sum(r["score"]["ex"] for r in rows.values())
            graphs = {
                g: [r for r in rows.values() if r["graph"] == g]
                for g in {r["graph"] for r in groups}
            }
            per_graph = {g: sum(r["score"]["ex"] for r in v) / len(v) for g, v in graphs.items()}
            table.append(
                dict(
                    model=model,
                    method=label,
                    N=n,
                    correct=correct,
                    EX=correct / n,
                    EX_macro=sum(per_graph.values()) / len(per_graph),
                    per_graph=per_graph,
                    returned_answer=sum(r["score"]["returned_answer"] for r in rows.values()),
                    failure_status=dict(
                        Counter(r["status"] for r in rows.values() if r["status"] != "EXECUTED")
                    ),
                )
            )
            indices[model, label] = rows
    return table, indices


def paired_intervals(indices, groups, *, replicates=10000, seed=20261024):
    families = defaultdict(lambda: defaultdict(list))
    for row in groups:
        families[row["graph"]][row["family"]].append(row["qid"])
    n = len(groups)
    results = []
    for model in sorted({model for model, _ in indices}):
        ours, baseline = indices[model, "Ours"], indices[model, "Direct+feedback"]
        difference = {q: ours[q]["score"]["ex"] - baseline[q]["score"]["ex"] for q in ours}
        clusters = {
            g: [(sum(difference[q] for q in qs), len(qs)) for qs in values.values()]
            for g, values in families.items()
        }
        weights = {g: sum(size for _, size in cs) / n for g, cs in clusters.items()}
        rng = random.Random(seed)
        draws = []
        for _ in range(replicates):
            total = 0
            for g, cs in clusters.items():
                sample = rng.choices(cs, k=len(cs))
                total += (
                    weights[g] * sum(value for value, _ in sample) / sum(size for _, size in sample)
                )
            draws.append(total)
        draws.sort()

        def quantile(p):
            index = (len(draws) - 1) * p
            lo = int(index)
            hi = min(lo + 1, len(draws) - 1)
            return draws[lo] + (draws[hi] - draws[lo]) * (index - lo)

        results.append(
            dict(
                model=model,
                contrast="Ours - Direct+feedback",
                N=n,
                difference_percentage_points=100 * sum(difference.values()) / n,
                CI95_percentage_points=[100 * quantile(0.025), 100 * quantile(0.975)],
                replicates=replicates,
                seed=seed,
                method="within-graph paired family bootstrap; fixed graph weights",
                population="observed tasks/families in these graphs; not repeated-generation uncertainty",
            )
        )
    return results


def recovery_audit(root: Path, stages, inherited_unknown):
    physical = {}
    for stage in stages:
        if stage.get("blocked_reason"):
            continue
        for path in (root / "runs" / stage["name"]).glob("*/model_events.json"):
            for event in json.loads(path.read_text(encoding="utf-8")):
                if event.get("charged") is True:
                    physical[event["request_id"]] = event
    latest = {}
    unknown_keys = set()
    for event in sorted(physical.values(), key=lambda e: e["budget_slot"]):
        latest[event["logical_key"]] = event
        if event["status"] == "unknown":
            unknown_keys.add(event["logical_key"])
    unresolved = sum(e["status"] in ("unknown", "infra", "throttled") for e in latest.values())
    unknown_attempts = sum(e["status"] == "unknown" for e in physical.values())
    return dict(
        unique_physical_attempts=len(physical),
        unknown_physical_attempts=unknown_attempts,
        unresolved_transport_paths=unresolved,
        recovered_unknown_paths=sum(
            latest[k]["status"] in ("ok", "length", "empty", "refused") for k in unknown_keys
        ),
        inherited_history_accounted=unknown_attempts >= inherited_unknown,
    )


def finalize_pipeline(root: Path, stages, settings):
    groups_path = Path(settings["groups_path"])
    if digest(groups_path.read_bytes()) != settings["groups_sha256"]:
        raise ValueError("Evaluator groups changed")
    groups = [json.loads(line) for line in groups_path.read_text(encoding="utf-8").splitlines()]
    scores = []
    for stage in stages:
        if stage.get("blocked_reason"):
            continue
        scores.extend(
            json.loads((root / "runs" / stage["name"] / "scores.json").read_text(encoding="utf-8"))
        )
    table, indices = summarize(scores, groups)
    intervals = paired_intervals(indices, groups)
    controls = []
    for stage in stages:
        if stage.get("blocked_reason") or stage.get("method") != "ours":
            continue
        for path in (root / "runs" / stage["name"]).glob("*/shared_control_scores.json"):
            controls.extend(json.loads(path.read_text(encoding="utf-8")))
    for model in sorted({m for m, _ in indices}):
        measured = {r["qid"]: r["full_detail_score"] for r in controls if r["model"] == model}
        fallback = indices[model, "Ours"]
        values = {q: measured.get(q, r["score"]) for q, r in fallback.items()}
        per_graph = {
            g: sum(values[r["qid"]]["ex"] for r in groups if r["graph"] == g)
            / sum(r["graph"] == g for r in groups)
            for g in {r["graph"] for r in groups}
        }
        for label in [
            "Fixed-plan full detail",
            "Simple correct (same fixed-plan result)",
        ]:
            table.append(
                dict(
                    model=model,
                    method=label,
                    N=len(groups),
                    correct=sum(v["ex"] for v in values.values()),
                    EX=sum(v["ex"] for v in values.values()) / len(groups),
                    EX_macro=sum(per_graph.values()) / len(per_graph),
                    per_graph=per_graph,
                    returned_answer=sum(v["returned_answer"] for v in values.values()),
                    shared_planner=True,
                    independent_model_method=False,
                    measured_plan_replays=len(measured),
                    no_plan_retained_in_denominator=len(groups) - len(measured),
                )
            )
    audit = recovery_audit(root, stages, settings.get("inherited_unknown_attempts", 0))
    clean = not audit["unresolved_transport_paths"] and audit["inherited_history_accounted"]
    result = dict(
        primary_comparison_clean=clean,
        transport_recovery=audit,
        inherited_unknown_attempts=settings.get("inherited_unknown_attempts", 0),
        primary_claim_blocker=(
            "Transport unknowns remain; no method-superiority claim" if not clean else None
        ),
        table=table,
        primary_contrasts=intervals,
        other_T1_cells_complete=False,
        incomplete_blocks=[s for s in stages if s.get("blocked_reason")],
    )
    write_new(root / "main_table_results.json", canonical(result))
    lines = [
        "# 新版 CypherBench 结果块",
        "",
        "| Model | Method | N | EX | 七图宏平均 | 返回完整答案 |",
        "|---|---|---:|---:|---:|---:|",
    ]
    lines.extend(
        "| {model} | {method} | {N} | {EX:.4f} | {EX_macro:.4f} | {returned_answer} |".format(**r)
        for r in table
    )
    lines += [
        "",
        "不同model不合并。共享计划控制不是独立模型方法。其他必要T1单元格仍未完成。",
    ]
    if not clean:
        lines.append("仍有未恢复或未核清的传输结果；本次对照不能支撑方法优势。")
    lines.append("原失败与重试全部计费并保留。恢复运行单列版本，不替换旧结果。")
    write_new(root / "main_table_results.md", "\n".join(lines).encode("utf-8"))
    return result
