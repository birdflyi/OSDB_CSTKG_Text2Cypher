"""Read-only dashboard projection. No keys, requests, database calls or dispatch."""

from collections import Counter
import json
from pathlib import Path
import time

from .ids import component, logical_key
from .campaign import distribution


TERMINAL = {"完成", "存在未完成项", "失败"}
STATES = {"completed": "已完成", "incomplete": "未完成", "skipped": "未运行"}


def estimate_remaining(completions, remaining, now):
    if remaining == 0:
        return 0
    recent = sorted(t for t in completions if now - 300 <= t <= now)
    if len(recent) < 3 or recent[-1] <= recent[0]:
        return None
    return remaining * (recent[-1] - recent[0]) / (len(recent) - 1)


class SnapshotReader:
    """Cache unchanged files so refreshes do not reload immutable result bodies."""

    def __init__(self):
        self.cache = {}

    def read(self, path, lines=False):
        path = Path(path)
        try:
            stamp = (path.stat().st_mtime_ns, path.stat().st_size)
            if path not in self.cache or self.cache[path][0] != stamp:
                text = path.read_text(encoding="utf-8-sig")
                if lines:
                    value = []
                    for line in text.splitlines():
                        try:
                            value.append(json.loads(line))
                        except ValueError:
                            continue  # A writer may still be appending its last line.
                else:
                    value = json.loads(text)
                self.cache[path] = (stamp, value)
            return self.cache[path][1]
        except (OSError, ValueError):
            return [] if lines else {}

    def cell(self, cell, root, configs, now):
        config = self.read(cell.get("pipeline_config", ""))
        configs.append(config)
        receipt_root = Path(cell.get("receipt_root", config.get("run_dir", root)))
        block = receipt_root / "blocks" / cell["name"]
        receipt = next(
            (
                r
                for r in (
                    self.read(block / (n + ".json")) for n in ("completed", "incomplete", "skipped")
                )
                if r
            ),
            {},
        )
        stage = next((s for s in config.get("stages", []) if s["name"] == cell["name"]), {})
        directory = (
            Path(cell["progress_path"]).parent
            if cell.get("progress_path")
            else receipt_root / "runs" / cell["name"]
        )
        manifest = self.read(stage.get("manifest_path", "")) or []
        models = self.read(config.get("admission_path", "")).get("models", [])
        cases, lookup = [], {}
        for number, row in enumerate(manifest, 1):
            model = row.get("model") or (models[0] if len(models) == 1 else "")
            if not model:
                model = self.read(row["wire_path"]).get("model", "")
            case = dict(
                number=number,
                task_id=row["task_id"],
                graph=row.get("graph", ""),
                state="等待发送",
                started=None,
                stage_started=None,
                model_done=False,
                final_done=False,
                scored=False,
                correct=None,
                attempts=0,
                endpoint=None,
                cached=False,
                model=model,
            )
            cases.append(case)
            for phase in ("initial", "feedback"):
                lookup[
                    component(model)
                    + "__"
                    + logical_key(row["benchmark"], row["task_id"], row["method"], phase)
                ] = case
        by_id = {c["task_id"]: c for c in cases}
        events = []
        samples = []
        central = self.read(receipt_root / "endpoint_progress.json")
        if central:
            samples.append(central)
        directories = [directory] + [Path(p) for p in cell.get("additional_runs", [])]
        for path in (p for run in directories for p in run.glob("*/progress.json")):
            value = self.read(path)
            if value:
                samples.append(value)
        for path in (p for run in directories for p in run.glob("*/journal.jsonl")):
            events.extend(
                dict(e, generation=path.parent.name == "_generation") for e in self.read(path, True)
            )
        for event in sorted(events, key=lambda e: e.get("t", 0)):
            case = lookup.get(event.get("logical_key"))
            if not case:
                continue
            kind, stamp = event["kind"], event.get("t")
            case["started"] = case["started"] or stamp
            if kind == "case_stage":
                case.update(state=event["stage"], stage_started=stamp)
                if event["stage"] == "复用模型回复":
                    case.update(cached=True, model_done=True)
            elif kind == "send_started":
                case.update(state="模型处理中", stage_started=stamp, endpoint=event.get("endpoint"))
                case["attempts"] += 1
            elif kind == "attempt_terminal":
                case.update(state="模型已终结", stage_started=stamp, model_done=True)
            elif kind == "path_final_saved":
                case.update(
                    state="等待数据库" if event["generation"] else "等待评分",
                    stage_started=stamp,
                    model_done=True,
                    final_done=not event["generation"] or case["final_done"],
                )
        for path in (p for run in directories for p in run.glob("*/scores.json")):
            stamp = next(
                (
                    e["t"]
                    for e in reversed(events)
                    if e["kind"] == "graph_scored" and e.get("graph") == path.parent.name
                ),
                None,
            )
            for score in self.read(path) or []:
                if score.get("view") != "final":
                    continue
                case = by_id.get(score["qid"])
                if case:
                    case.update(
                        state="已完成",
                        scored=True,
                        final_done=True,
                        scored_at=stamp,
                        correct=(score.get("score") or {}).get("ex"),
                    )
        generated = self.read(directory / "_generation" / "summary.json").get("counts", {})
        if cases and generated.get("reused_model_attempts") == len(cases):
            for case in cases:
                case["cached"] = True
        source_progress = self.read(receipt_root / "progress.json")
        missing = cell.get("missing", [])
        state = STATES.get(receipt.get("status")) or ("缺适配或资源" if missing else "等待启动")
        if (
            not receipt
            and source_progress.get("phase") == "运行中"
            and cases
            and ((block / "started.json").exists() or any(c["started"] for c in cases))
        ):
            state = "处理中" if (block / "started.json").exists() else "模型派发或等待数据库"
        started = self.read(block / "started.json").get("started_epoch") or self.read(
            directory / "_generation" / "run_meta.json"
        ).get("started_epoch")
        count = len(cases) or cell.get("planned_paths")
        completed = sum(c["scored"] for c in cases)
        recovered = bool(cell.get("additional_runs"))
        if recovered and count and completed == count:
            state = "已完成"
        eta = estimate_remaining(
            [c["scored_at"] for c in cases if c.get("scored_at")], (count or 0) - completed, now
        )
        return dict(
            name=cell["name"],
            method=cell["method"],
            benchmark=cell["benchmark"],
            state=state,
            role=cell.get("role", "independent"),
            recovered=recovered,
            missing="; ".join(missing),
            planned=count,
            completed=completed,
            model_completed=sum(c["model_done"] for c in cases),
            final_completed=sum(c["final_done"] for c in cases),
            cases=cases,
            eta_seconds=eta if state not in ("未完成", "缺适配或资源", "未运行") else None,
            elapsed_seconds=(
                now - started
                if started and not receipt
                else (receipt.get("ended_epoch", now) - started if started else None)
            ),
            smoke=receipt.get("result", {}).get("smoke_passed", False),
            source_phase=source_progress.get("phase"),
            samples=samples,
            events=[dict(e, cell=cell["name"]) for e in events],
        )

    def snapshot(self, plan_path, root, now=None):
        now = time.time() if now is None else now
        plan = self.read(plan_path)
        configs = []
        cells = [
            self.cell(c, root, configs, now)
            for c in plan.get("cells", [])
            if not c.get("shared_with")
        ]
        by_name = {c["name"]: c for c in cells}
        for c in plan.get("cells", []):
            if c.get("shared_with"):
                source = by_name.get(c["shared_with"], {})
                cells.append(
                    dict(
                        source,
                        name=c["name"],
                        method=c["method"],
                        benchmark=c["benchmark"],
                        role="共享首轮结果",
                    )
                )
        order = {c["name"]: n for n, c in enumerate(plan.get("cells", []))}
        cells.sort(key=lambda c: order[c["name"]])
        latest = max(
            (p for c in cells for p in c.get("samples", [])),
            key=lambda p: p.get("updated_epoch", 0),
            default={},
        )
        active = any(c.get("source_phase") == "运行中" for c in cells)
        age = now - latest.get("updated_epoch", now)
        fresh = age <= plan.get("dashboard", {}).get("stale_seconds", 15)
        network = latest.get("network_inflight") if active and fresh else (None if active else 0)
        registry = next(
            (self.read(c["registry_path"]) for c in configs if c.get("registry_path")), {}
        )
        pool_rows = {e["name"]: e for e in latest.get("endpoints", [])}
        endpoints = []
        events = sorted(
            (e for c in cells if c.get("role") != "共享首轮结果" for e in c.get("events", [])),
            key=lambda e: e.get("t", 0),
        )
        for e in registry.get("endpoints", []):
            pool = pool_rows.get(e["name"], {})
            actual = latest.get("network_by_endpoint", {}).get(e["name"])
            recent = [
                v
                for v in events
                if v.get("endpoint") == e["name"] and now - 60 <= v.get("t", 0) <= now
            ]
            attempts = [
                v
                for v in events
                if v["kind"] == "attempt_terminal"
                and v.get("endpoint") == e["name"]
                and now - 300 <= v.get("t", 0) <= now
            ]
            endpoints.append(
                dict(
                    name=e["name"],
                    model=e["model"],
                    url=e["url"],
                    enabled=e["enabled"],
                    cap=e["cap"],
                    effective_cap=pool.get("effective_cap", e["cap"]),
                    inflight=actual if active and fresh else (None if active else 0),
                    permits=pool.get("inflight"),
                    sent_60s=sum(v["kind"] == "send_started" for v in recent),
                    completed_60s=sum(v["kind"] == "attempt_terminal" for v in recent),
                    latency=distribution(v.get("elapsed_seconds") for v in attempts),
                )
            )
        progress = self.read(Path(root) / "progress.json")
        if not progress:
            progress = dict(phase="运行中" if active else "无活动实验", current_block="")
        for c in cells:
            c.pop("samples", None)
            c.pop("events", None)
        return dict(
            mode=plan.get("mode"),
            progress=progress,
            cells=cells,
            endpoints=endpoints,
            generated_epoch=now,
            source_age_seconds=age,
            stale=active and not fresh,
            network_inflight=network,
            global_cap=latest.get(
                "global_cap",
                next((c.get("global_cap") for c in configs if c.get("global_cap")), None),
            ),
            recent_events=events[-8:],
            counts=dict(Counter(c["state"] for c in cells)),
        )
