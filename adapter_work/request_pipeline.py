"""Request-only matrix entry point. Uses the existing scheduler and ledger.

No database imports or reference loading. A fresh, frozen run can explicitly reuse
a reconciled response manifest; an unexamined crashed run is never resumed silently.
"""

import argparse
import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import sys

WORK = Path(__file__).resolve().parent
sys.path.insert(0, str(WORK))
sys.path.insert(0, str(WORK.parent))

from common import loader
from common.dispatch import generate_path, request_item
from engine.campaign import (
    code_manifest,
    execute_campaign,
    finalize_campaign,
    validate_paths,
)
from engine.config import (
    CampaignSpec,
    Endpoint,
    canonical,
    digest,
    registry_from_file,
    retry_policy,
    timeouts_from,
)
from engine.errors import IntegrityError
from engine.fake import FakeTransport, MemoryLedger
from engine.journal import Journal, write_new
from engine.ledger import Ledger, scan_legacy
from engine.pool import DynamicPool, serves
from engine.recovery import saved_replies
from engine.scheduler import Scheduler
from engine.transport import HTTPTransport


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def source_manifest():
    return dict(
        engine=code_manifest(),
        adapters={
            p.relative_to(WORK).as_posix(): digest(p.read_bytes())
            for p in sorted(WORK.rglob("*.py"))
            if "tests" not in p.relative_to(WORK).parts
        },
        profiles={
            p.name: digest(p.read_bytes())
            for p in sorted((WORK / "prompts").glob("*.json"))
        },
    )


def verify(receipt):
    path = Path(receipt["path"])
    if digest(path.read_bytes()) != receipt["sha256"]:
        raise IntegrityError("Changed preparation artifact: " + str(path))
    return path


def method_binding(bindings, method, benchmark, graph):
    base = bindings.get(method)
    if not base:
        raise ValueError("Missing explicit method binding: " + method)
    if method == "cyquark":
        key = benchmark + "/" + graph
        if key not in base.get("graphs", {}):
            raise ValueError(
                "CYQUARK needs a graph-specific checkpoint binding: " + key
            )
        return base["graphs"][key]
    return base


@dataclass(frozen=True)
class Job:
    cell_id: str
    position_id: str
    adapter: object
    spec: object
    item: object
    sampling: dict


def load_feedback_jobs(inventory, bindings, queue_pin):
    """Reconstruct one feedback stage from pinned public input and DB failure."""
    from common.stages import advance
    from feedback_queue import operational_error

    cells = {c["cell_id"]: c for c in inventory["cells"]}
    jobs, positions = [], []
    for pin in read(verify(queue_pin)):
        entry = read(verify(pin))
        row = entry["position"]
        cell = cells[row["cell_id"]]
        if cell["method"] != "direct-feedback":
            raise IntegrityError("Feedback queue contains another method")
        adapter = loader.load(verify(cell["adapter"]))
        specs = adapter.prepare(str(verify(cell["positions_source"])))
        spec = next(s for s in specs if s.position_id == row["position_id"])
        if (spec.task_id, spec.graph, spec.benchmark) != (
            row["task_id"],
            row["graph"],
            row["benchmark"],
        ):
            raise IntegrityError("Feedback position identity changed")
        initial = read(verify(entry["source"]))
        if (
            initial["method"] != "direct"
            or initial["position_id"] != row["position_id"]
            or initial["benchmark"] != row["benchmark"]
        ):
            raise IntegrityError("Feedback does not reference its Direct source")
        error = operational_error(read(verify(entry["database_final"])))
        if error is None:
            raise IntegrityError("Feedback requires a known operational rejection")
        step = advance(
            adapter, spec, initial["final"]["raw_prediction"], db_error=error
        )
        if step.action != "request":
            raise IntegrityError(
                "Feedback protocol did not produce its one allowed stage"
            )
        binding = method_binding(bindings, "direct", row["benchmark"], row["graph"])
        item = request_item(
            step.request, model=binding["model"], sampling=binding.get("sampling", {})
        )
        jobs.append(
            Job(
                cell["cell_id"],
                row["position_id"],
                adapter,
                step.request,
                item,
                binding.get("sampling", {}),
            )
        )
        positions.append(
            dict(
                cell_id=cell["cell_id"],
                position_id=row["position_id"],
                method="direct-feedback",
                benchmark=row["benchmark"],
                resource_ready=True,
                logical_key=item.key,
            )
        )
    if len({j.item.key for j in jobs}) != len(jobs):
        raise IntegrityError("Duplicate feedback stage")
    return jobs, positions


def load_jobs(inventory, bindings, *, simulated=False):
    jobs, positions = [], []
    for cell in inventory["cells"]:
        verify(cell["positions_source"])
        adapter = loader.load(verify(cell["adapter"]))
        for pos in cell["positions"]:
            verify(pos["public_input"])
        if not any("request_template" in p for p in cell["positions"]):
            positions.extend(
                dict(
                    cell_id=cell["cell_id"],
                    method=cell["method"],
                    benchmark=cell["benchmark"],
                    position_id=p["position_id"],
                    source=p["source"],
                )
                for p in cell["positions"]
            )
            continue
        specs = adapter.prepare(cell["positions_source"]["path"])
        if len(specs) != len(cell["positions"]):
            raise IntegrityError("Prepared position count changed")
        for pos, spec in zip(cell["positions"], specs):
            binding = method_binding(
                bindings, cell["method"], cell["benchmark"], pos["graph"]
            )
            ready = simulated or binding.get("ready") is True
            if (
                ready
                and not simulated
                and cell["method"] in ("cypher-ri", "cyphersmith", "cyquark")
            ):
                asset = read(verify(binding["asset_manifest"]))
                if (
                    asset.get("method") != cell["method"]
                    or asset.get("model") != binding["model"]
                ):
                    raise IntegrityError(
                        "Author asset identity differs from method binding"
                    )
                if not asset.get("checkpoint_sha256") or not asset.get(
                    "tokenizer_sha256"
                ):
                    raise IntegrityError(
                        "Author service requires checkpoint and tokenizer evidence"
                    )
                if cell["method"] == "cyquark" and (
                    asset.get("benchmark"),
                    asset.get("graph"),
                ) != (cell["benchmark"], pos["graph"]):
                    raise IntegrityError(
                        "CYQUARK checkpoint belongs to another target graph"
                    )
            template = read(verify(pos["request_template"]))
            value = spec if isinstance(spec, dict) else spec.as_dict()
            body = value.get("body") or {"messages": list(value["messages"])}
            if (
                canonical(body) != canonical(template["body"])
                or value["task_id"] != pos["task_id"]
                or value["position_id"] != pos["position_id"]
            ):
                raise IntegrityError("Adapter changed frozen request or position")
            row = dict(
                cell_id=cell["cell_id"],
                method=cell["method"],
                benchmark=cell["benchmark"],
                position_id=pos["position_id"],
                resource_ready=ready,
                reason=None if ready else binding.get("missing", "resource_pending"),
            )
            if ready:
                item = request_item(
                    spec, model=binding["model"], sampling=binding.get("sampling", {})
                )
                row["logical_key"] = item.key
                jobs.append(
                    Job(
                        cell["cell_id"],
                        pos["position_id"],
                        adapter,
                        spec,
                        item,
                        binding.get("sampling", {}),
                    )
                )
            positions.append(row)
    if len({j.item.key for j in jobs}) != len(jobs):
        raise IntegrityError("Duplicate prepared path identity")
    return jobs, positions


class AdapterScheduler(Scheduler):
    """Only the method protocol differs; all physical scheduling stays in Scheduler."""

    async def _path(self, item, execute, feedback):
        job = self.jobs[item.key]
        result = await generate_path(
            job.adapter, job.spec, self, model=item.model, sampling=job.sampling
        )
        prediction = result.get("prediction")
        final = dict(
            status=result["status"],
            reason=result.get("reason"),
            prediction=prediction.as_dict() if prediction else None,
            raw_prediction=prediction.raw if prediction else None,
            database_started=False,
        )
        return await self._save_path(
            dict(
                logical_key=item.key,
                task_id=item.task_id,
                graph=item.graph,
                method=item.method,
                benchmark=item.benchmark,
                model=item.model,
                cell_id=job.cell_id,
                position_id=job.position_id,
                final=final,
                model_attempts=result["replies"],
                views=[dict(phase="request_pipeline", outcome=final)],
            )
        )


def position_results(positions, paths):
    finished = {p["logical_key"]: p for p in paths}
    # The scheduler's isolated-error receipt intentionally has less metadata.
    # Position identity comes from the frozen inventory, including failed paths.
    source_paths = {
        (p["method"], p["benchmark"], p["position_id"]): finished[p["logical_key"]]
        for p in positions
        if p.get("logical_key") in finished
        and all(k in p for k in ("method", "benchmark", "position_id"))
    }
    rows = []
    for pos in positions:
        row = dict(pos)
        if "source" in pos:
            source = pos["source"]
            path = source_paths.get(
                (source["method"], source["benchmark"], source["position_id"])
            )
            row.update(status="dependency_pending", database_started=False)
            if path:
                row.update(
                    source_logical_key=path["logical_key"],
                    source_status=path["final"]["status"],
                    status=(
                        "dependency_bound"
                        if path["final"]["status"] in ("ready", "answered")
                        else "dependency_failed"
                    ),
                )
        else:
            path = finished.get(pos.get("logical_key"))
            row["status"] = path["final"]["status"] if path else "not_dispatched"
        rows.append(row)
    return rows


async def execute(
    spec,
    jobs,
    positions,
    pool,
    ledger,
    transport,
    *,
    simulated=False,
    registry=None,
    saved=None
):
    scheduler = AdapterScheduler(
        spec, pool, ledger, Journal(spec.run_dir / "journal.jsonl"), transport
    )
    scheduler.jobs = {j.item.key: j for j in jobs}
    scheduler.saved_replies = saved or {}
    summary = await execute_campaign(
        scheduler,
        [j.item for j in jobs],
        registry_path=registry,
        simulated=simulated,
        metadata=dict(
            kind="adapter_request_matrix",
            database_started=False,
            logical_positions=len(positions),
            source=source_manifest(),
        ),
        defer_seal=True,
    )
    result = dict(
        positions=position_results(positions, scheduler.paths),
        simulated=simulated,
        database_started=False,
        real_network_calls=0 if simulated else None,
        reserved_physical_attempts=summary["counts"]["dispatched"],
        paper_result=False,
        stop_reason=summary["stop_reason"],
    )
    write_new(spec.run_dir / "matrix_result.json", canonical(result))
    finalize_campaign(spec.run_dir, summary)
    return result


async def run(config, *, simulated=False):
    if config["source"] != source_manifest():
        raise IntegrityError("Source changed; prepare a new freeze")
    inventory = read(verify(config["inventory"]))
    jobs, positions = (
        load_feedback_jobs(inventory, config["bindings"], config["feedback_queue"])
        if config.get("feedback_queue")
        else load_jobs(inventory, config["bindings"], simulated=simulated)
    )
    if not jobs:
        raise ValueError("No ready requests; do not start an idle runner")
    models = tuple(sorted({j.item.model for j in jobs}))
    settings = registry_from_file(config["registry_path"]) if not simulated else None
    cap = config["simulation_cap"] if simulated else settings["global_cap"]
    spec = CampaignSpec(
        config["name"],
        models[0],
        Path(config["run_dir"]),
        Path(config["ledger_dir"]),
        Path(config["approval_path"]),
        config["budget_scope"],
        global_cap=cap,
        models=models,
        retry=retry_policy(config.get("retry", {})),
        timeouts=timeouts_from(config.get("timeouts", {})),
    )
    pool = DynamicPool(cap, ceiling=cap)
    if simulated:
        endpoints = [
            Endpoint(
                "fake" + str(n),
                "http://127.0.0.1/not-called",
                m,
                "unused",
                cap,
                wire_params=("model", "messages", "prompt", "max_tokens", "stop"),
            )
            for n, m in enumerate(models)
        ]
        await pool.update(endpoints)
        selections = {
            digest(j.item.wire): j.spec.schema.graph
            for j in jobs
            if j.item.phase == "schema_select"
        }
        ours = {digest(j.item.wire) for j in jobs if j.item.method == "ours"}

        def respond(call, wire, endpoint):
            body = json.loads(wire)
            content = "<answer>RETURN 1</answer>" if "prompt" in body else "RETURN 1"
            if digest(wire) in selections:
                content = (
                    "<json>"
                    + json.dumps(
                        dict(name=selections[digest(wire)], entities=[], relations=[])
                    )
                    + "</json>"
                )
            elif digest(wire) in ours:
                # A protocol fixture, never compiled or presented as a real graph result.
                content = json.dumps(
                    dict(
                        status="ANSWER",
                        calls=[
                            dict(
                                nodes=[dict(id="n", label="Fixture")],
                                returns=[dict(field="n", alias="x")],
                            )
                        ],
                        combine=dict(op="single"),
                    )
                )
            return dict(
                status="ok",
                content=content,
                http_status=200,
                finish_reason="stop",
                usage={"completion_tokens": 10},
            )

        return await execute(
            spec,
            jobs,
            positions,
            pool,
            MemoryLedger(config["physical_attempt_cap"]),
            FakeTransport(0.02, respond),
            simulated=True,
        )
    if config.get("execute_authorized") is not True:
        raise ValueError("Preparation configuration is not live admission")
    validate_paths(
        spec.run_dir,
        spec.ledger_dir,
        Path(config["legacy_ledger"]),
        config.get("protected_roots", []),
    )
    for job in jobs:
        if not any(
            serves(e, job.item.model)
            and e.enabled
            and set(json.loads(job.item.wire)) <= set(e.wire_params)
            for e in settings["endpoints"]
        ):
            raise ValueError("No compatible configured endpoint for " + job.item.model)
    await pool.update(
        settings["endpoints"],
        global_cap=settings["global_cap"],
        per_model_cap=settings["per_model_cap"],
    )
    legacy = scan_legacy(Path(config["legacy_ledger"]), config.get("path_rewrites", []))
    with Ledger(
        spec.ledger_dir, spec.approval_path, legacy, spec.budget_scope
    ) as ledger:
        if any(
            r["scope"] == spec.budget_scope for r in ledger.records.values()
        ) and not config.get("recovery"):
            raise IntegrityError(
                "This scope has prior attempts; supply an audited recovery manifest before continuing"
            )
        ledger.limit_run(spec.run_dir, config["physical_attempt_cap"])
        saved = (
            saved_replies(
                config["recovery"]["path"], config["recovery"]["sha256"], ledger
            )
            if config.get("recovery")
            else {}
        )
        transport = HTTPTransport(
            Path(config["key_dir"]), connect_timeout=spec.timeouts.http_connect
        )
        transport.validate_keys(
            [
                e
                for e in settings["endpoints"]
                if e.enabled and any(serves(e, m) for m in models)
            ]
        )
        return await execute(
            spec,
            jobs,
            positions,
            pool,
            ledger,
            transport,
            registry=Path(config["registry_path"]),
            saved=saved,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("audit", "simulate", "run"))
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    config = read(args.config)
    if args.command == "audit":
        jobs, positions = load_jobs(
            read(verify(config["inventory"])), config["bindings"]
        )
        print(
            json.dumps(
                dict(
                    ready_paths=len(jobs),
                    logical_positions=len(positions),
                    model_calls=0,
                )
            )
        )
    else:
        result = asyncio.run(run(config, simulated=args.command == "simulate"))
        print(
            json.dumps(
                dict(
                    positions=len(result["positions"]),
                    simulated=result["simulated"],
                    stop_reason=result["stop_reason"],
                )
            )
        )
