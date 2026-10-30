"""Request-only matrix entry point. Uses the existing scheduler and ledger.

No database imports or reference loading. A fresh, frozen run can explicitly reuse
a reconciled response manifest; an unexamined crashed run is never resumed silently.
"""

from __future__ import annotations

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
from common.author_assets import method_binding, validate_asset
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
from engine.recovery import saved_replies, merge_saved_reply
from engine.scheduler import Scheduler
from engine.transport import HTTPTransport
from cohort_runtime import execution_spec, is_cohort, provenance, validate_inventory


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


def load_reuse_aliases(pin, jobs, ledger, graph_snapshots):
    """Bind audited old paid answers to new position IDs without changing history."""
    targets = {job.item.key: job for job in jobs}
    reused = {}
    for row in read(verify(pin)):
        target = row["target_key"]
        if target in reused or target not in targets:
            raise IntegrityError("Duplicate or unknown reuse target")
        job = targets[target]
        expected = dict(logical_model=job.item.model, graph=job.item.graph,
                        profile_sha256=(job.cohort or {}).get("profile_sha256"),
                        public_input_sha256=(job.cohort or {}).get("public_input_sha256"),
                        graph_snapshot_sha256=graph_snapshots[job.item.benchmark][job.item.graph])
        source_compatibility = dict(row["source_compatibility"])
        equivalent = row.get("public_equivalence")
        if equivalent is not None:
            source_pin, target_pin = equivalent["source"], equivalent["target"]
            if (source_pin["sha256"] != source_compatibility.get("public_input_sha256")
                    or target_pin["sha256"] != expected["public_input_sha256"]):
                raise IntegrityError("Public equivalence pins differ from declared inputs")
            old_public, new_public = canonical(read(verify(source_pin))), canonical(read(verify(target_pin)))
            if old_public != new_public or digest(new_public) != equivalent["canonical_sha256"]:
                raise IntegrityError("Public inputs differ semantically")
            # Different JSON formatting is permitted; retain both original pins.
            source_compatibility["public_input_sha256"] = expected["public_input_sha256"]
        if any(value is None for value in expected.values()) or row["compatibility"] != expected or source_compatibility != expected:
            raise IntegrityError("Reuse input/profile/model/snapshot differs")
        if not row.get("evidence"):
            raise IntegrityError("Reuse needs explicit source compatibility evidence")
        for evidence in row["evidence"]:
            verify(evidence)
        records = sorted((record for record in ledger.records.values()
                          if record["logical_key"] == row["source_key"] and record["scope"] == row["source_scope"]),
                         key=lambda record: record["slot"])
        if not records:
            raise IntegrityError("Reuse source has no paid history")
        evidence_paths = {str(Path(p["path"]).resolve()) for p in row["evidence"]}
        if any(str(Path(record["request_path"]).with_name("terminal.json").resolve()) not in evidence_paths
               for record in records):
            raise IntegrityError("Reuse evidence must pin every source terminal")
        old = {}
        for record in records:
            merge_saved_reply(old, record)
        saved = old[row["source_key"]]
        if saved["wire_sha256"] != digest(job.item.wire):
            raise IntegrityError("Reuse request bytes differ")
        if saved["response"]["status"] not in ("ok", "length", "refused", "empty"):
            raise IntegrityError("Only terminal model replies may be aliased")
        reused[target] = saved
    return reused


@dataclass(frozen=True)
class Job:
    cell_id: str
    position_id: str
    adapter: object
    spec: object
    item: object
    sampling: dict
    original_task_id: str | None = None
    cohort: dict | None = None


def load_feedback_jobs(inventory, bindings, queue_pin):
    """Reconstruct one feedback stage from pinned public input and DB failure."""
    from common.stages import advance
    from feedback_queue import operational_error

    if is_cohort(inventory):
        validate_inventory(inventory)
    cells = {c["cell_id"]: c for c in inventory["cells"]}
    prepared_specs = {}
    jobs, positions = [], []
    for pin in read(verify(queue_pin)):
        entry = read(verify(pin))
        row = entry["position"]
        cell = cells[row["cell_id"]]
        if cell["method"] != "direct-feedback":
            raise IntegrityError("Feedback queue contains another method")
        adapter = loader.load(verify(cell["adapter"]))
        if cell["cell_id"] not in prepared_specs:
            if is_cohort(inventory):
                from common.cohort_specs import prepare_cell_specs
                specs = prepare_cell_specs(cell)
            else:
                specs = adapter.prepare(str(verify(cell["positions_source"])))
            prepared_specs[cell["cell_id"]] = {s.position_id: s for s in specs}
        spec = prepared_specs[cell["cell_id"]][row["position_id"]]
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
        request = execution_spec(step.request, row)
        item = request_item(
            request, model=binding["model"], sampling=binding.get("sampling", {})
        )
        jobs.append(
            Job(
                cell["cell_id"],
                row["position_id"],
                adapter,
                request,
                item,
                binding.get("sampling", {}),
                row["task_id"],
                provenance(row),
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
    if is_cohort(inventory):
        validate_inventory(inventory)
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
        if is_cohort(inventory):
            from common.cohort_specs import prepare_cell_specs
            specs = prepare_cell_specs(cell)
        else:
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
                validate_asset(
                    binding,
                    cell["method"],
                    cell["benchmark"],
                    pos["graph"],
                    read_pin=lambda pin: read(verify(pin)),
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
                spec = execution_spec(spec, pos)
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
                        pos["task_id"],
                        dict(provenance(pos), public_input_sha256=pos["public_input"]["sha256"],
                             profile_sha256=pos.get("profile", {}).get("sha256")),
                    )
                )
            positions.append(row)
    if len({j.item.key for j in jobs}) != len(jobs):
        raise IntegrityError("Duplicate prepared path identity")
    return jobs, positions


class AdapterScheduler(Scheduler):
    """Only the method protocol differs; all physical scheduling stays in Scheduler."""

    async def _save_path(self, value):
        # Ordinary isolated failures must preserve the same provenance as answers.
        job = self.jobs.get(value["logical_key"])
        if job is not None:
            value = dict(value, cell_id=job.cell_id, position_id=job.position_id,
                         benchmark=job.item.benchmark,
                         task_id=job.original_task_id or job.item.task_id,
                         **(job.cohort or {}))
        return await super()._save_path(value)

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
                task_id=job.original_task_id or item.task_id,
                graph=item.graph,
                method=item.method,
                benchmark=item.benchmark,
                model=item.model,
                cell_id=job.cell_id,
                position_id=job.position_id,
                final=final,
                model_attempts=result["replies"],
                views=[dict(phase="request_pipeline", outcome=final)],
                **(job.cohort or {}),
            )
        )

    async def run(self, items, *, execute=None, feedback=None):
        """Cohort paths enter admission only when a bounded submission slot opens."""
        if not getattr(self, "bounded_submission", False):
            return await super().run(items, execute=execute, feedback=feedback)
        items = list(items)
        if len({item.key for item in items}) != len(items):
            raise IntegrityError("Duplicate cohort request identity")
        allowed = set(self.spec.models or (self.spec.model,))
        if any(item.model not in allowed for item in items):
            raise IntegrityError("Unbound cohort model")
        waiting = iter(items)
        active = set()
        width = self.spec.global_cap

        def fill():
            while len(active) < width and not self.draining.is_set():
                item = next(waiting, None)
                if item is None:
                    break
                active.add(asyncio.create_task(self._isolated_path(item, execute, feedback)))

        try:
            fill()
            while active:
                done, active = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    try:
                        await task
                    except Exception as error:
                        await self.drain("path_controller_error:" + type(error).__name__)
                fill()
            return self.paths
        finally:
            if active:
                await asyncio.gather(*active, return_exceptions=True)
            self.http_workers.shutdown(wait=True)
            self.db_workers.shutdown(wait=True)
            if self.close_transport:
                self.transport.close()


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
    saved=None,
    drain_paths=(),
    stop_event=None,
    bounded_submission=False,
):
    scheduler = AdapterScheduler(
        spec, pool, ledger, Journal(spec.run_dir / "journal.jsonl"), transport
    )
    scheduler.jobs = {j.item.key: j for j in jobs}
    scheduler.extra_drain_paths = [Path(path) for path in drain_paths]
    scheduler.external_stop_event = stop_event
    scheduler.saved_replies = saved or {}
    scheduler.bounded_submission = bounded_submission
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


async def run(config, *, simulated=False, admitted_session=None):
    if admitted_session is not None:
        from request_session import RequestSession

        if simulated or type(admitted_session) is not RequestSession:
            raise IntegrityError("A live admitted RequestSession is required")
        admitted_session.validate(config)
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
    if admitted_session is not None and any(
        j.item.method != "direct-feedback" for j in jobs
    ):
        raise IntegrityError("RequestSession accepts only Direct-feedback jobs")
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
        queue_wait_until_available=is_cohort(inventory),
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
            drain_paths=config.get("drain_paths", ()),
            bounded_submission=is_cohort(inventory),
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
    from contextlib import nullcontext

    if admitted_session is None:
        legacy = scan_legacy(
            Path(config["legacy_ledger"]), config.get("path_rewrites", [])
        )
        ledger_context = Ledger(
            spec.ledger_dir, spec.approval_path, legacy, spec.budget_scope
        )
    else:
        ledger_context = nullcontext(admitted_session.ledger)
    with ledger_context as ledger:
        if (
            admitted_session is None
            and any(r["scope"] == spec.budget_scope for r in ledger.records.values())
            and not config.get("recovery")
        ):
            raise IntegrityError(
                "This scope has prior attempts; supply an audited recovery manifest before continuing"
            )
        ledger.limit_run(spec.run_dir, config["physical_attempt_cap"])
        saved = (
            admitted_session.replies_for(j.item.key for j in jobs)
            if admitted_session
            else (
                saved_replies(
                    config["recovery"]["path"], config["recovery"]["sha256"], ledger
                )
                if config.get("recovery")
                else {}
            )
        )
        if config.get("reuse_aliases"):
            aliases = load_reuse_aliases(config["reuse_aliases"], jobs, ledger, config["graph_snapshots"])
            if set(saved) & set(aliases):
                raise IntegrityError("Current-scope recovery and cross-scope alias overlap")
            saved.update(aliases)
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
        result = await execute(
            spec,
            jobs,
            positions,
            pool,
            ledger,
            transport,
            registry=Path(config["registry_path"]),
            saved=saved,
            drain_paths=config.get("drain_paths", ()),
            stop_event=admitted_session.stop if admitted_session else None,
            bounded_submission=is_cohort(inventory),
        )
        if admitted_session:
            admitted_session.refresh()
        return result


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
