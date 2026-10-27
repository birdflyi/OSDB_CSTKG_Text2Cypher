from __future__ import annotations

import importlib
import json
from pathlib import Path
import sys

from .adapters.cypherbench import GRAPHS, model_wire
from .config import Sampling, digest


class TraceExecutor:
    """Replay saved DB replies, while checking the exact generated query/params."""

    def __init__(self, directory: Path):
        self.folders = sorted(Path(directory).glob("[0-9][0-9][0-9]"))
        self.index = 0

    def execute(self, query, parameters):
        if self.index >= len(self.folders):
            raise ValueError("Interpreter requested a DB read missing from the trace")
        folder = self.folders[self.index]
        self.index += 1
        intent = json.loads((folder / "intent.json").read_text(encoding="utf-8"))
        if intent != dict(query=query, parameters=parameters):
            raise ValueError("Generated DB query/parameters changed")
        saved = json.loads((folder / "result.json").read_text(encoding="utf-8"))
        return saved["result"]


def frozen_modules(code_root: Path, freeze_path: Path):
    frozen = json.loads(Path(freeze_path).read_text(encoding="utf-8"))
    for name, expected in frozen["code_files"].items():
        if digest((Path(code_root) / name).read_bytes()) != expected:
            raise ValueError("Selected legacy code tree does not match its32 frozen hashes")
    harness = (Path(code_root) / "experiment-harness/public_benchmark_v1").resolve()
    for name in ("dev_preparation", "schema_services", "answer_scoring", "model_transport"):
        existing = sys.modules.get(name)
        if existing and Path(existing.__file__).resolve().parent != harness:
            raise ValueError("A different legacy module is already loaded; use a fresh process")
    sys.path.insert(0, str(harness))
    return (
        frozen,
        importlib.import_module("dev_preparation"),
        importlib.import_module("answer_scoring"),
    )


def audit_history(
    root: Path, code_root: Path, freeze_path: Path, data: Path, official_source: Path
) -> dict:
    frozen, methods, scoring = frozen_modules(code_root, freeze_path)
    settings = frozen["model_settings"]
    sampling = Sampling(
        **{
            k: settings[k]
            for k in ("model", "temperature", "top_p", "reasoning_effort", "max_tokens")
        }
    )
    compare = scoring.load_official_comparator(official_source)
    refs = {
        v["qid"]: v
        for v in (
            json.loads(line)
            for line in (Path(data) / "evaluator/test.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        )
    }
    mismatches, seen = [], set()
    paths = physical = views = trace_reads = replayed_views = 0
    missing_traces = []
    for graph in GRAPHS:
        schema = json.loads(
            (Path(data) / "upstream/graphs/schemas" / (graph + "_schema.json")).read_text(
                encoding="utf-8"
            )
        )
        scored = json.loads(
            (Path(root) / graph / "scores" / (graph + ".json")).read_text(encoding="utf-8")
        )
        for expected_record in scored:
            qid, method = expected_record["qid"], expected_record["arm"]
            folder = Path(root) / graph / "tasks" / qid / method
            intent = json.loads((folder / "intent.json").read_text(encoding="utf-8"))
            messages = intent["messages"]
            paths += 1
            for view in expected_record["views"]:
                step = view["step"]
                expected_wire = model_wire(sampling.body(messages))
                for attempt in view["reply"]["attempt_folders"]:
                    request = Path(attempt) / "request.json"
                    if not request.exists():
                        mismatches.append(
                            dict(qid=qid, method=method, step=step, kind="missing_old_request_path")
                        )
                        continue
                    if str(request.resolve()) not in seen:
                        seen.add(str(request.resolve()))
                        physical += 1
                    actual = json.loads(request.read_text(encoding="utf-8"))
                    if model_wire(actual) != expected_wire:
                        mismatches.append(
                            dict(qid=qid, method=method, step=step, kind="input_wire_contract")
                        )
                executor = TraceExecutor(folder / "query_trace" / ("step_" + str(step)))
                reply = view["reply"]
                views += 1
                try:
                    if (
                        reply.get("receipt_kind")
                        == "NO_RESPONSE_EXHAUSTED_TRANSPORT_NOT_MODEL_OUTPUT"
                    ):
                        terminals = [
                            json.loads((Path(p) / "terminal.json").read_text(encoding="utf-8"))
                            for p in reply["attempt_folders"]
                        ]
                        if not terminals or any(t.get("http_status") == 200 for t in terminals):
                            raise ValueError(
                                "No-response receipt contradicts raw transport evidence"
                            )
                        actual = dict(status="INFRASTRUCTURE_ERROR", rows=None, columns=None)
                    else:
                        actual = methods.interpret(
                            reply["content"], reply["usable"], method, schema, executor
                        )
                except FileNotFoundError as exc:
                    # Recovered historical outcomes are not a substitute oracle
                    # for the missing independent database response trace.
                    missing_traces.append(
                        dict(qid=qid, method=method, step=step, missing_file=str(exc.filename))
                    )
                    actual = None
                if actual is None:
                    messages = messages + [
                        dict(role="assistant", content=reply["content"]),
                        dict(
                            role="user",
                            content="Repair once using only this format/compile/execution "
                            "diagnostic, not correctness feedback: "
                            + view["outcome"].get("error", "Execution failed")[:1000],
                        ),
                    ]
                    continue
                replayed_views += 1
                trace_reads += executor.index
                if executor.index != len(executor.folders):
                    mismatches.append(
                        dict(qid=qid, method=method, step=step, kind="unused_query_trace")
                    )
                expected = view["outcome"]
                for field in ("status", "rows", "columns"):
                    if actual.get(field) != expected.get(field):
                        mismatches.append(
                            dict(qid=qid, method=method, step=step, kind="outcome_" + field)
                        )
                reference = refs[qid]
                score = scoring.score_executed_answer(
                    status=actual["status"],
                    predicted_rows=actual["rows"],
                    reference_rows=scoring.read_reference_answer(reference["answer_json"]),
                    order_matters="order by" in reference["gold_cypher"].lower(),
                    qid=qid,
                    compare=compare,
                )
                if score != view["score"]:
                    mismatches.append(dict(qid=qid, method=method, step=step, kind="score"))
                messages = messages + [
                    dict(role="assistant", content=reply["content"]),
                    dict(
                        role="user",
                        content="Repair once using only this format/compile/execution "
                        "diagnostic, not correctness feedback: "
                        + actual.get("error", "Execution failed")[:1000],
                    ),
                ]
    return dict(
        initial_paths=paths,
        generation_views=views,
        unique_physical_attempts=physical,
        replayed_views=replayed_views,
        missing_independent_traces=missing_traces,
        full_downstream_equivalence_verified=not mismatches and not missing_traces,
        saved_DB_reads_replayed=trace_reads,
        mismatches=mismatches,
        golden_outputs_written=0,
        actual_model_calls=0,
        actual_DB_queries=0,
        raw_HTTP_wire_capture_available=False,
        input_wire_check="Explicit legacy serializer reconstruction, per path/phase/attempt",
    )
