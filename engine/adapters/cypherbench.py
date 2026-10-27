from __future__ import annotations

import json
from pathlib import Path

from ..config import Sampling, digest
from ..scheduler import Item


GRAPHS = {
    "flight_accident": 189,
    "nba": 270,
    "fictional_character": 385,
    "company": 347,
    "geography": 366,
    "movie": 401,
    "politics": 390,
}


def model_wire(body: dict) -> bytes:
    # Preserve the old request field insertion order. Canonical sorting belongs
    # to evidence records, not to a frozen model input byte contract.
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(
        "utf-8"
    )


def from_saved_intents(root: Path, sampling: Sampling) -> list:
    """Offline compatibility input only; never dispatch an old experiment queue."""
    result = []
    for graph, expected in GRAPHS.items():
        tasks = Path(root) / graph / "tasks"
        folders = sorted(p for p in tasks.iterdir() if p.is_dir())
        if len(folders) != expected:
            raise ValueError("Full frozen graph denominator required")
        for index, task in enumerate(folders):
            for method in ("direct", "ours") if index % 2 == 0 else ("ours", "direct"):
                intent = json.loads((task / method / "intent.json").read_text(encoding="utf-8"))
                if intent["qid"] != task.name or intent["arm"] != method:
                    raise ValueError("Intent identity mismatch")
                result.append(
                    Item(
                        "CypherBench",
                        task.name,
                        method,
                        graph,
                        model_wire(sampling.body(intent["messages"])),
                        sampling.model,
                        read_timeout_seconds=sampling.read_timeout_seconds,
                    )
                )
    return result


def audit_saved_inputs(root: Path, sampling: Sampling) -> dict:
    items = from_saved_intents(root, sampling)
    expected = {digest(item.wire): item for item in items}
    matched, observed, file_differs_from_wire = set(), 0, 0
    for graph in GRAPHS:
        for path in (Path(root) / graph / "segments").rglob("request.json"):
            raw = path.read_bytes()
            body = json.loads(raw)
            if not isinstance(body, dict) or "messages" not in body or "model" not in body:
                continue
            observed += 1
            reconstructed = model_wire(body)
            if digest(reconstructed) in expected:
                matched.add(digest(reconstructed))
            file_differs_from_wire += raw != reconstructed
    return dict(
        expected_initial_paths=len(items),
        observed_model_request_files=observed,
        matched_initial_paths=len(matched),
        unmatched_initial_paths=len(expected) - len(matched),
        source_files_not_raw_HTTP_wire=file_differs_from_wire,
        wire_provenance="Reconstructed with the explicit legacy JSON serializer; raw network capture unavailable",
        actual_model_calls=0,
        actual_DB_calls=0,
    )


def feedback_item(item: Item, reply: dict, outcome: dict):
    if item.phase != "initial":
        raise ValueError("Second feedback forbidden")
    if outcome.get("legacy_status") not in ("MODEL_ERROR", "EXECUTION_ERROR"):
        return None
    body = json.loads(item.wire)
    body["messages"] += [
        dict(role="assistant", content=reply["content"]),
        dict(
            role="user",
            content="Repair once using only this format/compile/execution "
            "diagnostic, not correctness feedback: "
            + outcome.get("error", "Execution failed")[:1000],
        ),
    ]
    return Item(
        item.benchmark,
        item.task_id,
        item.method,
        item.graph,
        model_wire(body),
        item.model,
        "feedback",
        item.read_timeout_seconds,
    )


class ExecutedAnswerAdapter:
    """Delegate algorithms and scoring to explicitly hash-verified functions.

    No alternative parser, method fallback, score tolerance, or gold feedback.
    The caller supplies an owned graph RPC executor and the frozen interpreter.
    """

    def __init__(self, schemas, interpreter, executor_factory, scorer):
        self.schemas, self.interpreter = schemas, interpreter
        self.executor_factory, self.scorer = executor_factory, scorer

    def execute(self, item, reply):
        outcome = self.interpreter(
            reply.get("content"),
            reply["status"] == "ok",
            item.method,
            self.schemas[item.graph],
            self.executor_factory(item),
        )
        old = outcome["status"]
        status = "query_error" if old in ("MODEL_ERROR", "EXECUTION_ERROR") else old.lower()
        return dict(outcome, status=status, legacy_status=old)

    def score(self, outcome, reference, **kwargs):
        return self.scorer(outcome, reference, **kwargs)
