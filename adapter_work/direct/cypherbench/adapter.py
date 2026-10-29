"""M01-B01: direct x CypherBench adapter (offline preparation only).

The adapter never sends HTTP and never opens a database. ``prepare`` turns the
frozen V22 positions into request specs, ``parse`` classifies a model reply,
and ``consume`` runs the real answer through an injected runtime so a fake
object can exercise the whole path without a server.

Order is part of the contract: the method answer is saved *before* the database
read and *before* scoring, so a crash keeps the answer and leaves the rest
unknown rather than silently dropping it.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

FENCE = re.compile(r"```(?:cypher)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
REFUSAL = re.compile(r"\b(i can('t|not)|unable to|sorry|as an ai)\b", re.IGNORECASE)
ANSWER_HINT = re.compile(r"\b(match|optional match|return|with|call)\b", re.IGNORECASE)

STATUSES = (
    "answered",
    "refused",
    "format_error",
    "run_error",
    "not_dispatched",
    "unknown",
)


class IntegrityError(RuntimeError):
    """Frozen input bytes do not match the SHA recorded in the position list."""


@dataclass(frozen=True)
class RequestSpec:
    position_id: str
    task_id: str
    graph: str
    category: str | None
    ordinal: int
    full_flow_five: bool
    messages: tuple
    input_path: str
    input_sha256: str

    def as_dict(self) -> dict:
        return dict(
            position_id=self.position_id,
            task_id=self.task_id,
            graph=self.graph,
            category=self.category,
            ordinal=self.ordinal,
            full_flow_five=self.full_flow_five,
            messages=[dict(m) for m in self.messages],
            input_path=self.input_path,
            input_sha256=self.input_sha256,
        )


@dataclass(frozen=True)
class Prediction:
    status: str
    cypher: str | None = None
    raw: str = ""
    http_status: int | None = None
    attempts: int = 1
    cost: float | None = None

    def as_dict(self) -> dict:
        return dict(
            status=self.status,
            cypher=self.cypher,
            http_status=self.http_status,
            attempts=self.attempts,
            cost=self.cost,
        )


@dataclass(frozen=True)
class CellResult:
    positions: int
    answered: int
    refused: int
    format_error: int
    run_error: int
    not_dispatched: int
    unknown: int
    scored: int
    attempts: int
    cost: float | None
    rows: tuple = field(default_factory=tuple)

    def as_dict(self) -> dict:
        return dict(
            positions=self.positions,
            answered=self.answered,
            refused=self.refused,
            format_error=self.format_error,
            run_error=self.run_error,
            not_dispatched=self.not_dispatched,
            unknown=self.unknown,
            scored=self.scored,
            attempts=self.attempts,
            cost=self.cost,
            rows=list(self.rows),
        )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(positions_path, *, limit=None) -> list:
    """Load frozen positions and return request specs, verifying every input SHA."""
    document = json.loads(Path(positions_path).read_text(encoding="utf-8-sig"))
    positions = document["positions"]
    if limit is not None:
        positions = positions[:limit]
    specs = []
    for row in positions:
        path = Path(row["public_input_path"])
        if not path.is_file() or _sha(path) != row["public_input_sha256"]:
            raise IntegrityError("public input changed: " + str(path))
        payload = json.loads(path.read_text(encoding="utf-8"))
        if "gold" in payload or "reference" in payload:
            raise IntegrityError("public input leaks gold: " + str(path))
        messages = tuple(dict(m) for m in payload["messages"])
        specs.append(
            RequestSpec(
                position_id=row["position_id"],
                task_id=row["task_id"],
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                messages=messages,
                input_path=str(path),
                input_sha256=row["public_input_sha256"],
            )
        )
    return specs


def parse(reply: str, *, http_status=None, attempts=1, cost=None) -> Prediction:
    """Classify a model reply; never treat a 200 as a correct answer."""
    text = reply or ""
    if http_status is not None and not 200 <= http_status < 300:
        return Prediction("unknown", None, text, http_status, attempts, cost)
    stripped = text.strip()
    if not stripped:
        return Prediction("format_error", None, text, http_status, attempts, cost)
    if REFUSAL.search(stripped):
        return Prediction("refused", None, text, http_status, attempts, cost)
    fenced = FENCE.search(stripped)
    candidate = (fenced.group(1) if fenced else stripped).strip()
    candidate = candidate.rstrip(";").strip()
    if not candidate or not ANSWER_HINT.search(candidate):
        return Prediction("format_error", None, text, http_status, attempts, cost)
    return Prediction("answered", candidate, text, http_status, attempts, cost)


def consume(positions, predictions, runtime) -> CellResult:
    """Run saved predictions through the injected runtime and keep every position.

    ``runtime`` provides ``save_answer``, ``graph_lease``, ``db_client`` and
    ``scorer``. A raising db client marks the row ``run_error``; the answer
    stays saved either way.
    """
    counts = {name: 0 for name in STATUSES}
    counts["scored"] = 0
    attempts = 0
    costs = []
    rows = []

    for position in positions:
        prediction = predictions.get(position.position_id)
        if prediction is None:
            counts["not_dispatched"] += 1
            rows.append(dict(position_id=position.position_id, status="not_dispatched"))
            continue
        attempts += prediction.attempts
        costs.append(prediction.cost)

        if prediction.status != "answered":
            counts[prediction.status] = counts.get(prediction.status, 0) + 1
            rows.append(
                dict(
                    position_id=position.position_id,
                    status=prediction.status,
                    cypher=None,
                    http_status=prediction.http_status,
                )
            )
            continue

        runtime.save_answer(position.position_id, prediction.cypher, prediction.raw)
        lease = runtime.graph_lease(position.graph)
        try:
            records = runtime.db_client(lease, prediction.cypher)
        except Exception as exc:  # DB failure is evidence, not an empty result
            counts["run_error"] += 1
            rows.append(
                dict(
                    position_id=position.position_id,
                    status="run_error",
                    cypher=prediction.cypher,
                    error=type(exc).__name__,
                )
            )
            continue

        score = runtime.scorer(position.task_id, prediction.cypher, records)
        counts["answered"] += 1
        counts["scored"] += 1
        rows.append(
            dict(
                position_id=position.position_id,
                status="answered",
                cypher=prediction.cypher,
                rows=len(records) if hasattr(records, "__len__") else None,
                score=score,
            )
        )

    total_cost = None if any(c is None for c in costs) else round(sum(costs), 6)
    return CellResult(
        positions=len(positions),
        answered=counts["answered"],
        refused=counts["refused"],
        format_error=counts["format_error"],
        run_error=counts["run_error"],
        not_dispatched=counts["not_dispatched"],
        unknown=counts["unknown"],
        scored=counts["scored"],
        attempts=attempts,
        cost=total_cost,
        rows=tuple(rows),
    )
