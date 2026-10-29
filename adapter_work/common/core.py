"""Shared consumption core for the B-group direct adapters (offline only).

Implements the CONTRACT_V2 order that the pilots were missing:

    save_prediction -> execute -> save_final_answer -> score

Every position stays in the denominator. A database failure, a scoring failure
or a missing reference is recorded and the next position continues.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

STATUSES = (
    "answered",
    "refused",
    "format_error",
    "run_error",
    "scoring_error",
    "reference_missing",
    "not_dispatched",
    "unknown",
)


class IntegrityError(RuntimeError):
    """Frozen input bytes do not match the recorded SHA."""


@dataclass(frozen=True)
class Prediction:
    status: str
    cypher: str | None = None
    raw: str = ""
    http_status: int | None = None
    attempts: int = 1
    cost: float | None = None
    stage: str = "generation"
    fallback: bool = False


@dataclass(frozen=True)
class CellResult:
    positions: int
    answered: int
    refused: int
    format_error: int
    run_error: int
    scoring_error: int
    reference_missing: int
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
            scoring_error=self.scoring_error,
            reference_missing=self.reference_missing,
            not_dispatched=self.not_dispatched,
            unknown=self.unknown,
            scored=self.scored,
            attempts=self.attempts,
            cost=self.cost,
            rows=list(self.rows),
        )


def load_positions(path, *, limit=None) -> list:
    document = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    rows = document["positions"]
    return rows[:limit] if limit is not None else rows


def verify_public_input(row) -> dict:
    path = Path(row["public_input_path"])
    if not path.is_file():
        raise IntegrityError("missing public input: " + str(path))
    if hashlib.sha256(path.read_bytes()).hexdigest() != row["public_input_sha256"]:
        raise IntegrityError("public input changed: " + str(path))
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if "gold" in payload or "reference" in payload:
        raise IntegrityError("public input leaks gold: " + str(path))
    return payload


class ReferenceMissing(LookupError):
    """Raised by a runtime when a position has no reference to score against."""


def consume(positions, predictions, runtime) -> CellResult:
    """Run predictions through ``runtime``; no position is ever dropped.

    ``runtime`` provides ``save_prediction``, ``graph_lease``, ``db_client``,
    ``save_final_answer``, ``score_saved_answer`` and may raise
    ``ReferenceMissing`` to mark a position without a reference.
    """
    counts = {name: 0 for name in STATUSES}
    counts["scored"] = 0
    attempts = 0
    costs = []
    rows = []

    for position in positions:
        prediction = predictions.get(position["position_id"])
        if prediction is None:
            counts["not_dispatched"] += 1
            rows.append(dict(position_id=position["position_id"], status="not_dispatched"))
            continue
        attempts += prediction.attempts
        costs.append(prediction.cost)

        if prediction.status != "answered":
            counts[prediction.status] = counts.get(prediction.status, 0) + 1
            rows.append(dict(position_id=position["position_id"], status=prediction.status))
            continue

        runtime.save_prediction(position["position_id"], prediction)
        lease = runtime.graph_lease(position["graph"])
        try:
            records = runtime.db_client(lease, prediction.cypher)
        except Exception as exc:
            counts["run_error"] += 1
            rows.append(
                dict(
                    position_id=position["position_id"],
                    status="run_error",
                    error=type(exc).__name__,
                )
            )
            continue

        runtime.save_final_answer(position["position_id"], records)
        try:
            score = runtime.score_saved_answer(position["position_id"])
        except ReferenceMissing:
            counts["reference_missing"] += 1
            rows.append(dict(position_id=position["position_id"], status="reference_missing"))
            continue
        except Exception as exc:
            counts["scoring_error"] += 1
            rows.append(
                dict(
                    position_id=position["position_id"],
                    status="scoring_error",
                    error=type(exc).__name__,
                )
            )
            continue

        counts["answered"] += 1
        counts["scored"] += 1
        rows.append(
            dict(
                position_id=position["position_id"],
                status="answered",
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
        scoring_error=counts["scoring_error"],
        reference_missing=counts["reference_missing"],
        not_dispatched=counts["not_dispatched"],
        unknown=counts["unknown"],
        scored=counts["scored"],
        attempts=attempts,
        cost=total_cost,
        rows=tuple(rows),
    )


class ReferenceMissing(LookupError):
    """Raised by a runtime when a position has no reference to score against."""
