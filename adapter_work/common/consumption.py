"""Single consumption core for every adapter cell (CONTRACT_V2 order).

Canonical order with a V2 runtime:

    save_prediction -> graph_lease -> execute -> save_final_answer
    -> replay_controls -> score_saved_answer

Older B-group fakes expose ``db_client``/``score_saved_answer(position_id)``
instead. That shape is still accepted through ``legacy`` calls, so one file
serves both and the two divergent cores disappear. New code must use the V2
shape; the legacy branch exists only to keep the first B cells runnable.

No HTTP, no driver, no reference set: the runtime owns all three.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from common.errors import HARD_ERRORS

METHOD_STATUSES = (
    "executed",
    "unsupported",
    "refused",
    "format_error",
    "run_error",
    "not_dispatched",
    "unknown",
)
SCORE_STATUSES = ("scored", "reference_missing", "scoring_error", "not_scored")


class ReferenceMissing(LookupError):
    """Raised by a runtime when a position has no reference to score against."""


@dataclass(frozen=True)
class Prediction:
    status: str
    cypher: str | None = None
    raw: str = ""
    http_status: int | None = None
    attempts: int = 1
    cost: float | None = None
    fallback: bool = False
    parameters: dict | None = None
    compiled_plan: object | None = None

    @property
    def executable(self) -> bool:
        return self.status in ("ready", "answered")

    def as_dict(self) -> dict:
        return dict(
            status=self.status,
            cypher=self.cypher,
            http_status=self.http_status,
            attempts=self.attempts,
            cost=self.cost,
            fallback=self.fallback,
            parameters=self.parameters,
            compiled_plan_present=self.compiled_plan is not None,
        )


@dataclass(frozen=True)
class CellResult:
    positions: int
    selected_positions: int
    executed: int
    unsupported: int
    answered: int
    refused: int
    format_error: int
    run_error: int
    not_dispatched: int
    unknown: int
    scored: int
    scoring_error: int
    reference_missing: int
    not_scored: int
    attempts: int
    cost: float | None
    rows: tuple = field(default_factory=tuple)

    def as_dict(self) -> dict:
        return dict(
            positions=self.positions,
            selected_positions=self.selected_positions,
            executed=self.executed,
            unsupported=self.unsupported,
            answered=self.answered,
            refused=self.refused,
            format_error=self.format_error,
            run_error=self.run_error,
            not_dispatched=self.not_dispatched,
            unknown=self.unknown,
            scored=self.scored,
            scoring_error=self.scoring_error,
            reference_missing=self.reference_missing,
            not_scored=self.not_scored,
            attempts=self.attempts,
            cost=self.cost,
            rows=list(self.rows),
        )


def _view(position):
    """Cells may pass dict rows or spec objects; one shape inside consume."""
    return SimpleNamespace(**position) if isinstance(position, dict) else position


def _legacy(runtime) -> bool:
    """A runtime without ``execute`` is a B-group fake; keep it working."""
    return not hasattr(runtime, "execute")


def _score(runtime, position, saved, legacy):
    try:
        result = runtime.score_saved_answer(position.position_id) if legacy else runtime.score_saved_answer(position, saved)
    except ReferenceMissing:
        return dict(status="reference_missing", value=None, error=None)
    except HARD_ERRORS:
        raise
    except Exception as exc:
        return dict(status="scoring_error", value=None, error=type(exc).__name__)
    if result is None:
        return dict(status="not_scored", value=None, error=None)
    if hasattr(result, "status"):
        return dict(status=result.status, value=getattr(result, "value", None), error=getattr(result, "error", None))
    return dict(status="scored", value=result, error=None)


def _controls(runtime, position, saved):
    hook = getattr(runtime, "replay_controls", None)
    if hook is None:
        return None
    try:
        result = hook(position, saved)
        return getattr(result, "status", "control_ok") if result is not None else None
    except HARD_ERRORS:
        raise
    except Exception as exc:
        return "control_error:" + type(exc).__name__


def _run_one(runtime, position, prediction, legacy):
    """Return (row, outcome) where outcome is a method status or 'scored'."""
    if legacy:
        runtime.save_prediction(position.position_id, prediction)
        lease = runtime.graph_lease(position.graph)
        handle = lease.__enter__() if hasattr(lease, "__enter__") else lease
        try:
            records = runtime.db_client(handle, prediction.cypher)
        finally:
            if hasattr(lease, "__exit__"):
                lease.__exit__(None, None, None)
        runtime.save_final_answer(position.position_id, records)
        score = _score(runtime, position, None, legacy)
        return (
            dict(
                position_id=position.position_id,
                status="executed",
                prediction=prediction.cypher,
                score=score["value"],
                score_status=score["status"],
                score_error=score.get("error"),
            ),
            score,
        )

    receipt = runtime.save_prediction(position, prediction)
    lease = runtime.graph_lease(position.graph)
    handle = lease.__enter__() if hasattr(lease, "__enter__") else lease
    try:
        if getattr(prediction, "compiled_plan", None) is not None:
            execution = runtime.execute_plan(handle, position, prediction.compiled_plan)
        else:
            execution = runtime.execute(handle, position, prediction)
    finally:
        if hasattr(lease, "__exit__"):
            lease.__exit__(None, None, None)
    saved = runtime.save_final_answer(position, execution)
    control = _controls(runtime, position, saved)
    score = _score(runtime, position, saved, legacy)
    return (
        dict(
            position_id=position.position_id,
            status="executed",
            prediction=prediction.cypher,
            prediction_evidence=getattr(receipt, "path", None),
            final_answer_evidence=getattr(saved, "path", None),
            control=control,
            score=score["value"],
            score_status=score["status"],
            score_error=score.get("error"),
        ),
        score,
    )


def consume(positions, predictions, runtime) -> CellResult:
    legacy = _legacy(runtime)
    counts = {name: 0 for name in METHOD_STATUSES}
    scores = {name: 0 for name in SCORE_STATUSES}
    attempts = 0
    costs = []
    rows = []

    for raw in positions:
        position = _view(raw)
        prediction = predictions.get(position.position_id)
        if prediction is None:
            counts["not_dispatched"] += 1
            rows.append(dict(position_id=position.position_id, status="not_dispatched"))
            continue

        attempts += prediction.attempts
        costs.append(prediction.cost)

        if prediction.status not in ("ready", "answered"):
            counts[prediction.status] = counts.get(prediction.status, 0) + 1
            rows.append(dict(position_id=position.position_id, status=prediction.status))
            continue

        try:
            row, score = _run_one(runtime, position, prediction, legacy)
        except ReferenceMissing:
            counts["run_error"] += 1
            rows.append(dict(position_id=position.position_id, status="run_error", error="ReferenceMissing"))
            continue
        except TimeoutError as exc:
            counts["run_error"] += 1
            rows.append(dict(position_id=position.position_id, status="run_error", error=type(exc).__name__))
            continue
        except HARD_ERRORS:
            raise
        except Exception as exc:
            counts["run_error"] += 1
            rows.append(
                dict(position_id=position.position_id, status="run_error", error=type(exc).__name__)
            )
            continue

        counts["executed"] += 1
        scores[score["status"]] = scores.get(score["status"], 0) + 1
        rows.append(row)

    total_cost = None if any(c is None for c in costs) else round(sum(costs), 6)
    return CellResult(
        positions=len(positions),
        selected_positions=len(positions),
        executed=counts["executed"],
        unsupported=counts["unsupported"],
        answered=counts["executed"],
        refused=counts["refused"],
        format_error=counts["format_error"],
        run_error=counts["run_error"],
        not_dispatched=counts["not_dispatched"],
        unknown=counts["unknown"],
        scored=scores["scored"],
        scoring_error=scores["scoring_error"],
        reference_missing=scores["reference_missing"],
        not_scored=scores["not_scored"],
        attempts=attempts,
        cost=total_cost,
        rows=tuple(rows),
    )
