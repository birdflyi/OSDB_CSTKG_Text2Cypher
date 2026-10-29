"""CONTRACT_V2 consumption: one order, one status column, failures stay visible.

Order (contract section 4):
    save_prediction -> graph lease -> execute -> save_final_answer
    -> replay_controls -> score_saved_answer

The adapter never opens a driver; the runtime owns leases, transactions and the
reference set. A failing execute or scorer keeps the position in the table and
moves on to the next one.
"""

from __future__ import annotations

from dataclasses import dataclass, field

METHOD_STATUSES = (
    "executed",
    "refused",
    "format_error",
    "run_error",
    "not_dispatched",
    "unknown",
)
SCORE_STATUSES = ("scored", "reference_missing", "scoring_error", "not_scored")


@dataclass(frozen=True)
class CellResult:
    positions: int
    selected_positions: int
    executed: int
    refused: int
    format_error: int
    run_error: int
    not_dispatched: int
    unknown: int
    scored: int
    scoring_error: int
    not_scored: int
    attempts: int
    cost: float | None
    rows: tuple = field(default_factory=tuple)

    def as_dict(self) -> dict:
        return dict(
            positions=self.positions,
            selected_positions=self.selected_positions,
            executed=self.executed,
            refused=self.refused,
            format_error=self.format_error,
            run_error=self.run_error,
            not_dispatched=self.not_dispatched,
            unknown=self.unknown,
            scored=self.scored,
            scoring_error=self.scoring_error,
            not_scored=self.not_scored,
            attempts=self.attempts,
            cost=self.cost,
            rows=list(self.rows),
        )


def _score(runtime, position, saved):
    try:
        result = runtime.score_saved_answer(position, saved)
    except Exception as exc:  # scoring failure is evidence, never a zero score
        return dict(status="scoring_error", value=None, error=type(exc).__name__)
    if result is None:
        return dict(status="not_scored", value=None, error=None)
    status = getattr(result, "status", None) or "not_scored"
    value = getattr(result, "value", None)
    return dict(status=status, value=value, error=getattr(result, "error", None))


def _controls(runtime, position, saved):
    hook = getattr(runtime, "replay_controls", None)
    if hook is None:
        return None
    try:
        result = hook(position, saved)
        return getattr(result, "status", "control_ok") if result is not None else None
    except Exception as exc:
        return "control_error:" + type(exc).__name__


def consume(positions, predictions, runtime) -> CellResult:
    counts = {name: 0 for name in METHOD_STATUSES}
    scores = {name: 0 for name in SCORE_STATUSES}
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

        if prediction.status != "ready":
            counts[prediction.status] = counts.get(prediction.status, 0) + 1
            rows.append(dict(position_id=position.position_id, status=prediction.status))
            continue

        receipt = runtime.save_prediction(position, prediction)
        lease = runtime.graph_lease(position.graph)
        try:
            execution = runtime.execute(lease, position, prediction)
        except Exception as exc:
            counts["run_error"] += 1
            rows.append(
                dict(
                    position_id=position.position_id,
                    status="run_error",
                    error=type(exc).__name__,
                    prediction_evidence=getattr(receipt, "path", None),
                )
            )
            continue

        saved = runtime.save_final_answer(position, execution)
        control = _controls(runtime, position, saved)
        score = _score(runtime, position, saved)

        counts["executed"] += 1
        scores[score["status"]] = scores.get(score["status"], 0) + 1
        rows.append(
            dict(
                position_id=position.position_id,
                status="executed",
                prediction=prediction.cypher,
                final_answer_evidence=getattr(saved, "path", None),
                control=control,
                score=score["value"],
                score_status=score["status"],
                score_error=score.get("error"),
            )
        )

    total_cost = None if any(c is None for c in costs) else round(sum(costs), 6)
    return CellResult(
        positions=len(positions),
        selected_positions=len(positions),
        executed=counts["executed"],
        refused=counts["refused"],
        format_error=counts["format_error"],
        run_error=counts["run_error"],
        not_dispatched=counts["not_dispatched"],
        unknown=counts["unknown"],
        scored=scores["scored"],
        scoring_error=scores["scoring_error"],
        not_scored=scores["not_scored"] + scores["reference_missing"],
        attempts=attempts,
        cost=total_cost,
        rows=tuple(rows),
    )
