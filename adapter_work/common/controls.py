"""Control replay wrapper (v24 M3).

``replay`` is our integration API, not an upstream function: the runtime owns
the frozen plan, the graph lease and the evidence, while this wrapper keeps the
control status vocabulary and never fabricates a zero for a failed read.

Statuses: executed / not_applicable / backend_pending / unsupported_plan /
execution_error / not_run. A normal failure returns a result and the caller
continues with the next position; identity and persistence failures propagate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

CONTROL_PROFILES = {
    "correct-detail": "full_detail",
    "simple-correct": "simple-correct-20261027",
    "simple-correct-legacy": "simple-correct-full-detail-legacy",
    "native-set-count": "native-set-count",
}
STATUSES = (
    "executed",
    "not_applicable",
    "backend_pending",
    "unsupported_plan",
    "execution_error",
    "not_run",
)
HARD_NAMES = {"OSError", "IdentityScopeConflict", "EvidenceConflict", "PersistenceError"}


class UnsupportedPlan(Exception):
    """The frozen plan cannot express this control; keep the position."""


class BackendPending(Exception):
    """Structurally applicable but this backend has no control path yet."""


@dataclass(frozen=True)
class ControlResult:
    position_id: str
    source_plan_sha256: str | None
    control_id: str
    profile_id: str
    status: str
    reason_code: str | None = None
    actual_answer: dict | None = None
    evidence_receipt: str | None = None
    representation: dict | None = None
    costs: dict | None = None
    shared_generation_charge_ids: tuple = field(default_factory=tuple)

    def as_dict(self) -> dict:
        return dict(
            position_id=self.position_id,
            source_plan_sha256=self.source_plan_sha256,
            control_id=self.control_id,
            profile_id=self.profile_id,
            status=self.status,
            reason_code=self.reason_code,
            actual_answer=self.actual_answer,
            evidence_receipt=self.evidence_receipt,
            representation=self.representation,
            costs=self.costs,
            shared_generation_charge_ids=list(self.shared_generation_charge_ids),
        )


def _hard(exc) -> bool:
    return type(exc).__name__ in HARD_NAMES


def _answer_complete(read) -> bool:
    answer = read.get("answer") if isinstance(read, dict) else None
    return (
        isinstance(answer, dict)
        and answer.get("complete") is True
        and answer.get("columns") is not None
        and answer.get("rows") is not None
    )


def _result(envelope, control_id, profile_id, status, reason=None, **kwargs):
    return ControlResult(
        position_id=envelope.get("position_id", ""),
        source_plan_sha256=envelope.get("source_plan_sha256"),
        control_id=control_id,
        profile_id=profile_id,
        status=status,
        reason_code=reason,
        shared_generation_charge_ids=tuple(envelope.get("charge_ids") or ()),
        **kwargs,
    )


def replay(envelope, runtime, *, control_id: str, profile_id: str | None = None) -> ControlResult:
    profile = profile_id or CONTROL_PROFILES.get(control_id, control_id)
    plan = envelope.get("plan") or {}
    try:
        runtime.verify_source(envelope)
    except UnsupportedPlan as exc:
        return _result(envelope, control_id, profile, "unsupported_plan", str(exc))
    except Exception as exc:
        if _hard(exc):
            raise
        return _result(envelope, control_id, profile, "execution_error", type(exc).__name__)

    if not plan or plan.get("status") not in (None, "ANSWER"):
        return _result(envelope, control_id, profile, "unsupported_plan", "no_answer_plan")

    if control_id == "native-set-count":
        combine = plan.get("combine") or {}
        if combine.get("op") not in ("union", "intersection", "difference"):
            return _result(envelope, control_id, profile, "not_applicable", "not_a_two_input_set")

    try:
        read = runtime.run_control(envelope, control_id=control_id, profile_id=profile)
    except UnsupportedPlan as exc:
        return _result(envelope, control_id, profile, "unsupported_plan", str(exc))
    except BackendPending as exc:
        return _result(envelope, control_id, profile, "backend_pending", str(exc))
    except Exception as exc:
        if _hard(exc):
            raise
        return _result(envelope, control_id, profile, "execution_error", type(exc).__name__)

    if not _answer_complete(read):
        return _result(envelope, control_id, profile, "execution_error", "incomplete_answer")
    receipt = runtime.save_control_result(envelope, read)
    if not receipt:
        return _result(envelope, control_id, profile, "execution_error", "missing_receipt")
    return _result(
        envelope,
        control_id,
        profile,
        "executed",
        actual_answer=read.get("answer"),
        evidence_receipt=receipt,
        representation=read.get("representation"),
        costs=read.get("costs"),
    )


def replay_all(envelope, runtime, control_ids):
    """Run several controls; a failure is recorded and the rest continue."""
    results = []
    for control_id in control_ids:
        try:
            results.append(replay(envelope, runtime, control_id=control_id))
        except Exception as exc:
            if _hard(exc):
                raise
            results.append(_result(envelope, control_id, CONTROL_PROFILES.get(control_id, control_id), "execution_error", type(exc).__name__))
    return results


# Defined late so ``except`` clauses above can name them without import cycles.
class IdentityScopeConflict(RuntimeError):
    """Identity scope changed under the capture; the capture is invalid."""


class EvidenceConflict(RuntimeError):
    """Persistence or evidence conflict; hand it to the central session."""
