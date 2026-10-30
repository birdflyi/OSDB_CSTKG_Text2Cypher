"""Multi-stage advance for method paths (CONTRACT_V2 section 2).

A path is ``position + method + logical model + phase``. Each generation stage
is new work, not a retry of the previous one:

    schema_select -> answer_continue -> (done)

The adapter decides what the next request looks like; this module only keeps
the allowed transitions, the parent identity and the state persisted with the
step. ``run_fake_scheduler`` exercises a path offline with scripted replies.
"""

from __future__ import annotations

from dataclasses import dataclass, field

ACTIONS = ("request", "complete", "blocked", "failed")
TRANSITIONS = {"schema_select": "answer_continue", "initial": "feedback"}


class StageError(RuntimeError):
    """The adapter asked for a transition the contract does not allow."""


@dataclass(frozen=True)
class NextStep:
    action: str
    request: object = None
    state: dict = field(default_factory=dict)
    reason: str | None = None

    def __post_init__(self):
        if self.action not in ACTIONS:
            raise StageError("unknown action: " + str(self.action))
        if self.action == "request" and self.request is None:
            raise StageError("a request step needs a request")
        if self.action != "request" and self.request is not None:
            raise StageError("only a request step carries a request")


def check(request, step: NextStep, *, previous_stage: str, position_id: str) -> None:
    """Reject a step that changes identity or loops."""
    if step.action != "request":
        return
    stage = getattr(step.request, "generation_stage", None)
    if TRANSITIONS.get(previous_stage) != stage:
        raise StageError("invalid generation transition: " + previous_stage + " -> " + str(stage))
    if getattr(step.request, "position_id", None) != position_id:
        raise StageError("next step changed the position")
    if getattr(step.request, "task_id", None) != getattr(request, "task_id", None):
        raise StageError("next step changed the task")
    if getattr(step.request, "benchmark", None) != getattr(request, "benchmark", None):
        raise StageError("next step changed the benchmark")
    if getattr(step.request, "method", None) != getattr(request, "method", None):
        raise StageError("next step changed the method")


def advance(adapter, request, reply, *, transport_unknown=False, **metadata) -> NextStep:
    """Call the cell's next_request and validate the transition."""
    step = adapter.next_request(request, reply, transport_unknown=transport_unknown, **metadata)
    check(request, step, previous_stage=request.generation_stage, position_id=request.position_id)
    if step.action == "request":
        state = dict(step.state or {})
        state.setdefault("parent_request_id", request.position_id)
        state.setdefault("previous_stage", request.generation_stage)
        state.setdefault("completed_stages", [request.generation_stage])
        return NextStep("request", step.request, state, step.reason)
    return step


def run_fake_scheduler(adapter, request, replies):
    """Drive a path with scripted replies and return (steps, final_state)."""
    steps = []
    state = {}
    current = request
    for reply in replies:
        step = advance(adapter, current, reply)
        steps.append(step)
        if step.action != "request":
            return steps, dict(step.state)
        current = step.request
        state = dict(step.state)
    return steps, state
