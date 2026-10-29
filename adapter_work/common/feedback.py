"""Direct-feedback: one non-gold operational feedback round (CONTRACT_V2 §1).

Round one is the plain direct request. A second round exists only when the
database actually failed with an operational error; the feedback message
carries that error, never a reference answer. At most one feedback stage, so a
broken query cannot loop.

``next_request`` is the integration point for the central NextStep dispatcher;
until that exists this module is exercised with a fake scheduler in tests.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import core, direct  # noqa: E402

MESSAGE_BENCHMARKS = ("CypherBench", "Text2GQL", "OSDB-STKG")
QUESTION_BENCHMARKS = ("MindTheQuery", "ZOGRASCOPE")
OPERATIONAL = ("TimeoutError", "ServiceUnavailable", "TransientError", "ClientError", "Neo4jError")


@dataclass(frozen=True)
class RequestSpec:
    cell_id: str
    position_id: str
    task_id: str
    benchmark: str
    method: str
    graph: str
    category: str | None
    ordinal: int
    full_flow_five: bool
    generation_stage: str
    protocol: str
    messages: tuple
    input_path: str
    input_sha256: str
    state: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return dict(
            cell_id=self.cell_id,
            position_id=self.position_id,
            task_id=self.task_id,
            benchmark=self.benchmark,
            method=self.method,
            graph=self.graph,
            category=self.category,
            ordinal=self.ordinal,
            full_flow_five=self.full_flow_five,
            generation_stage=self.generation_stage,
            protocol=self.protocol,
            public_input_sha256=self.input_sha256,
            body={"messages": [dict(m) for m in self.messages]},
            state=dict(self.state),
        )


@dataclass(frozen=True)
class NextStep:
    action: str
    request: RequestSpec | None = None
    state: dict = field(default_factory=dict)
    reason: str | None = None


def _messages(payload, benchmark):
    if benchmark in MESSAGE_BENCHMARKS:
        return tuple(dict(m) for m in payload["messages"])
    if benchmark in QUESTION_BENCHMARKS:
        return direct.compose("MindTheQuery", payload)
    raise ValueError("unsupported benchmark for feedback adapter: " + benchmark)


def prepare(positions_path, benchmark: str, cell_id: str, *, limit=None) -> list:
    rows = core.load_positions(positions_path, limit=limit)
    specs = []
    for row in rows:
        payload = core.verify_public_input(row)
        specs.append(
            RequestSpec(
                cell_id=cell_id,
                position_id=row["position_id"],
                task_id=row["task_id"],
                benchmark=benchmark,
                method="direct-feedback",
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                generation_stage="initial",
                protocol="chat",
                messages=_messages(payload, benchmark),
                input_path=row["public_input_path"],
                input_sha256=row["public_input_sha256"],
                state=dict(stage="initial"),
            )
        )
    return specs


def feedback_message(error_type: str, error_text: str, stage: str = "initial") -> str:
    return (
        "The previous query failed while executing on the graph.\n"
        "Database error (%s): %s\n"
        "Return one corrected query in a single Cypher statement."
        % (error_type, (error_text or "").strip()[:400])
    )


def next_request(spec: RequestSpec, reply: str, *, db_error=None, transport_unknown=False) -> NextStep:
    state = dict(spec.state)
    state.update(stage=spec.generation_stage, parent_request_id=spec.position_id)
    if transport_unknown:
        return NextStep("failed", None, state, "transport_unknown")
    if spec.generation_stage != "initial":
        return NextStep("complete", None, state, "feedback_already_used")
    if db_error is None:
        return NextStep("complete", None, state, "no_operational_error")
    error_type = getattr(db_error, "type", None) or type(db_error).__name__
    if error_type not in OPERATIONAL:
        return NextStep("complete", None, state, "non_operational_error")
    messages = tuple(spec.messages) + (
        dict(role="assistant", content=reply or ""),
        dict(role="user", content=feedback_message(error_type, str(db_error))),
    )
    state.update(stage="feedback", feedback_reason=error_type)
    return NextStep(
        "request",
        RequestSpec(
            cell_id=spec.cell_id,
            position_id=spec.position_id,
            task_id=spec.task_id,
            benchmark=spec.benchmark,
            method=spec.method,
            graph=spec.graph,
            category=spec.category,
            ordinal=spec.ordinal,
            full_flow_five=spec.full_flow_five,
            generation_stage="feedback",
            protocol="chat",
            messages=messages,
            input_path=spec.input_path,
            input_sha256=spec.input_sha256,
            state=state,
        ),
        state,
        None,
    )


def parse(reply: str, **kwargs):
    return direct.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return core.consume(positions, predictions, runtime)
