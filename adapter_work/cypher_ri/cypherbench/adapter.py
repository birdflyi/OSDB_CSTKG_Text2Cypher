"""M07-B01: Cypher-RI x CypherBench adapter under CONTRACT_V2.

The authors published the paper and an anonymous Hugging Face release but no
controller, so this protocol is *our* implementation. It follows CONTRACT_V2:

* stage ``schema_select`` stops at ``</json>``; a missing stop is a truncation,
  not a selection;
* stage ``answer_continue`` continues the same assistant prefix
  (prompt + stage-1 text + ``<schema>...</schema>``), it is not a second user
  question;
* invalid JSON or unknown names fall back to the full schema and record why;
  an explicit empty list is recorded as our own choice, not silently expanded;
* transport-unknown replies never produce a second stage.

No HTTP, no driver, no reference set.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.consumption import consume as consume_positions  # noqa: E402

SCHEMA_HEAD = "Graph schema:"
QUESTION_HEAD = "Question:"
JSON_BLOCK = re.compile(r"<json>(.*?)</json>", re.DOTALL | re.IGNORECASE)
JSON_OPEN = re.compile(r"<json>", re.IGNORECASE)
ANSWER_BLOCK = re.compile(r"<answer>(.*?)</answer>", re.DOTALL | re.IGNORECASE)
REFUSAL = re.compile(
    r"^\s*(i\s*(am|'m)\s+sorry|sorry[,.]|i\s+can('t|not)|i\s+am\s+unable|as an ai)",
    re.IGNORECASE,
)
CLAUSE = re.compile(
    r"\b(match|optional\s+match|unwind|return|with|call|create|merge|delete|set|remove)\b",
    re.IGNORECASE,
)


class IntegrityError(RuntimeError):
    """Frozen input bytes do not match the SHA recorded in the position list."""


@dataclass(frozen=True)
class Property:
    name: str
    type: str


@dataclass(frozen=True)
class Entity:
    label: str
    properties: tuple


@dataclass(frozen=True)
class Relation:
    label: str
    subject: str
    object: str
    properties: tuple
    direction: str = "->"


@dataclass(frozen=True)
class Schema:
    graph: str
    entities: tuple
    relations: tuple

    def compact_text(self) -> str:
        """Names only: labels and relation labels."""
        return "Labels: %s\nRelationships: %s" % (
            ", ".join(e.label for e in self.entities),
            ", ".join(r.label for r in self.relations),
        )

    def expanded_text(self, selection) -> str:
        """Selected entities/relations with property types and endpoints."""
        entities = [e for e in self.entities if not selection.labels or e.label in selection.labels]
        relations = [
            r for r in self.relations if not selection.relations or r.label in selection.relations
        ]
        lines = ["Labels:"]
        for entity in entities:
            props = ", ".join("%s:%s" % (p.name, p.type) for p in entity.properties)
            lines.append("  %s(%s)" % (entity.label, props))
        lines.append("Relationships:")
        for relation in relations:
            props = ", ".join("%s:%s" % (p.name, p.type) for p in relation.properties)
            lines.append(
                "  (%s)-[%s%s]->(%s)"
                % (relation.subject, relation.label, (" " + props) if props else "", relation.object)
            )
        return "\n".join(lines)


@dataclass(frozen=True)
class Selection:
    labels: tuple
    relations: tuple
    accepted: bool
    reason: str


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
    question: str = ""
    schema: Schema | None = None
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
            body=(
                {"messages": [dict(m) for m in self.messages]}
                if self.protocol == "chat"
                else {"prefix": self.messages[0]["content"]}
            ),
            state=dict(self.state),
        )


@dataclass(frozen=True)
class Prediction:
    status: str
    cypher: str | None = None
    raw: str = ""
    http_status: int | None = None
    attempts: int = 1
    cost: float | None = None
    fallback: bool = False

    def as_dict(self) -> dict:
        return dict(
            status=self.status,
            cypher=self.cypher,
            http_status=self.http_status,
            attempts=self.attempts,
            cost=self.cost,
            fallback=self.fallback,
        )


@dataclass(frozen=True)
class NextStep:
    action: str
    request: RequestSpec | None = None
    state: dict = field(default_factory=dict)
    reason: str | None = None


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def schema_from_message(content: str) -> Schema:
    start = content.index(SCHEMA_HEAD) + len(SCHEMA_HEAD)
    text = content[start:].lstrip()
    document, end = json.JSONDecoder().raw_decode(text)
    entities = []
    for entity in document.get("entities", []):
        props = tuple(Property(name, str(kind)) for name, kind in (entity.get("properties") or {}).items())
        entities.append(Entity(entity["label"], props))
    relations = []
    for relation in document.get("relations") or document.get("relationships") or []:
        props = tuple(Property(name, str(kind)) for name, kind in (relation.get("properties") or {}).items())
        relations.append(
            Relation(
                relation.get("label") or relation.get("type") or relation.get("name"),
                relation.get("subj_label") or relation.get("subject") or "",
                relation.get("obj_label") or relation.get("object") or "",
                props,
            )
        )
    return Schema(document.get("name", ""), tuple(entities), tuple(relations))


def first_prompt(question: str, schema: Schema) -> str:
    return (
        "You generate Cypher for a property graph.\n"
        "Think first, then choose the labels and relationships you need.\n"
        'Finish with <json>{"name": [...], "entities": [...], "relations": [...]}</json> '
        "and stop immediately after </json>.\n\n"
        "Schema (names only):\n%s\n\nQuestion: %s" % (schema.compact_text(), question)
    )


def continuation_prefix(prompt: str, stage1: str, schema: Schema, selection: Selection) -> str:
    return "%s%s<schema>\n%s\n</schema>\n" % (prompt, stage1, schema.expanded_text(selection))


def _names(value) -> tuple:
    if not isinstance(value, list):
        raise ValueError("selection field is not a list")
    names = []
    for item in value:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict) and isinstance(item.get("label"), str):
            names.append(item["label"])
        else:
            raise ValueError("selection element is neither a name nor a label object")
    return tuple(names)


def parse_selection(reply: str, schema: Schema) -> Selection:
    """Accept the paper protocol and the public-schema field names, nothing else."""
    text = reply or ""
    match = JSON_BLOCK.search(text)
    if not match:
        return Selection((), (), False, "missing_json_block")
    try:
        value = json.loads(match.group(1).strip())
    except ValueError:
        return Selection((), (), False, "invalid_json")
    if not isinstance(value, dict):
        return Selection((), (), False, "invalid_shape")
    try:
        if "name" in value or "entities" in value:
            labels = _names(value.get("entities", []))
            relations = _names(value.get("relations", []))
        else:
            labels = _names(value.get("labels", []))
            relations = _names(value.get("relationships", []))
    except ValueError as exc:
        return Selection((), (), False, "invalid_selection:" + str(exc))
    if not labels and not relations:
        return Selection((), (), False, "empty_selection")
    known_labels = {e.label for e in schema.entities}
    known_relations = {r.label for r in schema.relations}
    if not set(labels) <= known_labels or not set(relations) <= known_relations:
        return Selection((), (), False, "unknown_names")
    return Selection(labels, relations, True, "ok")


def _full_selection(schema: Schema) -> Selection:
    return Selection(
        tuple(e.label for e in schema.entities),
        tuple(r.label for r in schema.relations),
        False,
        "fallback_full_schema",
    )


def prepare(positions_path, *, limit=None, cell_id="M07-B01") -> list:
    document = json.loads(Path(positions_path).read_text(encoding="utf-8-sig"))
    rows = document["positions"]
    if limit is not None:
        rows = rows[:limit]
    specs = []
    for row in rows:
        path = Path(row["public_input_path"])
        if not path.is_file() or _sha(path) != row["public_input_sha256"]:
            raise IntegrityError("public input changed: " + str(path))
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if "gold" in payload or "reference" in payload:
            raise IntegrityError("public input leaks gold: " + str(path))
        content = payload["messages"][0]["content"]
        schema = schema_from_message(content)
        question = content.split(QUESTION_HEAD, 1)[-1].strip() if QUESTION_HEAD in content else content
        prompt = first_prompt(question, schema)
        specs.append(
            RequestSpec(
                cell_id=cell_id,
                position_id=row["position_id"],
                task_id=row["task_id"],
                benchmark=row.get("benchmark") or document["benchmark"],
                method=document.get("method") or "cypher-ri",
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                generation_stage="schema_select",
                protocol="chat",
                messages=(dict(role="user", content=prompt),),
                input_path=str(path),
                input_sha256=row["public_input_sha256"],
                question=question,
                schema=schema,
                state=dict(stage="schema_select", token_budget_remaining=None),
            )
        )
    return specs


def next_request(spec: RequestSpec, reply: str, *, transport_unknown=False) -> NextStep:
    """Derive stage 2 from a stage-1 reply; never guess past a bad stop."""
    state = dict(spec.state)
    state.update(stage="schema_select", parent_request_id=spec.position_id)
    if transport_unknown:
        return NextStep("failed", None, state, "transport_unknown")
    text = reply or ""
    if not JSON_BLOCK.search(text):
        if JSON_OPEN.search(text):
            return NextStep("failed", None, state, "truncated_before_stop")
        return NextStep("failed", None, state, "missing_json_block")
    selection = parse_selection(text, spec.schema)
    accepted = selection.accepted
    if selection.reason == "empty_selection":
        state["selection_note"] = "explicit_empty_selection"
    if not accepted and selection.reason not in ("empty_selection",):
        selection = _full_selection(spec.schema)
        state["fallback_reason"] = selection.reason
    state.update(
        stage="answer_continue",
        selection={"labels": list(selection.labels), "relations": list(selection.relations)},
        selection_accepted=accepted,
        selection_reason=selection.reason,
    )
    prefix = continuation_prefix(spec.messages[0]["content"], text, spec.schema, selection)
    request = RequestSpec(
        cell_id=spec.cell_id,
        position_id=spec.position_id,
        task_id=spec.task_id,
        benchmark=spec.benchmark,
        method=spec.method,
        graph=spec.graph,
        category=spec.category,
        ordinal=spec.ordinal,
        full_flow_five=spec.full_flow_five,
        generation_stage="answer_continue",
        protocol="completion_prefix",
        messages=(dict(role="assistant", content=prefix),),
        input_path=spec.input_path,
        input_sha256=spec.input_sha256,
        question=spec.question,
        schema=spec.schema,
        state=state,
    )
    return NextStep("request", request, state, None)


def parse(reply: str, *, http_status=None, attempts=1, cost=None, fallback=False) -> Prediction:
    text = reply or ""
    if http_status is not None and not 200 <= http_status < 300:
        return Prediction("unknown", None, text, http_status, attempts, cost, fallback)
    block = ANSWER_BLOCK.search(text)
    candidate = (block.group(1) if block else "").strip().rstrip(";").strip()
    if REFUSAL.search(candidate) and not CLAUSE.search(candidate):
        return Prediction("refused", None, text, http_status, attempts, cost, fallback)
    if not candidate or not CLAUSE.search(candidate):
        return Prediction("format_error", None, text, http_status, attempts, cost, fallback)
    return Prediction("ready", candidate, text, http_status, attempts, cost, fallback)


def consume(positions, predictions, runtime):
    return consume_positions(positions, predictions, runtime)
