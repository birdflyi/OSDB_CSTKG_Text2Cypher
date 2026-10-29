"""Shared Cypher-RI protocol for all five benchmarks (CONTRACT_V2 section 3).

Stage 1 chooses a schema slice and stops at ``</json>``; stage 2 continues the
same assistant prefix with ``<schema>...</schema>`` appended. The control code
is ours: the authors published the paper and an anonymous weight release, not a
controller. No HTTP, no driver, no reference set.

Public inputs differ in layout only: some carry chat ``messages`` with a JSON
document, others carry ``question`` and ``schema`` directly.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from common.consumption import consume as consume_positions

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


@dataclass(frozen=True)
class Schema:
    graph: str
    entities: tuple
    relations: tuple

    def compact_text(self) -> str:
        return "Labels: %s\nRelationships: %s" % (
            ", ".join(e.label for e in self.entities),
            ", ".join(r.label for r in self.relations),
        )

    def expanded_text(self, selection) -> str:
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


def _properties(items) -> tuple:
    return tuple(Property(name, str(kind)) for name, kind in (items or {}).items())


def schema_from_dict(document: dict) -> Schema:
    """Normalize the three public schema dialects into one Schema.

    * Neo4j-style: `entities`/`relations` (CypherBench, Text2GQL, OSDB)
    * MindTheQuery: `node_props`/`relationships`
    * ZOGRASCOPE: `classes`/`relations` with global `properties`
    """
    if "entities" in document:
        entities = [
            Entity(entity["label"], _properties(entity.get("properties")))
            for entity in document["entities"]
        ]
        relations = [
            Relation(
                relation.get("label") or relation.get("type") or relation.get("name"),
                relation.get("subj_label") or relation.get("subject") or "",
                relation.get("obj_label") or relation.get("object") or "",
                _properties(relation.get("properties")),
            )
            for relation in document.get("relations") or document.get("relationships") or []
        ]
    elif "node_props" in document:
        entities = [
            Entity(label, tuple(Property(item["property"], str(item.get("datatype", ""))) for item in props))
            for label, props in document["node_props"].items()
        ]
        relations = [
            Relation(item["type"], item.get("start", ""), item.get("end", ""), ())
            for item in document.get("relationships", [])
        ]
    else:
        entities = [Entity(label, ()) for label in document.get("classes", {})]
        relations = [
            Relation(name, spec.get("domain", ""), spec.get("range", ""), ())
            for name, spec in (document.get("relations") or {}).items()
        ]
    return Schema(document.get("name") or document.get("graph") or "", tuple(entities), tuple(relations))


def payload_of(row) -> dict:
    """Return the JSON document the frozen public input carries."""
    path = Path(row["public_input_path"])
    if not path.is_file() or _sha(path) != row["public_input_sha256"]:
        raise IntegrityError("public input changed: " + str(path))
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if "gold" in payload or "reference" in payload:
        raise IntegrityError("public input leaks gold: " + str(path))
    if "schema" in payload:
        return payload
    content = payload["messages"][0]["content"].lstrip()
    if content.startswith(SCHEMA_HEAD):
        content = content[len(SCHEMA_HEAD):].lstrip()
    document, end = json.JSONDecoder().raw_decode(content)
    remainder = content[end:].strip()
    if "question" not in document and remainder:
        document["question"] = remainder.split(QUESTION_HEAD, 1)[-1].strip()
    if "schema" not in document:
        document = {"schema": document, "question": document.get("question", "")}
    return document


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
        if "entities" in value:
            labels = _names(value.get("entities", []))
            relations = _names(value.get("relations", []))
        elif "name" in value:
            labels = _names(value.get("name", []))
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


def prepare(positions_path, benchmark: str, cell_id: str, *, limit=None) -> list:
    document = json.loads(Path(positions_path).read_text(encoding="utf-8-sig"))
    rows = document["positions"]
    if limit is not None:
        rows = rows[:limit]
    specs = []
    for row in rows:
        payload = payload_of(row)
        schema = schema_from_dict(payload["schema"])
        question = payload.get("question") or ""
        prompt = first_prompt(question, schema)
        specs.append(
            RequestSpec(
                cell_id=cell_id,
                position_id=row["position_id"],
                task_id=row["task_id"],
                benchmark=benchmark,
                method="cypher-ri",
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                generation_stage="schema_select",
                protocol="chat",
                messages=(dict(role="user", content=prompt),),
                input_path=row["public_input_path"],
                input_sha256=row["public_input_sha256"],
                question=question,
                schema=schema,
                state=dict(stage="schema_select", token_budget_remaining=None),
            )
        )
    return specs


def next_request(spec: RequestSpec, reply: str, *, transport_unknown=False) -> NextStep:
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
        state["fallback_reason"] = "empty_selection_explicit"
    if not accepted and selection.reason != "empty_selection":
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
