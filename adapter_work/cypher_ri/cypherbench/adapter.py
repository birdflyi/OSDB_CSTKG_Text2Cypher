"""M07-B01: Cypher-RI x CypherBench adapter (offline preparation only).

Cypher-RI needs two model turns: pick a schema slice, then answer with the
selected slice expanded. The paper describes this protocol but publishes no
controller, so the protocol below is *our* implementation; only the weights are
the authors' (anonymous Hugging Face release).

``prepare`` builds turn 1, ``next_request`` derives turn 2 from a reply, and
``parse`` reads the final ``<answer>``. Nothing here sends HTTP or opens a DB.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA_HEAD = "Graph schema:"
QUESTION_HEAD = "Question:"
SELECTION_BLOCK = re.compile(r"<json>(.*?)</json>", re.DOTALL | re.IGNORECASE)
ANSWER_BLOCK = re.compile(r"<answer>(.*?)</answer>", re.DOTALL | re.IGNORECASE)
ANSWER_HINT = re.compile(r"\b(match|optional match|return|with|call)\b", re.IGNORECASE)

STATUSES = ("answered", "refused", "format_error", "run_error", "not_dispatched", "unknown")


class IntegrityError(RuntimeError):
    """Frozen input bytes do not match the SHA recorded in the position list."""


@dataclass(frozen=True)
class Schema:
    labels: tuple = ()
    relationships: tuple = ()
    properties: dict = field(default_factory=dict)

    def compact_text(self) -> str:
        return "Labels: %s\nRelationships: %s" % (
            ", ".join(self.labels),
            ", ".join(self.relationships),
        )

    def full_text(self, selection: dict) -> str:
        labels = selection.get("labels") or list(self.labels)
        rels = selection.get("relationships") or list(self.relationships)
        lines = ["Labels:"]
        for name in labels:
            lines.append("  %s(%s)" % (name, ", ".join(self.properties.get(name, ()))))
        lines.append("Relationships:")
        for name in rels:
            lines.append("  %s(%s)" % (name, ", ".join(self.properties.get(name, ()))))
        return "\n".join(lines)


@dataclass(frozen=True)
class RequestSpec:
    position_id: str
    task_id: str
    graph: str
    category: str | None
    ordinal: int
    full_flow_five: bool
    turn: int
    messages: tuple
    input_path: str
    input_sha256: str
    question: str = ""
    schema: Schema = field(default_factory=Schema, compare=False)

    def as_dict(self) -> dict:
        return dict(
            position_id=self.position_id,
            task_id=self.task_id,
            graph=self.graph,
            category=self.category,
            ordinal=self.ordinal,
            full_flow_five=self.full_flow_five,
            turn=self.turn,
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
    fallbacks: int
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
            fallbacks=self.fallbacks,
            rows=list(self.rows),
        )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def schema_from_message(content: str) -> Schema:
    start = content.index(SCHEMA_HEAD) + len(SCHEMA_HEAD)
    text = content[start:].lstrip()
    document, end = json.JSONDecoder().raw_decode(text)
    remainder = text[end:]
    labels, properties = [], {}
    for entity in document.get("entities", []):
        labels.append(entity["label"])
        properties[entity["label"]] = sorted((entity.get("properties") or {}).keys())
    relationships = []
    for relation in document.get("relations") or document.get("relationships") or []:
        name = relation.get("label") or relation.get("type") or relation.get("name")
        if name:
            relationships.append(name)
            properties[name] = sorted((relation.get("properties") or {}).keys())
    question = remainder.split(QUESTION_HEAD, 1)[-1].strip() if remainder else ""
    return Schema(labels=tuple(labels), relationships=tuple(relationships), properties=properties)


def first_prompt(question: str, schema: Schema) -> str:
    return (
        "You generate Cypher for a property graph.\n"
        "First think, then choose the labels and relationships you need.\n"
        'Output a JSON object <json>{"labels": [...], "relationships": [...]}</json>.\n\n'
        "Schema (names only):\n%s\n\nQuestion: %s" % (schema.compact_text(), question)
    )


def second_prompt(question: str, schema: Schema, selection: dict, reasoning: str) -> str:
    return (
        "The schema you selected is now expanded. Continue the same answer.\n"
        "Think, then finish with <answer>...</answer> containing one Cypher query.\n\n"
        "Full schema:\n%s\n\nQuestion: %s\n\nYour earlier reasoning:\n%s"
        % (schema.full_text(selection), question, reasoning)
    )


def parse_selection(reply: str, schema: Schema) -> tuple:
    block = SELECTION_BLOCK.search(reply or "")
    fallback = {"labels": list(schema.labels), "relationships": list(schema.relationships)}
    if not block:
        return fallback, False
    try:
        value = json.loads(block.group(1).strip())
    except ValueError:
        return fallback, False
    labels, rels = value.get("labels"), value.get("relationships")
    if not isinstance(labels, list) or not isinstance(rels, list):
        return fallback, False
    if not labels or not rels:
        return fallback, False
    if not set(labels) <= set(schema.labels) or not set(rels) <= set(schema.relationships):
        return fallback, False
    return {"labels": labels, "relationships": rels}, True


def prepare(positions_path, *, limit=None) -> list:
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
        question = content.split(QUESTION_HEAD, 1)[-1].strip()
        specs.append(
            RequestSpec(
                position_id=row["position_id"],
                task_id=row["task_id"],
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                turn=1,
                messages=(dict(role="user", content=first_prompt(question, schema)),),
                input_path=str(path),
                input_sha256=row["public_input_sha256"],
                question=question,
                schema=schema,
            )
        )
    return specs


def next_request(spec: RequestSpec, reply: str) -> tuple:
    """Return (turn-2 spec, accepted selection) or (None, False) when turn 1 failed."""
    if not (reply or "").strip():
        return None, False
    selection, accepted = parse_selection(reply, spec.schema)
    reasoning = SELECTION_BLOCK.sub("", reply).strip()
    turn2 = RequestSpec(
        position_id=spec.position_id,
        task_id=spec.task_id,
        graph=spec.graph,
        category=spec.category,
        ordinal=spec.ordinal,
        full_flow_five=spec.full_flow_five,
        turn=2,
        messages=(
            dict(
                role="user",
                content=second_prompt(spec.question, spec.schema, selection, reasoning),
            ),
        ),
        input_path=spec.input_path,
        input_sha256=spec.input_sha256,
        question=spec.question,
        schema=spec.schema,
    )
    return turn2, accepted


def parse(reply: str, *, http_status=None, attempts=1, cost=None, accepted=True) -> Prediction:
    text = reply or ""
    if http_status is not None and not 200 <= http_status < 300:
        return Prediction("unknown", None, text, http_status, attempts, cost, not accepted)
    block = ANSWER_BLOCK.search(text)
    candidate = (block.group(1) if block else "").strip().rstrip(";").strip()
    if not candidate or not ANSWER_HINT.search(candidate):
        return Prediction("format_error", None, text, http_status, attempts, cost, not accepted)
    return Prediction("answered", candidate, text, http_status, attempts, cost, not accepted)


def consume(positions, predictions, runtime) -> CellResult:
    counts = {name: 0 for name in STATUSES}
    counts["scored"] = 0
    attempts = 0
    costs = []
    fallbacks = 0
    rows = []
    for position in positions:
        prediction = predictions.get(position.position_id)
        if prediction is None:
            counts["not_dispatched"] += 1
            rows.append(dict(position_id=position.position_id, status="not_dispatched"))
            continue
        attempts += prediction.attempts
        costs.append(prediction.cost)
        fallbacks += 1 if prediction.fallback else 0
        if prediction.status != "answered":
            counts[prediction.status] = counts.get(prediction.status, 0) + 1
            rows.append(dict(position_id=position.position_id, status=prediction.status))
            continue
        runtime.save_answer(position.position_id, prediction.cypher, prediction.raw)
        lease = runtime.graph_lease(position.graph)
        try:
            records = runtime.db_client(lease, prediction.cypher)
        except Exception as exc:
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
                fallback=prediction.fallback,
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
        fallbacks=fallbacks,
        rows=tuple(rows),
    )
