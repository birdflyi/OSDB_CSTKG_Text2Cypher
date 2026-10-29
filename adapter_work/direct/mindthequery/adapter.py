"""M01-B03: direct x MindTheQuery adapter (offline preparation only).

CONTRACT_V2 shape. MindTheQuery public inputs carry ``question`` and ``schema``
instead of chat messages, so ``prepare`` composes one user message from the
benchmark prompt profile; everything else follows the shared contract.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.consumption import consume as consume_positions  # noqa: E402

FENCE = re.compile(r"```(?:cypher)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
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
            messages=[dict(m) for m in self.messages],
            public_input_sha256=self.input_sha256,
            body={"messages": [dict(m) for m in self.messages]},
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


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _prompt(question: str, schema) -> str:
    rendered = schema if isinstance(schema, str) else json.dumps(schema, ensure_ascii=False)
    return (
        "Graph schema:\n%s\n\nWrite one Cypher query that answers the question.\n"
        "Return only the Cypher statement.\n\nQuestion: %s" % (rendered, question)
    )


def prepare(positions_path, *, limit=None, cell_id="M01-B03") -> list:
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
        messages = (dict(role="user", content=_prompt(payload["question"], payload["schema"])),)
        specs.append(
            RequestSpec(
                cell_id=cell_id,
                position_id=row["position_id"],
                task_id=row["task_id"],
                benchmark=row.get("benchmark") or document["benchmark"],
                method=document.get("method") or "direct",
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                generation_stage="initial",
                protocol="chat",
                messages=messages,
                input_path=str(path),
                input_sha256=row["public_input_sha256"],
            )
        )
    return specs


def _extract(text: str) -> str:
    block = ANSWER_BLOCK.search(text)
    if block:
        return block.group(1).strip()
    fenced = FENCE.search(text)
    if fenced:
        return fenced.group(1).strip()
    return text.strip()


def parse(reply: str, *, http_status=None, attempts=1, cost=None) -> Prediction:
    text = reply or ""
    if http_status is not None and not 200 <= http_status < 300:
        return Prediction("unknown", None, text, http_status, attempts, cost)
    stripped = text.strip()
    if not stripped:
        return Prediction("format_error", None, text, http_status, attempts, cost)
    candidate = _extract(stripped).rstrip(";").strip()
    has_clause = bool(CLAUSE.search(candidate))
    if REFUSAL.search(stripped) and not has_clause:
        return Prediction("refused", None, text, http_status, attempts, cost)
    if not candidate or not has_clause:
        return Prediction("format_error", None, text, http_status, attempts, cost)
    return Prediction("ready", candidate, text, http_status, attempts, cost)


def consume(positions, predictions, runtime):
    return consume_positions(positions, predictions, runtime)
