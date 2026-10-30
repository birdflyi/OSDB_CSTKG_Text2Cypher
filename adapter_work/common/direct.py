"""Prompt composition and reply classification for the direct adapters.

The three benchmark families differ only in how the frozen public input is laid
out, so one factory serves them; each cell keeps a thin ``adapter.py`` that
binds its benchmark name and position file.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from common import core
from common.prompts import compose_direct

FENCE = re.compile(r"```(?:cypher|gql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
REFUSAL = re.compile(
    r"^\s*(i can('t|not)|i am unable|unable to|as an ai|sorry,|i'm sorry)", re.IGNORECASE
)
ANSWER_HINT = re.compile(r"\b(match|optional match|return|with|call)\b", re.IGNORECASE)

MESSAGE_BENCHMARKS = ("Text2GQL", "OSDB-STKG")
QUESTION_BENCHMARKS = ("MindTheQuery", "ZOGRASCOPE")


def compose(benchmark: str, payload: dict) -> tuple:
    """Return chat messages for the frozen public input of one benchmark."""
    return compose_direct(benchmark, payload)


def prepare(positions_path, benchmark: str, *, limit=None) -> list:
    rows = core.load_positions(positions_path, limit=limit)
    specs = []
    for row in rows:
        payload = core.verify_public_input(row)
        specs.append(
            dict(
                position_id=row["position_id"],
                task_id=row["task_id"],
                benchmark=benchmark,
                method="direct",
                generation_stage="initial",
                protocol="chat",
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                messages=compose(benchmark, payload),
                input_path=row["public_input_path"],
                input_sha256=row["public_input_sha256"],
            )
        )
    return specs


def parse(reply: str, *, http_status=None, attempts=1, cost=None) -> core.Prediction:
    text = reply or ""
    if http_status is not None and not 200 <= http_status < 300:
        return core.Prediction("unknown", None, text, http_status, attempts, cost)
    stripped = text.strip()
    if not stripped:
        return core.Prediction("format_error", None, text, http_status, attempts, cost)
    if REFUSAL.search(stripped):
        return core.Prediction("refused", None, text, http_status, attempts, cost)
    if stripped.startswith("{"):
        try:
            document = json.loads(stripped)
        except ValueError:
            return core.Prediction("format_error", None, text, http_status, attempts, cost)
        status = document.get("status")
        if status in ("UNSUPPORTED", "INSUFFICIENT_EVIDENCE") and isinstance(document.get("reason"), str) and document["reason"].strip():
            return core.Prediction("unsupported" if status == "UNSUPPORTED" else "refused", None, text, http_status, attempts, cost)
        if status != "ANSWER" or not isinstance(document.get("query"), str):
            return core.Prediction("format_error", None, text, http_status, attempts, cost)
        stripped = document["query"]
    fenced = FENCE.search(stripped)
    candidate = (fenced.group(1) if fenced else stripped).strip().rstrip(";").strip()
    if not candidate or not ANSWER_HINT.search(candidate):
        return core.Prediction("format_error", None, text, http_status, attempts, cost)
    return core.Prediction("answered", candidate, text, http_status, attempts, cost)


def consume(positions, predictions, runtime) -> core.CellResult:
    return core.consume(positions, predictions, runtime)
