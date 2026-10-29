"""Shared inference-side adapter for the two training-based author methods.

CypherSmith and CYQUARK are training/data methods: their paper contribution is
data generation plus fine-tuning, and neither ships a runnable solver. This
factory therefore covers only the thin inference adapter that the comparison
needs — compose the benchmark prompt, classify the reply, hand it to the shared
consumer. Training assets stay ``resource_pending`` in each cell's delivery.

Prompt choice is our engineering setting, not the authors' template: the frozen
V22 public input is used verbatim where it is a chat message, and composed from
``question``/``schema`` otherwise (same rule as the direct factory).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import core, direct  # noqa: E402

MESSAGE_BENCHMARKS = ("CypherBench", "Text2GQL", "OSDB-STKG")
QUESTION_BENCHMARKS = ("MindTheQuery", "ZOGRASCOPE")


def prepare(positions_path, benchmark: str, cell_id: str, method: str, *, limit=None) -> list:
    rows = core.load_positions(positions_path, limit=limit)
    specs = []
    for row in rows:
        payload = core.verify_public_input(row)
        if benchmark in MESSAGE_BENCHMARKS:
            messages = tuple(dict(message) for message in payload["messages"])
        elif benchmark in QUESTION_BENCHMARKS:
            messages = direct.compose("MindTheQuery", payload)
        else:
            raise ValueError("unsupported benchmark for author adapter: " + benchmark)
        specs.append(
            dict(
                cell_id=cell_id,
                position_id=row["position_id"],
                task_id=row["task_id"],
                benchmark=benchmark,
                method=method,
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                generation_stage="initial",
                protocol="chat",
                messages=messages,
                input_path=row["public_input_path"],
                input_sha256=row["public_input_sha256"],
                body={"messages": [dict(m) for m in messages]},
            )
        )
    return specs


def parse(reply: str, **kwargs):
    return direct.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return core.consume(positions, predictions, runtime)
