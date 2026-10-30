"""Ours: plan generation, compile and execute for the five benchmarks (v24 M1/M2).

The model returns the v24 plan JSON; this module parses it, asks the injected
runtime to compile it with the benchmark's existing compiler (never a new one),
and only then executes. Unsupported plans keep their position in the full
denominator and do not count as correct answers.

Compilers stay in the runtime: the adapter never imports a driver and never
starts a graph service. Offline tests inject a fake compiler and a fake
executor; the real entry points are listed in ``backend_capabilities.json``.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import consumption, core, direct, plans  # noqa: E402
from common.errors import HARD_ERRORS, UnsupportedPlan, UnsupportedSelection
from common.prompts import DEFAULT_PROFILES, compose_ours


PROFILES = {
    "CypherBench": "cb-neo4j-eid-v1",
    "Text2GQL": "t2g-tugraph-4.5.2-v1",
    "OSDB-STKG": "osdb-observed-v2",
    "MindTheQuery": "mtq-native-v2",
    "ZOGRASCOPE": "zog-native-v2",
}


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
    profile_id: str

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
            profile_id=self.profile_id,
        )


def prepare(positions_path, benchmark: str, cell_id: str, *, limit=None, prompt_profiles=DEFAULT_PROFILES) -> list:
    rows = core.load_positions(positions_path, limit=limit)
    specs = []
    for row in rows:
        payload = core.verify_public_input(row)
        messages = compose_ours(benchmark, payload, prompt_profiles)
        specs.append(
            RequestSpec(
                cell_id=cell_id,
                position_id=row["position_id"],
                task_id=row["task_id"],
                benchmark=benchmark,
                method="ours",
                graph=row["graph"],
                category=row.get("category"),
                ordinal=row["ordinal"],
                full_flow_five=bool(row.get("full_flow_five")),
                generation_stage="initial",
                protocol="chat",
                messages=messages,
                input_path=row["public_input_path"],
                input_sha256=row["public_input_sha256"],
                profile_id=PROFILES.get(benchmark, "unknown"),
            )
        )
    return specs


def parse(reply: str, *, http_status=None, attempts=1, cost=None) -> consumption.Prediction:
    if http_status is not None and not 200 <= http_status < 300:
        return consumption.Prediction("unknown", None, reply or "", http_status, attempts, cost)
    result = plans.parse(reply)
    if result.status == "answer":
        return consumption.Prediction("ready", None, reply or "", http_status, attempts, cost)
    if result.status == "unsupported":
        return consumption.Prediction("unsupported", None, reply or "", http_status, attempts, cost)
    return consumption.Prediction("format_error", None, reply or "", http_status, attempts, cost)


def compile_plans(predictions, runtime, benchmark: str) -> dict:
    """Turn plan predictions into executable ones; unsupported plans stay counted."""
    compiled = {}
    for position_id, prediction in predictions.items():
        if prediction.status != "ready":
            compiled[position_id] = prediction
            continue
        result = plans.parse(prediction.raw)
        if result.status == "unsupported":
            compiled[position_id] = consumption.Prediction(
                "unsupported", None, prediction.raw, prediction.http_status,
                prediction.attempts, prediction.cost,
            )
            continue
        if result.status != "answer":
            compiled[position_id] = consumption.Prediction(
                "format_error", None, prediction.raw, prediction.http_status,
                prediction.attempts, prediction.cost,
            )
            continue
        try:
            piece = runtime.compile(result.plan, benchmark=benchmark, profile_id=PROFILES.get(benchmark))
        except (UnsupportedPlan, UnsupportedSelection):
            compiled[position_id] = consumption.Prediction(
                "unsupported", None, prediction.raw, prediction.http_status,
                prediction.attempts, prediction.cost,
            )
            continue
        except HARD_ERRORS:
            raise
        except Exception as exc:
            compiled[position_id] = consumption.Prediction(
                "run_error", None, prediction.raw, prediction.http_status,
                prediction.attempts, prediction.cost,
            )
            continue
        compiled[position_id] = consumption.Prediction(
            "ready",
            piece.get("query") if isinstance(piece, dict) else None,
            prediction.raw,
            prediction.http_status,
            prediction.attempts,
            prediction.cost,
            parameters=(piece.get("parameters") or {}) if isinstance(piece, dict) else {},
            compiled_plan=piece if not (isinstance(piece, dict) and "query" in piece) else None,
        )
    return compiled


def _benchmark(positions) -> str:
    if not positions:
        return ""
    first = positions[0]
    return first["benchmark"] if isinstance(first, dict) else getattr(first, "benchmark", "")


def consume(positions, predictions, runtime):
    return consumption.consume(positions, compile_plans(predictions, runtime, _benchmark(positions)), runtime)
