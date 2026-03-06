from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ExampleMetrics:
    execution_accuracy: float
    schema_validity: float
    invalid: float
    abstain_or_fallback: float
    repair_attempted: float
    repair_succeeded: float
    post_repair_schema_validity: float
    post_repair_execution_accuracy: float
    repair_latency_ms: float
    latency_ms: float


def aggregate_metrics(rows: list[ExampleMetrics]) -> dict[str, float]:
    if not rows:
        return {
            "execution_accuracy": 0.0,
            "schema_validity": 0.0,
            "invalid_rate": 0.0,
            "abstain_or_fallback_rate": 0.0,
            "repair_success_rate": 0.0,
            "post_repair_schema_validity": 0.0,
            "post_repair_execution_accuracy": 0.0,
            "repair_added_latency_ms": 0.0,
            "latency_ms": 0.0,
            "count": 0.0,
        }
    total = float(len(rows))
    repair_attempts = sum(r.repair_attempted for r in rows)
    repair_successes = sum(r.repair_succeeded for r in rows)
    return {
        "execution_accuracy": sum(r.execution_accuracy for r in rows) / total,
        "schema_validity": sum(r.schema_validity for r in rows) / total,
        "invalid_rate": sum(r.invalid for r in rows) / total,
        "abstain_or_fallback_rate": sum(r.abstain_or_fallback for r in rows) / total,
        "repair_success_rate": (
            repair_successes / repair_attempts if repair_attempts > 0 else 0.0
        ),
        "post_repair_schema_validity": (
            sum(r.post_repair_schema_validity for r in rows) / repair_attempts
            if repair_attempts > 0
            else 0.0
        ),
        "post_repair_execution_accuracy": (
            sum(r.post_repair_execution_accuracy for r in rows) / repair_attempts
            if repair_attempts > 0
            else 0.0
        ),
        "repair_added_latency_ms": (
            sum(r.repair_latency_ms for r in rows) / repair_attempts
            if repair_attempts > 0
            else 0.0
        ),
        "latency_ms": sum(r.latency_ms for r in rows) / total,
        "count": total,
    }
