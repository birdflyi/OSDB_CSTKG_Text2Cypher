from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any

from data.models import GraphMetadata, QueryExample
from generators.base import BaseGenerator
from metrics.core import ExampleMetrics, aggregate
from repair.base import BaseRepairModule
from validators.cypher_validator import normalize_cypher, validate_cypher


@dataclass
class RunnerConfig:
    apply_repair: bool = False
    normalized_match: bool = True


class ExperimentRunner:
    def __init__(
        self,
        graph_metadata: GraphMetadata,
        generator: BaseGenerator,
        config: RunnerConfig,
        repair_module: BaseRepairModule | None = None,
    ) -> None:
        self.graph_metadata = graph_metadata
        self.generator = generator
        self.config = config
        self.repair_module = repair_module

    def run(self, examples: list[QueryExample]) -> dict[str, Any]:
        rows: list[ExampleMetrics] = []
        details: list[dict[str, Any]] = []
        for example in examples:
            t0 = perf_counter()
            generated = self.generator.generate(example, self.graph_metadata)
            initial_validation = validate_cypher(
                example, generated.cypher, self.graph_metadata
            )
            final_cypher = generated.cypher
            final_validation = initial_validation
            repair_attempted = 0.0
            repair_succeeded = 0.0
            repair_trace: dict[str, Any] | None = None
            applied_edits: list[str] = []
            repair_cost = 0
            repair_added_latency_ms = 0.0

            if (
                self.config.apply_repair
                and not initial_validation.valid
                and self.repair_module is not None
            ):
                repair_attempted = 1.0
                repair_t0 = perf_counter()
                repaired = self.repair_module.repair(
                    example=example,
                    graph_metadata=self.graph_metadata,
                    generated_cypher=generated.cypher,
                    validation_errors=initial_validation.errors,
                )
                repair_added_latency_ms = (perf_counter() - repair_t0) * 1000.0
                repair_trace = repaired.trace
                applied_edits = repaired.applied_edits
                repair_cost = repaired.repair_cost
                if repaired.changed:
                    final_cypher = repaired.repaired_cypher
                    final_validation = validate_cypher(
                        example, final_cypher, self.graph_metadata
                    )
                    if final_validation.valid:
                        repair_succeeded = 1.0

            latency_ms = (perf_counter() - t0) * 1000.0
            execution_accuracy = self._execution_accuracy(example.gold_cypher, final_cypher)
            rows.append(
                ExampleMetrics(
                    execution_accuracy=execution_accuracy,
                    schema_validity=1.0 if final_validation.schema_valid else 0.0,
                    invalid=0.0 if final_validation.valid else 1.0,
                    abstain_or_fallback=1.0
                    if generated.abstained or generated.used_fallback
                    else 0.0,
                    repair_attempted=repair_attempted,
                    repair_succeeded=repair_succeeded,
                    post_repair_schema_validity=(
                        1.0 if (repair_attempted and final_validation.schema_valid) else 0.0
                    ),
                    post_repair_execution_accuracy=(
                        execution_accuracy if repair_attempted else 0.0
                    ),
                    repair_added_latency_ms=repair_added_latency_ms,
                    latency_ms=latency_ms,
                )
            )
            details.append(
                {
                    "id": example.id,
                    "query_type": example.query_type,
                    "nl_query": example.nl_query,
                    "generator": self.generator.name,
                    "gold_cypher": example.gold_cypher,
                    "generated_cypher": generated.cypher,
                    "final_cypher": final_cypher,
                    "initial_valid": initial_validation.valid,
                    "final_valid": final_validation.valid,
                    "initial_errors": initial_validation.errors,
                    "final_errors": final_validation.errors,
                    "execution_accuracy": execution_accuracy,
                    "latency_ms": latency_ms,
                    "generation_trace": generated.trace,
                    "repair_attempted": bool(repair_attempted),
                    "repair_success": bool(repair_succeeded),
                    "repair_added_latency_ms": repair_added_latency_ms,
                    "repair_trace": repair_trace,
                    "repair_applied_edits": applied_edits,
                    "repair_cost": repair_cost,
                }
            )
        return {"summary": aggregate(rows), "details": details}

    def _execution_accuracy(self, gold: str, predicted: str) -> float:
        # TODO(neo4j): Replace text match proxy with real DB execution equivalence check.
        if self.config.normalized_match:
            return 1.0 if normalize_cypher(gold) == normalize_cypher(predicted) else 0.0
        return 1.0 if gold.strip() == predicted.strip() else 0.0
