from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any

from data.schema import QueryExample
from eval.metrics import ExampleMetrics, aggregate_metrics
from generators.base import BaseGenerator
from repair.base import BaseRepairModule
from validators.cypher_validator import normalize_cypher, validate_cypher


@dataclass
class RunnerConfig:
    apply_repair: bool = False
    normalized_match: bool = True


class ExperimentRunner:
    def __init__(
        self,
        generator: BaseGenerator,
        config: RunnerConfig,
        repair_module: BaseRepairModule | None = None,
    ) -> None:
        self.generator = generator
        self.config = config
        self.repair_module = repair_module

    def run(self, examples: list[QueryExample]) -> dict[str, Any]:
        rows: list[ExampleMetrics] = []
        details: list[dict[str, Any]] = []

        for example in examples:
            t0 = perf_counter()
            generation = self.generator.generate(example)
            initial_validation = validate_cypher(example, generation.cypher)

            final_cypher = generation.cypher
            final_validation = initial_validation
            repair_attempted = 0.0
            repair_succeeded = 0.0
            repair_used = False
            repair_latency_ms = 0.0
            repair_cost = 0
            applied_edits: list[str] = []
            diagnosis: dict[str, Any] | None = None

            if (
                self.config.apply_repair
                and not initial_validation.valid
                and self.repair_module is not None
            ):
                repair_attempted = 1.0
                repair_t0 = perf_counter()
                repaired = self.repair_module.repair(
                    example, generation.cypher, initial_validation.errors
                )
                repair_latency_ms = (perf_counter() - repair_t0) * 1000.0
                repair_used = repaired.changed
                applied_edits = list(repaired.metadata.get("applied_edits", []))
                repair_cost = int(repaired.metadata.get("repair_cost", 0))
                diagnosis = repaired.metadata.get("diagnosis")
                if repaired.changed:
                    final_cypher = repaired.cypher
                    final_validation = validate_cypher(example, final_cypher)
                    if final_validation.valid:
                        repair_succeeded = 1.0

            latency_ms = (perf_counter() - t0) * 1000.0
            exec_acc = self._execution_accuracy(
                gold=example.gold_cypher, pred=final_cypher
            )
            row = ExampleMetrics(
                execution_accuracy=exec_acc,
                schema_validity=1.0 if final_validation.schema_valid else 0.0,
                invalid=0.0 if final_validation.valid else 1.0,
                abstain_or_fallback=1.0
                if generation.abstained or generation.used_fallback
                else 0.0,
                repair_attempted=repair_attempted,
                repair_succeeded=repair_succeeded,
                post_repair_schema_validity=(
                    1.0 if (repair_attempted and final_validation.schema_valid) else 0.0
                ),
                post_repair_execution_accuracy=(
                    exec_acc if repair_attempted else 0.0
                ),
                repair_latency_ms=repair_latency_ms,
                latency_ms=latency_ms,
            )
            rows.append(row)
            details.append(
                {
                    "id": example.id,
                    "nl_query": example.nl_query,
                    "generator": self.generator.name,
                    "generated_cypher": generation.generated_cypher,
                    "initial_cypher": generation.cypher,
                    "final_cypher": final_cypher,
                    "initial_valid": initial_validation.valid,
                    "final_valid": final_validation.valid,
                    "errors": final_validation.errors,
                    "repair_used": repair_used,
                    "repair_applied_edits": applied_edits,
                    "repair_cost": repair_cost,
                    "repair_success": bool(repair_succeeded),
                    "repair_added_latency_ms": repair_latency_ms,
                    "repair_diagnosis": diagnosis,
                    "latency_ms": latency_ms,
                    "execution_accuracy": exec_acc,
                    "generation_trace": generation.metadata.get("generation_trace"),
                }
            )

        summary = aggregate_metrics(rows)
        return {"summary": summary, "details": details}

    def _execution_accuracy(self, gold: str, pred: str) -> float:
        # TODO(neo4j-exec): Replace this proxy with true DB execution comparison.
        if self.config.normalized_match:
            return 1.0 if normalize_cypher(gold) == normalize_cypher(pred) else 0.0
        return 1.0 if gold.strip() == pred.strip() else 0.0
