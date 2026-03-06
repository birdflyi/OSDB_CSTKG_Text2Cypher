from __future__ import annotations

from data.schema import QueryExample
from generators.base import BaseGenerator, GenerationResult


class TemplateFirstGenerator(BaseGenerator):
    name = "template_first"

    def generate(self, example: QueryExample) -> GenerationResult:
        constraints = example.expected_constraints
        if not constraints.allowed_node_labels:
            return GenerationResult(
                cypher="",
                abstained=True,
                used_fallback=True,
                metadata={"strategy": self.name, "reason": "missing_labels"},
            )
        primary_label = constraints.allowed_node_labels[0]
        cypher = f"MATCH (n:{primary_label}) RETURN n LIMIT 25"
        # TODO(templates): Expand template catalog per query_type and slot filling logic.
        return GenerationResult(
            cypher=cypher, used_fallback=True, metadata={"strategy": self.name}
        )

