from __future__ import annotations

from data.models import GraphMetadata, QueryExample
from generators.base import BaseGenerator, GenerationResult


class TemplateFirstGenerator(BaseGenerator):
    name = "template_first"

    def generate(
        self, example: QueryExample, graph_metadata: GraphMetadata
    ) -> GenerationResult:
        labels = example.expected_constraints.allowed_node_labels
        if not labels:
            labels = sorted(graph_metadata.allowed_node_labels)
        if not labels:
            return GenerationResult(
                cypher="",
                abstained=True,
                used_fallback=True,
                trace={"strategy": self.name, "reason": "missing_candidate_labels"},
            )
        primary = labels[0]
        cypher = f"MATCH (n:{primary}) RETURN n LIMIT 25"
        # TODO(templates): Extend template inventory per query_type.
        return GenerationResult(
            cypher=cypher,
            used_fallback=True,
            trace={"strategy": self.name, "selected_template": "entity_retrieval"},
        )

