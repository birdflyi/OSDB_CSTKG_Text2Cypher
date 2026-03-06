from __future__ import annotations

from data.models import GraphMetadata, QueryExample
from generators.base import BaseGenerator, GenerationResult


class FreeFormGenerator(BaseGenerator):
    name = "free_form"

    def generate(
        self, example: QueryExample, graph_metadata: GraphMetadata
    ) -> GenerationResult:
        _ = example
        _ = graph_metadata
        # TODO(llm): Replace with actual free-form model call.
        return GenerationResult(
            cypher="MATCH (x:Entity)-[:RELATED_TO]->(y:Entity) RETURN x, y LIMIT 10",
            trace={"strategy": self.name, "note": "placeholder_free_form_decode"},
        )

