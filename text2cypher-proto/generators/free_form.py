from __future__ import annotations

from data.schema import QueryExample
from generators.base import BaseGenerator, GenerationResult


class FreeFormGenerator(BaseGenerator):
    name = "free_form"

    def generate(self, example: QueryExample) -> GenerationResult:
        # TODO(neo4j/llm): Replace this placeholder with real free-form LLM decoding.
        # Baseline behavior: intentionally unconstrained and prone to schema drift.
        cypher = "MATCH (a:Entity)-[:RELATED_TO]->(b:Entity) RETURN a, b LIMIT 10"
        return GenerationResult(cypher=cypher, metadata={"strategy": self.name})
