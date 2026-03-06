from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from data.models import GraphMetadata, QueryExample


@dataclass
class GenerationResult:
    cypher: str
    abstained: bool = False
    used_fallback: bool = False
    trace: dict[str, Any] = field(default_factory=dict)


class BaseGenerator:
    name = "base"

    def generate(
        self, example: QueryExample, graph_metadata: GraphMetadata
    ) -> GenerationResult:
        raise NotImplementedError

