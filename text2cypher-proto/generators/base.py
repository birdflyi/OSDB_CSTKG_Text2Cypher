from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from data.schema import QueryExample


@dataclass
class GenerationResult:
    cypher: str
    abstained: bool = False
    used_fallback: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def generated_cypher(self) -> str:
        return self.cypher


class BaseGenerator:
    name = "base"

    def generate(self, example: QueryExample) -> GenerationResult:
        raise NotImplementedError
