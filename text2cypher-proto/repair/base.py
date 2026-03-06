from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from data.schema import QueryExample


@dataclass
class RepairResult:
    cypher: str
    changed: bool
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def repaired_cypher(self) -> str:
        return self.cypher

    @property
    def applied_edits(self) -> list[str]:
        return list(self.metadata.get("applied_edits", []))

    @property
    def repair_cost(self) -> int:
        return int(self.metadata.get("repair_cost", 0))

    @property
    def repair_success(self) -> bool:
        return bool(self.metadata.get("repair_success", self.changed))


class BaseRepairModule:
    name = "base_repair"

    def repair(self, example: QueryExample, cypher: str, errors: list[str]) -> RepairResult:
        raise NotImplementedError
