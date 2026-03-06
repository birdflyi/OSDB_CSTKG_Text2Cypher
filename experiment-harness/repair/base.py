from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from data.models import GraphMetadata, QueryExample


@dataclass
class RepairResult:
    repaired_cypher: str
    changed: bool
    applied_edits: list[str] = field(default_factory=list)
    repair_cost: int = 0
    trace: dict[str, Any] = field(default_factory=dict)


class BaseRepairModule:
    name = "base_repair"

    def repair(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        generated_cypher: str,
        validation_errors: list[str],
    ) -> RepairResult:
        raise NotImplementedError

