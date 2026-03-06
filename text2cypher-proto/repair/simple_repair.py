from __future__ import annotations

from data.schema import QueryExample
from repair.base import BaseRepairModule, RepairResult


class SimpleRepairModule(BaseRepairModule):
    name = "simple_repair"

    def repair(self, example: QueryExample, cypher: str, errors: list[str]) -> RepairResult:
        repaired = cypher.strip()
        changed = False

        if not repaired:
            labels = example.expected_constraints.allowed_node_labels
            if labels:
                repaired = f"MATCH (n:{labels[0]}) RETURN n LIMIT 25"
                changed = True

        if repaired and "RETURN " not in repaired.upper():
            repaired = repaired.rstrip(";") + " RETURN n LIMIT 25"
            changed = True

        # TODO(repair-llm): Replace rule-based repair with targeted correction model.
        return RepairResult(
            cypher=repaired if repaired else cypher,
            changed=changed,
            metadata={"repair_errors_seen": errors},
        )

