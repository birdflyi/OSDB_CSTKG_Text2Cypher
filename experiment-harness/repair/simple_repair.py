from __future__ import annotations

import re

from data.models import GraphMetadata, QueryExample
from repair.base import BaseRepairModule, RepairResult

REL_PATTERN = re.compile(r"\[:([A-Za-z_][A-Za-z0-9_]*)\]")
NODE_LABEL_PATTERN = re.compile(r"\((?P<alias>[A-Za-z_][A-Za-z0-9_]*):(?P<label>[A-Za-z_][A-Za-z0-9_]*)")


class SimpleRuleRepair(BaseRepairModule):
    name = "simple_rule_repair"

    def repair(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        generated_cypher: str,
        validation_errors: list[str],
    ) -> RepairResult:
        cypher = generated_cypher.strip()
        edits: list[str] = []

        allowed_labels = (
            example.expected_constraints.allowed_node_labels
            or sorted(graph_metadata.allowed_node_labels)
        )
        allowed_rels = (
            example.expected_constraints.allowed_rel_types
            or sorted(graph_metadata.allowed_rel_types)
        )

        if not cypher and allowed_labels:
            cypher = f"MATCH (n:{allowed_labels[0]}) RETURN n LIMIT 25"
            edits.append("fallback_entity_template")

        if "missing_return" in validation_errors and cypher:
            cypher = cypher.rstrip(";") + " RETURN n LIMIT 25"
            edits.append("append_return")

        if any(e.startswith("disallowed_relationships") for e in validation_errors) and allowed_rels:
            rel_match = REL_PATTERN.search(cypher)
            if rel_match:
                old = rel_match.group(1)
                cypher = cypher.replace(f"[:{old}]", f"[:{allowed_rels[0]}]", 1)
                edits.append("replace_relation_type")

        if any(e.startswith("disallowed_labels") for e in validation_errors) and allowed_labels:
            label_match = NODE_LABEL_PATTERN.search(cypher)
            if label_match:
                old_label = label_match.group("label")
                cypher = cypher.replace(f":{old_label}", f":{allowed_labels[0]}", 1)
                edits.append("replace_node_label")

        return RepairResult(
            repaired_cypher=cypher if cypher else generated_cypher,
            changed=(len(edits) > 0),
            applied_edits=edits,
            repair_cost=len(edits),
            trace={"module": self.name},
        )
