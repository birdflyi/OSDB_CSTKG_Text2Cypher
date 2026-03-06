from __future__ import annotations

from dataclasses import dataclass
from difflib import get_close_matches
import re

from data.schema import QueryExample
from repair.base import BaseRepairModule, RepairResult
from repair.diagnosis import DiagnosisItem, StructuredDiagnosis
from validators.cypher_validator import validate_cypher

_REL_TYPE_RE = re.compile(r"\[:([A-Za-z_][A-Za-z0-9_]*)\]")
_PROP_RE = re.compile(r"\.([A-Za-z_][A-Za-z0-9_]*)")
_EDGE_RE = re.compile(
    r"\([^)]+:(?P<src>[A-Za-z_][A-Za-z0-9_]*)\)\s*-\s*\[:(?P<rel>[A-Za-z_][A-Za-z0-9_]*)\]\s*->\s*\([^)]+:(?P<dst>[A-Za-z_][A-Za-z0-9_]*)\)"
)


@dataclass(frozen=True)
class RepairCandidate:
    cypher: str
    applied_edits: list[str]
    repair_cost: int


class LightweightRepairModule(BaseRepairModule):
    name = "lightweight_repair"

    def __init__(self, top_k: int = 5, max_edits: int = 2) -> None:
        self.top_k = top_k
        self.max_edits = max_edits

    def repair(self, example: QueryExample, cypher: str, errors: list[str]) -> RepairResult:
        diagnosis = self._diagnose(example, cypher, errors)
        candidates = self._search_repair_candidates(example, cypher, diagnosis)
        if not candidates:
            return RepairResult(
                cypher=cypher,
                changed=False,
                metadata={
                    "diagnosis": diagnosis.to_dict(),
                    "applied_edits": [],
                    "repair_cost": 0,
                    "repair_success": False,
                },
            )

        best = candidates[0]
        repair_success = best.cypher != cypher
        return RepairResult(
            cypher=best.cypher,
            changed=repair_success,
            metadata={
                "diagnosis": diagnosis.to_dict(),
                "applied_edits": list(best.applied_edits),
                "repair_cost": best.repair_cost,
                "repair_success": repair_success,
            },
        )

    def _diagnose(
        self, example: QueryExample, cypher: str, errors: list[str]
    ) -> StructuredDiagnosis:
        items: list[DiagnosisItem] = []
        constraints = example.expected_constraints
        allowed_props = set(constraints.allowed_properties)

        for error in errors:
            if error.startswith("disallowed_relationships:") or error == "missing_required_relationship":
                items.append(
                    DiagnosisItem(
                        error_type="relation",
                        error_location="relationship_pattern",
                        violated_constraint=error,
                        candidate_fix_types=[
                            "replace_relation_type",
                            "flip_relation_direction",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif error == "direction_constraint_violation":
                items.append(
                    DiagnosisItem(
                        error_type="direction",
                        error_location="relationship_pattern",
                        violated_constraint=error,
                        candidate_fix_types=[
                            "flip_relation_direction",
                            "replace_relation_type",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif error.startswith("disallowed_labels:"):
                items.append(
                    DiagnosisItem(
                        error_type="label",
                        error_location="node_pattern",
                        violated_constraint=error,
                        candidate_fix_types=["fallback_simpler_template"],
                    )
                )
            elif error.startswith("disallowed_properties:"):
                items.append(
                    DiagnosisItem(
                        error_type="property",
                        error_location="where_or_return_clause",
                        violated_constraint=error,
                        candidate_fix_types=[
                            "replace_property_nearest",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif error in {"missing_match", "missing_return", "empty_query"}:
                items.append(
                    DiagnosisItem(
                        error_type="path",
                        error_location="query_skeleton",
                        violated_constraint=error,
                        candidate_fix_types=["fallback_simpler_template"],
                    )
                )

        # Direction diagnostics inferred from current edge shape vs allowed direction constraints.
        if constraints.direction_constraints:
            allowed_directions = set(constraints.direction_constraints)
            for src, rel, dst in self._extract_edges(cypher):
                signature = f"{src}-[:{rel}]->{dst}"
                if signature not in allowed_directions:
                    reverse_signature = f"{dst}-[:{rel}]->{src}"
                    if reverse_signature in allowed_directions:
                        items.append(
                            DiagnosisItem(
                                error_type="direction",
                                error_location=f"{src}-[:{rel}]->{dst}",
                                violated_constraint=f"expected:{reverse_signature}",
                                candidate_fix_types=[
                                    "flip_relation_direction",
                                    "replace_relation_type",
                                ],
                            )
                        )

        # Path-length diagnostic: minimal heuristic based on number of relationships.
        if len(_REL_TYPE_RE.findall(cypher)) > 1:
            items.append(
                DiagnosisItem(
                    error_type="path",
                    error_location="match_path",
                    violated_constraint="path_too_long_for_template",
                    candidate_fix_types=["shorten_path_length", "fallback_simpler_template"],
                )
            )

        # If validator did not expose property error but properties are out of spec, still diagnose.
        invalid_props = [prop for prop in _PROP_RE.findall(cypher) if allowed_props and prop not in allowed_props]
        for prop in invalid_props:
            items.append(
                DiagnosisItem(
                    error_type="property",
                    error_location=f"property:{prop}",
                    violated_constraint=f"not_in_allowed_properties:{prop}",
                    candidate_fix_types=["replace_property_nearest", "fallback_simpler_template"],
                )
            )

        return StructuredDiagnosis(items=items)

    def _search_repair_candidates(
        self, example: QueryExample, cypher: str, diagnosis: StructuredDiagnosis
    ) -> list[RepairCandidate]:
        queue: list[RepairCandidate] = [RepairCandidate(cypher=cypher, applied_edits=[], repair_cost=0)]
        explored: set[str] = set()
        accepted: list[tuple[float, RepairCandidate]] = []

        while queue:
            current = queue.pop(0)
            if current.cypher in explored:
                continue
            explored.add(current.cypher)

            result = validate_cypher(example, current.cypher)
            score = self._candidate_score(cypher, current, result.valid)
            accepted.append((score, current))

            if len(current.applied_edits) >= self.max_edits:
                continue

            expansions = self._expand_once(example, current, diagnosis)
            for nxt in expansions[: self.top_k]:
                if nxt.cypher not in explored:
                    queue.append(nxt)

            if len(explored) >= (self.top_k * 6):
                break

        valid_candidates = [
            (score, cand)
            for score, cand in accepted
            if validate_cypher(example, cand.cypher).valid and cand.cypher != cypher
        ]
        if valid_candidates:
            valid_candidates.sort(key=lambda item: item[0], reverse=True)
            return [item[1] for item in valid_candidates[: self.top_k]]

        accepted.sort(key=lambda item: item[0], reverse=True)
        return [item[1] for item in accepted[:1] if item[1].cypher != cypher]

    def _expand_once(
        self, example: QueryExample, current: RepairCandidate, diagnosis: StructuredDiagnosis
    ) -> list[RepairCandidate]:
        fix_types: list[str] = []
        for item in diagnosis.items:
            for fix in item.candidate_fix_types:
                if fix not in fix_types:
                    fix_types.append(fix)

        expansions: list[RepairCandidate] = []
        for fix_type in fix_types:
            repaired = self._apply_fix_type(example, current.cypher, fix_type)
            if repaired is None or repaired == current.cypher:
                continue
            expansions.append(
                RepairCandidate(
                    cypher=repaired,
                    applied_edits=current.applied_edits + [fix_type],
                    repair_cost=current.repair_cost + 1,
                )
            )
        return expansions[: self.top_k]

    def _apply_fix_type(self, example: QueryExample, cypher: str, fix_type: str) -> str | None:
        if fix_type == "replace_relation_type":
            return self._replace_relation_type(example, cypher)
        if fix_type == "flip_relation_direction":
            return self._flip_relation_direction(cypher)
        if fix_type == "replace_property_nearest":
            return self._replace_property_nearest(example, cypher)
        if fix_type == "shorten_path_length":
            return self._shorten_path_length(cypher)
        if fix_type == "fallback_simpler_template":
            return self._fallback_simpler_template(example)
        return None

    def _replace_relation_type(self, example: QueryExample, cypher: str) -> str | None:
        allowed = example.expected_constraints.allowed_rel_types
        if not allowed:
            return None
        match = _REL_TYPE_RE.search(cypher)
        if not match:
            return None
        current = match.group(1)
        replacement = allowed[0] if allowed[0] != current else (allowed[1] if len(allowed) > 1 else None)
        if replacement is None:
            return None
        start, end = match.span(1)
        return cypher[:start] + replacement + cypher[end:]

    def _flip_relation_direction(self, cypher: str) -> str | None:
        if "-[" in cypher and "]->" in cypher:
            return cypher.replace("-[", "<-[", 1).replace("]->", "]-", 1)
        if "<-[" in cypher and "]-" in cypher:
            return cypher.replace("<-[", "-[", 1).replace("]-", "]->", 1)
        return None

    def _replace_property_nearest(self, example: QueryExample, cypher: str) -> str | None:
        allowed = example.expected_constraints.allowed_properties
        if not allowed:
            return None
        for prop in _PROP_RE.findall(cypher):
            if prop in allowed:
                continue
            nearest = get_close_matches(prop, allowed, n=1, cutoff=0.0)
            if not nearest:
                continue
            return cypher.replace(f".{prop}", f".{nearest[0]}", 1)
        return None

    def _shorten_path_length(self, cypher: str) -> str | None:
        rel_count = len(_REL_TYPE_RE.findall(cypher))
        if rel_count <= 1:
            return None
        # Keep only first hop and normalize to generic projection.
        match = re.search(
            r"(MATCH\s+\([^)]+\)\s*-\s*\[[^]]+\]\s*->\s*\([^)]+\))",
            cypher,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        return f"{match.group(1)} RETURN n LIMIT 25"

    def _fallback_simpler_template(self, example: QueryExample) -> str | None:
        labels = example.expected_constraints.allowed_node_labels
        if not labels:
            return None
        return f"MATCH (n:{labels[0]}) RETURN n LIMIT 25"

    def _candidate_score(self, original: str, cand: RepairCandidate, is_valid: bool) -> float:
        shape_penalty = self._shape_distance(original, cand.cypher)
        return (
            (1000.0 if is_valid else 0.0)
            - float(cand.repair_cost) * 3.0
            - shape_penalty
        )

    def _shape_distance(self, a: str, b: str) -> float:
        def sig(text: str) -> tuple[int, int, int, int]:
            upper = text.upper()
            return (
                upper.count("MATCH"),
                upper.count("WHERE"),
                upper.count("RETURN"),
                len(_REL_TYPE_RE.findall(text)),
            )

        sa = sig(a)
        sb = sig(b)
        return float(sum(abs(x - y) for x, y in zip(sa, sb)))

    def _extract_edges(self, cypher: str) -> list[tuple[str, str, str]]:
        out: list[tuple[str, str, str]] = []
        for match in _EDGE_RE.finditer(cypher):
            out.append((match.group("src"), match.group("rel"), match.group("dst")))
        return out
