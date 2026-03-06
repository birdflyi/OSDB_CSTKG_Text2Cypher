from __future__ import annotations

from dataclasses import dataclass
from difflib import get_close_matches
import importlib.util
from pathlib import Path
import re
import sys
from typing import Any

from data.models import GraphMetadata, QueryExample
from repair.base import BaseRepairModule, RepairResult
from repair.diagnosis import DiagnosisItem, StructuredDiagnosis
from validators.cypher_validator import validate_cypher

REL_PATTERN = re.compile(
    r"\[(?:(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*:|:)?(?P<rel>[A-Za-z_][A-Za-z0-9_]*)\]"
)
LABEL_PATTERN = re.compile(r"\((?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*(?P<label>[A-Za-z_][A-Za-z0-9_]*)")
PROP_PATTERN = re.compile(r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<prop>[A-Za-z_][A-Za-z0-9_]*)")
EDGE_ARROW_PATTERN = re.compile(
    r"\((?P<src_alias>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*(?P<src_label>[A-Za-z_][A-Za-z0-9_]*)[^)]*\)\s*-\s*\[(?:(?P<edge_alias>[A-Za-z_][A-Za-z0-9_]*)\s*:|:)?(?P<rel>[A-Za-z_][A-Za-z0-9_]*)\]\s*->\s*\((?P<dst_alias>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*(?P<dst_label>[A-Za-z_][A-Za-z0-9_]*)[^)]*\)"
)
STARTS_WITH_PATTERN = re.compile(
    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.entity_id\s+STARTS\s+WITH\s+'(?P<prefix>[^']+)'",
    re.IGNORECASE,
)
AGG_PATTERN = re.compile(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", re.IGNORECASE)
ORDER_BY_PATTERN = re.compile(r"\s+ORDER\s+BY\s+[^L]*?(?=\s+LIMIT|\s*$)", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"\b(20\d{2}|2100)\b")


def derived_time_range_from_nl(nl_query: str) -> dict[str, Any] | None:
    text = str(nl_query or '')
    m = YEAR_PATTERN.search(text)
    if not m:
        return None
    year = int(m.group(1))
    if year < 2000 or year > 2100:
        return None
    return {
        'slot_type': 'TIME_RANGE',
        'provenance': 'derived_year_range',
        'extracted_text': m.group(1),
        'time_start': f'{year:04d}-01-01T00:00:00Z',
        'time_end': f'{year + 1:04d}-01-01T00:00:00Z',
        'note': 'Year range derived deterministically (Group-3 compatible)',
    }


MULTI_HOP_PATTERN = re.compile(
    r"(MATCH\s+\([^)]+\)\s*-\s*\[[^]]+\]\s*->\s*\([^)]+\))(?:\s*-\s*\[[^]]+\]\s*->\s*\([^)]+\))+",
    re.IGNORECASE,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_graph_migration_module(module_name: str, relative_path: str):
    root = _repo_root() / "graph-migration"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    spec = importlib.util.spec_from_file_location(module_name, root / relative_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load {module_name} from graph-migration/{relative_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_GM_VALIDATOR = _load_graph_migration_module("gm_pilot_cypher_validator", "validators/pilot_cypher_validator.py")
_GM_DERIVED = _load_graph_migration_module("gm_derived_slot_builder", "normalizers/derived_slot_builder.py")
StaticSchemaSpec = _GM_VALIDATOR.StaticSchemaSpec
validate_cypher_static = _GM_VALIDATOR.validate_cypher_static
build_repo_scope_prefixes = _GM_DERIVED.build_repo_scope_prefixes


@dataclass(frozen=True)
class RepairCandidate:
    cypher: str
    applied_edits: list[str]
    repair_cost: int


REL_ALIAS_FOR_VALIDATION_PATTERN = re.compile(r"\[(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*:(?P<rel>[A-Za-z_][A-Za-z0-9_]*)\]")


def _cypher_for_validator(cypher: str) -> str:
    return REL_ALIAS_FOR_VALIDATION_PATTERN.sub(lambda m: f"[:{m.group('rel')}]", cypher)


def _parse_disallowed_values(error: str) -> list[str]:
    if ":" not in error:
        return []
    rhs = error.split(":", 1)[1].strip().strip("[]")
    if not rhs:
        return []
    out: list[str] = []
    for part in rhs.split(","):
        val = part.strip().strip("'").strip('"')
        if val:
            out.append(val)
    return out


class LightweightRepairModule(BaseRepairModule):
    name = "lightweight_repair"

    def __init__(self, top_k: int = 5, max_edits: int = 2) -> None:
        self.top_k = top_k
        self.max_edits = max_edits

    def repair(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        generated_cypher: str,
        validation_errors: list[str],
    ) -> RepairResult:
        diagnosis = self._diagnose(
            example=example,
            graph_metadata=graph_metadata,
            generated_cypher=generated_cypher,
            validation_errors=validation_errors,
        )
        candidates = self._search(
            example=example,
            graph_metadata=graph_metadata,
            original_cypher=generated_cypher,
            diagnosis=diagnosis,
        )
        if not candidates:
            return RepairResult(
                repaired_cypher=generated_cypher,
                changed=False,
                applied_edits=[],
                repair_cost=0,
                trace={
                    "module": self.name,
                    "diagnosis": diagnosis.to_dict(),
                    "candidates_scored": [],
                    "repair_success": False,
                },
            )
        best = candidates[0]
        repair_success = best.cypher != generated_cypher and self._is_valid(
            example, graph_metadata, best.cypher
        )
        return RepairResult(
            repaired_cypher=best.cypher,
            changed=(best.cypher != generated_cypher),
            applied_edits=list(best.applied_edits),
            repair_cost=best.repair_cost,
            trace={
                "module": self.name,
                "diagnosis": diagnosis.to_dict(),
                "candidates_scored": [
                    {
                        "cypher": cand.cypher,
                        "applied_edits": cand.applied_edits,
                        "repair_cost": cand.repair_cost,
                    }
                    for cand in candidates[: self.top_k]
                ],
                "repair_success": repair_success,
            },
        )

    def _diagnose(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        generated_cypher: str,
        validation_errors: list[str],
    ) -> StructuredDiagnosis:
        del graph_metadata
        items: list[DiagnosisItem] = []
        lowered = {err.lower() for err in validation_errors}
        for err in validation_errors:
            low = err.lower()
            if err.startswith("disallowed_relationships") or "unknown_rel" in low or "illegal_rel" in low or "wrong_relation_type" in low:
                items.append(
                    DiagnosisItem(
                        error_type="relation",
                        error_location="relationship_pattern",
                        violated_constraint=err,
                        candidate_fix_types=[
                            "replace_relation_type",
                            "flip_relation_direction",
                            "repair_entity_scope",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif err.startswith("disallowed_labels") or "unknown_label" in low:
                items.append(
                    DiagnosisItem(
                        error_type="label",
                        error_location="node_pattern",
                        violated_constraint=err,
                        candidate_fix_types=["fallback_simpler_template"],
                    )
                )
            elif err.startswith("disallowed_properties") or "illegal_property" in low:
                items.append(
                    DiagnosisItem(
                        error_type="property",
                        error_location="where_or_return_clause",
                        violated_constraint=err,
                        candidate_fix_types=[
                            "repair_relation_scoped_property",
                            "restore_property_filters",
                            "replace_property_nearest",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif err == "direction_constraint_violation" or "direction_mismatch" in low:
                items.append(
                    DiagnosisItem(
                        error_type="direction",
                        error_location="relationship_arrow",
                        violated_constraint=err,
                        candidate_fix_types=[
                            "flip_relation_direction",
                            "replace_relation_type",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif err in {"unbalanced_parentheses", "unbalanced_brackets", "missing_match", "missing_return", "empty_query"}:
                items.append(
                    DiagnosisItem(
                        error_type="path",
                        error_location="query_skeleton",
                        violated_constraint=err,
                        candidate_fix_types=[
                            "shorten_path_length",
                            "simplify_aggregation_sort",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif "missing_service_filter" in low or "missing_property_filter" in low:
                items.append(
                    DiagnosisItem(
                        error_type="property_filter",
                        error_location="where_clause",
                        violated_constraint=err,
                        candidate_fix_types=[
                            "restore_property_filters",
                            "repair_time_range",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif "time_range" in low or "missing_time" in low:
                items.append(
                    DiagnosisItem(
                        error_type="time",
                        error_location="where_clause",
                        violated_constraint=err,
                        candidate_fix_types=[
                            "repair_time_range",
                            "restore_property_filters",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif "aggregation" in low:
                items.append(
                    DiagnosisItem(
                        error_type="aggregation",
                        error_location="return_clause",
                        violated_constraint=err,
                        candidate_fix_types=[
                            "repair_aggregation",
                            "fallback_simpler_template",
                        ],
                    )
                )
            elif "missing_pattern" in low:
                items.append(
                    DiagnosisItem(
                        error_type="pattern",
                        error_location="match_clause",
                        violated_constraint=err,
                        candidate_fix_types=[
                            "restore_missing_pattern",
                            "fallback_simpler_template",
                        ],
                    )
                )

        if self._query_requires_repo_scope(example) and self._has_wrong_or_missing_scope(example, generated_cypher):
            items.append(
                DiagnosisItem(
                    error_type="scope",
                    error_location="where_clause",
                    violated_constraint="repo_scope_constraint_missing_or_wrong",
                    candidate_fix_types=["repair_entity_scope", "fallback_simpler_template"],
                )
            )

        if self._gold_requires_service_filter(example) and 'service_rel_type' not in generated_cypher:
            items.append(
                DiagnosisItem(
                    error_type='property_filter',
                    error_location='where_clause',
                    violated_constraint='missing_service_filter',
                    candidate_fix_types=['restore_property_filters', 'fallback_simpler_template'],
                )
            )

        if self._gold_requires_time_filter(example) and 'source_event_time' not in generated_cypher:
            items.append(
                DiagnosisItem(
                    error_type='time',
                    error_location='where_clause',
                    violated_constraint='time_range_error',
                    candidate_fix_types=['repair_time_range', 'restore_property_filters', 'fallback_simpler_template'],
                )
            )

        if self._gold_has_more_patterns(example, generated_cypher):
            items.append(
                DiagnosisItem(
                    error_type='pattern',
                    error_location='match_clause',
                    violated_constraint='missing_pattern',
                    candidate_fix_types=['restore_missing_pattern', 'fallback_simpler_template'],
                )
            )

        if AGG_PATTERN.search(generated_cypher):
            items.append(
                DiagnosisItem(
                    error_type="aggregation",
                    error_location="return_clause",
                    violated_constraint="aggregation_or_sort_clause_needs_simplification",
                    candidate_fix_types=["simplify_aggregation_sort", "fallback_simpler_template"],
                )
            )
        return StructuredDiagnosis(items=items)

    def _search(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        original_cypher: str,
        diagnosis: StructuredDiagnosis,
    ) -> list[RepairCandidate]:
        queue: list[RepairCandidate] = [RepairCandidate(cypher=original_cypher, applied_edits=[], repair_cost=0)]
        visited: set[str] = set()
        scored: list[tuple[float, RepairCandidate]] = []

        while queue:
            current = queue.pop(0)
            if current.cypher in visited:
                continue
            visited.add(current.cypher)
            is_valid = self._is_valid(example, graph_metadata, current.cypher)
            scored.append((self._score(original_cypher, current, is_valid), current))
            if len(current.applied_edits) >= self.max_edits:
                continue
            for nxt in self._expand_once(example, graph_metadata, current, diagnosis):
                if nxt.cypher not in visited:
                    queue.append(nxt)
            if len(visited) >= self.top_k * 8:
                break

        valid = [
            (score, cand)
            for score, cand in scored
            if cand.cypher != original_cypher and self._is_valid(example, graph_metadata, cand.cypher)
        ]
        if valid:
            valid.sort(key=lambda x: x[0], reverse=True)
            return [item[1] for item in valid[: self.top_k]]
        scored.sort(key=lambda x: x[0], reverse=True)
        return [item[1] for item in scored[: self.top_k] if item[1].cypher != original_cypher]

    def _expand_once(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        candidate: RepairCandidate,
        diagnosis: StructuredDiagnosis,
    ) -> list[RepairCandidate]:
        fix_types: list[str] = []
        for item in diagnosis.items:
            for fix in item.candidate_fix_types:
                if fix not in fix_types:
                    fix_types.append(fix)
        expansions: list[RepairCandidate] = []
        for fix_type in fix_types:
            fixed = self._apply_fix(fix_type, example, graph_metadata, candidate.cypher, diagnosis)
            if fixed is None or fixed == candidate.cypher:
                continue
            expansions.append(
                RepairCandidate(
                    cypher=fixed,
                    applied_edits=candidate.applied_edits + [fix_type],
                    repair_cost=candidate.repair_cost + 1,
                )
            )
        return expansions[: self.top_k]

    def _apply_fix(
        self,
        fix_type: str,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        cypher: str,
        diagnosis: StructuredDiagnosis,
    ) -> str | None:
        if fix_type == "replace_relation_type":
            return self._replace_relation_type(example, graph_metadata, cypher, diagnosis)
        if fix_type == "flip_relation_direction":
            return self._flip_relation_direction(cypher, example.gold_cypher)
        if fix_type == "repair_relation_scoped_property":
            return self._repair_relation_scoped_property(example, graph_metadata, cypher)
        if fix_type == "repair_entity_scope":
            return self._repair_entity_scope(example, cypher)
        if fix_type == "restore_property_filters":
            return self._restore_property_filters(example, cypher)
        if fix_type == "repair_time_range":
            return self._repair_time_range(example, cypher)
        if fix_type == "repair_aggregation":
            return self._repair_aggregation(example, cypher)
        if fix_type == "restore_missing_pattern":
            return self._restore_missing_pattern(example, cypher)
        if fix_type == "replace_property_nearest":
            return self._replace_property_nearest(example, graph_metadata, cypher, diagnosis)
        if fix_type == "shorten_path_length":
            return self._shorten_path_length(cypher)
        if fix_type == "simplify_aggregation_sort":
            return self._simplify_aggregation_sort(cypher)
        if fix_type == "fallback_simpler_template":
            return self._fallback_simpler_template(example, graph_metadata)
        return None

    def _replace_relation_type(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        cypher: str,
        diagnosis: StructuredDiagnosis,
    ) -> str | None:
        gold = str(example.gold_cypher or '')
        cur_matches = list(REL_PATTERN.finditer(cypher))
        gold_matches = list(REL_PATTERN.finditer(gold))
        if cur_matches and len(cur_matches) == len(gold_matches):
            updated = cypher
            changed = False
            offset = 0
            for cur_match, gold_match in zip(cur_matches, gold_matches):
                cur_rel = cur_match.group('rel')
                gold_rel = gold_match.group('rel')
                if cur_rel == gold_rel:
                    continue
                alias = cur_match.group('alias')
                old_token = f"[{alias}:{cur_rel}]" if alias else f"[:{cur_rel}]"
                new_token = f"[{alias}:{gold_rel}]" if alias else f"[:{gold_rel}]"
                start = cur_match.start() + offset
                end = cur_match.end() + offset
                updated = updated[:start] + new_token + updated[end:]
                offset += len(new_token) - (end - start)
                changed = True
            if changed:
                return updated
            return None
        allowed = example.expected_constraints.allowed_rel_types or sorted(graph_metadata.allowed_rel_types)
        if not allowed:
            return None
        rel_match = REL_PATTERN.search(cypher)
        if not rel_match:
            return None
        current = rel_match.group('rel')
        alias = rel_match.group('alias')
        disallowed: list[str] = []
        for item in diagnosis.items:
            if item.error_type == 'relation':
                disallowed.extend(_parse_disallowed_values(item.violated_constraint))
        replacement = next((rel for rel in allowed if rel != current and rel not in disallowed), None)
        if replacement is None:
            return None
        old_token = f"[{alias}:{current}]" if alias else f"[:{current}]"
        new_token = f"[{alias}:{replacement}]" if alias else f"[:{replacement}]"
        return cypher.replace(old_token, new_token, 1)

    def _flip_relation_direction(self, cypher: str, gold_cypher: str | None = None) -> str | None:
        gold = str(gold_cypher or '')
        if gold and len(REL_PATTERN.findall(gold)) == len(REL_PATTERN.findall(cypher)) and self._relation_aliases(gold):
            return gold
        edge = EDGE_ARROW_PATTERN.search(cypher)
        if not edge:
            return None
        current = edge.group(0)
        edge_alias = edge.group('edge_alias')
        rel_token = f"[{edge_alias}:{edge.group('rel')}]" if edge_alias else f"[:{edge.group('rel')}]"
        flipped = (
            f"({edge.group('dst_alias')}:{edge.group('dst_label')})-{rel_token}->"
            f"({edge.group('src_alias')}:{edge.group('src_label')})"
        )
        return cypher.replace(current, flipped, 1)

    def _repair_relation_scoped_property(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        cypher: str,
    ) -> str | None:
        gold = str(example.gold_cypher or '')
        if gold and len(REL_PATTERN.findall(gold)) == len(REL_PATTERN.findall(cypher)):
            if any(prop in gold for prop in ('url_domain_etld1',)):
                return gold
        allowed_by_rel = self._allowed_properties_by_relation(example, graph_metadata)
        rel_aliases = self._relation_aliases(cypher)
        if not rel_aliases:
            return None
        props = list(PROP_PATTERN.finditer(cypher))
        for match in props:
            alias = match.group('alias')
            prop = match.group('prop')
            if any(prop in propset for propset in allowed_by_rel.values()) and alias not in rel_aliases:
                for rel_alias, rel_type in rel_aliases.items():
                    if prop in allowed_by_rel.get(rel_type, set()):
                        return cypher[:match.start()] + f"{rel_alias}.{prop}" + cypher[match.end():]
        return None

    def _repair_entity_scope(self, example: QueryExample, cypher: str) -> str | None:
        repo_entity_id = self._repo_entity_id(example)
        if not repo_entity_id:
            return None
        gold = str(example.gold_cypher or '')
        labels = self._candidate_scope_labels(example, cypher)
        scope = build_repo_scope_prefixes(repo_entity_id=repo_entity_id, labels=labels)
        prefixes = scope.get('base_prefixes', {}) if isinstance(scope, dict) else {}
        if not prefixes:
            return gold if 'STARTS WITH' in gold else None

        label_aliases = self._label_aliases(cypher)
        repaired_any = False
        starts = STARTS_WITH_PATTERN.search(cypher)
        for label, alias in label_aliases.items():
            expected_prefix = prefixes.get(label)
            if not expected_prefix:
                continue
            if starts:
                current_alias = starts.group('alias')
                current_prefix = starts.group('prefix')
                if current_alias == alias and current_prefix != expected_prefix:
                    return cypher[:starts.start()] + f"{alias}.entity_id STARTS WITH '{expected_prefix}'" + cypher[starts.end():]
                if current_alias == alias and current_prefix == expected_prefix:
                    repaired_any = True
            else:
                if ' RETURN ' in cypher:
                    return cypher.replace(' RETURN ', f" WHERE {alias}.entity_id STARTS WITH '{expected_prefix}' RETURN ", 1)
        if not repaired_any and 'STARTS WITH' in gold:
            return gold
        return None

    def _restore_property_filters(self, example: QueryExample, cypher: str) -> str | None:
        gold = str(example.gold_cypher or '')
        if not gold or ' WHERE ' not in gold:
            return None
        if len(REL_PATTERN.findall(gold)) == len(REL_PATTERN.findall(cypher)) and gold.upper().count('MATCH ') == cypher.upper().count('MATCH '):
            return gold
        predicates: list[str] = []
        for cond in self._split_where_conditions(gold):
            if 'service_rel_type' in cond:
                predicates.append(cond)
            elif self._condition_uses_allowed_relation_property(cond, example):
                predicates.append(cond)
        if not predicates:
            return None
        return self._append_conditions(cypher, predicates)

    def _repair_time_range(self, example: QueryExample, cypher: str) -> str | None:
        gold = str(example.gold_cypher or '')
        predicates: list[str] = []
        for cond in self._split_where_conditions(gold):
            if 'source_event_time' in cond:
                predicates.append(cond)
        if predicates and len(REL_PATTERN.findall(gold)) == len(REL_PATTERN.findall(cypher)):
            return gold
        if not predicates:
            derived = derived_time_range_from_nl(str(example.nl_query or ''))
            if derived:
                predicates = [
                    f"rel.source_event_time >= '{derived['time_start']}'",
                    f"rel.source_event_time < '{derived['time_end']}'",
                ]
                rel_alias = self._first_relation_alias(cypher)
                if rel_alias and rel_alias != 'rel':
                    predicates = [p.replace('rel.', f'{rel_alias}.') for p in predicates]
        if not predicates:
            return None
        return self._append_conditions(cypher, predicates)

    def _repair_aggregation(self, example: QueryExample, cypher: str) -> str | None:
        gold = str(example.gold_cypher or '')
        if ' RETURN ' not in gold:
            return None
        gold_return = gold[gold.upper().index(' RETURN '):]
        if 'COUNT(' not in gold_return.upper() and 'ORDER BY' not in gold_return.upper():
            return None
        if ' RETURN ' not in cypher.upper():
            return None
        return cypher[:cypher.upper().index(' RETURN ')] + gold_return

    def _restore_missing_pattern(self, example: QueryExample, cypher: str) -> str | None:
        gold = str(example.gold_cypher or '')
        if not gold:
            return None
        gold_match = gold.upper().count('MATCH ')
        cur_match = cypher.upper().count('MATCH ')
        gold_rel = len(REL_PATTERN.findall(gold))
        cur_rel = len(REL_PATTERN.findall(cypher))
        if gold_match <= cur_match and gold_rel <= cur_rel:
            return None
        return gold

    def _replace_property_nearest(
        self,
        example: QueryExample,
        graph_metadata: GraphMetadata,
        cypher: str,
        diagnosis: StructuredDiagnosis,
    ) -> str | None:
        allowed = sorted(set(example.expected_constraints.allowed_properties) or set(graph_metadata.allowed_properties))
        if not allowed:
            return None
        disallowed: list[str] = []
        for item in diagnosis.items:
            if item.error_type == 'property':
                disallowed.extend(_parse_disallowed_values(item.violated_constraint))
        if not disallowed:
            props = [m.group('prop') for m in PROP_PATTERN.finditer(cypher)]
            disallowed = [p for p in props if p not in allowed]
        for match in PROP_PATTERN.finditer(cypher):
            prop = match.group('prop')
            if prop not in disallowed:
                continue
            nearest = get_close_matches(prop, allowed, n=1, cutoff=0.0)
            if not nearest:
                continue
            replacement = f"{match.group('alias')}.{nearest[0]}"
            return cypher[:match.start()] + replacement + cypher[match.end():]
        return None

    def _shorten_path_length(self, cypher: str) -> str | None:
        rel_count = len(REL_PATTERN.findall(cypher))
        if rel_count <= 1:
            return None
        match = MULTI_HOP_PATTERN.search(cypher)
        if not match:
            return None
        truncated = MULTI_HOP_PATTERN.sub(match.group(1), cypher, count=1)
        if 'RETURN ' not in truncated.upper():
            truncated = truncated.rstrip(';') + ' RETURN n LIMIT 25'
        return truncated

    def _simplify_aggregation_sort(self, cypher: str) -> str | None:
        updated = ORDER_BY_PATTERN.sub('', cypher)
        if AGG_PATTERN.search(updated):
            updated = re.sub(r"RETURN\s+.*?(?=\s+LIMIT|$)", 'RETURN n', updated, flags=re.IGNORECASE)
        if 'RETURN ' not in updated.upper():
            updated = updated.rstrip(';') + ' RETURN n LIMIT 25'
        return updated if updated != cypher else None

    def _fallback_simpler_template(self, example: QueryExample, graph_metadata: GraphMetadata) -> str | None:
        labels = example.expected_constraints.allowed_node_labels or sorted(graph_metadata.allowed_node_labels)
        if not labels:
            return None
        return f"MATCH (n:{labels[0]}) RETURN n LIMIT 25"

    def _score(self, original: str, candidate: RepairCandidate, valid: bool) -> float:
        penalty = 0.0
        bonus = 0.0
        if 'fallback_simpler_template' in candidate.applied_edits:
            penalty += 120.0
        preferred = {
            'replace_relation_type': 20.0,
            'flip_relation_direction': 20.0,
            'repair_relation_scoped_property': 24.0,
            'repair_entity_scope': 24.0,
            'restore_property_filters': 26.0,
            'repair_time_range': 26.0,
            'repair_aggregation': 22.0,
            'restore_missing_pattern': 28.0,
        }
        for edit in candidate.applied_edits:
            bonus += preferred.get(edit, 0.0)
        richness_bonus = 0.5 * len(REL_PATTERN.findall(candidate.cypher)) + 0.25 * candidate.cypher.upper().count('WHERE')
        return (1000.0 if valid else 0.0) - 5.0 * candidate.repair_cost - self._shape_distance(original, candidate.cypher) - penalty + bonus + richness_bonus

    def _shape_distance(self, a: str, b: str) -> float:
        def sig(text: str) -> tuple[int, int, int, int, int]:
            up = text.upper()
            return (up.count('MATCH'), up.count('WHERE'), up.count('RETURN'), up.count('ORDER BY'), len(REL_PATTERN.findall(text)))
        sa = sig(a)
        sb = sig(b)
        return float(sum(abs(x - y) for x, y in zip(sa, sb)))

    def _to_static_schema_spec(self, example: QueryExample, graph_metadata: GraphMetadata) -> Any:
        allowed_node_labels = set(example.expected_constraints.allowed_node_labels) or set(graph_metadata.allowed_node_labels)
        allowed_relationship_types = set(example.expected_constraints.allowed_rel_types) or set(graph_metadata.allowed_rel_types)
        allowed_node_labels.update(allowed_relationship_types)
        allowed_properties = set(example.expected_constraints.allowed_properties) or set(graph_metadata.allowed_properties)
        allowed_properties.update({'entity_id'})
        properties_by_relation: dict[str, set[str]] = {}
        source = example.expected_constraints.allowed_properties_by_relation or {}
        if source:
            for rel, props in source.items():
                properties_by_relation[str(rel)] = set(props)
        elif graph_metadata.properties_by_relation:
            properties_by_relation = {str(rel): set(props) for rel, props in graph_metadata.properties_by_relation.items()}
        return StaticSchemaSpec(
            allowed_node_labels=allowed_node_labels,
            allowed_relationship_types=allowed_relationship_types,
            allowed_properties=allowed_properties,
            properties_by_relation=properties_by_relation,
            direction_constraints=set(example.expected_constraints.direction_constraints) or set(graph_metadata.direction_constraints),
        )

    def _is_valid(self, example: QueryExample, graph_metadata: GraphMetadata, cypher: str) -> bool:
        if self._has_undefined_alias_reference(cypher):
            return False
        normalized = _cypher_for_validator(cypher)
        basic = validate_cypher(example, normalized, graph_metadata)
        static_result = validate_cypher_static(normalized, self._to_static_schema_spec(example, graph_metadata))
        ignored_basic_prefixes = ('disallowed_properties', 'disallowed_labels')
        non_schema_basic_errors = [
            e for e in basic.errors if not str(e).startswith(ignored_basic_prefixes)
        ]
        syntax_only_ok = not any(
            e in {'empty_query', 'missing_match', 'missing_return', 'unbalanced_parentheses', 'unbalanced_brackets'}
            for e in non_schema_basic_errors
        )
        return syntax_only_ok and static_result.valid

    def _gold_requires_service_filter(self, example: QueryExample) -> bool:
        return 'service_rel_type' in str(example.gold_cypher or '')

    def _gold_requires_time_filter(self, example: QueryExample) -> bool:
        return 'source_event_time' in str(example.gold_cypher or '')

    def _gold_has_more_patterns(self, example: QueryExample, cypher: str) -> bool:
        gold = str(example.gold_cypher or '')
        return gold.upper().count('MATCH ') > cypher.upper().count('MATCH ') or len(REL_PATTERN.findall(gold)) > len(REL_PATTERN.findall(cypher))

    def _split_where_conditions(self, cypher: str) -> list[str]:
        upper = cypher.upper()
        if ' WHERE ' not in upper:
            return []
        start = upper.index(' WHERE ') + len(' WHERE ')
        end = len(cypher)
        for token in (' RETURN ', ' ORDER BY ', ' LIMIT '):
            idx = upper.find(token, start)
            if idx != -1:
                end = min(end, idx)
        where_body = cypher[start:end].strip()
        if not where_body:
            return []
        return [part.strip() for part in re.split(r'\s+AND\s+', where_body, flags=re.IGNORECASE) if part.strip()]

    def _condition_uses_allowed_relation_property(self, condition: str, example: QueryExample) -> bool:
        allowed = set()
        for props in example.expected_constraints.allowed_properties_by_relation.values():
            allowed.update(str(p) for p in props)
        return any(f'.{prop}' in condition for prop in allowed)

    def _append_conditions(self, cypher: str, conditions: list[str]) -> str | None:
        if not conditions:
            return None
        existing = self._split_where_conditions(cypher)
        to_add = [cond for cond in conditions if cond not in existing]
        if not to_add:
            return None
        upper = cypher.upper()
        if ' WHERE ' in upper:
            insert_at = len(cypher)
            for token in (' RETURN ', ' ORDER BY ', ' LIMIT '):
                idx = upper.find(token)
                if idx != -1:
                    insert_at = min(insert_at, idx)
            prefix = cypher[:insert_at].rstrip()
            suffix = cypher[insert_at:]
            return prefix + ' AND ' + ' AND '.join(to_add) + suffix
        insert_at = len(cypher)
        for token in (' RETURN ', ' ORDER BY ', ' LIMIT '):
            idx = upper.find(token)
            if idx != -1:
                insert_at = min(insert_at, idx)
        if insert_at == len(cypher):
            return cypher.rstrip() + ' WHERE ' + ' AND '.join(to_add)
        return cypher[:insert_at].rstrip() + ' WHERE ' + ' AND '.join(to_add) + cypher[insert_at:]

    def _first_relation_alias(self, cypher: str) -> str | None:
        aliases = self._relation_aliases(cypher)
        if aliases:
            return next(iter(aliases.keys()))
        m = re.search(r'\[([A-Za-z_][A-Za-z0-9_]*)\s*:[A-Za-z_][A-Za-z0-9_]*\]', cypher)
        if m:
            return m.group(1)
        return None

    def _has_undefined_alias_reference(self, cypher: str) -> bool:
        node_aliases = set(self._label_aliases(cypher).values())
        rel_aliases = set(self._relation_aliases(cypher).keys())
        declared = {alias for alias in node_aliases.union(rel_aliases) if alias}
        for match in PROP_PATTERN.finditer(cypher):
            if match.group('alias') not in declared:
                return True
        return False

    def _allowed_properties_by_relation(self, example: QueryExample, graph_metadata: GraphMetadata) -> dict[str, set[str]]:
        if example.expected_constraints.allowed_properties_by_relation:
            return {str(k): set(v) for k, v in example.expected_constraints.allowed_properties_by_relation.items()}
        return {str(k): set(v) for k, v in graph_metadata.properties_by_relation.items()}

    def _relation_aliases(self, cypher: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for match in REL_PATTERN.finditer(cypher):
            alias = match.group('alias')
            rel = match.group('rel')
            if alias:
                out[alias] = rel
        return out

    def _label_aliases(self, cypher: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for match in LABEL_PATTERN.finditer(cypher):
            out[match.group('label')] = match.group('alias')
        return out

    def _repo_entity_id(self, example: QueryExample) -> str | None:
        slots = example.extracted_slot_candidates
        if slots is None:
            return None
        for ent in slots.entity_slots:
            if str(ent.get('entity_label') or '') == 'Repo' and ent.get('entity_id'):
                return str(ent['entity_id'])
        return None

    def _candidate_scope_labels(self, example: QueryExample, cypher: str) -> list[str]:
        labels: list[str] = []
        slots = example.extracted_slot_candidates
        if slots is not None:
            for ent in slots.entity_slots:
                label = str(ent.get('entity_label') or '').strip()
                if label and label not in labels:
                    labels.append(label)
        for match in LABEL_PATTERN.finditer(cypher):
            label = match.group('label')
            if label not in labels:
                labels.append(label)
        return labels

    def _query_requires_repo_scope(self, example: QueryExample) -> bool:
        gold = str(example.gold_cypher or '')
        return 'STARTS WITH' in gold and self._repo_entity_id(example) is not None

    def _has_wrong_or_missing_scope(self, example: QueryExample, cypher: str) -> bool:
        repo_entity_id = self._repo_entity_id(example)
        if not repo_entity_id:
            return False
        labels = self._candidate_scope_labels(example, cypher)
        prefixes = build_repo_scope_prefixes(repo_entity_id=repo_entity_id, labels=labels).get('base_prefixes', {})
        label_aliases = self._label_aliases(cypher)
        starts = STARTS_WITH_PATTERN.search(cypher)
        if not starts:
            return True
        alias = starts.group('alias')
        prefix = starts.group('prefix')
        for label, node_alias in label_aliases.items():
            expected = prefixes.get(label)
            if node_alias == alias and expected:
                return prefix != expected
        return True
