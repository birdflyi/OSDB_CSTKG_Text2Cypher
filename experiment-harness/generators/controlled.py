from __future__ import annotations

from data.models import GraphMetadata, QueryExample
from generators.base import BaseGenerator, GenerationResult
from generators.constraints import ConstraintSpec, build_constraint_spec
from generators.slots import EntitySlot, RelationSlot, TypedSlots, filter_typed_slots, parse_typed_slots
from generators.templates import (
    build_candidate_combinations,
    render_cypher,
    select_allowed_templates,
)


class ControlledGenerator(BaseGenerator):
    name = "controlled"

    def generate(
        self, example: QueryExample, graph_metadata: GraphMetadata
    ) -> GenerationResult:
        spec = build_constraint_spec(example, graph_metadata)
        raw_slots = parse_typed_slots(example)
        raw_slots = self._ensure_minimum_slots(raw_slots, spec)
        filtered_slots, accepted_candidates, rejected_candidates = filter_typed_slots(
            raw_slots, spec
        )
        predicted_query_type = example.predicted_query_type or example.query_type
        templates = select_allowed_templates(predicted_query_type, spec)

        trace: dict[str, object] = {
            "strategy": self.name,
            "predicted_query_type": predicted_query_type,
            "selected_template": None,
            "candidate_templates": [t.name for t in templates],
            "slot_counts_before_filter": raw_slots.counts(),
            "slot_counts_after_filter": filtered_slots.counts(),
            "accepted_candidates": accepted_candidates,
            "rejected_candidates": list(rejected_candidates),
            "fallback_reason": None,
        }

        for template in templates:
            combos, combo_rejections = build_candidate_combinations(
                template, filtered_slots, spec
            )
            trace["rejected_candidates"] = list(trace["rejected_candidates"]) + combo_rejections
            if combos:
                chosen = combos[0]
                trace["selected_template"] = template.name
                trace["accepted_candidates"] = list(trace["accepted_candidates"]) + [
                    f"combo:{template.name}:{chosen.src_entity.label}:"
                    f"{chosen.relation_slot.rel_type if chosen.relation_slot else 'none'}"
                ]
                return GenerationResult(cypher=render_cypher(chosen), trace=trace)

        fallback_templates = [t for t in templates if t.name == "entity_retrieval"]
        if fallback_templates:
            combos, combo_rejections = build_candidate_combinations(
                fallback_templates[0], filtered_slots, spec
            )
            trace["rejected_candidates"] = list(trace["rejected_candidates"]) + combo_rejections
            if combos:
                trace["selected_template"] = fallback_templates[0].name
                trace["fallback_reason"] = "no_valid_complex_combo"
                return GenerationResult(
                    cypher=render_cypher(combos[0]),
                    used_fallback=True,
                    trace=trace,
                )

        trace["fallback_reason"] = "no_valid_template_combination"
        return GenerationResult(cypher="", abstained=True, trace=trace)

    def _ensure_minimum_slots(self, slots: TypedSlots, spec: ConstraintSpec) -> TypedSlots:
        entities = list(slots.entity_slots)
        relations = list(slots.relation_slots)
        if not entities and spec.allowed_node_labels:
            for idx, label in enumerate(sorted(spec.allowed_node_labels)[:2]):
                alias = "a" if idx == 0 else "b"
                entities.append(EntitySlot(label=label, alias=alias))
        if not relations and spec.allowed_rel_types:
            relations.append(RelationSlot(rel_type=sorted(spec.allowed_rel_types)[0]))
        return TypedSlots(
            entity_slots=entities,
            relation_slots=relations,
            property_slots=list(slots.property_slots),
            time_range_slots=list(slots.time_range_slots),
            sort_slots=list(slots.sort_slots),
            aggregation_slots=list(slots.aggregation_slots),
        )
