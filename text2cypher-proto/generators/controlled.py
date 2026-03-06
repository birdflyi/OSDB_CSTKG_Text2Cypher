from __future__ import annotations

from data.schema import QueryExample
from generators.constraints import ConstraintSpec, build_constraint_spec
from generators.base import BaseGenerator, GenerationResult
from generators.controlled_templates import (
    CandidateCombination,
    RenderedTemplate,
    enumerate_relation_combinations,
    render_entity_retrieval,
    render_relation_traversal,
    select_families,
)
from generators.slots import (
    PropertySlot,
    SortSlot,
    TimeRangeSlot,
    TypedSlotSet,
    parse_typed_slots,
    validate_slot_set,
)


class ControlledGenerator(BaseGenerator):
    name = "controlled"

    def generate(self, example: QueryExample) -> GenerationResult:
        spec = build_constraint_spec(example)
        slots = parse_typed_slots(example)
        validated_slots, slot_rejections = validate_slot_set(slots, spec)

        predicted_query_type = example.predicted_query_type or example.query_type
        families = select_families(
            predicted_query_type,
            spec,
            fallback_families=["join_traversal", "entity_filter"],
        )
        trace: dict[str, object] = {
            "predicted_query_type": predicted_query_type,
            "selected_template_families": families,
            "slot_counts_before_filter": slots.summary(),
            "slot_counts_after_filter": validated_slots.summary(),
            "rejected_candidates": list(slot_rejections),
        }

        if "join_traversal" in families:
            relation_combos, combo_rejections = enumerate_relation_combinations(
                validated_slots, spec
            )
            trace["rejected_candidates"] = list(trace["rejected_candidates"]) + combo_rejections
            if relation_combos:
                combo = relation_combos[0]
                # Attach optional node-centric constraints from compatible slots.
                prop_slot = self._pick_property_slot(
                    validated_slots, owner_kind="node", owner_type=combo.left_entity.label
                )
                time_slot = self._pick_time_slot(
                    validated_slots, owner_type=combo.left_entity.label
                )
                sort_slot = self._pick_sort_slot(
                    validated_slots, combo.left_entity.label, spec
                )
                rendered = render_relation_traversal(
                    CandidateCombination(
                        left_entity=combo.left_entity,
                        right_entity=combo.right_entity,
                        relation_type=combo.relation_type,
                        property_slot=prop_slot,
                        time_range_slot=time_slot,
                        sort_slot=sort_slot,
                    )
                )
                trace["selected_template"] = rendered.template_name
                trace["selected_combination"] = {
                    "left_label": combo.left_entity.label,
                    "relation_type": combo.relation_type,
                    "right_label": combo.right_entity.label,
                    "property_slot": (
                        f"{prop_slot.owner_type}.{prop_slot.property_name}"
                        if prop_slot is not None
                        else None
                    ),
                }
                return GenerationResult(
                    cypher=rendered.cypher,
                    metadata={"strategy": self.name, "generation_trace": trace},
                )

        if "entity_filter" in families:
            fallback = self._constraint_aware_fallback(validated_slots, spec)
            if fallback is not None:
                trace["selected_template"] = fallback.template_name
                trace["selected_combination"] = {"fallback": "entity_retrieval"}
                return GenerationResult(
                    cypher=fallback.cypher,
                    used_fallback=True,
                    metadata={"strategy": self.name, "generation_trace": trace},
                )

        trace["abstain_reason"] = "no_valid_template_combination"
        return GenerationResult(
            cypher="",
            abstained=True,
            metadata={"strategy": self.name, "generation_trace": trace},
        )

    def _pick_property_slot(
        self, slots: TypedSlotSet, owner_kind: str, owner_type: str
    ) -> PropertySlot | None:
        for slot in slots.property_slots:
            if (
                slot.owner_kind == owner_kind
                and (slot.owner_type is None or slot.owner_type == owner_type)
            ):
                return slot
        return None

    def _pick_time_slot(
        self, slots: TypedSlotSet, owner_type: str
    ) -> TimeRangeSlot | None:
        for slot in slots.time_range_slots:
            if slot.owner_type is None or slot.owner_type == owner_type:
                return slot
        return None

    def _pick_sort_slot(
        self, slots: TypedSlotSet, owner_type: str, spec: ConstraintSpec
    ) -> SortSlot | None:
        for slot in slots.sort_slots:
            if spec.is_allowed_node_property(owner_type, slot.property_name):
                return slot
        return None

    def _constraint_aware_fallback(
        self, slots: TypedSlotSet, spec: ConstraintSpec
    ) -> RenderedTemplate | None:
        if not slots.entity_slots:
            return None
        for entity in slots.entity_slots:
            if not spec.is_allowed_label(entity.label):
                continue
            property_slot = self._pick_property_slot(
                slots, owner_kind="node", owner_type=entity.label
            )
            time_slot = self._pick_time_slot(slots, owner_type=entity.label)
            sort_slot = self._pick_sort_slot(slots, entity.label, spec)
            return render_entity_retrieval(
                entity=entity,
                property_slot=property_slot,
                time_range_slot=time_slot,
                sort_slot=sort_slot,
            )
        return None
