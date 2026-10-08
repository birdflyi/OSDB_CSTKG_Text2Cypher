from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))

from runners.independent_controlled_pipeline import (  # noqa: E402
    IndependentTemplate,
    ScopeSlotConflictError,
    _slot_values,
    _projection_contract_order_matches_skeleton,
    _return_has_tuple_distinct,
    _return_clause,
    audit_ir_constraint_coverage,
    generate_independent,
    load_independent_schema,
    load_independent_templates,
    parse_nl_to_ir,
)


V5 = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml"
V4 = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v4.yaml"
SCHEMA = ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"


def _templates():
    return load_independent_templates(V5)


def _generate(query: str):
    return generate_independent(query, query, _templates(), load_independent_schema(SCHEMA))


def test_typed_prefix_scopes_render_as_typed_starts_with_and_are_consumed() -> None:
    cases = [
        ("List pull request IDs beginning with PR_900001, limit 20.", "PullRequest", "PR_900001"),
        ("Show issues whose ID starts with I_880002.", "Issue", "I_880002"),
    ]
    for query, label, value in cases:
        result = _generate(query)
        assert len(result.ir.entity_scopes) == 1
        scope = result.ir.entity_scopes[0]
        assert (scope.label, scope.property, scope.operator, scope.value) == (
            label,
            "entity_id",
            "STARTS_WITH",
            value,
        )
        assert value in (result.rendered_cypher or "")
        coverage = result.validation["selection"]["ir_constraint_coverage"]["entity_scopes"]
        assert len(coverage["consumed"]) == 1
        assert not coverage["unconsumed"]


def test_bounded_prefix_cue_variants_share_typed_grammar() -> None:
    examples = [
        "List pull request IDs beginning with PR_900001.",
        "Show PR prefix PR_900001.",
        "Show PR IDs start with PR_900001.",
        "Show pull requests whose ID begins with PR_900001.",
        "Scope to Issue prefix I_880002.",
        "Show issues whose identifier starts with I_880002.",
    ]
    for query in examples:
        ir = parse_nl_to_ir("synthetic", query)
        assert len(ir.entity_scopes) == 1, query
        assert ir.entity_scopes[0].operator == "STARTS_WITH"


def test_canonical_entity_id_remains_equality_anchor_not_prefix_scope() -> None:
    result = _generate("For issue I_880002#77, who opened it?")
    assert not result.ir.entity_scopes
    assert result.ir.source_entity["entity_id"] == "I_880002#77"
    assert result.template_id == "indv4_issue_opened_by"
    assert "entity_id: 'I_880002#77'" in result.rendered_cypher
    assert result.ir.projection_items[0].label == "Actor"


def test_source_anchor_and_requested_target_are_independent_roles() -> None:
    result = _generate("For pull request PR_900001#12, which unknown objects does it reference?")
    assert result.ir.source_entity["entity_label"] == "PullRequest"
    assert result.ir.projection_items[0].label == "UnknownObject"
    assert "RETURN x.entity_id" in result.rendered_cypher
    assert "RETURN pr.entity_id" not in result.rendered_cypher


def test_ordered_two_column_projection_keeps_domain_then_resource_id() -> None:
    result = _generate(
        "For pull request PR_900001#12, show the link domain and the external resource ID for each resource it links to."
    )
    assert [(item.label, item.property) for item in result.ir.projection_items] == [
        ("ExternalResource", "url_domain_etld1"),
        ("ExternalResource", "entity_id"),
    ]
    assert "RETURN rel.url_domain_etld1, e.entity_id" in result.rendered_cypher
    coverage = result.validation["selection"]["ir_constraint_coverage"]["projection"]
    assert len(coverage["consumed_items"]) == 2
    assert coverage["order_preserved"]


def test_identifier_then_domain_uses_compatible_ordered_contract() -> None:
    result = _generate(
        "For pull request PR_900001#12, show the linked resource ID and domain."
    )
    assert [(item.label, item.property) for item in result.ir.projection_items] == [
        ("ExternalResource", "entity_id"),
        ("ExternalResource", "url_domain_etld1"),
    ]
    assert result.template_id == "indv5_reference_external_id_domain"
    assert "RETURN e.entity_id, rel.url_domain_etld1" in result.rendered_cypher


def test_bare_numeric_fragment_is_not_a_typed_scope() -> None:
    ir = parse_nl_to_ir("synthetic", "Show objects whose IDs start with 900001.")
    assert not ir.entity_scopes


def test_issue_scope_cannot_be_consumed_by_pull_request_only_contract() -> None:
    templates = _templates()
    ir = parse_nl_to_ir("synthetic", "Show issues whose ID starts with I_880002.")
    pr_template = next(item for item in templates if item.template_id == "indv4_repo_pull_request_filter")
    coverage = audit_ir_constraint_coverage(ir, pr_template)
    assert not coverage["accepted"]
    assert coverage["entity_scopes"]["unconsumed"][0]["label"] == "Issue"
    assert coverage["entity_scopes"]["unconsumed"][0]["status"] == "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON"


def test_scope_slot_metadata_without_matching_rendered_operator_is_not_consumption() -> None:
    ir = parse_nl_to_ir("synthetic", "Show issues whose ID starts with I_880002.")
    base = next(item for item in _templates() if item.template_id == "indv5_issue_prefix_list")
    equality_only = replace(
        base,
        skeleton="MATCH (i:Issue) WHERE i.entity_id = $issue_scope_prefix RETURN i.entity_id LIMIT 25",
    )
    coverage = audit_ir_constraint_coverage(ir, equality_only)
    assert not coverage["accepted"]
    assert coverage["entity_scopes"]["unconsumed"]


def test_explicit_target_actor_does_not_fall_back_to_source_noun() -> None:
    result = _generate("For issue I_880002#77, return the Actor ID of whoever opened it.")
    assert result.ir.source_entity["entity_label"] == "Issue"
    assert [item.label for item in result.ir.projection_items] == ["Actor"]


def test_template_extra_projection_is_rejected_without_entailment() -> None:
    templates = _templates()
    ir = parse_nl_to_ir("synthetic", "For pull request PR_900001#12, return the external resource ID.")
    template = next(item for item in templates if item.template_id == "indv4_reference_external_property")
    coverage = audit_ir_constraint_coverage(ir, template)
    assert not coverage["accepted"]
    assert any(item["reason"] == "template would add an unrequested projection item" for item in coverage["projection"]["unconsumed_items"])


def test_single_projection_contract_cannot_drop_second_requested_column() -> None:
    ir = parse_nl_to_ir(
        "synthetic",
        "For pull request PR_900001#12, show the link domain and the external resource ID for each resource it links to.",
    )
    template = next(item for item in _templates() if item.template_id == "indv4_reference_object")
    coverage = audit_ir_constraint_coverage(ir, template)
    assert not coverage["accepted"]
    assert len(coverage["projection"]["unconsumed_items"]) == 3


def test_one_item_contract_cannot_claim_two_column_template_is_fully_consumed() -> None:
    ir = parse_nl_to_ir(
        "synthetic",
        "For pull request PR_900001#12, return the external resource ID.",
    )
    template = next(item for item in _templates() if item.template_id == "indv4_reference_external_property")
    single_column = replace(template, projection_contract=[template.projection_contract[1]])
    coverage = audit_ir_constraint_coverage(ir, single_column)
    assert not coverage["accepted"]
    assert not coverage["projection"]["order_preserved"]


def test_typed_prefix_without_compatible_template_slot_abstains() -> None:
    ir = parse_nl_to_ir("synthetic", "Show issues whose ID starts with I_880002.")
    template = next(item for item in load_independent_templates(V4) if item.template_id == "indv4_reference_object")
    uncovered = replace(template, projection_contract=[{"role": "target_entity", "label": "Issue", "property": "entity_id"}])
    coverage = audit_ir_constraint_coverage(ir, uncovered)
    assert not coverage["accepted"]
    assert coverage["entity_scopes"]["unconsumed"]


def test_multiple_distinct_issue_prefixes_for_one_slot_abstain() -> None:
    result = _generate("Show issues whose ID starts with I_880002 or I_880003.")
    assert len(result.ir.entity_scopes) == 2
    assert result.template_id is None
    assert result.rendered_cypher is None
    candidate_coverage = result.validation["selection"]["candidate_ir_constraint_coverage"]
    issue_coverage = candidate_coverage["indv5_issue_prefix_list"]["entity_scopes"]
    assert issue_coverage["slot_conflicts"][0]["reason_code"] == (
        "MULTIPLE_DISTINCT_VALUES_FOR_SINGULAR_SCOPE_SLOT"
    )
    assert len(issue_coverage["unconsumed"]) == 2


def test_multiple_distinct_pull_request_prefixes_do_not_render_last_value() -> None:
    result = _generate(
        "Show pull requests whose ID starts with PR_900001 or PR_900002."
    )
    assert len(result.ir.entity_scopes) == 2
    assert result.rendered_cypher is None
    assert result.template_id is None
    assert "PR_900001" not in (result.rendered_cypher or "")
    assert "PR_900002" not in (result.rendered_cypher or "")
    pr_coverage = result.validation["selection"]["candidate_ir_constraint_coverage"][
        "indv4_repo_pull_request_filter"
    ]["entity_scopes"]
    assert pr_coverage["slot_conflicts"][0]["reason_code"] == (
        "MULTIPLE_DISTINCT_VALUES_FOR_SINGULAR_SCOPE_SLOT"
    )
    assert len(pr_coverage["unconsumed"]) == 2


def test_repeated_identical_scope_values_deduplicate_without_conflict() -> None:
    ir = parse_nl_to_ir(
        "synthetic",
        "Show issues whose ID starts with I_880002 and IDs start with I_880002.",
    )
    assert len(ir.entity_scopes) == 2
    template = next(item for item in _templates() if item.template_id == "indv5_issue_prefix_list")
    coverage = audit_ir_constraint_coverage(ir, template)
    assert coverage["accepted"]
    assert len(coverage["entity_scopes"]["consumed"]) == 2
    assert not coverage["entity_scopes"]["slot_conflicts"]
    values = _slot_values(ir, template)
    assert values["issue_scope_prefix"] == "I_880002"


def test_scope_materialization_guard_raises_on_distinct_values_for_one_slot() -> None:
    ir = parse_nl_to_ir(
        "synthetic",
        "Show issues whose ID starts with I_880002 or I_880003.",
    )
    template = next(item for item in _templates() if item.template_id == "indv5_issue_prefix_list")
    try:
        _slot_values(ir, template)
    except ScopeSlotConflictError as exc:
        assert exc.reason_code == "MULTIPLE_DISTINCT_VALUES_FOR_SINGULAR_SCOPE_SLOT"
        assert exc.slot == "issue_scope_prefix"
    else:
        raise AssertionError("multi-value singular scope slot should fail closed")


def test_canonical_source_introduction_noun_is_not_an_output_projection() -> None:
    result = _generate("Show issue I_880002#77 and tell me who opened it.")
    assert result.ir.source_entity["entity_label"] == "Issue"
    assert result.template_id == "indv4_issue_opened_by"
    assert [(item.label, item.property) for item in result.ir.projection_items] == [
        ("Actor", "entity_id")
    ]
    assert "RETURN a.entity_id" in result.rendered_cypher
    assert "RETURN i.entity_id" not in result.rendered_cypher


def test_canonical_pull_request_source_is_not_projected_with_linked_resources() -> None:
    result = _generate(
        "Display pull request PR_900001#12 and give the linked resource IDs."
    )
    assert result.ir.source_entity["entity_label"] == "PullRequest"
    assert [(item.label, item.property) for item in result.ir.projection_items] == [
        ("ExternalResource", "entity_id")
    ]
    assert result.ir.target_labels == ["ExternalResource"]
    # Current contracts would add an unrequested column, so fail closed rather
    # than leaking the source noun into the requested output.
    assert result.rendered_cypher is None


def test_explicit_id_projection_for_source_label_remains_represented() -> None:
    ir = parse_nl_to_ir(
        "synthetic", "Show issue ID I_880002#77 and return the issue ID."
    )
    assert [(item.label, item.property) for item in ir.projection_items] == [
        ("Issue", "entity_id")
    ]


def test_tuple_distinct_request_is_separate_from_projection_item_flags() -> None:
    query = (
        "For actor A_900001, return distinct repo IDs and external resource IDs "
        "from mentioned repos and external links, if any."
    )
    result = _generate(query)
    assert result.template_id == "indv4_actor_multi_target_reference"
    assert result.ir.projection_distinct
    assert [(item.label, item.property, item.distinct) for item in result.ir.projection_items] == [
        ("Repo", "entity_id", False),
        ("ExternalResource", "entity_id", False),
    ]
    distinct_audit = result.validation["selection"]["ir_constraint_coverage"]["projection"][
        "tuple_distinct"
    ]
    assert distinct_audit["requested"]
    assert distinct_audit["return_clause_distinct"]
    assert distinct_audit["accepted"]
    assert "RETURN DISTINCT repo.entity_id, e.entity_id" in result.rendered_cypher
    assert result.validation["selection"]["ir_constraint_coverage"]["projection"]["order_preserved"]


def test_item_and_aggregate_distinct_do_not_become_tuple_distinct() -> None:
    query = "return distinct actor IDs and count distinct pull request IDs"
    ir = parse_nl_to_ir("synthetic", query)
    assert not ir.projection_distinct
    assert [(item.label, item.distinct) for item in ir.projection_items] == [("Actor", True)]
    assert ir.aggregation == [
        {
            "function": "count",
            "field": "PullRequest.entity_id",
            "distinct": True,
            "provenance": "aggregate_argument_distinct_from_nl",
        }
    ]

    full_query = (
        "For repo R_900001 in 2024, show comprehensive domain aggregation for involved actors; "
        "return distinct actor IDs and count distinct pull request IDs."
    )
    full_ir = parse_nl_to_ir("synthetic", full_query)
    template = next(
        item for item in _templates()
        if item.template_id == "indv4_comprehensive_external_actor_aggregation"
    )
    coverage = audit_ir_constraint_coverage(full_ir, template)
    assert coverage["aggregation"]["consumed"] == full_ir.aggregation
    assert not coverage["projection"]["tuple_distinct"]["requested"]
    assert _projection_contract_order_matches_skeleton(
        template, _return_clause(template.skeleton)
    )


def test_item_distinct_does_not_leak_to_an_ordinary_projection_item() -> None:
    ir = parse_nl_to_ir(
        "synthetic", "return distinct actor IDs and ordinary external resource IDs"
    )
    assert not ir.projection_distinct
    assert [(item.label, item.distinct) for item in ir.projection_items] == [
        ("Actor", True),
        ("ExternalResource", False),
    ]


def test_unique_combinations_is_an_explicit_tuple_distinct_cue() -> None:
    ir = parse_nl_to_ir(
        "synthetic", "return unique combinations of repo ID and resource ID"
    )
    assert ir.projection_distinct
    assert [(item.label, item.distinct) for item in ir.projection_items] == [
        ("Repo", False),
        ("ExternalResource", False),
    ]


def test_local_distinct_phrase_is_not_promoted_to_tuple_scope() -> None:
    ir = parse_nl_to_ir(
        "synthetic", "Show distinct actor IDs alongside the ordinary external resource IDs."
    )
    assert not ir.projection_distinct


def test_skeleton_distinct_scope_is_read_from_return_prefix_only() -> None:
    assert _return_has_tuple_distinct("MATCH (a:Actor) RETURN DISTINCT a.entity_id, a.name")
    assert not _return_has_tuple_distinct(
        "MATCH (a:Actor) RETURN collect(DISTINCT a.entity_id) AS actors"
    )
    assert _return_has_tuple_distinct(
        "MATCH (a:Actor) RETURN DISTINCT collect(DISTINCT a.entity_id) AS actors"
    )


def test_entailed_tuple_distinct_contract_adjudicates_unmarked_request() -> None:
    query = (
        "For actor A_900001, return repo IDs and external resource IDs "
        "from mentioned repos and external links, if any."
    )
    result = _generate(query)
    assert result.template_id == "indv4_actor_multi_target_reference"
    assert not result.ir.projection_distinct
    coverage = result.validation["selection"]["ir_constraint_coverage"]["projection"]["tuple_distinct"]
    assert coverage["return_clause_distinct"]
    assert coverage["contract_allows_implicit_tuple_distinct"]
    assert coverage["accepted"]

    ir = parse_nl_to_ir("synthetic", query)
    template = next(
        item for item in _templates() if item.template_id == "indv4_actor_multi_target_reference"
    )
    without_entailment = replace(template, projection_options={})
    rejected = audit_ir_constraint_coverage(ir, without_entailment)
    assert not rejected["accepted"]
    assert not rejected["projection"]["tuple_distinct"]["accepted"]


def test_tuple_distinct_request_abstains_without_return_or_compatible_contract() -> None:
    query = (
        "For actor A_900001, return distinct repo IDs and external resource IDs "
        "from mentioned repos and external links, if any."
    )
    ir = parse_nl_to_ir("synthetic", query)
    template = next(
        item for item in _templates() if item.template_id == "indv4_actor_multi_target_reference"
    )
    incompatible = replace(
        template,
        skeleton=template.skeleton.replace("RETURN DISTINCT", "RETURN"),
        projection_options={},
    )
    coverage = audit_ir_constraint_coverage(ir, incompatible)
    assert not coverage["accepted"]
    assert not coverage["projection"]["tuple_distinct"]["accepted"]


def test_single_column_distinct_remains_item_local() -> None:
    ir = parse_nl_to_ir(
        "synthetic", "For issue I_880002#77, return distinct Actor IDs of its opener."
    )
    assert not ir.projection_distinct
    assert len(ir.projection_items) == 1
    assert ir.projection_items[0].distinct is True

    template = next(item for item in _templates() if item.template_id == "indv4_issue_opened_by")
    with_distinct = replace(
        template,
        skeleton=template.skeleton.replace("RETURN a.entity_id", "RETURN DISTINCT a.entity_id"),
        projection_contract=[
            {**template.projection_contract[0], "distinct": True, "entailed": True}
        ],
        projection_options={"distinct": True, "distinct_scope": "tuple", "entailed": True},
    )
    coverage = audit_ir_constraint_coverage(ir, with_distinct)
    assert coverage["accepted"]
    assert not coverage["projection"]["tuple_distinct"]["requested"]
    assert coverage["projection"]["tuple_distinct"]["accepted"]


def test_projection_order_mismatch_is_rejected() -> None:
    ir = parse_nl_to_ir(
        "synthetic",
        "For pull request PR_900001#12, show the link domain and the external resource ID for each resource it links to.",
    )
    template = next(item for item in _templates() if item.template_id == "indv4_reference_external_property")
    reversed_contract = replace(template, projection_contract=list(reversed(template.projection_contract)))
    coverage = audit_ir_constraint_coverage(ir, reversed_contract)
    assert not coverage["accepted"]
    assert not coverage["projection"]["order_preserved"]


def test_out_of_scope_limit_form_conflicting_with_default_abstains() -> None:
    result = _generate(
        "For pull request PR_900001#12, show the link domain and the external resource ID for each resource it links to, 10 results max."
    )
    assert result.template_id is None
    assert result.failure_stage == "template_selection_or_abstention"
    assert result.ir.unnormalized_limit_values == [10]
    assert result.validation["selection"]["reason"] == "unconsumed IR constraint"


def test_unrecognized_limit_equal_to_contract_default_is_entailed() -> None:
    result = _generate(
        "For pull request PR_900001#12, show the linked resource ID and domain, up to 25 results."
    )
    assert result.template_id == "indv5_reference_external_id_domain"
    coverage = result.validation["selection"]["ir_constraint_coverage"]["limit"]
    assert coverage["contract_default_entailed_values"] == [25]


def test_production_scope_projection_files_have_no_heldout_literals_or_id_routing() -> None:
    production = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            ROOT / "graph-migration" / "runners" / "independent_controlled_pipeline.py",
            ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml",
        )
    )
    for literal in ("ho_q_", "intent_q_", "q_l1_", "q_l2_", "q_l3_", "q_l4_", "q_comp_", "156018", "7045099", "12095", "11659"):
        assert literal not in production
    assert "request_id ==" not in production
    assert "request_id in" not in production
