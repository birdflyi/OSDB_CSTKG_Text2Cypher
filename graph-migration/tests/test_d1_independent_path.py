from __future__ import annotations

from pathlib import Path
import json
import yaml

from repair.gold_blind_repair import repair_gold_blind
from runners.independent_controlled_pipeline import (
    audit_ir_constraint_coverage,
    generate_independent,
    load_independent_schema,
    load_independent_templates,
    parse_nl_to_ir,
    select_template,
)
from evaluation.semantic_signature import compare_semantic_signatures, semantic_signature


ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v4.yaml"
SCHEMA = ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"
REFERENCE_CORRECTIONS = ROOT / "data_real" / "pilot_queries" / "independent_eval_reference_corrections_v1.yaml"


def _generate(query: str):
    return generate_independent(
        "test",
        query,
        load_independent_templates(TEMPLATES),
        load_independent_schema(SCHEMA),
    )


def test_independent_modules_do_not_read_annotation_fields() -> None:
    pipeline = (ROOT / "graph-migration" / "runners" / "independent_controlled_pipeline.py").read_text(encoding="utf-8")
    repair = (ROOT / "graph-migration" / "repair" / "gold_blind_repair.py").read_text(encoding="utf-8")
    combined = pipeline + repair
    for forbidden in ["gold_cypher", "extracted_slot_candidates", "covered_queries", "query_type"]:
        assert forbidden not in combined


def test_generation_modules_do_not_access_evaluation_reference_corrections() -> None:
    pipeline = (ROOT / "graph-migration" / "runners" / "independent_controlled_pipeline.py").read_text(encoding="utf-8")
    repair = (ROOT / "graph-migration" / "repair" / "gold_blind_repair.py").read_text(encoding="utf-8")
    assert "independent_eval_reference_corrections_v1.yaml" not in pipeline + repair


def test_comprehensive_template_binds_link_and_time_filters_to_mandatory_match() -> None:
    text = TEMPLATES.read_text(encoding="utf-8")
    section = text.split("template_id: indv4_comprehensive_external_actor_aggregation", 1)[1]
    skeleton = section.split("    required_slots:", 1)[0]
    assert skeleton.index("MATCH (pr)-[rl:REFERENCE]->(e:ExternalResource)") < skeleton.index("WHERE rl.service_rel_type = 'LINKS_TO'")
    assert skeleton.index("WHERE rl.service_rel_type = 'LINKS_TO'") < skeleton.index("OPTIONAL MATCH (pr)-[ra:REFERENCE]->(a:Actor)")
    assert skeleton.index("rl.source_event_time >= $time_start") < skeleton.index("OPTIONAL MATCH (pr)-[ra:REFERENCE]->(a:Actor)")
    assert "WHERE ra.service_rel_type IN ['MENTIONS','REFERENCES']" in skeleton


def test_historical_q_comp_reference_is_preserved_and_correction_is_separate() -> None:
    source_text = (ROOT / "data_real" / "pilot_queries" / "queries_pilot.jsonl").read_text(encoding="utf-8")
    assert "OPTIONAL MATCH (pr)-[ra:REFERENCE]->(a:Actor) WHERE rl.service_rel_type = 'LINKS_TO'" in source_text
    corrections = yaml.safe_load(REFERENCE_CORRECTIONS.read_text(encoding="utf-8"))
    correction = corrections["corrections"]["q_comp_01"]["corrected_cypher"]
    assert "MATCH (pr)-[rl:REFERENCE]->(e:ExternalResource)\nWHERE rl.service_rel_type = 'LINKS_TO'" in correction
    assert "OPTIONAL MATCH (pr)-[ra:REFERENCE]->(a:Actor)\nWHERE ra.service_rel_type" in correction


def test_q_comp_original_source_mismatch_and_effective_reference_match() -> None:
    query = "Comprehensive: For repo R_156018 in 2023, find PRs linked to external resources, group by domain, and return involved actors and latest interaction time."
    result = _generate(query)
    source = next(
        json.loads(line)["gold_cypher"]
        for line in (ROOT / "data_real" / "pilot_queries" / "queries_pilot.jsonl").read_text(encoding="utf-8").splitlines()
        if json.loads(line).get("id") == "q_comp_01"
    )
    effective = yaml.safe_load(REFERENCE_CORRECTIONS.read_text(encoding="utf-8"))["corrections"]["q_comp_01"]["corrected_cypher"]
    assert not compare_semantic_signatures(result.rendered_cypher, source)["match"]
    assert compare_semantic_signatures(result.rendered_cypher, effective)["match"]


def test_parse_opened_by_and_selects_contract() -> None:
    ir = parse_nl_to_ir("q", "Who opened issue I_156018#12095?")
    assert any(x["semantic"] == "OPENED_BY" for x in ir.relation_semantics)
    templates = load_independent_templates(TEMPLATES)
    selected, trace = select_template(ir, templates)
    assert selected is not None
    assert selected.family == "OneHopEA"
    assert trace["status"] == "selected"


def test_placeholder_abstains() -> None:
    ir = parse_nl_to_ir("q", "Which repos are structurally coupled with R_156018?")
    selected, trace = select_template(ir, load_independent_templates(TEMPLATES))
    assert selected is None
    assert trace["status"] == "abstain"


def test_gold_blind_relation_scoped_property_repair() -> None:
    schema = load_independent_schema(SCHEMA)
    ir = parse_nl_to_ir("q", "Show top 10 external links mentioned by PR PR_156018#11659.")
    result = repair_gold_blind(
        "MATCH (pr:PullRequest)-[rl:REFERENCE]->(e:ExternalResource) RETURN e.url_domain_etld1",
        [{"code": "ILLEGAL_PROPERTY", "detail": {"properties": ["url_domain_etld1"]}}],
        ir,
        None,
        schema,
    )
    assert result.changed
    assert "rl.url_domain_etld1" in result.cypher


def test_lower_bound_only_time_renders_without_fabricated_end() -> None:
    result = _generate("Find PRs in repo R_156018 that mention actor A_7045099 and also link to external domains after 2023-06-01.")
    assert result.template_id == "indv4_repo_actor_external_lower_bound"
    assert result.validation["valid"]
    assert "source_event_time >= '2023-06-01T00:00:00Z'" in result.rendered_cypher
    assert "source_event_time <" not in result.rendered_cypher


def test_explicit_limit_overrides_contract_default() -> None:
    result = _generate("Show top 10 external links mentioned by PR PR_156018#11659.")
    assert result.validation["valid"]
    assert result.rendered_cypher.endswith("LIMIT 10")


def test_unconsumed_count_does_not_route_to_reference_object_template() -> None:
    result = _generate("Count referenced objects for PR PR_156018#11659")
    assert result.template_id is None
    assert result.failure_stage == "template_selection_or_abstention"
    coverage = result.validation["selection"]["candidate_ir_constraint_coverage"]["indv4_reference_object"]
    assert coverage["aggregation"]["unconsumed"]
    assert any("aggregation" in reason for reason in coverage["reasons"])


def test_unconsumed_time_does_not_route_to_timeless_reference_template() -> None:
    result = _generate("Find referenced objects for PR PR_156018#11659 in 2023")
    assert result.template_id is None
    assert result.failure_stage == "template_selection_or_abstention"
    coverage = result.validation["selection"]["candidate_ir_constraint_coverage"]["indv4_reference_object"]
    assert coverage["time"]["unconsumed"]
    assert coverage["time"]["requested_start"] == "2023-01-01T00:00:00Z"
    assert coverage["time"]["requested_end"] == "2024-01-01T00:00:00Z"


def test_aggregation_and_time_rejection_retains_both_unconsumed_dimensions() -> None:
    result = _generate("Count referenced objects for PR PR_156018#11659 in 2023")
    assert result.template_id is None
    coverage = result.validation["selection"]["candidate_ir_constraint_coverage"]["indv4_reference_object"]
    assert coverage["aggregation"]["unconsumed"]
    assert coverage["time"]["unconsumed"]


def test_explicit_domain_projection_and_sort_are_consumed_by_compatible_contract() -> None:
    result = _generate(
        "Comprehensive: For repo R_156018 in 2023, find PRs linked to external resources, "
        "group by domain, and return involved actors and latest interaction time."
    )
    coverage = result.validation["selection"]["ir_constraint_coverage"]
    assert coverage["projection"]["consumed"] == ["url_domain_etld1"]
    assert not coverage["projection"]["unconsumed"]
    assert not coverage["sort"]["unconsumed"]


def test_latest_aggregation_projection_is_not_misclassified_as_explicit_sort() -> None:
    ir = parse_nl_to_ir(
        "q",
        "Comprehensive: For repo R_156018 in 2023, find PRs linked to external resources, "
        "group by domain, and return involved actors and latest interaction time.",
    )
    assert not [item for item in ir.sort if item.get("provenance") == "explicit_sort_from_nl"]
    assert any(item.get("provenance") == "aggregation_latest_projection" for item in ir.aggregation)


def test_extra_explicit_target_class_cannot_be_dropped() -> None:
    result = _generate("For actor A_7045099, find mentioned repos and referenced objects.")
    assert result.template_id is None
    assert result.validation["selection"]["reason"] == "unconsumed IR constraint"


def test_explicit_projection_is_rejected_by_incompatible_template_contract() -> None:
    templates = load_independent_templates(TEMPLATES)
    ir = parse_nl_to_ir("q", "Show external links and their domains for PR PR_156018#11659.")
    template = next(item for item in templates if item.template_id == "indv4_reference_object")
    coverage = audit_ir_constraint_coverage(ir, template)
    assert coverage["projection"]["unconsumed"]
    assert not coverage["accepted"]


def test_explicit_sort_is_rejected_by_timeless_template_contract() -> None:
    result = _generate("Find referenced objects for PR PR_156018#11659 sorted by time.")
    assert result.template_id is None
    coverage = result.validation["selection"]["candidate_ir_constraint_coverage"]["indv4_reference_object"]
    assert coverage["sort"]["unconsumed"]


def test_contract_default_limits_are_family_specific() -> None:
    narrow = _generate("Count references by domain for PR PR_156018#11659 in 2023.")
    comprehensive = _generate("Comprehensive: For repo R_156018 in 2023, find PRs linked to external resources, group by domain, and return involved actors and latest interaction time.")
    assert narrow.rendered_cypher.endswith("LIMIT 20")
    assert comprehensive.rendered_cypher.endswith("LIMIT 30")


def test_semantic_ir_routes_external_link_count_to_narrow_aggregation() -> None:
    result = _generate("Count external links by domain for PR PR_156018#11659 in 2023.")
    assert result.template_id == "indv4_narrow_domain_aggregation"
    assert result.ir.intent_key == "narrow_domain_aggregation"
    assert result.ir.time_range == {
        "start": "2023-01-01T00:00:00Z",
        "end": "2024-01-01T00:00:00Z",
        "provenance": "year_range_from_nl",
        "year": 2023,
        "bounded": True,
    }


def test_narrow_external_link_aggregation_is_lexically_independent() -> None:
    result = _generate("Compute external links by domain for PR PR_156018#11659 during 2023.")
    assert "references" not in result.nl_query.lower()
    assert result.template_id == "indv4_narrow_domain_aggregation"
    assert result.ir.intent_key == "narrow_domain_aggregation"


def test_non_aggregation_external_link_request_keeps_projection_contract() -> None:
    result = _generate("Show external links and their domains for PR PR_156018#11659.")
    assert result.template_id == "indv4_reference_external_property"
    assert result.ir.intent_key == "typed_reference_external_property"


def test_comprehensive_aggregation_keeps_precedence_over_narrow_route() -> None:
    result = _generate(
        "Comprehensive: For repo R_156018 in 2023, find PRs linked to external "
        "resources, group by domain, and return involved actors and latest interaction time."
    )
    assert result.template_id == "indv4_comprehensive_external_actor_aggregation"
    assert result.ir.intent_key == "comprehensive_external_actor_aggregation"
    assert result.validation["valid"]


def test_narrow_aggregation_without_bounded_time_does_not_fabricate_bounds() -> None:
    result = _generate("Count external links by domain for PR PR_156018#11659.")
    assert result.template_id is None
    assert result.ir.intent_key != "narrow_domain_aggregation"
    assert result.ir.time_range is None


def test_multi_relation_target_contract_is_preserved() -> None:
    result = _generate("For actor A_7045099, find mentioned repos and external links.")
    assert result.template_id == "indv4_actor_multi_target_reference"
    assert "OPTIONAL MATCH" in result.rendered_cypher
    assert "repo.entity_id" in result.rendered_cypher and "e.entity_id" in result.rendered_cypher


def test_multiple_actor_ids_abstain_instead_of_dropping_an_actor() -> None:
    result = _generate("For actors A_7045099 and A_7045100, find mentioned repos and external links.")
    assert result.template_id is None
    assert result.failure_stage == "template_selection_or_abstention"
    assert result.ir.bounded_status == "ABSTAIN_MULTIPLE_ACTOR_IDS"


def test_entity_constraint_coverage_abstains_on_unrelated_direct_pr() -> None:
    result = _generate("For actor A_7045099 and PR PR_156018#11659, find mentioned repos and external links.")
    assert result.template_id is None
    coverage = result.validation["selection"]["candidate_entity_constraint_coverage"]["indv4_actor_multi_target_reference"]
    assert coverage["unconsumed"][0]["entity_id"] == "PR_156018#11659"
    assert {x["status"] for x in coverage["direct_constraints"]} == {"consumed", "unconsumed"}


def test_entity_constraint_coverage_allows_matching_repo_entailment() -> None:
    result = _generate("What objects are referenced by PR PR_156018#11659 and repo R_156018?")
    assert result.template_id == "indv4_reference_object"
    coverage = result.validation["selection"]["entity_constraint_coverage"]
    assert [x["entity_id"] for x in coverage["entailed"]] == ["R_156018"]
    assert not coverage["unconsumed"] and not coverage["conflicting"]


def test_entity_constraint_coverage_does_not_reverse_entail_specific_pr() -> None:
    result = _generate("List pull requests in repo R_156018 and PR PR_156018#11659.")
    assert result.template_id is None
    coverage = result.validation["selection"]["candidate_entity_constraint_coverage"]["indv4_repo_pull_request_filter"]
    assert any(x["entity_id"] == "PR_156018#11659" for x in coverage["unconsumed"])


def test_entity_constraint_coverage_rejects_conflicting_repo_scope() -> None:
    result = _generate("What objects are referenced by PR PR_156018#11659 and repo R_999999?")
    assert result.template_id is None
    coverage = result.validation["selection"]["candidate_entity_constraint_coverage"]["indv4_reference_object"]
    assert coverage["conflicting"]
    assert any("different repositories" in x.get("reason", "") for x in coverage["conflicting"])


def test_entity_constraint_coverage_rejects_multiple_direct_ids_for_singular_slot() -> None:
    result = _generate("What objects are referenced by PR PR_156018#11659 and PR PR_156018#11660?")
    assert result.template_id is None
    coverage = result.validation["selection"]["candidate_entity_constraint_coverage"]["indv4_reference_object"]
    assert len(coverage["conflicting"]) == 2


def test_narrow_and_comprehensive_aggregation_are_distinct_contracts() -> None:
    narrow = _generate("Count references by domain for PR PR_156018#11659 in 2023.")
    comprehensive = _generate("Comprehensive: For repo R_156018 in 2023, find PRs linked to external resources, group by domain, and return involved actors and latest interaction time.")
    assert narrow.template_id == "indv4_narrow_domain_aggregation"
    assert comprehensive.template_id == "indv4_comprehensive_external_actor_aggregation"
    assert "count(*) AS c" in narrow.rendered_cypher
    assert "collect(DISTINCT a.entity_id)" in comprehensive.rendered_cypher


def test_typed_target_projection_is_preserved() -> None:
    result = _generate("What objects are referenced by PR PR_156018#11659?")
    assert ":PullRequest" in result.rendered_cypher
    assert ":UnknownObject" in result.rendered_cypher


def test_pending_relations_continue_to_abstain() -> None:
    for query in [
        "Chapter 5 placeholder: For repo R_156018, which repos are structurally coupled with it?",
        "Chapter 6 placeholder: Which PRs resolve issue I_156018#12095 over time?",
    ]:
        result = _generate(query)
        assert result.template_id is None
        assert result.failure_stage == "template_selection_or_abstention"


def test_semantic_signature_evaluator_distinguishes_limit_and_shape() -> None:
    matched = compare_semantic_signatures(
        "MATCH (pr:PullRequest {entity_id: 'PR_1#2'}) RETURN pr.entity_id LIMIT 20",
        "MATCH (pr:PullRequest {entity_id: 'PR_1#2'}) RETURN pr.entity_id LIMIT 20",
    )
    mismatched = compare_semantic_signatures(
        "MATCH (pr:PullRequest {entity_id: 'PR_1#2'}) RETURN pr.entity_id LIMIT 25",
        "MATCH (pr:PullRequest {entity_id: 'PR_1#2'}) RETURN pr.entity_id LIMIT 20",
    )
    assert matched["match"]
    assert not mismatched["match"]
    assert "limit" in mismatched["differences"]


def test_d1_entity_alignment_and_relation_mapping_regress() -> None:
    ir = parse_nl_to_ir("q", "Which actors interacted with PR PR_156018#11659 via review comments and references in 2023?")
    assert {x["entity_id"] for x in ir.aligned_entities if x.get("entity_id")} >= {"PR_156018#11659", "R_156018"}
    assert {x["semantic"] for x in ir.relation_semantics} >= {"COMMENTED_ON_REVIEW", "MENTIONS", "REFERENCES"}


def test_d1_1_report_metadata_has_no_stale_recommendation_or_v3_id_score() -> None:
    script = (ROOT / "graph-migration" / "scripts" / "run_d1_independent.py").read_text(encoding="utf-8")
    assert "FIX_MAIN_PATH" not in script
    assert "v3_template_mapping_match_diagnostic" not in script
    assert "PROCEED_TO_D1_2_HELDOUT_NL_ROBUSTNESS" in script
    assert "v3_historical_mapping_reference" in script


def test_semantic_signature_binds_service_values_to_relationship_roles() -> None:
    reference = (
        "MATCH (c:IssueComment)-[r1:EVENT_ACTION]->(i:Issue) "
        "MATCH (c)-[r2:EVENT_ACTION]->(a:Actor) "
        "WHERE r1.service_rel_type = 'COMMENTED_ON_ISSUE' "
        "AND r2.service_rel_type = 'OPENED_BY' RETURN a.entity_id"
    )
    swapped = reference.replace(
        "r1.service_rel_type = 'COMMENTED_ON_ISSUE' AND r2.service_rel_type = 'OPENED_BY'",
        "r1.service_rel_type = 'OPENED_BY' AND r2.service_rel_type = 'COMMENTED_ON_ISSUE'",
    )
    result = compare_semantic_signatures(swapped, reference)
    assert not result["match"]
    assert "service_predicates_by_relationship_role" in result["differences"]


def test_semantic_signature_preserves_service_predicate_boolean_structure() -> None:
    reference = (
        "MATCH (c:IssueComment)-[r1:EVENT_ACTION]->(i:Issue) "
        "MATCH (c)-[r2:EVENT_ACTION]->(a:Actor) "
        "WHERE r1.service_rel_type = 'COMMENTED_ON_ISSUE' "
        "AND r2.service_rel_type = 'OPENED_BY' RETURN a.entity_id"
    )
    changed = reference.replace("AND r2.service_rel_type", "OR r2.service_rel_type")
    result = compare_semantic_signatures(changed, reference)
    assert not result["match"]
    assert "predicate_boolean_structure" in result["differences"]


def test_semantic_signature_preserves_service_literal_case() -> None:
    reference = (
        "MATCH (pr:PullRequest)-[rel:REFERENCE]->(x:UnknownObject) "
        "WHERE rel.service_rel_type = 'REFERENCES' RETURN x.entity_id"
    )
    changed = reference.replace("'REFERENCES'", "'references'")
    result = compare_semantic_signatures(changed, reference)
    assert not result["match"]
    assert "service_predicates_by_relationship_role" in result["differences"]


def test_semantic_signature_bounds_each_where_before_next_match_clause() -> None:
    reference = (
        "MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'PR_1' "
        "MATCH (pr)-[rr:REFERENCE]->(x:UnknownObject) "
        "WHERE rr.service_rel_type = 'REFERENCES' RETURN x.entity_id"
    )
    renamed = (
        "MATCH (pull:PullRequest) WHERE pull.entity_id STARTS WITH 'PR_1' "
        "MATCH (pull)-[edge:REFERENCE]->(obj:UnknownObject) "
        "WHERE edge.service_rel_type = 'REFERENCES' RETURN obj.entity_id"
    )
    assert compare_semantic_signatures(renamed, reference)["match"]


def test_semantic_signature_projection_alias_lookup_is_case_sensitive() -> None:
    valid = "MATCH (pr:PullRequest) RETURN count(*) AS c ORDER BY c LIMIT 20"
    invalid_case = "MATCH (pr:PullRequest) RETURN count(*) AS C ORDER BY c LIMIT 20"
    result = compare_semantic_signatures(invalid_case, valid)
    assert not result["match"]
    assert "sort_keys" in result["differences"]


def test_semantic_signature_binds_anchor_ids_to_node_roles() -> None:
    reference = (
        "MATCH (i:Issue {entity_id: 'I_1#1'})-[:EVENT_ACTION]->"
        "(a:Actor {entity_id: 'A_1'}) RETURN a.entity_id"
    )
    swapped = reference.replace("I_1#1", "TEMP").replace("A_1", "I_1#1").replace("TEMP", "A_1")
    result = compare_semantic_signatures(swapped, reference)
    assert not result["match"]
    assert "anchor_bindings" in result["differences"]


def test_semantic_signature_normalizes_aggregation_function_case() -> None:
    upper = "MATCH (pr:PullRequest) RETURN COUNT(*) AS c LIMIT 20"
    lower = "match (pr:PullRequest) return count(*) as c limit 20"
    assert compare_semantic_signatures(upper, lower)["match"]


def test_semantic_signature_is_alias_invariant() -> None:
    reference = (
        "MATCH (pr:PullRequest {entity_id: 'PR_1#2'})-[rel:REFERENCE]->"
        "(x:UnknownObject) WHERE rel.service_rel_type = 'REFERENCES' "
        "RETURN x.entity_id LIMIT 25"
    )
    renamed = (
        "MATCH (p:PullRequest {entity_id: 'PR_1#2'})-[edge:REFERENCE]->"
        "(obj:UnknownObject) WHERE edge.service_rel_type = 'REFERENCES' "
        "RETURN obj.entity_id LIMIT 25"
    )
    assert compare_semantic_signatures(renamed, reference)["match"]


def test_semantic_signature_ignores_whitespace_and_keyword_case() -> None:
    reference = "MATCH (pr:PullRequest) RETURN pr.entity_id LIMIT 20"
    formatted = "  match  (pr:PullRequest)  return  pr.entity_id  limit 20  "
    assert compare_semantic_signatures(formatted, reference)["match"]


def test_semantic_signature_normalizes_comparison_operator_spacing() -> None:
    compact = "MATCH (pr:PullRequest) WHERE pr.entity_id='PR_1#2' RETURN pr.entity_id"
    spaced = "MATCH (pr:PullRequest) WHERE pr.entity_id = 'PR_1#2' RETURN pr.entity_id"
    assert compare_semantic_signatures(compact, spaced)["match"]


def test_semantic_signature_normalizes_in_list_comma_spacing() -> None:
    compact = "MATCH (pr:PullRequest)-[rel:REFERENCE]->(a:Actor) WHERE rel.service_rel_type IN ['MENTIONS','REFERENCES'] RETURN a.entity_id"
    spaced = "MATCH (pr:PullRequest)-[rel:REFERENCE]->(a:Actor) WHERE rel.service_rel_type IN ['MENTIONS', 'REFERENCES'] RETURN a.entity_id"
    assert compare_semantic_signatures(compact, spaced)["match"]


def test_semantic_signature_preserves_multi_character_comparison_operators() -> None:
    greater_or_equal = "MATCH (pr:PullRequest) WHERE pr.event_time >= '2023-01-01' RETURN pr.entity_id"
    greater = "MATCH (pr:PullRequest) WHERE pr.event_time > '2023-01-01' RETURN pr.entity_id"
    assert not compare_semantic_signatures(greater_or_equal, greater)["match"]


def test_semantic_signature_preserves_distinct_equality_operators() -> None:
    equal = "MATCH (pr:PullRequest) WHERE pr.entity_id = 'PR_1#2' RETURN pr.entity_id"
    not_equal = "MATCH (pr:PullRequest) WHERE pr.entity_id <> 'PR_1#2' RETURN pr.entity_id"
    bang_not_equal = "MATCH (pr:PullRequest) WHERE pr.entity_id != 'PR_1#2' RETURN pr.entity_id"
    assert not compare_semantic_signatures(equal, not_equal)["match"]
    assert not compare_semantic_signatures(equal, bang_not_equal)["match"]


def test_semantic_signature_preserves_quoted_literal_comma_and_space() -> None:
    with_space = "MATCH (pr:PullRequest)-[rel:REFERENCE]->(a:Actor) WHERE rel.service_rel_type = 'A, B' RETURN a.entity_id"
    without_space = "MATCH (pr:PullRequest)-[rel:REFERENCE]->(a:Actor) WHERE rel.service_rel_type = 'A,B' RETURN a.entity_id"
    assert not compare_semantic_signatures(with_space, without_space)["match"]


def test_semantic_signature_canonicalizes_top_level_and_conjunction_order() -> None:
    first = (
        "MATCH (pr:PullRequest)-[rel:REFERENCE]->(x:ExternalResource) "
        "WHERE rel.service_rel_type = 'LINKS_TO' "
        "AND rel.source_event_time >= '2023-01-01T00:00:00Z' "
        "AND rel.source_event_time < '2024-01-01T00:00:00Z' "
        "RETURN x.entity_id"
    )
    reordered = (
        "MATCH (pr:PullRequest)-[rel:REFERENCE]->(x:ExternalResource) "
        "WHERE rel.source_event_time < '2024-01-01T00:00:00Z' "
        "AND rel.service_rel_type = 'LINKS_TO' "
        "AND rel.source_event_time >= '2023-01-01T00:00:00Z' "
        "RETURN x.entity_id"
    )
    assert compare_semantic_signatures(reordered, first)["match"]


def test_semantic_signature_preserves_top_level_or_sensitivity() -> None:
    and_query = "MATCH (pr:PullRequest) WHERE pr.a = 'A' AND pr.b = 'B' RETURN pr.entity_id"
    or_query = "MATCH (pr:PullRequest) WHERE pr.a = 'A' OR pr.b = 'B' RETURN pr.entity_id"
    assert not compare_semantic_signatures(and_query, or_query)["match"]


def test_semantic_signature_preserves_nested_or_grouping() -> None:
    grouped = "MATCH (pr:PullRequest) WHERE pr.a = 'A' AND (pr.b = 'B' OR pr.c = 'C') RETURN pr.entity_id"
    flattened = "MATCH (pr:PullRequest) WHERE pr.a = 'A' AND pr.b = 'B' AND pr.c = 'C' RETURN pr.entity_id"
    assert not compare_semantic_signatures(grouped, flattened)["match"]


def test_semantic_signature_reorders_parenthesized_top_level_terms_only() -> None:
    first = "MATCH (pr:PullRequest) WHERE (pr.a = 'A' OR pr.b = 'B') AND pr.c = 'C' RETURN pr.entity_id"
    reordered = "MATCH (pr:PullRequest) WHERE pr.c = 'C' AND (pr.a = 'A' OR pr.b = 'B') RETURN pr.entity_id"
    assert compare_semantic_signatures(reordered, first)["match"]


def test_semantic_signature_does_not_split_and_inside_quoted_literal() -> None:
    first = "MATCH (pr:PullRequest) WHERE pr.note = 'R&D AND REFERENCES' AND pr.kind = 'x' RETURN pr.entity_id"
    reordered = "MATCH (pr:PullRequest) WHERE pr.kind = 'x' AND pr.note = 'R&D AND REFERENCES' RETURN pr.entity_id"
    assert compare_semantic_signatures(reordered, first)["match"]


def test_semantic_signature_detects_path_role_change() -> None:
    reference = (
        "MATCH (pr:PullRequest)-[rel:REFERENCE]->(x:UnknownObject) "
        "WHERE rel.service_rel_type = 'REFERENCES' RETURN x.entity_id"
    )
    changed = reference.replace(":REFERENCE", ":EVENT_ACTION")
    result = compare_semantic_signatures(changed, reference)
    assert not result["match"]
    assert "paths" in result["differences"]


def test_semantic_signature_resolves_projection_aliases_in_sort_keys() -> None:
    reference = (
        "MATCH (pr:PullRequest) RETURN count(pr.entity_id) AS c "
        "ORDER BY c DESC LIMIT 20"
    )
    renamed = (
        "MATCH (pr:PullRequest) RETURN COUNT(pr.entity_id) AS total "
        "ORDER BY total DESC LIMIT 20"
    )
    assert compare_semantic_signatures(renamed, reference)["match"]


def test_semantic_signature_canonicalizes_independent_mandatory_match_order() -> None:
    first = (
        "MATCH (c:IssueComment)-[r1:EVENT_ACTION]->(i:Issue) "
        "MATCH (c)-[r2:EVENT_ACTION]->(a:Actor) "
        "WHERE r1.service_rel_type = 'COMMENTED_ON_ISSUE' "
        "AND r2.service_rel_type = 'OPENED_BY' RETURN a.entity_id"
    )
    reordered = (
        "MATCH (c:IssueComment)-[r2:EVENT_ACTION]->(a:Actor) "
        "MATCH (c)-[r1:EVENT_ACTION]->(i:Issue) "
        "WHERE r1.service_rel_type = 'COMMENTED_ON_ISSUE' "
        "AND r2.service_rel_type = 'OPENED_BY' RETURN a.entity_id"
    )
    assert compare_semantic_signatures(reordered, first)["match"]


def test_semantic_signature_canonicalizes_bare_alias_references() -> None:
    reference = (
        "MATCH (pr:PullRequest)-[ra:REFERENCE]->(a:Actor) "
        "WHERE ra IS NULL OR ra.service_rel_type IN ['MENTIONS','REFERENCES'] "
        "RETURN a.entity_id"
    )
    renamed = (
        "MATCH (pull:PullRequest)-[edge:REFERENCE]->(actor:Actor) "
        "WHERE edge IS NULL OR edge.service_rel_type IN ['MENTIONS','REFERENCES'] "
        "RETURN actor.entity_id"
    )
    assert compare_semantic_signatures(renamed, reference)["match"]


def test_semantic_signature_canonicalizes_predicate_order_with_match_order() -> None:
    reference = (
        "MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'PR_1' "
        "MATCH (pr)-[rr:REFERENCE]->(x:UnknownObject) "
        "WHERE rr.service_rel_type = 'REFERENCES' RETURN x.entity_id"
    )
    reordered = (
        "MATCH (pr:PullRequest)-[rr:REFERENCE]->(x:UnknownObject) "
        "WHERE rr.service_rel_type = 'REFERENCES' "
        "MATCH (pr:PullRequest) WHERE pr.entity_id STARTS WITH 'PR_1' "
        "RETURN x.entity_id"
    )
    assert compare_semantic_signatures(reordered, reference)["match"]


def test_semantic_signature_canonicalizes_independent_optional_branch_units() -> None:
    reference = (
        "MATCH (s:Actor)-[rm:REFERENCE]->(a:Actor) "
        "WHERE rm.service_rel_type = 'MENTIONS' "
        "OPTIONAL MATCH (s)-[rr:REFERENCE]->(repo:Repo) "
        "WHERE rr.service_rel_type = 'MENTIONS' "
        "OPTIONAL MATCH (s)-[rl:REFERENCE]->(e:ExternalResource) "
        "WHERE rl.service_rel_type = 'LINKS_TO' "
        "RETURN repo.entity_id, e.entity_id"
    )
    reordered = (
        "MATCH (source:Actor)-[main_rel:REFERENCE]->(actor:Actor) "
        "WHERE main_rel.service_rel_type = 'MENTIONS' "
        "OPTIONAL MATCH (source)-[external_rel:REFERENCE]->(external:ExternalResource) "
        "WHERE external_rel.service_rel_type = 'LINKS_TO' "
        "OPTIONAL MATCH (source)-[repo_rel:REFERENCE]->(repository:Repo) "
        "WHERE repo_rel.service_rel_type = 'MENTIONS' "
        "RETURN repository.entity_id, external.entity_id"
    )
    assert compare_semantic_signatures(reordered, reference)["match"]


def test_semantic_signature_preserves_optional_branch_predicate_ownership() -> None:
    reference = (
        "MATCH (s:Actor)-[rm:REFERENCE]->(a:Actor) "
        "WHERE rm.service_rel_type = 'MENTIONS' "
        "OPTIONAL MATCH (s)-[rr:REFERENCE]->(repo:Repo) "
        "WHERE rr.service_rel_type = 'MENTIONS' "
        "OPTIONAL MATCH (s)-[rl:REFERENCE]->(e:ExternalResource) "
        "WHERE rl.service_rel_type = 'LINKS_TO' "
        "RETURN repo.entity_id, e.entity_id"
    )
    swapped_predicates = reference.replace(
        "rr.service_rel_type = 'MENTIONS'",
        "rr.service_rel_type = 'LINKS_TO'",
    ).replace(
        "rl.service_rel_type = 'LINKS_TO'",
        "rl.service_rel_type = 'MENTIONS'",
    )
    result = compare_semantic_signatures(swapped_predicates, reference)
    assert not result["match"]
    assert "predicate_boolean_structure" in result["differences"]


def test_semantic_signature_preserves_dependent_optional_order() -> None:
    reference = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(repo:Repo) "
        "OPTIONAL MATCH (repo)-[rb:REFERENCE]->(e:ExternalResource) "
        "RETURN e.entity_id"
    )
    reordered = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (repo)-[rb:REFERENCE]->(e:ExternalResource) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(repo:Repo) "
        "RETURN e.entity_id"
    )
    assert not compare_semantic_signatures(reordered, reference)["match"]


def test_semantic_signature_ignores_unused_optional_relationship_alias() -> None:
    named = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[r:REFERENCE]->(repo:Repo) "
        "RETURN repo.entity_id"
    )
    anonymous = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[:REFERENCE]->(repo:Repo) "
        "RETURN repo.entity_id"
    )
    assert compare_semantic_signatures(named, anonymous)["match"]


def test_semantic_signature_ignores_unused_optional_relationship_alias_rename() -> None:
    first = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[r:REFERENCE]->(repo:Repo) "
        "RETURN repo.entity_id"
    )
    renamed = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[link_rel:REFERENCE]->(repo:Repo) "
        "RETURN repo.entity_id"
    )
    assert compare_semantic_signatures(renamed, first)["match"]


def test_semantic_signature_keeps_own_optional_relationship_predicate_alias_invariant() -> None:
    first = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[r:REFERENCE]->(repo:Repo) "
        "WHERE r.service_rel_type = 'MENTIONS' RETURN repo.entity_id"
    )
    renamed = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[link_rel:REFERENCE]->(repo:Repo) "
        "WHERE link_rel.service_rel_type = 'MENTIONS' RETURN repo.entity_id"
    )
    assert compare_semantic_signatures(renamed, first)["match"]


def test_semantic_signature_does_not_erase_owned_predicate_with_anonymous_edge() -> None:
    named = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[r:REFERENCE]->(repo:Repo) "
        "WHERE r.service_rel_type = 'MENTIONS' RETURN repo.entity_id"
    )
    anonymous_without_predicate = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[:REFERENCE]->(repo:Repo) "
        "RETURN repo.entity_id"
    )
    result = compare_semantic_signatures(named, anonymous_without_predicate)
    assert not result["match"]
    assert "predicate_boolean_structure" in result["differences"]


def test_semantic_signature_preserves_real_optional_relationship_dependency() -> None:
    first = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[r:REFERENCE]->(repo:Repo) "
        "OPTIONAL MATCH (r)-[:REFERENCE]->(e:ExternalResource) "
        "RETURN e.entity_id"
    )
    renamed = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[link_rel:REFERENCE]->(repo:Repo) "
        "OPTIONAL MATCH (link_rel)-[:REFERENCE]->(e:ExternalResource) "
        "RETURN e.entity_id"
    )
    anonymous_rewrite = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[:REFERENCE]->(repo:Repo) "
        "OPTIONAL MATCH (s)-[:REFERENCE]->(e:ExternalResource) "
        "RETURN e.entity_id"
    )
    assert compare_semantic_signatures(renamed, first)["match"]
    assert not compare_semantic_signatures(anonymous_rewrite, first)["match"]


def test_semantic_signature_binds_consecutive_optional_where_to_immediate_clause() -> None:
    reference = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "WHERE rb.service_rel_type = 'B' RETURN a.entity_id, b.entity_id"
    )
    moved_clause = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "WHERE rb.service_rel_type = 'B' RETURN a.entity_id, b.entity_id"
    )
    assert not compare_semantic_signatures(moved_clause, reference)["match"]


def test_semantic_signature_keeps_immediate_optional_owner_when_where_references_earlier_branch() -> None:
    query = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "WHERE ra.service_rel_type = 'A' RETURN a.entity_id, b.entity_id"
    )
    signature = semantic_signature(query)
    predicates = signature["predicate_boolean_structure"]
    assert len(predicates) == 2
    assert predicates[0]["structure"] == ""
    assert "node:REPO[0]" in predicates[1]["structure"]
    assert predicates[1]["owner"].endswith("node:REPO[1][0]")


def test_semantic_signature_terminal_return_relationship_use_does_not_order_optional_siblings() -> None:
    reference = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "RETURN ra.source_event_time, rb.source_event_time"
    )
    reordered = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "RETURN rb.source_event_time, ra.source_event_time"
    )
    assert compare_semantic_signatures(reordered, reference)["match"]


def test_semantic_signature_terminal_order_by_relationship_use_does_not_order_optional_siblings() -> None:
    reference = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "RETURN a.entity_id, b.entity_id ORDER BY ra.source_event_time"
    )
    reordered = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "RETURN b.entity_id, a.entity_id ORDER BY rb.source_event_time"
    )
    assert compare_semantic_signatures(reordered, reference)["match"]


def test_semantic_signature_preserves_later_optional_where_dependency_on_relationship_alias() -> None:
    reference = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "WHERE ra.service_rel_type = 'A' RETURN a.entity_id, b.entity_id"
    )
    reordered = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) "
        "WHERE rb.service_rel_type = 'A' RETURN a.entity_id, b.entity_id"
    )
    assert not compare_semantic_signatures(reordered, reference)["match"]


def test_semantic_signature_stabilizes_same_label_optional_sibling_identity() -> None:
    reference = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) WHERE ra.service_rel_type = 'A' "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) WHERE rb.service_rel_type = 'B' "
        "RETURN a.entity_id, b.entity_id"
    )
    reordered = (
        "MATCH (source:Actor) "
        "OPTIONAL MATCH (source)-[right_rel:REFERENCE]->(right:Repo) WHERE right_rel.service_rel_type = 'B' "
        "OPTIONAL MATCH (source)-[left_rel:REFERENCE]->(left:Repo) WHERE left_rel.service_rel_type = 'A' "
        "RETURN left.entity_id, right.entity_id"
    )
    assert compare_semantic_signatures(reordered, reference)["match"]


def test_semantic_signature_same_label_predicate_assignment_is_not_erased() -> None:
    reference = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) WHERE ra.service_rel_type = 'A' "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) WHERE rb.service_rel_type = 'B' "
        "RETURN a.entity_id, b.entity_id"
    )
    swapped_predicates = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) WHERE ra.service_rel_type = 'B' "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) WHERE rb.service_rel_type = 'A' "
        "RETURN a.entity_id, b.entity_id"
    )
    assert not compare_semantic_signatures(swapped_predicates, reference)["match"]


def test_semantic_signature_same_label_alias_rename_is_invariant() -> None:
    reference = (
        "MATCH (s:Actor) "
        "OPTIONAL MATCH (s)-[ra:REFERENCE]->(a:Repo) WHERE ra.service_rel_type = 'A' "
        "OPTIONAL MATCH (s)-[rb:REFERENCE]->(b:Repo) WHERE rb.service_rel_type = 'B' "
        "RETURN a.entity_id, b.entity_id"
    )
    renamed = (
        "MATCH (source:Actor) "
        "OPTIONAL MATCH (source)-[first_rel:REFERENCE]->(first:Repo) WHERE first_rel.service_rel_type = 'A' "
        "OPTIONAL MATCH (source)-[second_rel:REFERENCE]->(second:Repo) WHERE second_rel.service_rel_type = 'B' "
        "RETURN first.entity_id, second.entity_id"
    )
    assert compare_semantic_signatures(renamed, reference)["match"]


def test_semantic_signature_preserves_identical_same_label_optional_multiplicity() -> None:
    two_siblings = (
        "MATCH (s:Actor) OPTIONAL MATCH (s)-[:REFERENCE]->(a:Repo) "
        "OPTIONAL MATCH (s)-[:REFERENCE]->(b:Repo) RETURN a.entity_id, b.entity_id"
    )
    one_sibling = "MATCH (s:Actor) OPTIONAL MATCH (s)-[:REFERENCE]->(a:Repo) RETURN a.entity_id"
    assert not compare_semantic_signatures(two_siblings, one_sibling)["match"]


def test_semantic_signature_strips_repeated_redundant_outer_parentheses() -> None:
    atomic = "MATCH (r:REFERENCE) WHERE r.service_rel_type = 'REFERENCES' RETURN r.entity_id"
    wrapped = "MATCH (r:REFERENCE) WHERE ((r.service_rel_type = 'REFERENCES')) RETURN r.entity_id"
    assert compare_semantic_signatures(wrapped, atomic)["match"]


def test_semantic_signature_strips_outer_parentheses_around_whole_or_only() -> None:
    plain = "MATCH (r:REFERENCE) WHERE r.a = 'A' OR r.b = 'B' RETURN r.entity_id"
    wrapped = "MATCH (r:REFERENCE) WHERE (r.a = 'A' OR r.b = 'B') RETURN r.entity_id"
    assert compare_semantic_signatures(wrapped, plain)["match"]


def test_semantic_signature_preserves_meaningful_boolean_grouping() -> None:
    grouped = "MATCH (r:REFERENCE) WHERE r.a = 'A' AND (r.b = 'B' OR r.c = 'C') RETURN r.entity_id"
    redistributed = "MATCH (r:REFERENCE) WHERE (r.a = 'A' AND r.b = 'B') OR r.c = 'C' RETURN r.entity_id"
    assert not compare_semantic_signatures(redistributed, grouped)["match"]


def test_semantic_signature_preserves_parentheses_in_literals_and_bounded_lists() -> None:
    with_parentheses = (
        "MATCH (r:REFERENCE) WHERE r.service_rel_type IN ['A(B)', 'C'] "
        "RETURN count(r.entity_id)"
    )
    changed_literal = (
        "MATCH (r:REFERENCE) WHERE r.service_rel_type IN ['AB', 'C'] "
        "RETURN count(r.entity_id)"
    )
    assert not compare_semantic_signatures(with_parentheses, changed_literal)["match"]


def test_semantic_signature_preserves_inline_property_key_case() -> None:
    reference = "MATCH (pr:PullRequest {entity_id: 'PR_1#2'}) RETURN pr.entity_id"
    changed = "MATCH (pr:PullRequest {ENTITY_ID: 'PR_1#2'}) RETURN pr.entity_id"
    result = compare_semantic_signatures(changed, reference)
    assert not result["match"]
    assert "node_property_bindings" in result["differences"]


def test_semantic_signature_retains_where_branch_ownership() -> None:
    mandatory_where = (
        "MATCH (pr:PullRequest)-[rl:REFERENCE]->(e:ExternalResource) "
        "WHERE rl.service_rel_type = 'LINKS_TO' "
        "OPTIONAL MATCH (pr)-[ra:REFERENCE]->(a:Actor) "
        "RETURN e.url_domain_etld1"
    )
    optional_where = (
        "MATCH (pr:PullRequest)-[rl:REFERENCE]->(e:ExternalResource) "
        "OPTIONAL MATCH (pr)-[ra:REFERENCE]->(a:Actor) "
        "WHERE rl.service_rel_type = 'LINKS_TO' "
        "RETURN e.url_domain_etld1"
    )
    result = compare_semantic_signatures(optional_where, mandatory_where)
    assert not result["match"]
    assert "predicate_boolean_structure" in result["differences"]


def test_semantic_signature_retains_additional_labels_on_reused_aliases() -> None:
    reference = (
        "MATCH (c:IssueComment)-[r1:EVENT_ACTION]->(i:Issue) "
        "MATCH (c)-[r2:REFERENCE]->(i) RETURN i.entity_id"
    )
    changed = (
        "MATCH (c:IssueComment)-[r1:EVENT_ACTION]->(i:Issue) "
        "MATCH (c)-[r2:REFERENCE]->(i:Actor) RETURN i.entity_id"
    )
    result = compare_semantic_signatures(changed, reference)
    assert not result["match"]
    assert "node_roles" in result["differences"]


def test_d1_1_pilot_closure_regression() -> None:
    queries_path = ROOT / "data_real" / "pilot_queries" / "queries_pilot.jsonl"
    requests = []
    annotations = {}
    for line in queries_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        request_id = str(payload.get("id") or "")
        requests.append({"id": request_id, "nl_query": str(payload.get("nl_query") or "")})
        annotations[request_id] = payload

    results = __import__("runners.independent_controlled_pipeline", fromlist=["run_independent_requests"]).run_independent_requests(
        requests,
        TEMPLATES,
        SCHEMA,
    )
    executable = [result for result in results if annotations[result.request_id].get("gold_cypher")]
    pending = [result for result in results if not annotations[result.request_id].get("gold_cypher")]
    assert len(results) == 15
    assert len(executable) == 13
    assert len(pending) == 2
    assert sum(bool(result.validation.get("valid")) for result in executable) == 13
    corrections = yaml.safe_load(REFERENCE_CORRECTIONS.read_text(encoding="utf-8"))["corrections"]
    source_matches = sum(
        compare_semantic_signatures(
            result.rendered_cypher,
            str(annotations[result.request_id].get("gold_cypher") or ""),
        )["match"]
        for result in executable
    )
    effective_matches = sum(
        compare_semantic_signatures(
            result.rendered_cypher,
            str(corrections.get(result.request_id, {}).get("corrected_cypher") or annotations[result.request_id].get("gold_cypher") or ""),
        )["match"]
        for result in executable
    )
    assert source_matches == 12
    assert effective_matches == 13
    assert sum(result.failure_stage == "template_selection_or_abstention" for result in pending) == 2
    assert sum(bool(result.repair) for result in results) == 0
