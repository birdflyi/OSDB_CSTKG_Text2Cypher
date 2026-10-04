from __future__ import annotations

from pathlib import Path

from repair.gold_blind_repair import repair_gold_blind
from runners.independent_controlled_pipeline import (
    generate_independent,
    load_independent_schema,
    load_independent_templates,
    parse_nl_to_ir,
    select_template,
)
from evaluation.semantic_signature import compare_semantic_signatures


ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v4.yaml"
SCHEMA = ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"


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


def test_contract_default_limits_are_family_specific() -> None:
    narrow = _generate("Count references by domain for PR PR_156018#11659 in 2023.")
    comprehensive = _generate("Comprehensive: For repo R_156018 in 2023, find PRs linked to external resources, group by domain, and return involved actors and latest interaction time.")
    assert narrow.rendered_cypher.endswith("LIMIT 20")
    assert comprehensive.rendered_cypher.endswith("LIMIT 30")


def test_multi_relation_target_contract_is_preserved() -> None:
    result = _generate("For actor A_7045099, find mentioned repos and external links.")
    assert result.template_id == "indv4_actor_multi_target_reference"
    assert "OPTIONAL MATCH" in result.rendered_cypher
    assert "repo.entity_id" in result.rendered_cypher and "e.entity_id" in result.rendered_cypher


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
