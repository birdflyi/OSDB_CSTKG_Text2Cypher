from __future__ import annotations

from pathlib import Path

from repair.gold_blind_repair import repair_gold_blind
from runners.independent_controlled_pipeline import (
    load_independent_schema,
    load_independent_templates,
    parse_nl_to_ir,
    select_template,
)


ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "data_real" / "pilot_queries" / "minimal_template_pack_group3_v3.yaml"
SCHEMA = ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"


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
