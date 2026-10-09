from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))

from runners.independent_controlled_pipeline import (  # noqa: E402
    generate_independent,
    load_independent_schema,
    load_independent_templates,
    parse_nl_to_ir,
)


TEMPLATES = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml"
SCHEMA = ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"


def _generate(query: str):
    return generate_independent(
        "fix9",
        query,
        load_independent_templates(TEMPLATES),
        load_independent_schema(SCHEMA),
    )


def test_negated_typed_prefix_scopes_are_represented_and_fail_closed() -> None:
    cases = [
        "Show issues whose IDs do not start with I_880001",
        "Exclude issues whose IDs start with I_880001",
        "Show pull requests whose IDs don't begin with PR_880002",
        "Show issues whose IDs not starting with I_880001",
    ]
    for query in cases:
        result = _generate(query)
        assert len(result.ir.entity_scopes) == 1
        scope = result.ir.entity_scopes[0]
        assert scope.operator == "NOT_STARTS_WITH"
        assert scope.provenance == "negated_typed_prefix_scope_from_nl"
        assert result.template_id is None
        assert result.rendered_cypher is None
        assert "STARTS WITH" not in str(result.rendered_cypher)
        reason_codes = {
            item["reason_code"]
            for coverage in result.validation["selection"]["candidate_ir_constraint_coverage"].values()
            for item in coverage["entity_scopes"]["unconsumed"]
        }
        assert "UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR" in reason_codes


def test_positive_typed_prefix_scope_regression_still_renders() -> None:
    for query in (
        "Show issues whose IDs start with I_880001",
        "Show issues whose IDs begin with I_880001",
    ):
        result = _generate(query)
        assert result.ir.entity_scopes[0].operator == "STARTS_WITH"
        assert result.template_id == "indv5_issue_prefix_list"
        assert "STARTS WITH 'I_880001'" in (result.rendered_cypher or "")


def test_punctuated_source_introduction_keeps_actor_projection() -> None:
    for punctuation in ("(", ":", "["):
        query = f"Show issue {punctuation}I_880001#2{')' if punctuation == '(' else ']' if punctuation == '[' else ''} and tell me who opened it"
        result = _generate(query)
        assert result.ir.source_entity == {
            "entity_id": "I_880001#2",
            "entity_label": "Issue",
            "provenance": "canonical_id_from_nl",
            "alignment": "direct_canonical_id",
            "confidence": 1.0,
        }
        assert [(item.label, item.property) for item in result.ir.projection_items] == [
            ("Actor", "entity_id")
        ]
        assert result.template_id == "indv4_issue_opened_by"
        assert "I_880001#2" in (result.rendered_cypher or "")


def test_lexical_gap_is_not_source_introduction() -> None:
    ir = parse_nl_to_ir("fix9", "Show issue ID I_880001#2")
    assert ir.source_entity is not None
    assert ir.source_entity["entity_id"] == "I_880001#2"
    assert [(item.label, item.property) for item in ir.projection_items] == [
        ("Issue", "entity_id")
    ]


def test_composite_punctuated_source_anchor_suppresses_embedded_issue() -> None:
    ir = parse_nl_to_ir(
        "fix9",
        "Show issue comment (IC_880001#3) and list actors it mentions",
    )
    assert ir.source_entity["entity_label"] == "IssueComment"
    assert ir.source_entity["entity_id"] == "IC_880001#3"
    assert [(item.label, item.property) for item in ir.projection_items] == [
        ("Actor", "entity_id")
    ]
