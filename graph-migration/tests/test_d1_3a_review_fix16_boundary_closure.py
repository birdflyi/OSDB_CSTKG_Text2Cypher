from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))

from runners.independent_controlled_pipeline import (  # noqa: E402
    UNSUPPORTED_EXPLICIT_CONSTRAINT_REASON,
    generate_independent,
    load_independent_schema,
    load_independent_templates,
    parse_nl_to_ir,
)


TEMPLATES = load_independent_templates(
    ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml"
)
SCHEMA = load_independent_schema(
    ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"
)


def _generate(query: str):
    return generate_independent("fix16", query, TEMPLATES, SCHEMA)


@pytest.mark.parametrize("cue", ["other than", "apart from", "unless", "but those"])
def test_out_of_grammar_exclusion_surfaces_fail_closed(cue: str) -> None:
    query = (
        f"Show issues {cue} whose IDs start with I_900001"
        if cue == "but those"
        else f"Show issues {cue} those whose IDs start with I_900001"
    )
    ir = parse_nl_to_ir("fix16", query)
    result = _generate(query)

    assert ir.bounded_status == "ABSTAIN_UNSUPPORTED_EXPLICIT_CONSTRAINT"
    assert result.template_id is None
    assert result.rendered_cypher is None
    assert [item["kind"] for item in ir.unsupported_explicit_constraints] == [
        "unsupported_exclusion_surface"
    ]
    assert ir.unsupported_explicit_constraints[0]["reason_code"] == (
        UNSUPPORTED_EXPLICIT_CONSTRAINT_REASON
    )
    assert result.validation["selection"]["reason"] == "unsupported explicit NL constraint"
    coverage = next(
        iter(result.validation["selection"]["candidate_ir_constraint_coverage"].values())
    )
    assert coverage["unsupported_explicit_constraints"]["consumed"] == []
    assert coverage["unsupported_explicit_constraints"]["unconsumed"]


@pytest.mark.parametrize("suffix", ["without duplicates", "with no duplicates", "no duplicate results"])
def test_out_of_grammar_postfix_uniqueness_fails_closed(suffix: str) -> None:
    query = f"For issue comment IC_1#2, show which actors it mentions {suffix}"
    ir = parse_nl_to_ir("fix16", query)
    result = _generate(query)

    assert result.template_id is None
    assert result.rendered_cypher is None
    assert any(
        item["kind"] == "unsupported_postfix_uniqueness_surface"
        for item in ir.unsupported_explicit_constraints
    )


def test_supported_distinct_surface_is_not_reclassified_by_postfix_detector() -> None:
    ir = parse_nl_to_ir(
        "fix16", "For issue comment IC_1#2, show distinct actors it mentions without duplicates"
    )
    assert not any(
        item["kind"] == "unsupported_postfix_uniqueness_surface"
        for item in ir.unsupported_explicit_constraints
    )


@pytest.mark.parametrize("phrase", [
    "at most 10 people",
    "no more than 10 actors",
    "up to 10 users",
    "limited to 10 issues",
    "capped at 10 people",
])
def test_unsupported_entity_cardinality_never_inherits_template_default(phrase: str) -> None:
    query = f"For issue I_1#2, show {phrase} who opened it"
    ir = parse_nl_to_ir("fix16", query)
    result = _generate(query)

    assert result.template_id is None
    assert result.rendered_cypher is None
    assert any(
        item["kind"] == "unsupported_cardinality_surface"
        for item in ir.unsupported_explicit_constraints
    )


def test_supported_top_limit_remains_executable() -> None:
    result = _generate("For issue I_1#2, show top 10 actors who opened it")
    assert result.template_id == "indv4_issue_opened_by"
    assert result.rendered_cypher is not None
    assert result.rendered_cypher.endswith("LIMIT 10")


@pytest.mark.parametrize(
    "query",
    [
        "The process ran without duplicates during migration.",
        "Discuss other than the historical notes in the report.",
        "Show issues except those whose IDs start with I_900001",
    ],
)
def test_boundary_detector_has_false_positive_controls(query: str) -> None:
    ir = parse_nl_to_ir("fix16", query)
    assert not any(
        item["kind"] == "unsupported_postfix_uniqueness_surface"
        for item in ir.unsupported_explicit_constraints
    )
    if "except" in query:
        assert not any(
            item["kind"] == "unsupported_exclusion_surface"
            for item in ir.unsupported_explicit_constraints
        )
