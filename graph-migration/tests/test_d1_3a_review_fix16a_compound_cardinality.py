from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))

from runners.independent_controlled_pipeline import (  # noqa: E402
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
    return generate_independent("fix16a", query, TEMPLATES, SCHEMA)


@pytest.mark.parametrize(
    "cap_phrase",
    [
        "at most 10 people who opened it and their IDs",
        "at most 10 actors who opened it and return their IDs",
        "at most 10 users who opened it; give me the identifiers",
        "at most 25 people who opened it and their IDs",
    ],
)
def test_compound_entity_cardinality_with_later_ids_fails_closed(cap_phrase: str) -> None:
    query = f"For issue I_900001#2, show {cap_phrase}"
    ir = parse_nl_to_ir("fix16a", query)
    result = _generate(query)

    assert ir.bounded_status == "ABSTAIN_UNSUPPORTED_EXPLICIT_CONSTRAINT"
    assert any(
        item["kind"] == "unsupported_cardinality_surface"
        and item["surface_text"].startswith("at most")
        for item in ir.unsupported_explicit_constraints
    )
    assert result.template_id is None
    assert result.rendered_cypher is None
    assert result.validation["selection"]["reason"] == "unsupported explicit NL constraint"


def test_canonical_pull_request_id_list_default_cap_remains_entailed() -> None:
    query = (
        "List up to 25 pull requests whose IDs start with PR_900001, "
        "showing just their IDs."
    )
    ir = parse_nl_to_ir("fix16a", query)
    result = _generate(query)

    assert not ir.unsupported_explicit_constraints
    assert ir.unnormalized_limit_values == [25]
    assert result.template_id == "indv4_repo_pull_request_filter"
    assert result.rendered_cypher is not None
    assert result.rendered_cypher.endswith("LIMIT 25")
    coverage = result.validation["selection"]["ir_constraint_coverage"]["limit"]
    assert coverage["contract_default_entailed_values"] == [25]


def test_conflicting_pull_request_id_list_cap_does_not_inherit_default() -> None:
    query = (
        "List up to 10 pull requests whose IDs start with PR_900001, "
        "showing just their IDs."
    )
    ir = parse_nl_to_ir("fix16a", query)
    result = _generate(query)

    assert not ir.unsupported_explicit_constraints
    assert ir.unnormalized_limit_values == [10]
    assert result.template_id is None
    assert result.rendered_cypher is None


def test_unrelated_later_ids_do_not_exempt_entity_cardinality() -> None:
    query = "Show at most 10 issues linked from this report, then return their IDs"
    ir = parse_nl_to_ir("fix16a", query)

    assert any(
        item["kind"] == "unsupported_cardinality_surface"
        for item in ir.unsupported_explicit_constraints
    )


def test_supported_top_limit_remains_unchanged() -> None:
    result = _generate("For issue I_900001#2, show top 10 actors who opened it")

    assert result.template_id == "indv4_issue_opened_by"
    assert result.rendered_cypher is not None
    assert result.rendered_cypher.endswith("LIMIT 10")
