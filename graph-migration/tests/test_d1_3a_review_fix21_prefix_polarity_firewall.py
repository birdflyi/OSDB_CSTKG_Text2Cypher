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


@pytest.mark.parametrize(
    "query",
    [
        "Show issues except for the I_910101 prefix",
        "Show issues except for I_910101 prefix",
        "Show pull requests except for the PR_920202 prefix",
        "Show issues except the I_910101 prefix",
        "Show issues excluding the I_910101 prefix",
        "Show issues without the I_910101 prefix",
        "Show issues omitting the I_910101 prefix",
        "Show issues but not the I_910101 prefix",
    ],
)
def test_bounded_prefix_exclusion_surfaces_fail_closed(query: str) -> None:
    ir = parse_nl_to_ir("fix21", query)
    result = generate_independent("fix21", query, TEMPLATES, SCHEMA)

    assert ir.entity_scopes
    scope = ir.entity_scopes[0]
    assert scope.operator == "NOT_STARTS_WITH", query
    assert scope.provenance == "negated_typed_prefix_scope_from_nl", query
    assert ir.bounded_status == "ABSTAIN_UNSUPPORTED_EXPLICIT_CONSTRAINT", query
    assert result.template_id is None, query
    assert result.rendered_cypher is None, query
    entries = [
        item
        for item in ir.unsupported_explicit_constraints
        if item["kind"] == "unsupported_exclusion_surface"
    ]
    assert entries, query
    entry = entries[0]
    assert entry["reason_code"] == UNSUPPORTED_EXPLICIT_CONSTRAINT_REASON
    assert entry["source_span"] == scope.source_span
    assert entry["surface_text"]
    assert "STARTS WITH" not in (result.rendered_cypher or "")


@pytest.mark.parametrize(
    "query,template_id,prefix",
    [
        (
            "Show issues whose IDs start with I_910101",
            "indv5_issue_prefix_list",
            "I_910101",
        ),
        (
            "Show issues with I_910101 prefix",
            "indv5_issue_prefix_list",
            "I_910101",
        ),
        (
            "List pull requests whose IDs start with PR_920202",
            "indv4_repo_pull_request_filter",
            "PR_920202",
        ),
    ],
)
def test_positive_prefix_forms_remain_executable(
    query: str, template_id: str, prefix: str
) -> None:
    result = generate_independent("fix21", query, TEMPLATES, SCHEMA)
    assert result.ir.entity_scopes[0].operator == "STARTS_WITH"
    assert result.template_id == template_id
    assert f"STARTS WITH '{prefix}'" in (result.rendered_cypher or "")


def test_unrelated_sentence_negation_does_not_poison_positive_prefix() -> None:
    query = "Show issues without comments. Show issues with prefix I_910101"
    result = generate_independent("fix21", query, TEMPLATES, SCHEMA)
    assert result.ir.entity_scopes[0].operator == "STARTS_WITH"
    assert result.template_id == "indv5_issue_prefix_list"
    assert "STARTS WITH 'I_910101'" in (result.rendered_cypher or "")


def test_local_unrelated_negation_does_not_poison_positive_prefix() -> None:
    query = "Show issues except comments; use prefix I_910101"
    result = generate_independent("fix21", query, TEMPLATES, SCHEMA)
    assert result.ir.entity_scopes[0].operator == "STARTS_WITH"
    assert result.template_id == "indv5_issue_prefix_list"
    assert "STARTS WITH 'I_910101'" in (result.rendered_cypher or "")


def test_existing_trailing_exclusion_remains_fail_closed() -> None:
    query = "Show issues whose IDs start with I_910101 but not I_910102"
    result = generate_independent("fix21", query, TEMPLATES, SCHEMA)
    assert result.template_id is None
    assert result.rendered_cypher is None
    assert result.ir.unsupported_explicit_constraints
