from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "graph-migration"))

from runners.independent_controlled_pipeline import (  # noqa: E402
    RepoScopeTypedPrefixConflictError,
    _slot_values,
    audit_ir_constraint_coverage,
    generate_independent,
    load_independent_schema,
    load_independent_templates,
    parse_nl_to_ir,
)


TEMPLATES_PATH = ROOT / "data_real" / "pilot_queries" / "independent_template_pack_v5.yaml"
SCHEMA_PATH = ROOT / "data_real" / "pilot_queries" / "schema_metadata.yaml"
TEMPLATES = load_independent_templates(TEMPLATES_PATH)
SCHEMA = load_independent_schema(SCHEMA_PATH)


def _generate(query: str):
    return generate_independent("fix14", query, TEMPLATES, SCHEMA)


def _template(template_id: str):
    return next(item for item in TEMPLATES if item.template_id == template_id)


def _unconsumed_reason(result, reason_code: str) -> dict:
    candidates = result.validation["selection"]["candidate_ir_constraint_coverage"]
    return next(
        item
        for candidate in candidates.values()
        for item in candidate["entity_scopes"]["unconsumed"]
        if item.get("reason_code") == reason_code
    )


@pytest.mark.parametrize(
    ("query", "label", "expected_prefix", "explicit_prefix"),
    [
        (
            "For repo R_900001, show pull requests whose IDs start with PR_900002",
            "PullRequest",
            "PR_900001",
            "PR_900002",
        ),
        (
            "For repo R_900001, show issues whose IDs start with I_900002",
            "Issue",
            "I_900001",
            "I_900002",
        ),
        (
            "For pull request PR_900001#77, show pull requests whose IDs start with PR_900002",
            "PullRequest",
            "PR_900001",
            "PR_900002",
        ),
    ],
)
def test_repo_context_and_explicit_typed_prefix_conflict_abstains(
    query: str, label: str, expected_prefix: str, explicit_prefix: str
) -> None:
    result = _generate(query)
    assert result.template_id is None
    assert result.rendered_cypher is None
    conflict = _unconsumed_reason(result, "REPO_SCOPE_TYPED_PREFIX_CONFLICT")
    assert conflict["repo_entity_id"] == "R_900001"
    assert conflict["scope_label"] == label
    assert conflict["expected_repo_prefix"] == expected_prefix
    assert conflict["explicit_scope_value"] == explicit_prefix
    assert conflict["scope_operator"] == "STARTS_WITH"
    assert conflict["source_span"] == result.ir.entity_scopes[0].source_span
    if label == "PullRequest" and result.ir.aligned_entities:
        assert result.ir.repo_scope["repo_entity_id"] == "R_900001"
    assert all(
        not candidate["accepted"]
        for candidate in result.validation["selection"][
            "candidate_ir_constraint_coverage"
        ].values()
        if any(
            item.get("reason_code") == "REPO_SCOPE_TYPED_PREFIX_CONFLICT"
            for item in candidate["entity_scopes"]["unconsumed"]
        )
    )


def test_equal_repo_and_typed_prefix_constraints_remain_eligible() -> None:
    query = "For repo R_900001, show pull requests whose IDs start with PR_900001"
    result = _generate(query)
    assert result.template_id == "indv4_repo_pull_request_filter"
    assert result.rendered_cypher is not None
    assert "STARTS WITH 'PR_900001'" in result.rendered_cypher
    coverage = audit_ir_constraint_coverage(
        result.ir, _template("indv4_repo_pull_request_filter")
    )
    assert coverage["accepted"]
    assert not coverage["entity_scopes"]["slot_conflicts"]
    values = _slot_values(result.ir, _template("indv4_repo_pull_request_filter"))
    assert values["pr_base_prefix"] == "PR_900001"


def test_direct_slot_materialization_rejects_repo_prefix_conflict() -> None:
    ir = parse_nl_to_ir(
        "fix14",
        "For repo R_900001, show pull requests whose IDs start with PR_900002",
    )
    with pytest.raises(RepoScopeTypedPrefixConflictError) as captured:
        _slot_values(ir, _template("indv4_repo_pull_request_filter"))
    assert captured.value.reason_code == "REPO_SCOPE_TYPED_PREFIX_CONFLICT"
    assert captured.value.expected_repo_prefix == "PR_900001"
    assert captured.value.explicit_scope_value == "PR_900002"


def test_previously_supported_multiple_explicit_prefix_conflict_still_abstains() -> None:
    result = _generate(
        "Show pull requests whose ID starts with PR_900001 or PR_900002."
    )
    assert result.template_id is None
    assert result.rendered_cypher is None
    assert any(
        item.get("reason_code") == "MULTIPLE_DISTINCT_VALUES_FOR_SINGULAR_SCOPE_SLOT"
        for candidate in result.validation["selection"][
            "candidate_ir_constraint_coverage"
        ].values()
        for item in candidate["entity_scopes"]["slot_conflicts"]
    )


@pytest.mark.parametrize(
    "query",
    [
        "Show issues excluding I_900001 prefix",
        "Show issues exclude I_900001 prefix",
        "Show issues omitting I_900001 prefix",
        "Show issues omit I_900001 prefix",
        "Show issues without I_900001 prefix",
        "Show issues except I_900001 prefix",
        "Show issues but not I_900001 prefix",
        "Show pull requests excluding PR_900001 prefix",
        "Show pull requests exclude PR_900001 prefix",
        "Show pull requests omitting PR_900001 prefix",
        "Show pull requests omit PR_900001 prefix",
        "Show pull requests without PR_900001 prefix",
        "Show pull requests except PR_900001 prefix",
        "Show pull requests but not PR_900001 prefix",
    ],
)
def test_post_token_negation_is_preserved_and_fails_closed(query: str) -> None:
    result = _generate(query)
    assert len(result.ir.entity_scopes) == 1
    scope = result.ir.entity_scopes[0]
    assert scope.operator == "NOT_STARTS_WITH"
    assert scope.provenance == "negated_typed_prefix_scope_from_nl"
    assert result.template_id is None
    assert result.rendered_cypher is None
    assert _unconsumed_reason(
        result, "UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR"
    )["value"] == scope.value


@pytest.mark.parametrize(
    "query",
    [
        "Show issues whose IDs do not start with I_900001",
        "Show issues excluding IDs that start with I_900001",
        "Show issues excluding I_900001 prefix",
        "Show issues except I_900001 prefix",
        "Show pull requests, but not those whose IDs start with PR_900001",
    ],
)
def test_pre_token_and_existing_negation_forms_remain_negative(query: str) -> None:
    result = _generate(query)
    assert result.ir.entity_scopes[0].operator == "NOT_STARTS_WITH"
    assert result.ir.entity_scopes[0].provenance == "negated_typed_prefix_scope_from_nl"
    assert result.template_id is None
    assert result.rendered_cypher is None


@pytest.mark.parametrize(
    "query",
    [
        "Show issues whose IDs start with I_900001",
        "Use I_900001 prefix",
        "Show I_900001 prefix",
        "Show issues excluding comments; use I_900001 prefix",
        "Show issues but not comments; use I_900001 prefix",
        "Show issues except comments, whose IDs start with I_900001",
    ],
)
def test_positive_post_token_and_unrelated_negation_controls_remain_positive(
    query: str,
) -> None:
    result = _generate(query)
    assert result.ir.entity_scopes[0].operator == "STARTS_WITH"
    assert result.ir.entity_scopes[0].provenance == "typed_prefix_scope_from_nl"
    if query.startswith("Show issues"):
        assert result.template_id == "indv5_issue_prefix_list"
        assert "STARTS WITH 'I_900001'" in (result.rendered_cypher or "")
