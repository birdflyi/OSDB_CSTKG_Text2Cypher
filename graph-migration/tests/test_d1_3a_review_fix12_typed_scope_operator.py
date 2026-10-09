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
        "fix12", query, load_independent_templates(TEMPLATES), load_independent_schema(SCHEMA)
    )


def test_unsupported_ends_with_and_contains_are_preserved_and_fail_closed() -> None:
    for query, operator in (
        ("Show issues whose IDs end with I_880002", "ENDS_WITH"),
        ("Show issues whose IDs ending with I_880002", "ENDS_WITH"),
        ("Show issues whose IDs contain I_880002", "CONTAINS"),
        ("Show issues whose IDs containing I_880002", "CONTAINS"),
    ):
        result = _generate(query)
        assert len(result.ir.entity_scopes) == 1, query
        scope = result.ir.entity_scopes[0]
        assert scope.operator == operator, query
        assert scope.provenance == "unsupported_typed_scope_operator_from_nl", query
        assert result.template_id is None, query
        assert result.rendered_cypher is None, query
        reasons = {
            item["reason_code"]
            for candidate in result.validation["selection"]["candidate_ir_constraint_coverage"].values()
            for item in candidate["entity_scopes"]["unconsumed"]
        }
        assert "UNSUPPORTED_TYPED_SCOPE_OPERATOR" in reasons, query


def test_real_prefix_operator_is_required_and_positive_forms_remain_executable() -> None:
    for query in (
        "Show issues whose IDs start with I_880002",
        "Show issues whose IDs begin with I_880002",
        "I'm looking for pull request IDs that fall under the PR_156018 prefix; give me at most 25 of them.",
    ):
        result = _generate(query)
        assert result.ir.entity_scopes[0].operator == "STARTS_WITH", query
        assert result.template_id in {"indv5_issue_prefix_list", "indv4_repo_pull_request_filter"}, query


def test_no_operator_does_not_fabricate_a_prefix_scope() -> None:
    query = "Show issues whose IDs are I_880002"
    result = _generate(query)
    assert all(scope.operator != "STARTS_WITH" for scope in result.ir.entity_scopes)
    assert result.rendered_cypher is None
    assert any(
        item["reason_code"] == "UNSUPPORTED_TYPED_SCOPE_OPERATOR"
        for candidate in result.validation["selection"]["candidate_ir_constraint_coverage"].values()
        for item in candidate["entity_scopes"]["unconsumed"]
    )


def test_unsupported_scope_preserves_each_explicit_constraint_in_a_list() -> None:
    ir = parse_nl_to_ir(
        "fix12", "Show issues whose IDs end with I_880002 or I_880003"
    )
    assert [scope.operator for scope in ir.entity_scopes] == ["ENDS_WITH", "ENDS_WITH"]


def test_unrelated_typed_token_does_not_inherit_another_clause_operator() -> None:
    query = (
        "Show issues whose IDs are I_880002 and show pull requests "
        "whose IDs start with PR_880003"
    )
    ir = parse_nl_to_ir("fix12", query)
    assert [(scope.label, scope.operator, scope.value) for scope in ir.entity_scopes] == [
        ("Issue", "UNSPECIFIED", "I_880002"),
        ("PullRequest", "STARTS_WITH", "PR_880003"),
    ]
