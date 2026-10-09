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
        "fix11",
        query,
        load_independent_templates(TEMPLATES),
        load_independent_schema(SCHEMA),
    )


def _labels(query: str) -> list[str | None]:
    return [item.label for item in parse_nl_to_ir("fix11", query).projection_items]


def test_except_and_but_not_typed_prefix_forms_are_negated_and_fail_closed() -> None:
    cases = [
        "Show issues except those whose IDs start with I_880002",
        "Show issues except issues whose IDs start with I_880002",
        "Show issues except pull requests whose IDs begin with PR_880003",
        "Show issues, but not those whose IDs start with I_880002",
        "Show issues, but not issues whose IDs begin with I_880002",
        "Show issues, but not pull requests whose identifiers start with PR_880003",
    ]
    for query in cases:
        result = _generate(query)
        assert len(result.ir.entity_scopes) == 1, query
        scope = result.ir.entity_scopes[0]
        assert scope.operator == "NOT_STARTS_WITH", query
        assert scope.provenance == "negated_typed_prefix_scope_from_nl", query
        assert result.template_id is None, query
        assert result.rendered_cypher is None, query
        reasons = {
            item["reason_code"]
            for candidate in result.validation["selection"][
                "candidate_ir_constraint_coverage"
            ].values()
            for item in candidate["entity_scopes"]["unconsumed"]
        }
        assert "UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR" in reasons, query


def test_except_style_false_positive_controls_keep_positive_prefix_scope() -> None:
    cases = [
        "Show issues except comments, whose IDs start with I_880002",
        "Show issues, but not comments; use IDs starting with I_880002",
        "Show issues that start with I_880002, but not their comments",
    ]
    for query in cases:
        result = _generate(query)
        assert result.ir.entity_scopes[0].operator == "STARTS_WITH", query
        assert result.template_id == "indv5_issue_prefix_list", query
        assert "STARTS WITH 'I_880002'" in (result.rendered_cypher or ""), query


def test_prior_negation_and_positive_prefix_forms_remain_unchanged() -> None:
    negated = [
        "Show issues whose IDs do not start with I_880002",
        "Show issues excluding IDs that start with I_880002",
        "Show issues, but omit IDs beginning with I_880002",
        "Show issues without IDs starting with I_880002",
    ]
    for query in negated:
        result = _generate(query)
        assert result.ir.entity_scopes[0].operator == "NOT_STARTS_WITH", query
        assert result.template_id is None
        assert result.rendered_cypher is None

    for query in (
        "Show issues whose IDs start with I_880002",
        "Show issues whose IDs begin with I_880002",
    ):
        result = _generate(query)
        assert result.ir.entity_scopes[0].operator == "STARTS_WITH", query
        assert result.template_id == "indv5_issue_prefix_list", query


def test_who_and_whoever_pronouns_resolve_to_actor_before_source_anchor() -> None:
    cases = [
        "For issue I_880002#77, show who opened it and return their ID.",
        "For issue I_880002#77, show whoever opened it and return their identifier.",
    ]
    for query in cases:
        result = _generate(query)
        assert _labels(query) == ["Actor"]
        assert result.template_id == "indv4_issue_opened_by"
        assert "RETURN a.entity_id" in (result.rendered_cypher or "")
        assert "RETURN i.entity_id" not in (result.rendered_cypher or "")


def test_pronoun_reuses_explicit_output_role_without_duplicate_inflation() -> None:
    query = "Show actors and return their IDs"
    assert _labels(query) == ["Actor"]

    composite = "Show issue comment IC_900001#12 and show actors and return their IDs"
    ir = parse_nl_to_ir("fix11", composite)
    assert [(item.label, item.property) for item in ir.projection_items] == [
        ("Actor", "entity_id")
    ]


def test_pronoun_never_falls_back_to_source_anchor_and_explicit_source_projection_remains() -> None:
    source_only = "For issue I_880002#77, return their ID"
    assert _labels(source_only) == []

    explicit = "For issue I_880002#77, show who opened it and also return the issue ID."
    assert _labels(explicit) == ["Actor", "Issue"]


def test_ambiguous_two_output_roles_do_not_guess_a_pronoun_antecedent() -> None:
    query = "Show actors and list repositories and return their IDs"
    assert _labels(query) == ["Actor", "Repo"]
