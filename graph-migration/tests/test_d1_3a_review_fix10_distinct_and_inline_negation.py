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
        "fix10",
        query,
        load_independent_templates(TEMPLATES),
        load_independent_schema(SCHEMA),
    )


def _projection_signature(query: str):
    ir = parse_nl_to_ir("fix10", query)
    return ir.projection_distinct, [
        (item.label, item.property, item.distinct) for item in ir.projection_items
    ]


def test_tuple_distinct_and_item_local_distinct_are_independent() -> None:
    base = "from mentioned repos and external links, if any."
    cases = [
        (
            "For actor A_900001, return distinct repo IDs and external resource IDs " + base,
            (True, [("Repo", "entity_id", False), ("ExternalResource", "entity_id", False)]),
        ),
        (
            "For actor A_900001, return distinct repo IDs and unique external resource IDs " + base,
            (True, [("Repo", "entity_id", False), ("ExternalResource", "entity_id", True)]),
        ),
        (
            "For actor A_900001, return repo IDs and unique external resource IDs " + base,
            (False, [("Repo", "entity_id", False), ("ExternalResource", "entity_id", True)]),
        ),
        (
            "For actor A_900001, return unique repo IDs and external resource IDs " + base,
            (False, [("Repo", "entity_id", True), ("ExternalResource", "entity_id", False)]),
        ),
    ]
    for query, expected in cases:
        assert _projection_signature(query) == expected, query


def test_local_distinctness_is_not_satisfied_by_tuple_only_template() -> None:
    query = (
        "For actor A_900001, return distinct repo IDs and unique external resource IDs "
        "from mentioned repos and external links, if any."
    )
    result = _generate(query)
    assert result.template_id is None
    assert result.rendered_cypher is None
    coverage = result.validation["selection"]["candidate_ir_constraint_coverage"][
        "indv4_actor_multi_target_reference"
    ]
    projection = coverage["projection"]
    assert projection["tuple_distinct"]["accepted"]
    assert any(
        item.get("label") == "ExternalResource"
        and item.get("distinct") is True
        and item.get("status") == "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON"
        for item in projection["unconsumed_items"]
    )


def test_single_column_and_aggregate_distinct_regressions_remain_separate() -> None:
    single = parse_nl_to_ir("fix10", "return distinct actor IDs")
    assert not single.projection_distinct
    assert [(item.label, item.distinct) for item in single.projection_items] == [("Actor", True)]

    possessive = parse_nl_to_ir("fix10", "return the distinct IDs of actors")
    assert not possessive.projection_distinct
    assert [(item.label, item.distinct) for item in possessive.projection_items] == [("Actor", True)]

    aggregate = parse_nl_to_ir("fix10", "count distinct actor IDs")
    assert not aggregate.projection_distinct
    assert aggregate.projection_items == []
    assert aggregate.aggregation[0]["distinct"] is True


def test_inline_exclusion_phrases_are_negated_and_fail_closed() -> None:
    cases = [
        "Show issues excluding IDs that start with I_880001",
        "Show issues excluding IDs beginning with I_880001",
        "Show issues, but omit IDs that start with I_880001",
        "Show issues without IDs starting with I_880001",
        "Show issues without identifiers that begin with I_880001",
        "Show issues excluding pull requests whose IDs begin with PR_880002",
    ]
    for query in cases:
        result = _generate(query)
        assert result.ir.entity_scopes, query
        scope = result.ir.entity_scopes[-1]
        assert scope.operator == "NOT_STARTS_WITH", query
        assert scope.provenance == "negated_typed_prefix_scope_from_nl", query
        assert result.template_id is None, query
        assert result.rendered_cypher is None, query
        assert "STARTS WITH" not in str(result.rendered_cypher), query


def test_positive_prefix_and_false_positive_negation_controls_remain_positive() -> None:
    for query in (
        "Show issues whose IDs start with I_880001",
        "Show issues whose IDs begin with I_880001",
        "Show issues without comments, whose IDs start with I_880001",
        "Show issues without labels and whose IDs begin with I_880001",
        "Show issues and omit the comments; use IDs starting with I_880001",
    ):
        result = _generate(query)
        assert result.ir.entity_scopes[0].operator == "STARTS_WITH", query
        assert result.template_id == "indv5_issue_prefix_list", query
        assert "STARTS WITH 'I_880001'" in (result.rendered_cypher or ""), query


def test_prior_do_not_start_with_regression_remains_fail_closed() -> None:
    result = _generate("Show issues whose IDs do not start with I_880001")
    assert result.ir.entity_scopes[0].operator == "NOT_STARTS_WITH"
    assert result.template_id is None
    assert result.rendered_cypher is None
