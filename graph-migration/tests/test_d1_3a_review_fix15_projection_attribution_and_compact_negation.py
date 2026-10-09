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
        "fix15",
        query,
        load_independent_templates(TEMPLATES),
        load_independent_schema(SCHEMA),
    )


def _signature(query: str):
    ir = parse_nl_to_ir("fix15", query)
    return ir.projection_distinct, [
        (item.label, item.property, item.distinct) for item in ir.projection_items
    ]


def test_tuple_and_item_distinctness_keep_independent_cue_attribution() -> None:
    base = "from mentioned repos and external links"
    cases = [
        (
            f"return distinct repo IDs and external resource IDs {base}",
            (True, [("Repo", "entity_id", False), ("ExternalResource", "entity_id", False)]),
        ),
        (
            f"return distinct rows with unique repo IDs and external resource IDs {base}",
            (True, [("Repo", "entity_id", True), ("ExternalResource", "entity_id", False)]),
        ),
        (
            f"return distinct rows with repo IDs and unique external resource IDs {base}",
            (True, [("Repo", "entity_id", False), ("ExternalResource", "entity_id", True)]),
        ),
        (
            f"return unique repo IDs and external resource IDs {base}",
            (False, [("Repo", "entity_id", True), ("ExternalResource", "entity_id", False)]),
        ),
        (
            "return distinct IDs of actors and resource IDs",
            (False, [("Actor", "entity_id", True), ("ExternalResource", "entity_id", False)]),
        ),
        (
            "return distinct actor IDs",
            (False, [("Actor", "entity_id", True)]),
        ),
        (
            "count distinct actor IDs",
            (False, []),
        ),
    ]
    for query, expected in cases:
        assert _signature(query) == expected, query


def test_local_uniqueness_remains_unconsumed_by_tuple_only_contract() -> None:
    result = _generate(
        "For actor A_900001, return distinct rows with unique repo IDs and external resource IDs "
        "from mentioned repos and external links."
    )
    assert result.template_id is None
    assert result.rendered_cypher is None
    projection = result.validation["selection"]["candidate_ir_constraint_coverage"][
        "indv4_actor_multi_target_reference"
    ]["projection"]
    assert projection["tuple_distinct"]["accepted"]
    assert any(
        item.get("label") == "Repo"
        and item.get("distinct") is True
        and item.get("status") == "ABSTAIN_WITH_TYPED_UNCONSUMED_REASON"
        for item in projection["unconsumed_items"]
    )


def test_tuple_overlap_is_removed_only_when_it_is_the_items_sole_cue() -> None:
    tuple_only = parse_nl_to_ir(
        "fix15", "return distinct repo IDs and external resource IDs from links"
    )
    assert tuple_only.projection_items[0].distinct is False
    assert tuple_only.projection_items[0].distinct_source_spans == []

    independently_unique = parse_nl_to_ir(
        "fix15",
        "return distinct repo IDs and unique repo IDs and external resource IDs from links",
    )
    repo = next(item for item in independently_unique.projection_items if item.label == "Repo")
    assert independently_unique.projection_distinct is True
    assert repo.distinct is True
    assert len(repo.distinct_source_spans) == 2
    assert [
        independently_unique.nl_query[start:end]
        for start, end in repo.distinct_source_spans
    ] == ["distinct", "unique"]


def test_compact_pre_token_negation_is_fail_closed_for_issue_and_pr_prefixes() -> None:
    cases = [
        "Show issues without prefix I_900001",
        "Show issues excluding prefix I_900001",
        "Show issues exclude prefix I_900001",
        "Show issues omitting prefix I_900001",
        "Show issues omit prefix I_900001",
        "Show issues except prefix I_900001",
        "Show issues but not prefix I_900001",
        "Show pull requests without prefix PR_900001",
        "Show pull requests excluding prefix PR_900001",
        "Show pull requests exclude prefix PR_900001",
        "Show pull requests omitting prefix PR_900001",
        "Show pull requests omit prefix PR_900001",
        "Show pull requests except prefix PR_900001",
        "Show pull requests but not prefix PR_900001",
    ]
    for query in cases:
        result = _generate(query)
        assert len(result.ir.entity_scopes) == 1, query
        scope = result.ir.entity_scopes[0]
        assert scope.operator == "NOT_STARTS_WITH", query
        assert scope.provenance == "negated_typed_prefix_scope_from_nl", query
        assert result.template_id is None, query
        assert result.rendered_cypher is None, query
        assert any(
            item.get("reason_code") == "UNSUPPORTED_NEGATED_TYPED_SCOPE_OPERATOR"
            for candidate in result.validation["selection"][
                "candidate_ir_constraint_coverage"
            ].values()
            for item in candidate["entity_scopes"]["unconsumed"]
        ), query


def test_compact_negation_false_positive_controls_remain_positive() -> None:
    cases = [
        "Show issues without comments; use prefix I_900001",
        "Show issues except comments; prefix I_900001",
        "Show issues but not comments; use I_900001 prefix",
        "Show issues with prefix I_900001",
    ]
    for query in cases:
        result = _generate(query)
        assert result.ir.entity_scopes[0].operator == "STARTS_WITH", query
        assert result.template_id == "indv5_issue_prefix_list", query
        assert "STARTS WITH 'I_900001'" in (result.rendered_cypher or ""), query


def test_attributive_external_resource_domain_does_not_infer_entity_id() -> None:
    cases = [
        "external resource domains",
        "resource domains",
        "external resource registrable domains",
        "external resource site domains",
    ]
    for phrase in cases:
        ir = parse_nl_to_ir("fix15", f"For pull request PR_900001#12, return {phrase} from external links")
        assert [
            (item.label, item.property, item.distinct) for item in ir.projection_items
        ] == [("ExternalResource", "url_domain_etld1", False)], phrase


def test_explicit_id_coordination_domain_uniqueness_and_aggregate_remain_separate() -> None:
    explicit_id = parse_nl_to_ir(
        "fix15", "return external resource IDs and domains from external links"
    )
    assert [(item.label, item.property) for item in explicit_id.projection_items] == [
        ("ExternalResource", "entity_id"),
        ("ExternalResource", "url_domain_etld1"),
    ]

    coordinated = parse_nl_to_ir(
        "fix15", "return external resources and domains from external links"
    )
    assert [(item.label, item.property) for item in coordinated.projection_items] == [
        ("ExternalResource", "entity_id"),
        ("ExternalResource", "url_domain_etld1"),
    ]

    unique_domain = parse_nl_to_ir("fix15", "return unique external resource domains")
    assert [(item.label, item.property, item.distinct) for item in unique_domain.projection_items] == [
        ("ExternalResource", "url_domain_etld1", True)
    ]

    aggregate = parse_nl_to_ir("fix15", "count distinct domains")
    assert aggregate.projection_items == []
    assert aggregate.aggregation[0]["distinct"] is True
