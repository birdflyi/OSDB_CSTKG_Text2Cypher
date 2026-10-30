from adapter_work.task_scoring_policy import build_task_policy, classify_ordering
import pytest


@pytest.mark.parametrize("query,rule", [
    ("MATCH (n) RETURN n", "BAG"),
    ("RETURN 'order by'", "BAG"),
    ("RETURN 1 // order by x", "BAG"),
    ("RETURN 1 /* order by x */", "BAG"),
    ("MATCH (`order by`) RETURN `order by`", "BAG"),
    ("MATCH (n) RETURN n.name ORDER\n/*x*/BY n.name", "ORDERED"),
    ("CALL { MATCH (n) RETURN n ORDER BY n.x } RETURN n", "BAG"),
    ("MATCH (n) WITH n ORDER BY n.x RETURN n", "BAG"),
    ("MATCH (n) RETURN n ORDER BY n.x LIMIT 2", "ORDERED"),
    ("RETURN 1 UNION RETURN 2", "UNADJUDICATED"),
    ("CALL db.labels()", "UNADJUDICATED"),
    ("RETURN 'broken", "UNADJUDICATED"),
    ("RETURN [1", "UNADJUDICATED"),
    ("RETURN 1; RETURN 2", "UNADJUDICATED"),
    ("SHOW DATABASES", "UNADJUDICATED"),
])
def test_ordering_policy_is_reference_only_and_conservative(query, rule):
    assert classify_ordering(query)["order_rule"] == rule


def test_author_rules_are_preserved_even_when_strict_policy_differs():
    cb = build_task_policy("CypherBench", "x", "RETURN 'order by'")
    assert cb["order_rule"] == "ORDERED"
    assert cb["diagnostic_order_rule"] == "BAG"
    t2 = build_task_policy("Text2GQL", "x", "RETURN 1 ORDER BY 1")
    assert t2["order_rule"] == "BAG"
    assert t2["diagnostic_order_rule"] == "ORDERED"


def test_reference_concerns_are_recorded_not_used_for_silent_deletion():
    assert "slice_without_final_order" in classify_ordering("MATCH(n) RETURN n LIMIT 1")["concerns"]
    assert "ordered_slice_ties_unverified" in classify_ordering("MATCH(n) RETURN n ORDER BY n.x LIMIT 1")["concerns"]
    assert classify_ordering("RETURN rand()")["concerns"] == ["nondeterministic_function:rand"]
    assert classify_ordering("RETURN datetime()")["concerns"] == ["clock_function:datetime"]


def test_default_reference_is_not_assumed_verified():
    policy = build_task_policy("MindTheQuery", "x", "RETURN 1")
    assert policy["reference_status"] == "UNVERIFIED"
    assert policy["formal_admitted"] is False
