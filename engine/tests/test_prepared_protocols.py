import pytest

from engine.attempt_costs import summarize_attempts
from engine.author_protocols import ri_schema_selection, ri_append_schema, smith_filter_decision


def test_ri_known_labels_and_unknown_fallback():
    rollout = '<think>x</think><json>{"name":"g","entities":["Person"],"relations":[]}</json>'
    selected = ri_schema_selection(rollout, entity_labels=["Person", "Film"], relation_labels=["ACTED"])
    assert not selected["fallback"] and selected["entities"] == ["Person"]
    assert ri_append_schema(rollout, "exact renderer output") == rollout + "<schema>exact renderer output</schema>"
    for bad in [rollout.replace('Person', 'Unknown'), '<json>broken</json>', '<json>[]</json>']:
        fallback = ri_schema_selection(bad, entity_labels=["Person", "Film"], relation_labels=["ACTED"])
        assert fallback["fallback"] and fallback["entities"] == ["Person", "Film"]


def test_smith_rule_ambiguity_is_visible_and_no_default_is_chosen():
    reduced = [-2, -4, -5]
    assert not smith_filter_decision(-3, reduced, rule="prose_full_le_max")["retained"]
    assert smith_filter_decision(-3, reduced, rule="printed_full_lt_min")["retained"]
    assert not smith_filter_decision(-2, reduced, rule="prose_full_le_max")["retained"]
    with pytest.raises(ValueError):
        smith_filter_decision(-3, reduced, rule="author")


def test_all_attempts_are_charged_once_and_unknowns_remain_unknown():
    ok = dict(status="ok", charged=True, model="m", method="direct", graph="g",
              usage=dict(prompt_tokens=10, completion_tokens=2, total_tokens=12))
    bad = dict(status="unknown", charged=None, model="m", method="direct", graph="g", usage=None)
    value = summarize_attempts([dict(source_id="slot1", terminal=ok),
        dict(source_id="slot1", terminal=ok), dict(source_id="slot2", terminal=bad)])
    assert value["total"]["physical_records"] == 2
    assert value["total"]["charge_unknown"] == 1
    assert value["total"]["total_tokens"] == dict(known_sum=12, unknown_records=1)
    assert value["total"]["monetary_cost"] is None
    with pytest.raises(ValueError, match="Conflicting"):
        summarize_attempts([dict(source_id="same", terminal=ok), dict(source_id="same", terminal=bad)])


def test_costs_with_different_currencies_are_not_added():
    records = [dict(source_id=str(n),terminal=dict(status="ok",charged=True,
                monetary_cost=1,currency=currency)) for n,currency in enumerate(("USD","CNY"))]
    assert summarize_attempts(records)["total"]["monetary_cost"] is None
