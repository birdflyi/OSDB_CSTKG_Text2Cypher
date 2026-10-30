"""Fabricated tables only: no model or database access."""
from copy import deepcopy
import struct

import pytest

from adapter_work.scoring_contract import CAPTURE_PROFILE, ScoringContractError, UnresolvedNumericValue, compare_numeric_exact


def value(item):
    if item is None:
        return {"t": "null"}
    if type(item) is bool:
        return {"t": "bool", "v": item}
    if type(item) is int:
        return {"t": "int", "v": str(item)}
    if type(item) is float:
        return {"t": "float64", "v": struct.pack("!d", item).hex()}
    if type(item) is list:
        return {"t": "list", "v": [value(v) for v in item]}
    return {"t": "str", "v": item}


def answer(items):
    return dict(profile=CAPTURE_PROFILE, status="EXECUTED", complete=True, columns=["x"],
                rows=[[value(v)] for v in items], contains_graph_entities=False,
                scope=dict(snapshot_sha256="a"*64, database="neo4j", capture_id="one"))


@pytest.mark.parametrize("left,right,expected", [
    ([1], [1.0], True), ([0.0], [-0.0], True), ([True], [1], False),
    ([2**53+1], [float(2**53+1)], False), ([2**53], [float(2**53)], True),
    ([1.00000000001], [1.0], False), ([None], [], False), ([], [], True),
    ([[1,2]], [[2,1]], False), ([1,1], [1], False), ([1,2], [2,1], True),
])
def test_numeric_and_bag_contract(left, right, expected):
    assert compare_numeric_exact(answer(left), answer(right), ordered=False) is expected


def test_order_width_and_aliases():
    assert compare_numeric_exact(answer([1,2]), answer([2,1]), ordered=True) is False
    altered = answer([1.0]); altered["columns"] = ["other"]
    assert compare_numeric_exact(answer([1]), altered, ordered=False) is True
    altered["columns"] = ["a", "b"]
    with pytest.raises(ScoringContractError, match="width"):
        compare_numeric_exact(answer([1]), altered, ordered=False)


@pytest.mark.parametrize("item", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_not_silent_match(item):
    with pytest.raises(UnresolvedNumericValue):
        compare_numeric_exact(answer([item]), answer([item]), ordered=False)


def test_scope_and_immutable_inputs():
    original = answer([1]); saved = deepcopy(original)
    other = answer([1.0]); other["scope"]["capture_id"] = "two"
    assert compare_numeric_exact(original, other, ordered=False) is True
    assert original == saved
    other["contains_graph_entities"] = True
    with pytest.raises(ScoringContractError, match="flag contradicts"):
        compare_numeric_exact(original, other, ordered=False)
    other["contains_graph_entities"] = False
    other["scope"]["snapshot_sha256"] = "b"*64
    with pytest.raises(ScoringContractError, match="snapshot"):
        compare_numeric_exact(original, other, ordered=False)


def test_incomplete_is_unknown_not_empty_success():
    incomplete = answer([]); incomplete["complete"] = False
    assert compare_numeric_exact(answer([]), incomplete, ordered=False) is None
    assert compare_numeric_exact(None, answer([]), ordered=False) is None


def test_malformed_numeric_fails_explicitly():
    malformed = answer([1]); malformed["rows"][0][0]["v"] = "01"
    with pytest.raises(ScoringContractError, match="encoded int"):
        compare_numeric_exact(answer([1]), malformed, ordered=False)


def encoded_answer(cell, *, entities=False):
    result = answer([])
    result["rows"] = [[deepcopy(cell)]]
    result["contains_graph_entities"] = entities
    return result


EMPTY_MAP = {"t": "map", "v": []}
NODE = {"t": "node", "element_id": "n1", "labels": ["A"], "properties": EMPTY_MAP}
RELATIONSHIP = {"t": "relationship", "element_id": "r1", "type": "R", "start": "n1", "end": "n2", "properties": EMPTY_MAP}
NODE2 = {**NODE, "element_id": "n2"}
PATH = {"t": "path", "nodes": [NODE, NODE2], "relationships": [RELATIONSHIP], "directions": [1]}


@pytest.mark.parametrize("cell", [
    NODE, RELATIONSHIP, PATH,
    {"t": "list", "v": [NODE]},
    {"t": "map", "v": [["nested", {"t": "list", "v": [NODE]}]]},
])
def test_derived_entity_scope_cannot_be_bypassed_by_false_flag(cell):
    left = encoded_answer(cell)
    right = deepcopy(left); right["scope"]["capture_id"] = "two"
    with pytest.raises(ScoringContractError, match="flag contradicts"):
        compare_numeric_exact(left, right, ordered=False)
    left["contains_graph_entities"] = right["contains_graph_entities"] = True
    with pytest.raises(ScoringContractError, match="Cross-capture"):
        compare_numeric_exact(left, right, ordered=False)
    right["scope"]["capture_id"] = "one"
    assert compare_numeric_exact(left, right, ordered=False) is True


@pytest.mark.parametrize("cell", [
    {"t": "bool", "v": 1}, {"t": "made_up", "v": "x"}, 1,
    {"t": "str", "v": 1}, {"t": "null", "v": None},
    {"t": "list", "v": [1]}, {"t": "map", "v": [["x", 1]]},
    {"t": "map", "v": [["x", value(1)], ["x", value(2)]]},
    {"t": "bytes", "v": "x"}, {"t": "float64", "v": "00"},
    {"t": "date", "year": True, "month": 1, "day": 1},
    {**NODE, "labels": [1]}, {**NODE, "properties": {}},
    {**PATH, "directions": [True]}, {**RELATIONSHIP, "start": None},
    {"t": "point", "srid": 4326, "coordinates": [value("1")]},
])
def test_invalid_typed_cell_never_matches_itself(cell):
    malformed = encoded_answer(cell)
    with pytest.raises(ScoringContractError):
        compare_numeric_exact(malformed, deepcopy(malformed), ordered=False)


def test_validation_does_not_short_circuit_after_first_entity():
    cell = {"t": "list", "v": [NODE, {"t": "bool", "v": 1}]}
    malformed = encoded_answer(cell, entities=True)
    with pytest.raises(ScoringContractError, match="bool"):
        compare_numeric_exact(malformed, deepcopy(malformed), ordered=False)


def test_actual_encoder_shapes_remain_accepted():
    import datetime
    from neo4j.graph import Graph, Node
    from neo4j.spatial import WGS84Point
    from neo4j.time import Date, Time, DateTime, Duration
    from operations.analysis.neo4j_native_answer_contract_dev_20261029 import CaptureScope, freeze_answer

    graph = Graph()
    examples = [None, True, 1, 1.0, "x", b"ab", [1, "x"],
                {"t": "int", "v": "not a typed cell", "node": Node(graph, "n1", 1, ["A"], {"x": 1})},
                WGS84Point((1.0, 2.0)), Date(2020, 1, 2), Time(1, 2, 3), DateTime(2020, 1, 2, 1, 2, 3),
                Duration(months=1, days=2, seconds=3), datetime.timedelta(days=1, seconds=2)]
    frozen = freeze_answer(["x"], [[item] for item in examples], CaptureScope("a"*64, "neo4j", "one"), complete=True, max_bytes=100000)
    assert compare_numeric_exact(frozen, deepcopy(frozen), ordered=False) is True
