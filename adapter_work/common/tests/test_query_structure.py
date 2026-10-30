import pytest

from adapter_work.query_structure import extract_structure


@pytest.mark.parametrize("query", [None, "", "RETURN 'open", "/* open", "MATCH (n RETURN n", "RETURN ]"])
def test_malformed_is_unknown_and_not_simple(query):
    result = extract_structure(query)
    assert result["status"] == "unknown"
    assert result["structure_group"] == "unknown"


def test_keywords_inside_literals_comments_identifiers_and_properties_are_not_operators():
    q = "MATCH (n:`UNION`) // CALL { count(*) }\n RETURN n.COUNT, {MATCH: 'UNION COUNT(*)'}, `ORDER` /* SUM(x) */"
    result = extract_structure(q)
    assert result["structure_group"] == "simple"
    assert result["features"]["aggregate_calls"] == 0
    assert result["features"]["union_markers"] == 0


def test_comments_and_escaped_quotes_do_not_change_bucket():
    assert extract_structure(r'''RETURN 'it\'s UNION', "a\"COUNT(*)"''')["structure_group"] == "simple"


@pytest.mark.parametrize("query", ["CALL { MATCH(n) RETURN n } RETURN 1", "CALL (x) { RETURN x } RETURN 1", "RETURN EXISTS { MATCH(n) RETURN n }", "MATCH (a)-[:R*1..3]->(b) RETURN b", "RETURN 1 UNION RETURN 2"])
def test_nested_or_variable(query):
    assert extract_structure(query)["structure_group"] == "nested_or_variable"


def test_procedure_plus_later_return_map_is_not_subquery():
    result = extract_structure("CALL db.labels() YIELD label RETURN {x: label}")
    assert result["features"]["subquery_markers"] == 0


def test_relationship_property_multiplication_is_not_variable_length():
    result = extract_structure("MATCH (a)-[r:R {v: 2*3}]->(b) RETURN b")
    assert result["features"]["relationship_patterns"] == 1
    assert result["features"]["variable_length_patterns"] == 0


def test_aggregate_and_multiple_relationships_are_composed():
    result = extract_structure("MATCH (a)-[:R]->(b)-->(c) RETURN count(c)")
    assert result["structure_group"] == "composed"
    assert result["features"]["relationship_patterns"] == 2
    assert result["features"]["aggregate_calls"] == 1


def test_literal_template_fingerprint_is_only_literal_normalization():
    a = extract_structure("MATCH(n) WHERE n.x = 10 RETURN n")
    b = extract_structure("MATCH(n) WHERE n.x = 20 RETURN n")
    assert a["literal_normalized_fingerprint"] == b["literal_normalized_fingerprint"]
    assert a["is_ast"] is False
