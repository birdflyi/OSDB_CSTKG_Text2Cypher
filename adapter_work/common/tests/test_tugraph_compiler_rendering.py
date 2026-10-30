"""TuGraph REST rendering accepts compiler identifiers, not raw quoted Cypher."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "experiment-harness/public_benchmark_v1"),
               str(ROOT / "operations/analysis")]

import schema_services
from osdb_native_set_count_control_dev import compile_native_set_count
from public_read_guard import ReadOnlyViolation, read_query
from tugraph_dialect import render_compiler_query


@pytest.mark.parametrize("operation", ["union", "intersection", "difference"])
def test_render_actual_native_set_count_compiler_output(operation):
    schema = dict(entities=[dict(label="Person", properties={"key": "str"},
                                 identity_property="key")], relations=[])
    call = dict(nodes=[dict(id="n", label="Person")], edges=[], where=[],
                returns=[dict(field="n.key", alias="id")])
    plan = dict(status="ANSWER", calls=[call, call],
                combine=dict(op=operation, key="id", output="count"))
    compiled = compile_native_set_count(
        plan, schema,
        selection_compiler=lambda c, s: schema_services.compile_selection(
            c, s, backend="tugraph-4.5.2"),
        set_contract=schema_services._set_contract,
    )
    assert "`id`" in compiled["query"] and "`count`" in compiled["query"]
    script = render_compiler_query(compiled["query"], compiled["parameters"])
    assert script == compiled["query"]
    assert read_query(script) == script


def test_typed_parameter_remains_literal_even_with_cypher_tokens():
    value = "'` // ; $other CREATE (n)"
    script = render_compiler_query("RETURN $value AS `result`", {"value": value})
    assert script == "RETURN " + json.dumps(value) + " AS `result`"
    assert read_query(script) == script


@pytest.mark.parametrize("query", [
    "RETURN 1 AS `bad-name`", "RETURN 1 AS `with space`",
    "RETURN 1 AS `unterminated", "RETURN 1 AS `a``b`",
    "RETURN 1 AS `$parameter`", "RETURN 1 AS ``",
    "RETURN 'literal'", 'RETURN "literal"',
    "RETURN 1 // comment", "RETURN 1 /* comment */", "RETURN 1;",
])
def test_rejects_unsupported_compiler_quotations_and_tokens(query):
    with pytest.raises(schema_services.UnsupportedPlan, match="Unexpected token"):
        render_compiler_query(query, {})


def test_parameters_must_be_bound_exactly():
    with pytest.raises(schema_services.UnsupportedPlan, match="Missing"):
        render_compiler_query("RETURN $x AS `value`", {})
    with pytest.raises(schema_services.UnsupportedPlan, match="Unused"):
        render_compiler_query("RETURN 1 AS `value`", {"x": 1})


@pytest.mark.parametrize("query", [
    "CREATE (n:Person) RETURN n AS `result`",
    "MATCH (n) SET n.x = 1 RETURN n AS `result`",
    "CALL dbms.procedures() RETURN 1 AS `result`",
])
def test_downstream_read_guard_still_rejects_writes_and_procedures(query):
    rendered = render_compiler_query(query, {})
    with pytest.raises(ReadOnlyViolation):
        read_query(rendered)
