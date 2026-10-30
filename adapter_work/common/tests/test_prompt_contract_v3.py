"""Compile the future profile examples without API, driver connection or DB."""

from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [
    str(ROOT / "adapter_work"),
    str(ROOT / "operations/analysis"),
    str(ROOT / "experiment-harness/public_benchmark_v1"),
]

from common.prompts import DEFAULT_PROFILES, compose_ours
import observed_neo4j_services_dev_v2 as observed
import schema_services
from neo4j_native_plan_dev_20261029 import PreparedPlan
from neo4j_native_selection_dev_20261029 import (
    NativeSchema,
    UnsupportedSelection,
    compile_selection as compile_native,
)

PROFILE = DEFAULT_PROFILES.with_name("ours_contract_v3.json")
PROFILES = json.loads(PROFILE.read_text(encoding="utf-8"))
BENCHMARKS = tuple(PROFILES)
NATIVE = {"MindTheQuery", "ZOGRASCOPE"}


def schema_for(benchmark):
    if benchmark == "MindTheQuery":
        return NativeSchema(
            {
                "node_props": {
                    "ExampleEntity": [{"property": "key", "datatype": "STRING"}]
                },
                "rel_props": {},
                "relationships": [],
            },
            format="MTQ",
        )
    if benchmark == "ZOGRASCOPE":
        return NativeSchema(
            {
                "classes": {"ExampleEntity": {}},
                "properties": {"key": {"type": "STRING"}},
                "relations": {},
            },
            format="ZOGRASCOPE",
        )
    return {
        "entities": [
            {
                "label": "ExampleEntity",
                "identity_property": "key",
                "properties": {"key": "str"},
            }
        ],
        "relations": [],
    }


def compile_for(benchmark, selection):
    schema = schema_for(benchmark)
    if benchmark in NATIVE:
        return compile_native(selection, schema)
    if benchmark == "OSDB-STKG":
        return observed.compile_selection(selection, schema)
    backend = "tugraph-4.5.2" if benchmark == "Text2GQL" else "neo4j"
    return schema_services.compile_selection(selection, schema, backend=backend)


def check_set_key(benchmark, plan):
    schema = schema_for(benchmark)
    if benchmark in NATIVE:
        return PreparedPlan(plan, schema)
    contract = (
        observed._set_contract
        if benchmark == "OSDB-STKG"
        else schema_services._set_contract
    )
    return [
        contract(call, compile_for(benchmark, call), plan["combine"]["key"])
        for call in plan["calls"]
    ]


def rejection(benchmark):
    if benchmark in NATIVE:
        return UnsupportedSelection
    return (
        observed.UnsupportedPlan
        if benchmark == "OSDB-STKG"
        else schema_services.UnsupportedPlan
    )


@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_new_profile_is_explicit_and_keeps_public_input(benchmark):
    historical = json.loads(DEFAULT_PROFILES.read_text(encoding="utf-8"))[benchmark]
    assert (
        PROFILES[benchmark]["supersedes_prefix_sha256"] == historical["prefix_sha256"]
    )
    payload = {"graph": "synthetic", "question": "Example only", "schema": {}}
    old = compose_ours(benchmark, payload)
    new = compose_ours(benchmark, payload, PROFILE)
    assert new[1:] == old[1:]
    assert new[0]["content"].startswith(old[0]["content"])
    for example in PROFILES[benchmark]["syntax_examples"].values():
        assert json.dumps(example, separators=(",", ":")) in new[0]["content"]


@pytest.mark.parametrize("benchmark", BENCHMARKS)
@pytest.mark.parametrize(
    "example", ["distinct_projection", "distinct_count", "entity_set"]
)
def test_profile_examples_compile_with_real_backend(benchmark, example):
    plan = PROFILES[benchmark]["syntax_examples"][example]
    compiled = [compile_for(benchmark, call) for call in plan["calls"]]
    assert all(piece["columns"] for piece in compiled)
    if example == "distinct_projection":
        assert "RETURN DISTINCT" in compiled[0]["query"]
    elif example == "distinct_count":
        assert "count(DISTINCT" in compiled[0]["query"]
    else:
        check_set_key(benchmark, plan)


@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_nonaggregate_return_distinct_is_rejected(benchmark):
    selection = deepcopy(
        PROFILES[benchmark]["syntax_examples"]["distinct_projection"]["calls"][0]
    )
    selection["returns"][0]["distinct"] = True
    with pytest.raises(rejection(benchmark), match="Per-item DISTINCT"):
        compile_for(benchmark, selection)


@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_unqualified_predicate_is_rejected(benchmark):
    selection = deepcopy(
        PROFILES[benchmark]["syntax_examples"]["distinct_projection"]["calls"][0]
    )
    selection["where"] = [{"field": "key", "op": "eq", "value": "a"}]
    with pytest.raises(rejection(benchmark)):
        compile_for(benchmark, selection)


@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_grouping_requires_aggregation(benchmark):
    selection = deepcopy(
        PROFILES[benchmark]["syntax_examples"]["distinct_projection"]["calls"][0]
    )
    selection["group_by"] = [
        "n" if benchmark in NATIVE else selection["returns"][0]["field"]
    ]
    with pytest.raises(rejection(benchmark), match="grouping"):
        compile_for(benchmark, selection)


@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_grouped_projection_compiles_when_identity_key_is_explicit(benchmark):
    selection = deepcopy(
        PROFILES[benchmark]["syntax_examples"]["distinct_projection"]["calls"][0]
    )
    selection["group_by"] = [selection["returns"][0]["field"]]
    selection["returns"].append({"field": "*", "agg": "count", "alias": "total"})
    compiled = compile_for(benchmark, selection)
    assert compiled["columns"] == ["id", "total"]
    assert "__group_0" in compiled["query"]


@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_count_distinct_star_is_rejected(benchmark):
    selection = deepcopy(
        PROFILES[benchmark]["syntax_examples"]["distinct_count"]["calls"][0]
    )
    selection["returns"][0]["field"] = "*"
    with pytest.raises(rejection(benchmark)):
        compile_for(benchmark, selection)


@pytest.mark.parametrize("benchmark", BENCHMARKS)
def test_set_identity_dialects_are_not_interchangeable(benchmark):
    plan = deepcopy(PROFILES[benchmark]["syntax_examples"]["entity_set"])
    wrong = "n.key" if benchmark in NATIVE else "n"
    for call in plan["calls"]:
        call["returns"][0]["field"] = wrong
    with pytest.raises(rejection(benchmark)):
        check_set_key(benchmark, plan)
