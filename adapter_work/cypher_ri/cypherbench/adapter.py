"""M07-B01: Cypher-RI x CypherBench (offline preparation only).

The protocol lives in ``common/ri.py`` so all five Cypher-RI cells share it.
This file only binds the benchmark and its frozen position list.
"""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import ri  # noqa: E402

BENCHMARK = "CypherBench"
CELL = "M07-B01"
POSITIONS = r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\cypher-ri__CypherBench.positions.json"

Schema = ri.Schema
Selection = ri.Selection
Entity = ri.Entity
Relation = ri.Relation
Property = ri.Property
RequestSpec = ri.RequestSpec
Prediction = ri.Prediction
NextStep = ri.NextStep
parse_selection = ri.parse_selection
first_prompt = ri.first_prompt
continuation_prefix = ri.continuation_prefix


def prepare(positions_path=POSITIONS, *, limit=None):
    return ri.prepare(positions_path, BENCHMARK, CELL, limit=limit)


def next_request(spec, reply, *, transport_unknown=False, **metadata):
    return ri.next_request(spec, reply, transport_unknown=transport_unknown, **metadata)


def parse(reply, **kwargs):
    return ri.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return ri.consume(positions, predictions, runtime)
