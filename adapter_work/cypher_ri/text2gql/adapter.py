"""M07-B02: Cypher-RI x Text2GQL (offline preparation only).

Protocol lives in `common/ri.py`; this file binds the benchmark and its
frozen position list.
"""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import ri  # noqa: E402

BENCHMARK = "Text2GQL"
CELL = "M07-B02"
POSITIONS = r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\cypher-ri__Text2GQL.positions.json"

Schema = ri.Schema
Prediction = ri.Prediction
NextStep = ri.NextStep
parse_selection = ri.parse_selection


def prepare(positions_path=POSITIONS, *, limit=None):
    return ri.prepare(positions_path, BENCHMARK, CELL, limit=limit)


def next_request(spec, reply, *, transport_unknown=False, **metadata):
    return ri.next_request(spec, reply, transport_unknown=transport_unknown, **metadata)


def parse(reply, **kwargs):
    return ri.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return ri.consume(positions, predictions, runtime)
