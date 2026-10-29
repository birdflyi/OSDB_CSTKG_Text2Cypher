"""M02-B02: ours x Text2GQL (offline preparation only).

Plan parsing and the compile-then-execute path live in common/ours.py; the real
compiler is injected by the runtime, never imported here.
"""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import ours  # noqa: E402

BENCHMARK = "Text2GQL"
CELL = "M02-B02"
POSITIONS = r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\ours__Text2GQL.positions.json"


def prepare(positions_path=POSITIONS, *, limit=None):
    return ours.prepare(positions_path, BENCHMARK, CELL, limit=limit)


def parse(reply, **kwargs):
    return ours.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return ours.consume(positions, predictions, runtime)
