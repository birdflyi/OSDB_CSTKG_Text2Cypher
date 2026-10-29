"""M02-B03: ours x MindTheQuery (offline preparation only).

Plan parsing and the compile-then-execute path live in common/ours.py; the real
compiler is injected by the runtime, never imported here.
"""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import ours  # noqa: E402

BENCHMARK = "MindTheQuery"
CELL = "M02-B03"
POSITIONS = r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\ours__MindTheQuery.positions.json"


def prepare(positions_path=POSITIONS, *, limit=None):
    return ours.prepare(positions_path, BENCHMARK, CELL, limit=limit)


def parse(reply, **kwargs):
    return ours.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return ours.consume(positions, predictions, runtime)
