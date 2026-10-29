"""M03-B01: direct-feedback x CypherBench (offline preparation only).

One non-gold feedback round lives in common/feedback.py; dispatch itself waits
for the central NextStep integration.
"""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import feedback  # noqa: E402

BENCHMARK = "CypherBench"
CELL = "M03-B01"
POSITIONS = r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\direct-feedback__CypherBench.positions.json"


def prepare(positions_path=POSITIONS, *, limit=None):
    return feedback.prepare(positions_path, BENCHMARK, CELL, limit=limit)


def next_request(spec, reply, **kwargs):
    return feedback.next_request(spec, reply, **kwargs)


def parse(reply, **kwargs):
    return feedback.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return feedback.consume(positions, predictions, runtime)
