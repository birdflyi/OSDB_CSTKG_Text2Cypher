"""M09-B04: cyquark x ZOGRASCOPE (offline preparation only).

Inference-side adapter only: training assets stay resource_pending.
See common/author.py and the cell delivery.json.
"""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import author  # noqa: E402

BENCHMARK = "ZOGRASCOPE"
METHOD = "cyquark"
CELL = "M09-B04"
POSITIONS = r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\cyquark__ZOGRASCOPE.positions.json"


def prepare(positions_path=POSITIONS, *, limit=None):
    return author.prepare(positions_path, BENCHMARK, CELL, METHOD, limit=limit)


def parse(reply, **kwargs):
    return author.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return author.consume(positions, predictions, runtime)
