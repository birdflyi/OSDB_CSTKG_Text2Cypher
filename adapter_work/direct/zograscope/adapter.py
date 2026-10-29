"""M01-B04: direct x ZOGRASCOPE adapter (offline preparation only).

Uses the shared direct factory: compose from the frozen public input, classify
the reply, and consume through an injected runtime. No HTTP and no database.
"""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import direct  # noqa: E402

BENCHMARK = "ZOGRASCOPE"
POSITIONS = r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells\direct__ZOGRASCOPE.positions.json"


def prepare(positions_path=POSITIONS, *, limit=None):
    return direct.prepare(positions_path, BENCHMARK, limit=limit)


def parse(reply, **kwargs):
    return direct.parse(reply, **kwargs)


def consume(positions, predictions, runtime):
    return direct.consume(positions, predictions, runtime)
