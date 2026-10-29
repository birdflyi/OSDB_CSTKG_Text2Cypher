"""Make this cell import its own adapter, not a sibling cell's."""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ADAPTER_WORK))

from common.loader import load  # noqa: E402

sys.modules["adapter"] = load(Path(__file__).resolve().parents[1] / "adapter.py")
