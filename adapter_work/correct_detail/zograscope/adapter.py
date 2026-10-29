"""M04-B04: correct-detail x ZOGRASCOPE (offline preparation only).

Replay wrapper lives in common/controls.py; the runtime owns plan, lease and
evidence. This cell only binds the control id and profile.
"""

import sys
from pathlib import Path

ADAPTER_WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ADAPTER_WORK))

from common import controls  # noqa: E402

BENCHMARK = "ZOGRASCOPE"
CELL = "M04-B04"
CONTROL_ID = "correct-detail"
PROFILE_ID = "full_detail"


def replay(envelope, runtime):
    return controls.replay(envelope, runtime, control_id=CONTROL_ID, profile_id=PROFILE_ID)


def replay_all(envelope, runtime):
    return controls.replay_all(envelope, runtime, [CONTROL_ID])
