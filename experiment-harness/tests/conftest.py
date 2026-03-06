from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
for p in [ROOT, REPO_ROOT]:
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)
