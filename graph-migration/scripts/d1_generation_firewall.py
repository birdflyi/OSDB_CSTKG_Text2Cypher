from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TARGETS = [
    ROOT / "graph-migration" / "runners" / "independent_controlled_pipeline.py",
    ROOT / "graph-migration" / "repair" / "gold_blind_repair.py",
]
FORBIDDEN = ["gold_cypher", "extracted_slot_candidates", "covered_queries", "query_type"]


def main() -> int:
    violations: list[str] = []
    for path in TARGETS:
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN:
            if token in text:
                violations.append(f"{path}:{token}")
    if violations:
        print("D1_GENERATION_FIREWALL=FAIL")
        print("\n".join(violations))
        return 1
    print("D1_GENERATION_FIREWALL=PASS")
    print("checked=" + ",".join(str(p) for p in TARGETS))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
