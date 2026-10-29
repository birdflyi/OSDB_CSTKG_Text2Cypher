import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import run as rehearsal  # noqa: E402


def test_subset_rehearsal_counts_requests_and_replays():
    report = rehearsal.run({"direct/cypherbench", "ours/cypherbench", "cypher_ri/cypherbench", "correct_detail/cypherbench"})
    assert report["cells"] == 4
    assert report["positions"] == 60
    # RI asks twice per position, the other two model cells once
    assert report["model_requests"] == 80
    assert report["replays"] == 20
    assert report["executed"] == 60
    assert report["format_error"] == 0
    assert report["plan_artifacts"] == 20
    assert report["controls"]["executed"] == 20


def test_full_rehearsal_is_900_positions():
    report = rehearsal.run()
    assert report["cells"] == 45
    assert report["positions"] == 600
    assert report["replays"] == 300
    assert report["positions"] + report["replays"] == 900
    assert report["model_requests"] == 700
    assert report["executed"] == 600
    assert report["scored"] == 600
