from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from offline_performance import summarize


def test_parallel_intervals_and_partial_denominator():
    events = [dict(t=0, kind="graph_prepare_started", instance="g"),
              dict(t=1, kind="graph_ready", instance="g", seconds=1),
              dict(t=2, kind="position_started", work_id="a", wait_seconds=0),
              dict(t=3, kind="position_started", work_id="b", wait_seconds=0.1),
              dict(t=5, kind="position_finished", work_id="a"),
              dict(t=6, kind="position_finished", work_id="b"),
              dict(t=7, kind="graph_closed", instance="g")]
    result = summarize(events, [dict(cell_id="cell", seconds=3), dict(cell_id="cell")])
    assert result["position_workers"]["peak"] == 2
    assert result["position_workers"]["mean"] == 6 / 7
    assert result["cells"]["cell"]["planned"] == 2
    assert result["cells"]["cell"]["timed"] == 1
    assert result["query_only_concurrency"] is None
