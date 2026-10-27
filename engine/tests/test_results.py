from __future__ import annotations

import pytest
from engine.results import summarize, paired_intervals


def fixture():
    groups = [dict(qid=str(n), graph="a" if n < 2 else "b", family=str(n)) for n in range(4)]
    scores = []
    for method, view in [
        ("direct", "initial"),
        ("direct", "final"),
        ("ours", "initial"),
        ("ours", "final"),
    ]:
        for n in range(4):
            correct = method == "ours" or (view == "final" and n % 2 == 0)
            scores.append(
                dict(
                    qid=str(n),
                    graph=groups[n]["graph"],
                    model="m",
                    method=method,
                    view=view,
                    status="EXECUTED",
                    score=dict(ex=float(correct), returned_answer=True),
                )
            )
    return groups, scores


def test_complete_denominators_macro_and_paired_effect():
    groups, scores = fixture()
    table, indices = summarize(scores, groups)
    assert [r["EX"] for r in table] == [0, 0.5, 1]
    result = paired_intervals(indices, groups, replicates=50)
    assert result[0]["difference_percentage_points"] == 50
    assert result[0]["CI95_percentage_points"][0] <= 50 <= result[0]["CI95_percentage_points"][1]


def test_missing_or_duplicate_position_never_shrinks_denominator():
    groups, scores = fixture()
    with pytest.raises(ValueError):
        summarize(scores[:-1], groups)
    with pytest.raises(ValueError):
        summarize(scores + [scores[-1]], groups)


def test_recovery_audit_deduplicates_charged_history_and_retains_unknown_cost(tmp_path):
    from engine.results import recovery_audit
    from engine.config import canonical

    folder = tmp_path / "runs" / "direct" / "g"
    folder.mkdir(parents=True)
    old = dict(request_id="old", logical_key="k", budget_slot=1, charged=True, status="unknown")
    new = dict(request_id="new", logical_key="k", budget_slot=2, charged=True, status="ok")
    (folder / "model_events.json").write_bytes(canonical([old, old, new]))
    value = recovery_audit(tmp_path, [dict(name="direct")], 1)
    assert value["unique_physical_attempts"] == 2 and value["unknown_physical_attempts"] == 1
    assert value["unresolved_transport_paths"] == 0 and value["recovered_unknown_paths"] == 1
    assert value["inherited_history_accounted"]
