import pytest

from engine.config import canonical
from engine.table_results import shape


def setup_scores(tmp_path, method="ours", rows=None):
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(canonical([dict(task_id="one", graph="g", method=method, model="m"),
                                    dict(task_id="two", graph="g", method=method, model="m")]))
    directory = tmp_path / "runs" / "cell"
    directory.mkdir(parents=True)
    (directory / "scores.json").write_bytes(canonical(rows or [
        dict(qid="one", graph="g", method=method, model="m", view="final", score={"ex": 1}),
        dict(qid="two", graph="g", method=method, model="m", view="final", score=None)]))
    return dict(method=method, run_dir=str(tmp_path)), dict(name="cell", manifest_path=str(manifest))


def test_unscored_terminal_does_not_shrink_denominator(tmp_path):
    cell, stage = setup_scores(tmp_path)
    result = shape(cell, stage, "full", model="m", population={"g": 2})
    assert result["completed"] == 2 and result["scored"] == 1
    assert result["score"] is None and not result["complete"]


def test_foreign_model_task_is_not_counted(tmp_path):
    cell, stage = setup_scores(tmp_path, rows=[dict(qid="foreign", graph="g", method="ours",
                                                  model="m", view="final", score={"ex": 1})])
    with pytest.raises(ValueError, match="identity"):
        shape(cell, stage, "full", model="m", population={"g": 2})


def test_feedback_view_uses_shared_direct_final(tmp_path):
    rows = [dict(qid=qid, graph="g", method="direct", model="m", view=view, score={"ex": ex})
            for qid in ("one", "two") for view, ex in (("initial", 0), ("final", 1))]
    cell, stage = setup_scores(tmp_path, method="direct", rows=rows)
    cell["method"] = "direct-feedback"
    result = shape(cell, stage, "full", model="m", population={"g": 2})
    assert result["score"] == 1 and result["view"] == "final"
