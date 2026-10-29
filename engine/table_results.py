"""Close one cell's declared population without choosing favourable replies."""

import json
import math
from pathlib import Path

from .config import canonical


VIEWS = {
    "direct": ("direct", "initial"),
    "direct-feedback": ("direct", "final"),
    "ours": ("ours", "final"),
}


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def shape(cell, stage, phase, *, model, population=None):
    method, view = VIEWS[cell["method"]]
    expected = {}
    for row in read_json(stage["manifest_path"]):
        qid = row["task_id"]
        if qid in expected:
            raise ValueError("Duplicate manifest task: " + qid)
        if row["method"] != method or row["model"] != model:
            raise ValueError("Manifest method/model differs from the cell")
        expected[qid] = row["graph"]
    directory = Path(cell["run_dir"]) / "runs" / stage["name"]
    aggregate = directory / "scores.json"
    paths = [aggregate] if aggregate.exists() else sorted(directory.glob("*/scores.json"))
    answered = {}
    for path in paths:
        for row in read_json(path):
            if row.get("view") != view:
                continue
            qid = row["qid"]
            if (qid not in expected or row.get("graph") != expected[qid]
                    or row.get("method") != method or row.get("model") != model):
                raise ValueError("Score identity differs from the declared population")
            if qid in answered and canonical(answered[qid]) != canonical(row):
                raise ValueError("Conflicting scores for task: " + qid)
            answered[qid] = row

    def is_scored(row):
        score = (row.get("score") or {}).get("ex")
        return type(score) in (int, float, bool) and math.isfinite(score) and 0 <= score <= 1

    splits = {
        graph: dict(
            planned=sum(g == graph for g in expected.values()),
            terminal=sum(r["graph"] == graph for r in answered.values()),
            scored=sum(r["graph"] == graph and is_scored(r) for r in answered.values()),
        )
        for graph in sorted(set(expected.values()))
    }
    total = len(expected)
    scored = sum(is_scored(row) for row in answered.values())
    planned = {graph: row["planned"] for graph, row in splits.items()}
    full = phase == "full" and bool(population) and planned == population
    complete = bool(total) and len(answered) == total and scored == total
    # Unscored results stay unknown. Never divide only by successful scores.
    score = sum(row["score"]["ex"] for row in answered.values()) / total if full and complete else None
    return dict(stage=phase, complete=complete and full, full_population=full,
                model=model, score=score, evidence=str(directory), splits=splits,
                planned=total, completed=len(answered), scored=scored,
                missing_tasks=sorted(set(expected) - set(answered)), view=view)
