"""Zero-call rehearsal of the 45-cell smoke (900 positions).

Every cell is exercised with a deterministic fake model and a fake runtime, so
the rehearsal checks the *plumbing*: prompt preparation, reply parsing, the
compile-before-execute order, one feedback round, the two-turn Cypher-RI path,
control replay from frozen Ours plans, attempt accounting and charge de-dupe.

It makes no network call and touches no database. The report is written next to
this script; pass ``--cells`` to rehearse a subset.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common import controls, loader, ours, plans  # noqa: E402

CELLS = HERE.parent
POSITIONS = Path(r"D:\ch7\reports\t1_preparation_20261029\adapter_positions_v22\cells")

BENCH = {
    "cypherbench": "CypherBench",
    "text2gql": "Text2GQL",
    "mindthequery": "MindTheQuery",
    "zograscope": "ZOGRASCOPE",
    "osdb_stkg": "OSDB-STKG",
}

PLAN = {
    "status": "ANSWER",
    "calls": [
        {
            "nodes": [{"id": "n", "label": "Person"}],
            "edges": [],
            "where": [],
            "returns": [{"field": "n.age", "alias": "value"}],
        }
    ],
    "combine": {"op": "single"},
}
CYPHER = "MATCH (n) RETURN n"
RI_SELECT = '<json>{"labels":["Person"],"relationships":[]}</json>'


class FakeRuntime:
    """Records call order and returns deterministic answers."""

    def __init__(self, fail_graphs=()):
        self.calls = []
        self.fail_graphs = set(fail_graphs)
        self.saved = {}

    # ours compile path
    def compile(self, plan, *, benchmark, profile_id):
        self.calls.append(("compile", benchmark))
        return {"query": "MATCH (n) RETURN n.age AS value", "parameters": {}, "columns": ["value"]}

    # shared consumption path
    def save_prediction(self, position, prediction):
        self.calls.append(("save_prediction", getattr(position, "position_id", "")))
        return type("R", (), {"path": "pred" + str(len(self.calls))})()

    def graph_lease(self, graph):
        self.calls.append(("lease", graph))
        return "lease:" + graph

    def execute(self, lease, position, prediction):
        self.calls.append(("execute", getattr(prediction, "cypher", "")))
        if lease.split(":", 1)[1] in self.fail_graphs:
            raise TimeoutError("neo4j timeout")
        return {"rows": [[1]], "columns": ["value"], "complete": True}

    def save_final_answer(self, position, execution):
        self.calls.append(("final", getattr(position, "position_id", "")))
        return type("R", (), {"path": "final" + str(len(self.calls))})()

    def score_saved_answer(self, position, saved):
        self.calls.append(("score", getattr(position, "task_id", "")))
        return type("S", (), {"status": "scored", "value": 1.0, "error": None})()

    # control path
    def verify_source(self, envelope):
        return "receipt"

    def run_control(self, envelope, *, control_id, profile_id):
        self.calls.append(("control", control_id))
        return {"answer": {"columns": ["v"], "rows": [[1]], "complete": True}, "representation": {}, "costs": {}}

    def save_control_result(self, envelope, read):
        return "evidence/" + envelope["position_id"]


class FakeModel:
    """Deterministic replies per method/stage; counts every physical request."""

    def __init__(self):
        self.requests = []

    def reply(self, method, stage="initial", feedback=False):
        self.requests.append((method, stage, feedback))
        if method == "ours":
            return json.dumps(PLAN)
        if method == "cypher-ri":
            return RI_SELECT if stage == "schema_select" else "<answer>MATCH (n) RETURN n</answer>"
        return CYPHER


def _adapter(relative: str):
    return loader.load(CELLS / relative / "adapter.py")


METHOD_IDS = {
    "direct": "direct",
    "ours": "ours",
    "direct_feedback": "direct-feedback",
    "cypher_ri": "cypher-ri",
    "cyphersmith": "cyphersmith",
    "cyquark": "cyquark",
}


def _positions(method_slug: str, bench: str) -> Path:
    return POSITIONS / ("%s__%s.positions.json" % (METHOD_IDS.get(method_slug, method_slug), bench))


def _spec_dict(spec):
    return spec if isinstance(spec, dict) else spec.as_dict()


def _pid(spec):
    return spec["position_id"] if isinstance(spec, dict) else spec.position_id


def _run_model_cell(name, relative, method_slug, bench, model, runtime, limit=20):
    adapter = _adapter(relative)
    specs = adapter.prepare(_positions(method_slug, bench), limit=limit)
    method_id = METHOD_IDS.get(method_slug, method_slug)
    predictions = {}
    for spec in specs:
        if method_id == "cypher-ri":
            reply = model.reply(method_id, "schema_select")
            step = adapter.next_request(spec, reply)
            assert step.action == "request", name + " stage2"
            reply2 = model.reply(method_id, "answer_continue")
            predictions[_pid(spec)] = adapter.parse(reply2)
        elif method_id == "direct-feedback":
            reply = model.reply(method_id)
            predictions[_pid(spec)] = adapter.parse(reply)
            step = adapter.next_request(spec, reply)
            assert step.action == "complete"
        else:
            predictions[_pid(spec)] = adapter.parse(model.reply(method_id))
    result = adapter.consume(specs, predictions, runtime)
    return result, specs



def run(cells_filter=None):
    model = FakeModel()
    runtime = FakeRuntime()
    report = {
        "cells": 0,
        "positions": 0,
        "model_requests": 0,
        "replays": 0,
        "executed": 0,
        "unsupported": 0,
        "format_error": 0,
        "refused": 0,
        "run_error": 0,
        "scored": 0,
        "controls": {"executed": 0, "not_applicable": 0, "execution_error": 0},
        "plan_artifacts": 0,
        "charge_ids": set(),
    }

    for method in ("direct", "ours", "direct_feedback", "cypher_ri", "cyphersmith", "cyquark"):
        for slug, bench in BENCH.items():
            name = "%s/%s" % (method, slug)
            if cells_filter and name not in cells_filter:
                continue
            result, specs = _run_model_cell(name, name, method, bench, model, runtime)
            report["cells"] += 1
            report["positions"] += result.positions
            report["executed"] += result.executed
            report["unsupported"] += getattr(result, "unsupported", 0)
            report["format_error"] += result.format_error
            report["refused"] += result.refused
            report["run_error"] += result.run_error
            report["scored"] += result.scored
            if method == "ours":
                report["plan_artifacts"] += result.executed
            if method == "direct":
                report["charge_ids"].add(name)

    for group, control_id in (
        ("correct_detail", "correct-detail"),
        ("simple_correct", "simple-correct"),
        ("native_set_count", "native-set-count"),
    ):
        for slug, bench in BENCH.items():
            name = "%s/%s" % (group, slug)
            if cells_filter and name not in cells_filter:
                continue
            adapter = _adapter(name)
            report["cells"] += 1
            for index in range(1, 21):
                position_id = "%s-P%02d" % (name.split("/")[0][:3].upper(), index)
                plan = dict(PLAN)
                if control_id == "native-set-count":
                    plan = dict(PLAN, calls=[PLAN["calls"][0], PLAN["calls"][0]],
                                combine={"op": "union", "key": "value", "output": "count", "alias": "c"})
                envelope = {
                    "position_id": position_id,
                    "source_plan_sha256": hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest(),
                    "benchmark": bench,
                    "plan": plan,
                    "charge_ids": ["gen-" + bench],
                }
                result = adapter.replay(envelope, runtime)
                report["replays"] += 1
                report["controls"][result.status] = report["controls"].get(result.status, 0) + 1

    report["model_requests"] = len(model.requests)
    report["charge_ids"] = len(report["charge_ids"])
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cells", default="", help="comma separated method/benchmark")
    args = parser.parse_args()
    cells = [c.strip() for c in args.cells.split(",") if c.strip()] or None
    report = run(cells)
    out = HERE / "report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
