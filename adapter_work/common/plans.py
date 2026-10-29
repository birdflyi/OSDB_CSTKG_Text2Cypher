"""Plan language parser for the Ours cells (v24 contract, M1).

The model answers with one JSON object: either an ANSWER plan or UNSUPPORTED.
This module validates structure only; graph semantics stay with each benchmark
compiler. A malformed plan is a format error, an out-of-language plan is
UNSUPPORTED, and neither is silently repaired.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

JSON_BLOCK = re.compile(r"<json>(.*?)</json>", re.DOTALL | re.IGNORECASE)
FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)

PREDICATES = {
    "eq", "ne", "lt", "le", "gt", "ge", "in", "contains",
    "starts_with", "ends_with", "is_null", "not_null",
}
NULL_PREDICATES = {"is_null", "not_null"}
AGGS = {"count", "sum", "avg", "min", "max", "collect"}
SET_OPS = {"union", "intersection", "difference"}


class PlanError(ValueError):
    """Structural problem; the plan cannot be handed to a compiler."""


@dataclass(frozen=True)
class PlanResult:
    status: str                      # answer | unsupported | format_error
    plan: dict | None = None
    reason: str | None = None
    raw: str = ""

    def as_dict(self) -> dict:
        return dict(status=self.status, plan=self.plan, reason=self.reason)


def _extract(reply: str) -> str:
    block = JSON_BLOCK.search(reply or "")
    if block:
        return block.group(1).strip()
    fenced = FENCE.search(reply or "")
    if fenced:
        return fenced.group(1).strip()
    return (reply or "").strip()


def _require(condition, message):
    if not condition:
        raise PlanError(message)


def validate(plan: dict) -> dict:
    """Validate the structural subset of the v24 plan language."""
    _require(isinstance(plan, dict), "plan is not an object")
    calls = plan.get("calls")
    _require(isinstance(calls, list) and 1 <= len(calls) <= 2, "calls must be 1 or 2")
    for call in calls:
        nodes = call.get("nodes")
        _require(isinstance(nodes, list) and 1 <= len(nodes) <= 4, "nodes must be 1-4")
        ids = [node.get("id") for node in nodes]
        _require(all(isinstance(i, str) and i for i in ids), "node ids must be non-empty")
        _require(len(set(ids)) == len(ids), "node ids must be unique")
        _require(all(node.get("label") for node in nodes), "each node needs a label")
        edges = call.get("edges", [])
        _require(isinstance(edges, list) and len(edges) <= 3, "edges must be 0-3")
        optional = 0
        for edge in edges:
            _require(edge.get("source") in ids and edge.get("target") in ids, "edge endpoint unknown")
            _require(edge.get("label"), "edge needs a label")
            optional += 1 if edge.get("optional") else 0
        _require(optional <= 1, "at most one optional edge")
        where = call.get("where", [])
        _require(isinstance(where, list) and len(where) <= 16, "where must be <=16 predicates")
        for predicate in where:
            op = predicate.get("op")
            _require(op in PREDICATES, "unknown predicate: " + str(op))
            _require(predicate.get("field"), "predicate needs a field")
            if op in NULL_PREDICATES:
                _require("value" not in predicate and "other_field" not in predicate, "null predicate takes no value")
            else:
                given = ("value" in predicate) + ("other_field" in predicate)
                _require(given == 1, "predicate takes exactly one argument")
        returns = call.get("returns")
        _require(isinstance(returns, list) and 1 <= len(returns) <= 8, "returns must be 1-8")
        aliases = [item.get("alias") for item in returns]
        _require(all(aliases) and len(set(aliases)) == len(aliases), "aliases must be unique")
        for item in returns:
            _require(item.get("field"), "return needs a field")
            if item.get("agg"):
                _require(item["agg"] in AGGS, "unknown aggregate: " + str(item["agg"]))
        order = call.get("order_by", [])
        for item in order:
            _require(item.get("column") in aliases, "order_by must use a return alias")
            _require(item.get("direction") in ("ASC", "DESC"), "direction must be ASC or DESC")
        limit = call.get("limit")
        if limit is not None:
            _require(isinstance(limit, int) and 1 <= limit <= 10000, "limit must be 1-10000")
    combine = plan.get("combine") or {"op": "single"}
    _require(combine.get("op") == "single" or combine.get("op") in SET_OPS, "unknown combine op")
    if combine.get("op") in SET_OPS:
        _require(len(calls) == 2, "set combine needs exactly two calls")
        _require(combine.get("key") is not None, "set combine needs a key")
        _require(combine.get("output") in ("count", "rows"), "set output must be count or rows")
    else:
        _require(len(calls) == 1, "single combine needs exactly one call")
    return plan


def parse(reply: str) -> PlanResult:
    text = _extract(reply)
    if not text:
        return PlanResult("format_error", None, "empty_reply", reply or "")
    try:
        value = json.loads(text)
    except ValueError:
        return PlanResult("format_error", None, "invalid_json", reply or "")
    if not isinstance(value, dict):
        return PlanResult("format_error", None, "not_an_object", reply or "")
    status = str(value.get("status", "")).upper()
    if status == "UNSUPPORTED":
        return PlanResult("unsupported", None, value.get("reason") or "model_declined", reply or "")
    if status != "ANSWER":
        return PlanResult("format_error", None, "unknown_status:" + status, reply or "")
    try:
        validate(value)
    except PlanError as exc:
        return PlanResult("format_error", None, str(exc), reply or "")
    return PlanResult("answer", value, None, reply or "")
