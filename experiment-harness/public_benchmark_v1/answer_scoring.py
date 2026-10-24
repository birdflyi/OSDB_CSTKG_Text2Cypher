"""Common executed-answer scoring, using pinned upstream pure comparison code.

No database, gold-query execution or model calls occur in this module. The
runner must provide actual executed and normalized JSON result rows. A plan
or Cypher text is never accepted as an executed answer.
"""
from __future__ import annotations

import ast
from collections import defaultdict
import hashlib
from itertools import product
import json
from pathlib import Path
import random


UPSTREAM_SHA256 = 'a734463ec22d0b88b09bac6b925b70f94b428bd74c7426ea992045bf76d6e55a'
PURE_FUNCTIONS = frozenset({'permute_tuple', 'unorder_row', 'quick_rej',
                          'multiset_eq', 'get_constraint_permutation', 'result_eq'})


class ScoringError(ValueError):
    pass


def load_official_comparator(official_source):
    """Load only hash-verified, pure stdlib functions; no Neo4j import needed.

    Upstream execution_accuracy also has a query-text identity shortcut.
    We deliberately use only its result comparator after actual execution.
    """
    path = Path(official_source) / 'cypherbench/metrics/execution_accuracy.py'
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != UPSTREAM_SHA256:
        raise ScoringError('Upstream comparator identity changed')
    parsed = ast.parse(data.decode('utf-8'))
    selected = [node for node in parsed.body if isinstance(node, ast.FunctionDef)
                and node.name in PURE_FUNCTIONS]
    if {node.name for node in selected} != PURE_FUNCTIONS:
        raise ScoringError('Incomplete upstream comparator')
    module = ast.Module(body=selected, type_ignores=[])
    # Postponed annotations avoid importing the upstream driver/runtime.
    code = compile(module, str(path), 'exec', dont_inherit=False)

    def compare(left, right, *, order_matters, seed):
        namespace = {'defaultdict': defaultdict, 'product': product,
                     'random': random.Random(seed)}
        exec(code, namespace)
        return namespace['result_eq'](left, right, order_matters)

    return compare


def _hashable(value):
    if value is None or type(value) in (str, int, float, bool):
        return value
    if isinstance(value, list):
        # Matches upstream to_hashable's default nested-list semantics.
        return tuple(sorted(_hashable(item) for item in value))
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        return tuple(sorted((key, _hashable(v)) for key, v in value.items()))
    raise ScoringError('Result must contain normalized JSON values')


def _rows(value):
    if not isinstance(value, list):
        raise ScoringError('Expected executed rows, not a query or plan')
    if any(not isinstance(row, list) for row in value):
        raise ScoringError('Rows must be explicit JSON arrays')
    widths = {len(row) for row in value}
    if len(widths) > 1:
        raise ScoringError('Inconsistent row widths')
    try:
        return [tuple(_hashable(cell) for cell in row) for row in value]
    except (TypeError, ValueError) as exc:
        raise ScoringError('Unsupported result normalization; audit evaluator') from exc


def score_executed_answer(*, status, predicted_rows, reference_rows,
                          order_matters, qid, compare):
    gold = _rows(reference_rows)
    if type(order_matters) is not bool or not isinstance(qid, str) or not qid:
        raise ScoringError('Explicit order policy and task ID required')
    allowed = {'EXECUTED', 'REFUSED', 'UNSUPPORTED', 'MODEL_ERROR',
               'EXECUTION_ERROR', 'INFRASTRUCTURE_ERROR', 'TRUNCATED'}
    if status not in allowed:
        raise ScoringError('Invalid outcome or unexecuted plan')
    if status != 'EXECUTED':
        return {'ex': 0.0, 'status': status, 'returned_answer': False}
    predicted = _rows(predicted_rows)
    seed = int.from_bytes(hashlib.sha256(qid.encode('utf-8')).digest()[:8], 'big')
    try:
        exact = compare(gold, predicted, order_matters=order_matters, seed=seed)
    except (TypeError, ValueError) as exc:
        raise ScoringError('Comparator error; do not silently record model failure') from exc
    return {'ex': float(exact), 'status': status, 'returned_answer': True}


def read_reference_answer(value):
    """Published answer_json is an array of row arrays, not record objects."""
    parsed = json.loads(value)
    _rows(parsed)
    return parsed
