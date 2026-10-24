"""Conservative witness for one-column entity ranking with tied scalar keys.

Input candidates come from an independent source/data query, not model output.
This diagnoses reference ambiguity only. It NEVER changes primary EX scoring.
Projected duplicates are checked with counts at each rank. Unsupported keys
and incomplete candidates fail closed.
"""
from collections import Counter
import math


def ranking_tie_witness(candidates, published, actual, *, descending=False, limit=None):
    names = [r['name'] for r in candidates]
    if (any(type(n) is not str for n in names)
        or any(type(r['key']) not in (int, float, str, type(None)) for r in candidates)
        or any(type(r['key']) is float and not math.isfinite(r['key']) for r in candidates)):
        return {'certified': False, 'reason': 'Unsupported name or sort key'}
    kinds = {type(r['key']) for r in candidates if r['key'] is not None}
    if len(kinds) > 1:
        return {'certified': False, 'reason': 'Mixed sort key types'}
    if limit is not None and (type(limit) is not int or limit < 1):
        return {'certified': False, 'reason': 'Invalid explicit limit'}
    ordered = sorted(candidates, key=lambda r: (r['key'] is None, r['key']), reverse=descending)
    ranked_rows = []
    previous = object()
    rank = -1
    for row in ordered:
        if row['key'] != previous:
            rank += 1
        ranked_rows.append((row['name'], rank))
        previous = row['key']
    expected = [rank for _, rank in ranked_rows[:limit]]
    available = Counter(ranked_rows)

    def valid(rows):
        if not isinstance(rows, list) or any(not isinstance(r, list) or len(r) != 1 for r in rows):
            return False
        output = [r[0] for r in rows]
        if len(output) != len(expected):
            return False
        remaining = available.copy()
        for name, rank in zip(output, expected):
            if remaining[name, rank] <= 0:
                return False
            remaining[name, rank] -= 1
        return True

    counts = Counter(rank for _, rank in ranked_rows)
    valid_published, valid_actual = valid(published), valid(actual)
    return {'certified': valid_published and valid_actual and published != actual,
            'published_obeys_ranking': valid_published, 'actual_obeys_ranking': valid_actual,
            'candidate_rows': len(candidates), 'tie_groups': sum(n > 1 for n in counts.values()),
            'primary_score_unchanged': True}
