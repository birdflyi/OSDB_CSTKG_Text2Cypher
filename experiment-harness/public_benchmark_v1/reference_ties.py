"""Conservative witness for one-column entity ranking with tied scalar keys.

Input candidates come from an independent source/data query, not model output.
This diagnoses reference ambiguity only. It NEVER changes primary EX scoring.
Duplicate output names, unsupported keys, and incomplete candidates fail closed.
"""
from collections import Counter


def ranking_tie_witness(candidates, published, actual, *, descending=False, limit=None):
    names = [r['name'] for r in candidates]
    if len(set(names)) != len(names) or any(type(r['key']) not in (int, float, str, type(None)) for r in candidates):
        return {'certified': False, 'reason': 'Ambiguous names or unsupported sort key'}
    kinds = {type(r['key']) for r in candidates if r['key'] is not None}
    if len(kinds) > 1:
        return {'certified': False, 'reason': 'Mixed sort key types'}
    if limit is not None and (type(limit) is not int or limit < 1):
        return {'certified': False, 'reason': 'Invalid explicit limit'}
    ordered = sorted(candidates, key=lambda r: (r['key'] is None, r['key']), reverse=descending)
    ranks = {}
    previous = object()
    rank = -1
    for row in ordered:
        if row['key'] != previous:
            rank += 1
        ranks[row['name']] = rank
        previous = row['key']
    expected = [ranks[r['name']] for r in ordered[:limit]]

    def valid(rows):
        if not isinstance(rows, list) or any(not isinstance(r, list) or len(r) != 1 for r in rows):
            return False
        output = [r[0] for r in rows]
        if any(n not in ranks for n in output) or len(set(output)) != len(output):
            return False
        return [ranks[n] for n in output] == expected

    counts = Counter(ranks.values())
    valid_published, valid_actual = valid(published), valid(actual)
    return {'certified': valid_published and valid_actual and published != actual,
            'published_obeys_ranking': valid_published, 'actual_obeys_ranking': valid_actual,
            'candidate_rows': len(candidates), 'tie_groups': sum(n > 1 for n in counts.values()),
            'primary_score_unchanged': True}
