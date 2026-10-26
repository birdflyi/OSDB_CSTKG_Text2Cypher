"""ID-safe diagnostic scoring journal, not a benchmark's official evaluator.

Only immutable final observations are accepted. Missing positions stay UNSENT;
duplicates/unknown IDs/version mismatches abort instead of silently selecting a
row. Caller supplies the comparator; no model, database, network or file I/O.
"""
from collections import Counter
import hashlib
import json


FAILURES = frozenset(('UNSUPPORTED', 'MODEL_ERROR', 'EXECUTION_ERROR',
    'INFRASTRUCTURE_ERROR', 'TRUNCATED', 'UNKNOWN_COMPLETION'))
CONTEXT = ('cohort', 'method', 'model', 'input_contract', 'score_contract')


class PairingError(ValueError):
    pass


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(',', ':'), allow_nan=False).encode('utf-8')).hexdigest()


def key(row):
    if any(type(row.get(k)) is not str or not row[k] for k in ('graph', 'id')):
        raise PairingError('Nonempty string graph and id required')
    return row['graph'], row['id']


def valid_sha(value):
    return type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def evaluate(manifest, observations, *, context, validate_answer, compare):
    """Return all positions plus conservative, explicitly diagnostic counters.

    Manifest contains graph/id/task_sha256 and a reference object or None.
    Context is evaluator-side version metadata, never model input. An observation
    is one final outcome, not an attempt or an alternative candidate. References
    with unresolved execution have reference=None and are never invented gold.
    validate_answer must reject incomplete/malformed answer objects. compare
    must return an actual bool; errors remain scoring errors, never wrong answers.
    """
    if type(manifest) is not list or type(observations) is not list:
        raise PairingError('Explicit position lists required')
    if type(context) is not dict or set(context) != set(CONTEXT) or any(
            type(context[k]) is not str or not context[k] for k in CONTEXT):
        raise PairingError('Complete explicit version context required')
    expected = {}
    for task in manifest:
        position = key(task)
        if position in expected or not valid_sha(task.get('task_sha256')):
            raise PairingError('Duplicate manifest position or invalid task SHA')
        if 'reference' not in task:
            raise PairingError('Known reference or explicit None required')
        if task['reference'] is not None:
            try:
                validate_answer(task['reference'])
            except Exception as exc:
                raise PairingError('Malformed declared known reference') from exc
        expected[position] = task
    observed = {}
    for observation in observations:
        position = key(observation)
        if position not in expected or position in observed:
            raise PairingError('Unknown or duplicate final observation')
        if observation.get('task_sha256') != expected[position]['task_sha256']:
            raise PairingError('Task version mismatch')
        if observation.get('context') != context:
            raise PairingError('Method/model/input/score/cohort context mismatch')
        if observation.get('status') not in FAILURES | {'EXECUTED'}:
            raise PairingError('Unknown terminal status')
        if observation['status'] != 'EXECUTED' and observation.get('answer') is not None:
            raise PairingError('Failure may not masquerade as a successful answer')
        observed[position] = observation
    records = []
    for position, task in expected.items():
        observation = observed.get(position)
        row = {'graph':position[0], 'id':position[1], 'task_sha256':task['task_sha256'],
            'status':'UNSENT' if observation is None else observation['status'],
            'reference_known':task['reference'] is not None, 'complete_answer':False,
            'score':None, 'score_state':'NO_COMPLETE_ANSWER'}
        if observation is not None and row['status'] == 'EXECUTED':
            try:
                validate_answer(observation.get('answer'))
            except Exception as exc:
                row.update(status='INVALID_ANSWER', error_class=type(exc).__name__)
            else:
                row['complete_answer'] = True
        if not row['reference_known']:
            row['score_state'] = 'UNKNOWN_REFERENCE'
        elif row['complete_answer']:
            try:
                equal = compare(task['reference'], observation['answer'])
                if type(equal) is not bool:
                    raise PairingError('Comparator must return bool, not a numeric score')
            except Exception as exc:
                row.update(score_state='SCORING_ERROR', error_class=type(exc).__name__)
            else:
                row.update(score_state='COMPARED', score=int(equal))
        elif observation is not None:
            # A missing/invalid final answer is unsuccessful for a known reference.
            # No call to the answer comparator is made on failures or refusals.
            row.update(score_state='NO_VALID_ANSWER', score=0)
        records.append(row)
    counts = Counter(r['status'] for r in records)
    known = sum(r['reference_known'] for r in records)
    correct = sum(r['score'] == 1 for r in records)
    wrong_complete = sum(r['complete_answer'] and r['score'] == 0 for r in records)
    unresolved = sum(r['reference_known'] and r['score'] is None for r in records)
    summary = {'kind':'DIAGNOSTIC_PAIRING_NOT_FORMAL_EX', 'planned':len(records),
        'final_observations':len(observed), 'unsent':counts['UNSENT'],
        'status_counts':dict(sorted(counts.items())), 'known_references':known,
        'unknown_references':len(records)-known,
        'complete_answers':sum(r['complete_answer'] for r in records),
        'correct_complete_answers':correct, 'wrong_complete_answers':wrong_complete,
        'known_reference_unresolved_scores':unresolved,
        'scoring_errors':sum(r['score_state']=='SCORING_ERROR' for r in records),
        'coverage_denominator':len(records), 'known_reference_denominator':known,
        'diagnostic_known_reference_fraction':correct/known if known and not unresolved else None,
        'fraction_withheld_if_known_scores_unresolved':bool(unresolved),
        'formal_metric_frozen':False, 'context':dict(context)}
    assert sum(counts.values()) == len(manifest)
    assert summary['final_observations'] + summary['unsent'] == len(manifest)
    return {'summary':summary, 'records':records}
