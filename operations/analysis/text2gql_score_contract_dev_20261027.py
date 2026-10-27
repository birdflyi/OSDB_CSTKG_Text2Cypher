"""Pure development scoring contract for complete final answers.

No model/DB/transport I/O. The author comparator must be supplied by a caller
that verified its pinned source. This module neither activates a budget nor
certifies a cohort, an ordering requirement, or REST/Bolt equivalence.
"""
from collections import Counter, defaultdict
from copy import deepcopy

from schema_services import IncompleteEvidence
from tugraph_results import MalformedNativeResult, validate_rows, strict_auxiliary_equal


NONTERMINAL = {'UNDISPATCHED', 'UNKNOWN', 'UNTERMINATED'}
FAILURES = {'UNSUPPORTED', 'INSUFFICIENT_EVIDENCE', 'PROTOCOL_ERROR',
            'EXECUTION_ERROR', 'INFRASTRUCTURE_ERROR', 'MODEL_ERROR',
            'TRUNCATED', 'MODEL_TRUNCATED', 'REFUSED', 'DECLINED', 'MALFORMED_ANSWER', 'INCOMPLETE_ANSWER'}
ORDER_RULES = {'UNADJUDICATED', 'BAG', 'ORDERED'}


def key(value):
    if type(value) not in (list, tuple) or len(value) != 2 or any(type(v) is not str for v in value):
        raise ValueError('A graph/id pair of strings is required')
    return tuple(value)


def index_unique(records):
    indexed = {}
    for identity, value in records:
        identity = key(identity)
        if identity in indexed:
            raise ValueError('Duplicate graph/id; never deduplicate silently')
        indexed[identity] = value
    return indexed


def author_rows(answer):
    # Positional keys preserve duplicate column aliases and original order.
    # No value coercion, key-name alignment, sorting, dedup or JSON repair.
    return [{str(i): v for i, v in enumerate(row)} for row in answer['rows']]


class FinalAnswerContract:
    def __init__(self, cohort, author_compare, *, comparator_sha256):
        if not callable(author_compare) or type(comparator_sha256) is not str or len(comparator_sha256) != 64:
            raise ValueError('A verified author comparator and its SHA256 are required')
        self.cohort = deepcopy(index_unique(cohort))
        if not self.cohort:
            raise ValueError('Empty task population is not a completed experiment')
        for metadata in self.cohort.values():
            if type(metadata.get('reference_known')) is not bool:
                raise ValueError('Reference cohort membership must be fixed explicitly')
            if type(metadata.get('reference_contract_concern', False)) is not bool:
                raise ValueError('Reference concern flag must be explicit boolean metadata')
            if metadata.get('order_rule', 'UNADJUDICATED') not in ORDER_RULES:
                raise ValueError('Ordering rule must be fixed outside method outputs')
        self.compare = author_compare
        self.comparator_sha256 = comparator_sha256

    def score_one(self, identity, reference, prediction):
        identity = key(identity)
        if identity not in self.cohort:
            raise ValueError('Foreign task identity')
        m = self.cohort[identity]
        status = prediction.get('status')
        if status not in NONTERMINAL | FAILURES | {'EXECUTED'}:
            raise ValueError('Unrecognized prediction lifecycle status')
        rule = m.get('order_rule', 'UNADJUDICATED')
        result = {'task_key': list(identity), 'reference_known': m['reference_known'],
                  'reference_contract_concern': bool(m.get('reference_contract_concern', False)),
                  'prediction_status': status, 'prediction_terminal': status not in NONTERMINAL,
                  'complete_answer': False, 'primary': None, 'strict_bag': None,
                  'task_order_sensitive': None, 'order_rule': rule,
                  'comparator_sha256': self.comparator_sha256}
        # Completion/coverage is measured for every task, even unknown-gold11.
        if status == 'EXECUTED':
            if prediction.get('complete') is not True:
                result['prediction_status'] = 'INCOMPLETE_ANSWER'
            else:
                try:
                    validate_rows(prediction['columns'], prediction['rows'])
                except IncompleteEvidence:
                    result['prediction_status'] = 'TRUNCATED'
                except (KeyError, MalformedNativeResult):
                    result['prediction_status'] = 'MALFORMED_ANSWER'
                else:
                    result['complete_answer'] = True
        if not m['reference_known']:
            result['reason'] = 'FIXED_REFERENCE_UNKNOWN_NOT_CORRECT_OR_WRONG'
            return result
        if reference is None or reference.get('status') != 'EXECUTED' or reference.get('complete') is not True:
            result['reason'] = 'KNOWN_REFERENCE_UNAVAILABLE_NO_DENOMINATOR_SHRINK'
            return result
        try:
            validate_rows(reference['columns'], reference['rows'])
        except (KeyError, MalformedNativeResult, IncompleteEvidence):
            result['reason'] = 'MALFORMED_REFERENCE_NO_DENOMINATOR_SHRINK'
            return result
        if status in NONTERMINAL:
            result['reason'] = 'NO_TERMINAL_PREDICTION_NOT_A_COMPLETED_FAILURE'
            return result
        if not result['complete_answer']:
            result.update(primary=False, strict_bag=False,
                          task_order_sensitive=False if rule != 'UNADJUDICATED' else None,
                          reason='NO_COMPLETE_FINAL_ANSWER')
            return result
        same_width = len(reference['columns']) == len(prediction['columns'])
        result['primary'] = same_width and bool(self.compare(author_rows(reference), author_rows(prediction)))
        result['strict_bag'] = strict_auxiliary_equal(reference, prediction, ordered=False)
        if rule != 'UNADJUDICATED':
            result['task_order_sensitive'] = strict_auxiliary_equal(reference, prediction, ordered=rule == 'ORDERED')
        result['reason'] = 'COMPLETE_FINAL_ANSWER_COMPARED'
        return result

    def score_batch(self, references, predictions):
        refs, preds = index_unique(references), index_unique(predictions)
        if set(refs) != set(self.cohort) or set(preds) != set(self.cohort):
            raise ValueError('Missing or foreign identity; placeholders required, zip truncation forbidden')
        return [self.score_one(identity, refs[identity], preds[identity]) for identity in self.cohort]


def summarize(rows, *, expected_keys):
    if not rows:
        raise ValueError('No declared population')
    identities = [key(r['task_key']) for r in rows]
    if len(set(identities)) != len(identities):
        raise ValueError('Duplicate scored task')
    population = [key(k) for k in expected_keys]
    if len(set(population)) != len(population) or set(population) != set(identities):
        raise ValueError('Summary must retain the entire declared task population')
    known = [r for r in rows if r['reference_known']]
    unresolved = sum(r['primary'] is None for r in known)
    correct = sum(r['primary'] is True for r in known)
    n = len(known)
    all_terminal = all(r['prediction_terminal'] for r in rows)
    strata = defaultdict(list)
    for r in rows:
        strata[(r['task_key'][0], r['reference_contract_concern'])].append(r)
    return {'kind': 'DEVELOPMENT_FINAL_ANSWER_CONTRACT_NOT_FORMAL_ADMISSION',
            'all_positions': len(rows), 'reference_known': n, 'reference_unknown': len(rows)-n,
            'terminal': sum(r['prediction_terminal'] for r in rows),
            'undispatched_or_unknown': sum(not r['prediction_terminal'] for r in rows),
            'complete_answers_all_positions': sum(r['complete_answer'] for r in rows),
            'primary_correct_known': correct, 'primary_unresolved_known': unresolved,
            'primary_final_point': correct/n if n and not unresolved and all_terminal else None,
            'primary_lower_bound_fixed_known_N': correct/n if n else None,
            'primary_upper_bound_fixed_known_N': (correct+unresolved)/n if n else None,
            'primary_vs_strict_bag_disagreements': sum(r['primary'] is not None and r['strict_bag'] is not None
                                                       and r['primary'] != r['strict_bag'] for r in rows),
            'order_unadjudicated_positions': sum(r['order_rule']=='UNADJUDICATED' for r in rows),
            'statuses': dict(Counter(r['prediction_status'] for r in rows)),
            'strata': [{'graph': graph, 'reference_contract_concern': concern,
                        'all_positions': len(items), 'known_reference': sum(r['reference_known'] for r in items),
                        'primary_correct': sum(r['primary'] is True for r in items),
                        'primary_unresolved': sum(r['reference_known'] and r['primary'] is None for r in items)}
                       for (graph, concern), items in sorted(strata.items())],
            'official_reference_repaired': False, 'positions_excluded': 0,
            'formal_metric_frozen': False, 'formal_prediction_admitted': False}
