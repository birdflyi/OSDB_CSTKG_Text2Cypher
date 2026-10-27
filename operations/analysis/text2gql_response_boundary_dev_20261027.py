"""Native reply/final-answer bridge; no model transport, scorer or gold input.

Consumes pre-supplied normalized replies. Direct keeps the raw-query contract;
Ours keeps strict finite-IR JSON. The caller must separately admit/freeze and
budget any real model/DB runtime. Journal failures of unknown scope propagate.
"""
from copy import deepcopy
import json
import math
import time

from tugraph_execution import execute_native_plan, non_gold_feedback


def strict_object(text):
    if type(text) is not str or not text.strip() or len(text) > 200000:
        raise ValueError('Expected one bounded JSON object')
    def pairs(items):
        result = {}
        for k, v in items:
            if k in result:
                raise ValueError('Duplicate JSON key')
            result[k] = v
        return result
    def constant(value):
        raise ValueError('Nonfinite JSON number')
    def number(value):
        v = float(value)
        if not math.isfinite(v):
            raise ValueError('Nonfinite JSON number')
        return v
    try:
        value = json.loads(text, object_pairs_hook=pairs,
                           parse_constant=constant, parse_float=number)
    except (ValueError, RecursionError) as exc:
        raise ValueError('Invalid finite-IR JSON') from exc
    if type(value) is not dict:
        raise ValueError('Expected an object')
    if value.get('status') == 'UNSUPPORTED':
        if (set(value) != {'status', 'reason'} or type(value['reason']) is not str
                or not value['reason'].strip() or len(value['reason']) > 2000):
            raise ValueError('Unsupported response needs a bounded reason')
    elif value.get('status') != 'ANSWER' or set(value) != {'status', 'calls', 'combine'}:
        raise ValueError('Answer must follow the frozen finite-IR envelope')
    return value


def failure(status, error=''):
    return {'status': status, 'complete': False, 'rows': None, 'columns': None,
            'error': error[:1000], 'physical_database_requests': 0,
            'new_physical_requests': 0, 'sql_wall_seconds': 0.,
            'response_bytes_all_attempts': 0, 'monetary_cost': None}


def score_view(outcome):
    """Preserve the native status alongside an explicit scorer lifecycle view."""
    status = outcome['status']
    mapping = {'GUARD_REJECTED': 'EXECUTION_ERROR', 'EXPLAIN_ERROR': 'EXECUTION_ERROR',
               'QUERY_ERROR': 'EXECUTION_ERROR', 'READ_ONLY_NOT_CONFIRMED': 'EXECUTION_ERROR',
               'MODEL_ERROR': 'PROTOCOL_ERROR', 'MALFORMED_RESULT': 'MALFORMED_ANSWER',
               'UNKNOWN_EXECUTION': 'UNKNOWN'}
    allowed = {'EXECUTED', 'UNSUPPORTED', 'MODEL_TRUNCATED', 'INFRASTRUCTURE_ERROR',
               'TRUNCATED', 'UNKNOWN', 'REFUSED'}
    if status not in mapping and status not in allowed:
        raise RuntimeError('Unrecognized native final-answer status')
    return {'status': mapping.get(status, status), 'native_status': status,
            'complete': outcome.get('complete') is True,
            'columns': deepcopy(outcome.get('columns')),
            'rows': deepcopy(outcome.get('rows'))}


def consume_reply(arm, record, schema, journal, *, request_prefix, graph, strategy='contract'):
    """Process one actual or fabricated receipt; never guess an absent terminal."""
    if arm not in ('direct', 'ours') or type(record) is not dict:
        raise ValueError('Expected an explicit arm and normalized receipt')
    started = time.monotonic()
    if record.get('outcome_unknown') is True or record.get('http_status') is None:
        result = failure('UNKNOWN', 'Model request lacks an audited terminal')
    elif record.get('transport_error') or record['http_status'] != 200:
        result = failure('INFRASTRUCTURE_ERROR', 'Recorded model transport/HTTP failure')
    elif record.get('finish_reason') == 'length':
        result = failure('MODEL_TRUNCATED', 'Fixed output limit reached')
    elif record.get('finish_reason') == 'content_filter' or record.get('refusal'):
        result = failure('REFUSED', 'Recorded model refusal')
    elif record.get('finish_reason') != 'stop':
        result = failure('MODEL_ERROR', 'Expected a stopped completion')
    elif type(record.get('content')) is not str or not record['content'].strip():
        result = failure('MODEL_ERROR', 'Missing answer content')
    elif arm == 'direct':
        # No JSON envelope, code-fence extraction, query rewriting or finite-IR
        # restriction on Direct. The shared native read guard is authoritative.
        result = journal.execute(request_prefix + '_direct', graph, record['content'])
    else:
        try:
            value = strict_object(record['content'])
        except ValueError as exc:
            result = failure('MODEL_ERROR', str(exc))
        else:
            if value['status'] == 'UNSUPPORTED':
                result = failure('UNSUPPORTED', value['reason'])
            else:
                result = execute_native_plan(value, schema, journal, request_prefix=request_prefix,
                                             graph=graph, strategy=strategy)
                # The frozen finite compiler returns per-read completeness,
                # not a top-level flag. Derive it only from all observations;
                # do not infer completeness merely from EXECUTED status.
                if result.get('status') == 'EXECUTED' and 'complete' not in result:
                    observations = result.get('observations')
                    result['complete'] = bool(observations) and all(
                        o.get('complete') is True for o in observations)
    # JournalConflict/adapter exceptions are intentionally not swallowed: the
    # owner must stop/audit, not charge a possibly dispatched request as zero.
    view = score_view(result)
    return {'native': deepcopy(result), 'final_answer': view,
            'answer_processing_wall_seconds': time.monotonic() - started,
            'new_model_calls': 0, 'monetary_cost': None,
            'formal_admission': False}


def feedback_messages(initial, previous_text, result, *, previous_feedback_count):
    # The native helper inspects operational status only; no scorer/gold input.
    diagnostic = non_gold_feedback(result['native'], previous_feedback_count=previous_feedback_count)
    if diagnostic is None:
        return None
    if type(previous_text) is not str:
        previous_text = ''
    return deepcopy(initial) + [{'role': 'assistant', 'content': previous_text}, diagnostic]


def replay_path(arm, initial, schema, records, journal, *, request_prefix, graph, strategy='contract'):
    """At most two pre-supplied receipts, NOT a dispatcher or a resumable run."""
    if type(records) is not list or not 1 <= len(records) <= 2:
        raise ValueError('Expected one initial and at most one feedback receipt')
    history = []
    pending_feedback = None
    for index, record in enumerate(records):
        result = consume_reply(arm, record, schema, journal,
                               request_prefix=request_prefix + '_r' + str(index), graph=graph,
                               strategy=strategy)
        history.append(result)
        pending_feedback = feedback_messages(initial, record.get('content'), result,
                                            previous_feedback_count=index)
        if pending_feedback is None or index == len(records)-1:
            break
    def known_sum(field):
        values = [item['native'].get(field) for item in history]
        return None if any(type(v) not in (int, float) for v in values) else sum(values)
    return {'kind': 'PRE_SUPPLIED_REPLY_REPLAY_NOT_MODEL_EXPERIMENT',
            'initial_answer': deepcopy(history[0]['final_answer']),
            'final_answer': deepcopy(history[-1]['final_answer']), 'history': history,
            'response_records_consumed': len(history), 'next_feedback_messages': pending_feedback,
            'physical_database_requests': known_sum('physical_database_requests'),
            'new_physical_requests': known_sum('new_physical_requests'),
            'sql_wall_seconds': known_sum('sql_wall_seconds'),
            'response_bytes_all_attempts': known_sum('response_bytes_all_attempts'),
            'new_model_calls': 0, 'model_usage_from_receipts': [deepcopy(r.get('usage')) for r in records[:len(history)]],
            'monetary_cost': None, 'formal_admission': False}
