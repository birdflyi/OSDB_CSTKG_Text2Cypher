"""Consume an audited scheduler receipt with the existing native answer bridge.

Preparation component only: no model sender, budget approval or graph startup.
The caller supplies the expected receipt digest and frozen request identity.
References and scoring never enter this consumer. It is not registered as an
engine adapter until graph lifecycle, feedback dispatch and admission are wired.
"""

from copy import deepcopy
from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import re

from text2gql_response_boundary_dev_20261027 import consume_reply, feedback_messages


class ReceiptConflict(ValueError):
    """Receipt bytes or identity disagree; audit instead of guessing or resending."""


@dataclass(frozen=True)
class ReplyIdentity:
    benchmark: str
    task_id: str
    method: str
    graph: str
    model: str
    phase: str

    def __post_init__(self):
        if any(type(v) is not str or not v for v in asdict(self).values()):
            raise ValueError('Every request identity field must be explicit')
        if self.benchmark != 'text2gql' or self.method not in ('direct', 'ours'):
            raise ValueError('Expected a native Text2GQL generation identity')
        if self.phase not in ('initial', 'feedback'):
            raise ValueError('Expected initial or the single feedback phase')


def _unique_pairs(pairs):
    value = {}
    for name, item in pairs:
        if name in value:
            raise ReceiptConflict('Duplicate receipt JSON key')
        value[name] = item
    return value


def _finite_number(text):
    value = float(text)
    if not math.isfinite(value):
        raise ReceiptConflict('Nonfinite receipt number')
    return value


def _invalid_constant(text):
    raise ReceiptConflict('Nonfinite receipt constant')


def load_receipt(path, expected_sha256, identity):
    """Check bytes and request identity before any native database operation."""
    if not isinstance(identity, ReplyIdentity):
        raise ValueError('Expected a pinned ReplyIdentity')
    if type(expected_sha256) is not str or not re.fullmatch('[0-9a-f]{64}', expected_sha256):
        raise ValueError('Expected a pinned lowercase SHA256')
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ReceiptConflict('Receipt digest mismatch')
    try:
        receipt = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_pairs,
                             parse_float=_finite_number, parse_constant=_invalid_constant)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ReceiptConflict('Invalid receipt JSON') from exc
    if type(receipt) is not dict:
        raise ReceiptConflict('Expected one receipt object')
    if any(receipt.get(k) != v for k, v in asdict(identity).items()):
        raise ReceiptConflict('Receipt request identity mismatch')
    if type(receipt.get('charged')) is not bool:
        raise ReceiptConflict('Missing audited charge state')
    if type(receipt.get('request_id')) is not str or not receipt['request_id']:
        raise ReceiptConflict('Missing stable source request ID')
    return receipt


def normalized_reply(receipt, expected_model):
    """Preserve scheduler lifecycle; HTTP200 alone is not a usable completion."""
    status = receipt.get('status')
    charged = receipt['charged']
    undispatched = {'not_dispatched', 'local_resource_error', 'local_configuration_error'}
    if status in undispatched:
        if charged:
            raise ReceiptConflict('Undispatched receipt cannot be charged')
        return {'outcome_unknown': True}, 'UNDISPATCHED'
    if not charged:
        raise ReceiptConflict('A dispatched terminal needs audited charge evidence')
    if status == 'unknown' or receipt.get('generation_outcome_unknown') is True:
        return {'outcome_unknown': True}, 'UNKNOWN'
    if status in ('infra', 'throttled', 'auth_error'):
        return {'outcome_unknown': True}, 'INFRASTRUCTURE_ERROR'
    if status not in ('ok', 'empty', 'length', 'refused'):
        raise ReceiptConflict('Unrecognized scheduler terminal status')
    if receipt.get('transport_error') or receipt.get('http_status') != 200:
        raise ReceiptConflict('Successful generation lacks complete HTTP evidence')
    body = receipt.get('body')
    if type(body) is not dict or body.get('model') != expected_model:
        raise ReceiptConflict('Response model identity mismatch')
    choices = body.get('choices')
    if type(choices) is not list or len(choices) != 1 or type(choices[0]) is not dict:
        raise ReceiptConflict('Expected exactly one completion')
    choice = choices[0]
    message = choice.get('message')
    if type(message) is not dict:
        raise ReceiptConflict('Missing completion message')
    reason, content = choice.get('finish_reason'), message.get('content')
    if reason == 'length':
        kind = 'length'
    elif reason == 'content_filter' or message.get('refusal'):
        kind = 'refused'
    elif reason == 'stop' and type(content) is str:
        kind = 'ok' if content.strip() else 'empty'
    else:
        raise ReceiptConflict('Nonterminal completion marked usable')
    if kind != status or receipt.get('finish_reason') != reason or receipt.get('content') != content:
        raise ReceiptConflict('Scheduler projection disagrees with raw response')
    usage = body.get('usage') if type(body.get('usage')) is dict else None
    if receipt.get('usage') != usage:
        raise ReceiptConflict('Usage projection disagrees with raw response')
    return {'http_status': 200, 'finish_reason': reason, 'content': content,
            'refusal': message.get('refusal'), 'usage': deepcopy(usage)}, None


def consume_recorded_reply(path, expected_sha256, identity, schema, journal, *,
                           initial_messages, allow_feedback):
    """Produce the actual answer and an optional unsent operation diagnostic.

    Call separately for initial/feedback receipts. Direct+feedback is a view of
    the same Direct initial receipt; it is not a second first-round generator.
    Source charge evidence is a link, never another model attempt or zero fee.
    Native journal conflicts and persistence errors deliberately propagate.
    """
    if not isinstance(identity, ReplyIdentity):
        raise ValueError('Expected a pinned ReplyIdentity')
    if type(allow_feedback) is not bool or (identity.phase == 'feedback' and not allow_feedback):
        raise ValueError('Feedback must be an explicit allowed method view')
    if type(initial_messages) is not list or not initial_messages:
        raise ValueError('Expected the original frozen public messages')
    receipt = load_receipt(path, expected_sha256, identity)
    record, lifecycle = normalized_reply(receipt, identity.model)
    # Bounded deterministic paths retain the full identity in the source receipt.
    prefix = 'receipt_' + expected_sha256[:32]
    result = consume_reply(identity.method, record, schema, journal,
                           request_prefix=prefix, graph=identity.graph)
    if lifecycle is not None:
        result['final_answer']['status'] = lifecycle
    diagnostic = None
    if allow_feedback and identity.phase == 'initial' and lifecycle is None:
        diagnostic = feedback_messages(initial_messages, record.get('content'), result,
                                       previous_feedback_count=0)
    result.update(source_request_id=receipt['request_id'], source_receipt_sha256=expected_sha256,
                  source_identity=asdict(identity), source_status=receipt['status'],
                  source_charged=receipt['charged'],
                  source_usage=deepcopy(receipt.get('usage')), monetary_cost=None,
                  feedback_messages=diagnostic, feedback_dispatched=False,
                  new_model_calls=0, formal_admission=False)
    return result
