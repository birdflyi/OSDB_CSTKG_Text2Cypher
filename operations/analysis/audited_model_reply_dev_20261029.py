"""Read pinned scheduler evidence without sending, retrying or charging requests.

This preparation component checks one source snapshot. Its file digests must
come from an independently audited manifest; it does not audit the ledger chain
or grant runtime approval. Old receipts and the original ledger remain untouched.
"""

from copy import deepcopy
from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import re

from engine.ids import component, logical_key


class ReplyConflict(ValueError):
    pass


def strict_json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ReplyConflict('Duplicate JSON key')
            value[key] = item
        return value

    def number(text):
        value = float(text)
        if not math.isfinite(value):
            raise ReplyConflict('Nonfinite JSON number')
        return value

    def constant(text):
        raise ReplyConflict('Nonfinite JSON constant')

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_float=number,
                          parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ReplyConflict('Invalid strict JSON') from error


@dataclass(frozen=True)
class PinnedFile:
    path: str
    sha256: str

    def load(self, *, max_bytes=16000000):
        if type(self.sha256) is not str or not re.fullmatch('[0-9a-f]{64}', self.sha256):
            raise ReplyConflict('An externally pinned lowercase SHA256 is required')
        # Read at most the evidence bound; a size check alone has a race.
        with Path(self.path).open('rb') as stream:
            raw = stream.read(max_bytes+1)
        if len(raw) > max_bytes or hashlib.sha256(raw).hexdigest() != self.sha256:
            raise ReplyConflict('Pinned source bytes differ or exceed the evidence bound')
        value = strict_json(raw)
        if type(value) is not dict:
            raise ReplyConflict('Expected a source object')
        return value


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
            raise ReplyConflict('Explicit frozen request identity required')
        if self.phase not in ('initial', 'feedback') or self.method not in ('direct', 'ours'):
            raise ReplyConflict('Expected a Direct/Ours initial or feedback identity')

    @property
    def key(self):
        return component(self.model)+'__'+logical_key(
            self.benchmark, self.task_id, self.method, self.phase)


class RecordedReply:
    """Validate request, terminal and (when charged) the pinned source slot.

    A sparse controller-unknown record may have charged=None and no request ID.
    It remains UNKNOWN, needs audit and never produces a native DB operation.
    A charged usable reply requires all six terminal identity fields and an
    exact slot/request hash binding. No reasoning text becomes answer content.
    """

    def __init__(self, identity, *, request, terminal, slot=None):
        if not isinstance(identity, ReplyIdentity):
            raise ReplyConflict('Frozen ReplyIdentity required')
        self._identity = identity
        self._request = request.load()
        self._terminal = terminal.load()
        value = self._terminal
        if self._request.get('model') != identity.model:
            raise ReplyConflict('Request model differs from frozen identity')
        messages = self._request.get('messages')
        if (type(messages) is not list or not messages
                or any(type(m) is not dict or type(m.get('role')) is not str for m in messages)):
            raise ReplyConflict('Original public request messages are missing')
        if value.get('logical_key') != identity.key:
            raise ReplyConflict('Terminal logical identity mismatch')
        for key, expected in asdict(identity).items():
            if key in value and value[key] != expected:
                raise ReplyConflict('Terminal identity field mismatch')
        if 'charged' not in value or (value['charged'] is not None and type(value['charged']) is not bool):
            raise ReplyConflict('Charge state must be true, false or explicit unknown')
        self._slot = slot.load() if slot is not None else None
        if value['charged'] is True:
            if self._slot is None:
                raise ReplyConflict('Charged reply needs its audited source slot')
            if (type(value.get('request_id')) is not str or not value['request_id']
                    or any(self._slot.get(k) != value.get(v) for k, v in
                           (('request_id','request_id'),('slot','budget_slot'),
                            ('attempt_no','attempt_no'),('logical_key','logical_key')))
                    or self._slot.get('request_sha256') != request.sha256
                    or type(value.get('budget_slot')) is not int or value['budget_slot'] < 1
                    or type(value.get('attempt_no')) is not int or value['attempt_no'] < 1
                    or type(self._slot.get('scope')) is not str or not self._slot['scope']):
                raise ReplyConflict('Source slot, attempt and request bytes do not bind')
        elif self._slot is not None:
            raise ReplyConflict('A source slot contradicts false/unknown charge state; audit first')
        self._lifecycle = self._status()
        self._pins = dict(request=asdict(request), terminal=asdict(terminal),
                         slot=asdict(slot) if slot is not None else None)

    @property
    def identity(self):
        return self._identity

    @property
    def request(self):
        return deepcopy(self._request)

    @property
    def terminal(self):
        return deepcopy(self._terminal)

    @property
    def status(self):
        return self._lifecycle

    def _status(self):
        value = self._terminal
        status, charged = value.get('status'), value['charged']
        if status == 'unknown' or value.get('generation_outcome_unknown') is True:
            if charged is False:
                raise ReplyConflict('Unknown generation cannot be certified undispatched')
            return 'UNKNOWN'
        if status in ('not_dispatched','local_resource_error','local_configuration_error','admission_timeout'):
            if charged is not False:
                raise ReplyConflict('Undispatched result has inconsistent charge evidence')
            return 'UNDISPATCHED'
        if charged is not True:
            raise ReplyConflict('Dispatched result requires a source charge record')
        if status in ('infra','throttled','auth_error'):
            return 'INFRASTRUCTURE_ERROR'
        if status not in ('ok','length','refused','empty'):
            raise ReplyConflict('Unrecognized scheduler terminal')
        if any(value.get(k) != v for k, v in asdict(self.identity).items()):
            raise ReplyConflict('Usable reply lacks full terminal identity')
        if (value.get('transport_error') or value.get('protocol_error')
                or type(value.get('http_status')) is not int or value['http_status'] != 200):
            raise ReplyConflict('Usable reply lacks a complete HTTP success')
        body = value.get('body')
        if type(body) is not dict or body.get('model') != self.identity.model:
            raise ReplyConflict('Response model differs from request')
        choices = body.get('choices')
        if type(choices) is not list or len(choices) != 1 or type(choices[0]) is not dict:
            raise ReplyConflict('Expected one completion')
        choice = choices[0]
        message = choice.get('message')
        if type(message) is not dict:
            raise ReplyConflict('Completion message is missing')
        reason, content = choice.get('finish_reason'), message.get('content')
        if reason == 'length':
            actual = 'length'
        elif reason == 'content_filter' or message.get('refusal'):
            actual = 'refused'
        elif reason == 'stop' and type(content) is str:
            actual = 'ok' if content.strip() else 'empty'
        else:
            raise ReplyConflict('Nonterminal completion is not executable')
        usage = body.get('usage') if type(body.get('usage')) is dict else None
        if (actual != status or value.get('content') != content
                or value.get('finish_reason') != reason or value.get('usage') != usage):
            raise ReplyConflict('Terminal projection disagrees with raw completion')
        return dict(ok='READY',length='MODEL_TRUNCATED',refused='REFUSED',empty='EMPTY')[status]

    def source_link(self):
        value = self._terminal
        return dict(identity=asdict(self.identity), logical_key=self.identity.key,
            files=deepcopy(self._pins), source_request_id=value.get('request_id'),
            source_budget_slot=value.get('budget_slot'), source_attempt=value.get('attempt_no'),
            source_charge_key=value.get('request_id') if value['charged'] is True else None,
            source_budget_scope=self._slot['scope'] if self._slot is not None else None,
            source_charged=value['charged'], source_status=value['status'],
            source_usage=deepcopy(value.get('usage')), source_ledger_chain_reaudited=False,
            requires_outcome_audit=self.status=='UNKNOWN', new_model_calls=0,
            monetary_cost=None, formal_admission=False)
