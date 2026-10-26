"""Development-only bounded native execution; no model dispatcher or score.

Each logical query uses EXPLAIN plus execution, both journalled before sending.
An incomplete/pending request is never automatically retried. The caller owns
the graph snapshot, process/port ownership and whole-path SQL budget gates.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import time

from public_read_guard import read_query, ReadOnlyViolation
from schema_services import IncompleteEvidence, UnsupportedPlan, execute_plan
from tugraph_dialect import render_compiler_query
from tugraph_results import MAX_BYTES, parse_rest_result, MalformedNativeResult

VERSION = 'native-execution-dev-2'


class JournalConflict(RuntimeError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _write_new(path, raw):
    # A partial record is evidence to audit, never permission to overwrite.
    with path.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _now():
    return datetime.now(timezone.utc).isoformat()


class LocalRestTransport:
    """Private IPv4-loopback HTTP only; no proxy/redirect/login/retry.

    Captures partial bytes on failure. HTTP success is execution-complete only
    under the native v4.5.2 non-streaming /cypher response contract, still
    subject to independent JSON shape, size and row validation in the journal.
    This is not a server-memory limit or REST/Bolt equivalence proof.
    """
    def __init__(self, port, token):
        if type(port) is not int or not 1024 <= port <= 65535:
            raise ValueError('Expected an assigned private port')
        if type(token) is not str or not token or '\r' in token or '\n' in token:
            raise ValueError('Expected private runtime authorization')
        self.port, self._token = port, token

    def __call__(self, payload, wall_seconds, max_bytes):
        deadline = time.monotonic() + wall_seconds
        connection = http.client.HTTPConnection('127.0.0.1', self.port,
                                                timeout=wall_seconds)
        raw = bytearray()
        status = None
        try:
            connection.request('POST', '/cypher', body=payload, headers={
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': 'Bearer ' + self._token})
            wire_socket = connection.sock
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Native request deadline exceeded before response')
            wire_socket.settimeout(remaining)
            response = connection.getresponse()
            status = response.status
            while True:
                if response.isclosed() and response.length in (None, 0):
                    return {'raw': bytes(raw), 'http_status': status,
                            'transport_complete': True,
                            'execution_complete': status == 200}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('Total native response deadline exceeded')
                wire_socket.settimeout(remaining)
                block = response.read1(min(65536, max_bytes + 1 - len(raw)))
                if not block:
                    # A short Content-Length body must not become success.
                    if response.length not in (None, 0):
                        raise EOFError('Response ended before Content-Length')
                    return {'raw': bytes(raw), 'http_status': status,
                            'transport_complete': True,
                            'execution_complete': status == 200}
                raw.extend(block)
                if len(raw) >= max_bytes:
                    return {'raw': bytes(raw), 'http_status': status,
                            'transport_complete': False, 'execution_complete': False,
                            'failure_kind': 'BYTE_CAP'}
        except (OSError, EOFError, http.client.HTTPException) as exc:
            # Exception messages can contain endpoint/auth details. Keep type only.
            return {'raw': bytes(raw), 'http_status': status,
                    'transport_complete': False, 'execution_complete': False,
                    'failure_kind': type(exc).__name__}
        finally:
            connection.close()


class NativeQueryJournal:
    def __init__(self, directory, transport, *, query_seconds=15, wall_seconds=25):
        for value in (query_seconds, wall_seconds):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError('Expected positive finite time budgets')
        if not query_seconds < wall_seconds <= 120:
            raise ValueError('Wall budget must bound the declared query budget')
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.transport = transport
        self.query_seconds, self.wall_seconds = query_seconds, wall_seconds

    def _physical(self, folder, phase, graph, script):
        target = folder / phase
        target.mkdir()                 # exclusive physical request reservation
        payload = canonical({'graph': graph, 'script': script,
                             'timeout': self.query_seconds})
        _write_new(target / 'request.json', payload)
        _write_new(target / 'reserved.json', canonical({
            'phase': phase, 'started': _now(), 'request_sha256': digest(payload),
            'query_seconds': self.query_seconds, 'wall_seconds': self.wall_seconds,
            'max_response_bytes': MAX_BYTES, 'physical_database_requests': 1,
            'model_calls': 0, 'retry': False}))
        started = time.monotonic()
        try:
            wire = self.transport(payload, self.wall_seconds, MAX_BYTES)
        except Exception as exc:
            # A buggy transport is still possibly dispatched: retain UNKNOWN.
            wire = {'raw': b'', 'http_status': None, 'transport_complete': False,
                    'execution_complete': False, 'failure_kind': type(exc).__name__}
        raw = wire.get('raw')
        if type(raw) is not bytes or len(raw) > MAX_BYTES + 1:
            raise JournalConflict('Invalid transport receipt; audit reserved request')
        _write_new(target / 'response.bin', raw)
        receipt = {'http_status': wire.get('http_status'),
                   'transport_complete': wire.get('transport_complete') is True,
                   'execution_complete': wire.get('execution_complete') is True,
                   'failure_kind': wire.get('failure_kind'), 'finished': _now(),
                   'wall_seconds': time.monotonic() - started,
                   'request_sha256': digest(payload), 'response_sha256': digest(raw),
                   'response_bytes': len(raw), 'physical_database_requests': 1,
                   'model_calls': 0, 'monetary_cost': None, 'retry': False}
        _write_new(target / 'receipt.json', canonical(receipt))
        return receipt, raw

    @staticmethod
    def _response(receipt, raw, phase):
        if not receipt['transport_complete']:
            return {'status': 'TRUNCATED' if receipt['failure_kind'] == 'BYTE_CAP'
                    else 'UNKNOWN_EXECUTION', 'complete': False,
                    'failure_kind': receipt['failure_kind']}
        status = receipt['http_status']
        if status != 200:
            try:
                error = json.loads(raw.decode('utf-8')).get('error_message', '')
            except (ValueError, AttributeError):
                error = ''
            if type(error) is not str:
                error = ''
            # 500 alone is not evidence of a semantic error.
            semantic = (status in (400, 500) and re.match(
                r'^(?:\[(?:CypherException|ParserException|InputError|QueryException)\] )?'
                r'(?:CypherException|ParserException|InputError|QueryException):', error) is not None)
            return {'status': phase.upper() + '_ERROR' if semantic else 'INFRASTRUCTURE_ERROR',
                    'complete': False, 'http_status': status,
                    'error': error[:1000] if semantic else '',
                    'error_bytes_truncated': semantic and len(error) > 1000}
        if receipt['execution_complete'] is not True:
            return {'status': 'UNKNOWN_EXECUTION', 'complete': False}
        try:
            return parse_rest_result(raw, transport_complete=True,
                                     execution_complete=receipt['execution_complete'])
        except IncompleteEvidence:
            return {'status': 'TRUNCATED', 'complete': False}
        except MalformedNativeResult:
            return {'status': 'MALFORMED_RESULT', 'complete': False}

    def execute(self, request_id, graph, script):
        if type(request_id) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{1,120}', request_id):
            raise ValueError('Expected a stable, bounded request ID')
        if type(graph) is not str or not graph or len(graph) > 200:
            raise ValueError('Expected an explicit graph snapshot name')
        if type(script) is not str:
            raise ValueError('Expected script bytes represented as a string')
        identity = {'version': VERSION, 'graph': graph, 'script': script,
                    'query_seconds': self.query_seconds, 'wall_seconds': self.wall_seconds,
                    'max_bytes': MAX_BYTES}
        immutable = canonical(identity)
        folder = self.directory / request_id
        try:
            folder.mkdir()
        except FileExistsError:
            try:
                if (folder / 'identity.json').read_bytes() != immutable:
                    raise JournalConflict('Request ID reused with different content')
                seal = json.loads((folder / 'complete.json').read_bytes())
                outcome_raw = (folder / 'outcome.json').read_bytes()
                if digest(outcome_raw) != seal['outcome_sha256']:
                    raise JournalConflict('Completed result digest mismatch')
                for name, sha in seal['files'].items():
                    if digest((folder / name).read_bytes()) != sha:
                        raise JournalConflict('Completed request evidence changed')
                outcome = json.loads(outcome_raw)
            except (OSError, ValueError, KeyError) as exc:
                raise JournalConflict('Pending/incomplete journal: audit; do not resend') from exc
            return {**outcome, 'cache_replay': True, 'new_physical_requests': 0}
        _write_new(folder / 'identity.json', immutable)
        started = time.monotonic()
        calls = []
        # Lexical validation precedes any physical request. Rejections remain
        # local model/guard errors with durable records, not empty answers.
        try:
            clean = read_query(script)
            if re.match(r'(?is)^\s*(EXPLAIN|PROFILE)\b', clean):
                raise ReadOnlyViolation('Model output must be an answer query')
        except ReadOnlyViolation as exc:
            return self._finish(folder, request_id, script, started, calls, {
                'status': 'GUARD_REJECTED', 'complete': False, 'error': str(exc)})
        receipt, raw = self._physical(folder, 'explain', graph, 'EXPLAIN ' + clean)
        calls.append(receipt)
        outcome = self._response(receipt, raw, 'explain')
        if outcome['status'] == 'EXECUTED':
            rows = outcome['rows']
            if (outcome['columns'] != ['@plan'] or len(rows) != 1 or len(rows[0]) != 1
                    or type(rows[0][0]) is not str
                    or not rows[0][0].startswith('ReadOnly:1\nExecution Plan:\n')):
                outcome = {'status': 'READ_ONLY_NOT_CONFIRMED', 'complete': False}
            else:
                receipt, raw = self._physical(folder, 'query', graph, clean)
                calls.append(receipt)
                outcome = self._response(receipt, raw, 'query')
        return self._finish(folder, request_id, clean, started, calls, outcome)

    @staticmethod
    def _finish(folder, request_id, clean, started, calls, outcome):
        outcome.update({'request_id': request_id, 'script_sha256': digest(clean.encode('utf-8')),
                        'physical_database_requests': len(calls), 'model_calls': 0,
                        'sql_wall_seconds': sum(c['wall_seconds'] for c in calls),
                        'path_wall_seconds': time.monotonic() - started,
                        'response_bytes_all_attempts': sum(c['response_bytes'] for c in calls),
                        'monetary_cost': None, 'formal_admission': False})
        outcome_raw = canonical(outcome)
        _write_new(folder / 'outcome.json', outcome_raw)
        files = {str(p.relative_to(folder)).replace('\\', '/'): digest(p.read_bytes())
                 for p in sorted(folder.rglob('*')) if p.is_file()}
        _write_new(folder / 'complete.json', canonical({
            'outcome_sha256': digest(outcome_raw), 'files': files}))
        return {**deepcopy(outcome), 'cache_replay': False,
                'new_physical_requests': len(calls)}


def non_gold_feedback(outcome, *, previous_feedback_count):
    """At most one execution diagnostic, no reference/correctness input.

    Valid results, including empty or wrong answers, cannot trigger feedback.
    Unknown, infra and malformed transport results do not consume a semantic
    repair. The model-call budget/controller remains a separate required gate.
    """
    if type(previous_feedback_count) is not int or previous_feedback_count < 0:
        raise ValueError('Expected recorded feedback count')
    if previous_feedback_count != 0 or outcome.get('status') not in (
            'GUARD_REJECTED', 'EXPLAIN_ERROR', 'QUERY_ERROR', 'UNSUPPORTED', 'MODEL_ERROR'):
        return None
    return {'role': 'user', 'content': canonical({
        'kind': 'execution_diagnostic_not_correctness_feedback',
        'status': outcome['status'], 'error': str(outcome.get('error', ''))[:1000],
        'instruction': 'Return one revised answer in the original output format. '
                       'No reference answer is available.'}).decode('utf-8')}


def execute_native_plan(plan, schema, journal, *, request_prefix, graph, strategy='contract'):
    """Connect the unchanged finite compiler to the same Direct query journal.

    No automatic Direct fallback. Preserve the actual first failing SQL status,
    including UNKNOWN, instead of converting every failure into a model error.
    Full model/feedback accounting and batch admission remain separate gates.
    """
    observations = []
    def execute(query, parameters):
        observation = journal.execute(request_prefix + '_sql_' + str(len(observations)+1),
                                      graph, render_compiler_query(query, parameters))
        observations.append(observation)
        return observation
    started = time.monotonic()
    try:
        result = execute_plan(plan, schema, execute, backend='tugraph-4.5.2', strategy=strategy)
    except UnsupportedPlan as exc:
        result = {'status': 'UNSUPPORTED', 'complete': False, 'error': str(exc)[:1000]}
    except IncompleteEvidence:
        result = deepcopy(observations[-1]) if observations else {
            'status': 'UNKNOWN_EXECUTION', 'complete': False}
    result.update({'observations': observations, 'formal_admission': False,
        'physical_database_requests': sum(o['physical_database_requests'] for o in observations),
        'new_physical_requests': sum(o['new_physical_requests'] for o in observations),
        'sql_wall_seconds': sum(o['sql_wall_seconds'] for o in observations),
        'response_bytes_all_attempts': sum(o['response_bytes_all_attempts'] for o in observations),
        'native_plan_path_seconds': time.monotonic()-started,
        'model_calls': 0, 'monetary_cost': None})
    return result
