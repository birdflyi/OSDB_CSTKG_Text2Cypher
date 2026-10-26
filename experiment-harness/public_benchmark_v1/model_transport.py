"""Explicit, serial, UTF-8 model transport; every physical attempt is reserved.

Credentials remain in the supplied private file and are never written to traces.
There is no model SDK, environment proxy, hidden retry or correctness-aware retry.
"""
import http.client
import json
from copy import deepcopy
from pathlib import Path
import time
from urllib.parse import urlsplit

from attempt_budget import BudgetBlocked
from model_request_profile import build_model_payload


class BatchStopped(RuntimeError):
    pass


def immutable_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)


def http_send(config, wire):
    url = urlsplit(config['endpoint'])
    if url.scheme not in ('http', 'https') or url.username or url.password:
        raise ValueError('Expected HTTP endpoint with separate private credentials')
    cls = http.client.HTTPSConnection if url.scheme == 'https' else http.client.HTTPConnection
    connection = cls(url.hostname, url.port, timeout=10)
    try:
        path = url.path + ('?' + url.query if url.query else '')
        connection.request('POST', path, wire, {
            'Authorization': 'Bearer ' + config['api_key'],
            'Content-Type': 'application/json; charset=utf-8',
        })
        if connection.sock:
            connection.sock.settimeout(config.get('read_timeout_seconds', 120))
        response = connection.getresponse()
        raw = response.read()
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeError):
            body = {'unparsed_body': raw.decode('utf-8', errors='replace')}
        return {'http_status': response.status, 'body': body}
    finally:
        connection.close()


class ModelTransport:
    def __init__(self, config, budget, directory, *, max_physical_calls=12,
                 send=http_send, sleep=time.sleep):
        self.config = deepcopy(config)
        self.budget, self.directory = budget, Path(directory)
        self.max_physical_calls = max_physical_calls
        if type(max_physical_calls) is not int or max_physical_calls < 1:
            raise ValueError('Positive physical call allowance required')
        self.send, self.sleep = send, sleep
        self.max_tokens = config.get('max_tokens', 4096)
        self.read_timeout = config.get('read_timeout_seconds', 120)
        if (type(self.max_tokens) is not int or not 1 <= self.max_tokens <= 32768
            or type(self.read_timeout) is not int or not 1 <= self.read_timeout <= 600):
            raise ValueError('Invalid explicit token/timeout allowance')
        self.physical_calls = self.failures = 0
        self.stopped = False
        url = urlsplit(config['endpoint'])
        if (url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password
            or not isinstance(config.get('api_key'), str) or not config['api_key']):
            raise ValueError('Invalid private model configuration')
        generation_profile = build_model_payload(self.config, [], max_tokens=self.max_tokens)
        generation_profile.pop('messages')
        if self.directory.exists():
            raise BatchStopped('Existing batch must be audited, never implicitly restarted')
        self.directory.mkdir(parents=True)
        immutable_json(self.directory / 'transport_policy.json', {
            'max_physical_calls': max_physical_calls, 'max_attempts_per_request': 3,
            'consecutive_infrastructure_failure_stop': 3, 'retry_wait_seconds': 10,
            'environment_proxy': False, 'encoding': 'utf-8', 'timeout_seconds': [10, self.read_timeout],
            'max_completion_tokens': self.max_tokens,
            'wire_generation_profile': generation_profile,
            'transport_parallelism': 1,
            'model': config['model'], 'no_automatic_batch_restart': True})

    def stop(self, reason):
        self.stopped = True
        path = self.directory / 'batch_stop.json'
        if not path.exists():
            immutable_json(path, {'reason': reason, 'physical_calls': self.physical_calls,
                                  'consecutive_failures': self.failures})
        return BatchStopped(reason)

    def complete(self, messages, *, purpose):
        if self.stopped or self.failures >= 3:
            raise self.stop('Batch already stopped; do not restart automatically')
        payload = build_model_payload(self.config, messages, max_tokens=self.max_tokens)
        wire = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        attempts = []
        for retry in range(3):
            if self.physical_calls >= self.max_physical_calls:
                raise self.stop('Finite development call allowance reached')
            waited = 0.0
            if retry:
                started_wait = time.monotonic()
                self.sleep(10)
                waited = time.monotonic() - started_wait
            folder = self.directory / f'physical_{self.physical_calls + 1:04d}'
            folder.mkdir()
            request = folder / 'request.json'
            request.write_bytes(wire)
            slot = self.budget.reserve(request, purpose=purpose)
            self.physical_calls += 1
            started = time.monotonic()
            try:
                terminal = self.send(self.config, wire)
            except (OSError, http.client.HTTPException) as error:
                terminal = {'transport_error': type(error).__name__}
            except Exception as error:
                terminal = {'http_status': None, 'client_error': type(error).__name__}
            terminal.update(seconds=time.monotonic() - started,
                            retry_wait_seconds=waited, budget_slot=slot,
                            physical_attempt_in_request=retry + 1)
            immutable_json(folder / 'terminal.json', terminal)
            attempts.append(str(folder))
            status = terminal.get('http_status')
            failed = bool(terminal.get('transport_error')) or status == 429 or (
                isinstance(status, int) and status >= 500)
            self.failures = self.failures + 1 if failed else 0
            immutable_json(folder / 'batch_state.json', {
                'physical_calls': self.physical_calls,
                'consecutive_infrastructure_failures': self.failures,
                'stop': self.failures >= 3})
            if self.failures >= 3:
                raise self.stop('Three consecutive infrastructure failures; batch stopped')
            if failed:
                continue
            if status != 200:
                raise self.stop('Non-retryable HTTP/configuration failure')
            body = terminal.get('body', {})
            choices = body.get('choices') if isinstance(body, dict) else None
            message = choices[0].get('message', {}) if choices else {}
            content = message.get('content')
            usable = bool(choices and choices[0].get('finish_reason') == 'stop'
                          and isinstance(content, str) and content.strip())
            # A valid but wrong/unparsable/truncated model output is not a
            # transport retry. A separately budgeted feedback step may diagnose it.
            return {'content': content if isinstance(content, str) else '',
                    'usable': usable, 'finish_reason': choices[0].get('finish_reason') if choices else None,
                    'attempt_folders': attempts, 'usage': body.get('usage') if isinstance(body, dict) else None}
        raise self.stop('Bounded request attempts exhausted')
