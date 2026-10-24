"""Serial attempt accounting with immutable legacy slots and explicit approval.

This module performs no network requests. Historical request paths can be
translated to a verified migrated root without rewriting the original slots.
Never use it to restart a historical queue.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path


class BudgetBlocked(RuntimeError):
    pass


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class AttemptBudget:
    def __init__(self, ledger, approval, *, path_rewrites=()):
        self.ledger = Path(ledger)
        self.approval = Path(approval)
        self.path_rewrites = tuple(path_rewrites)

    def _request(self, stored):
        for source, destination in self.path_rewrites:
            prefix = str(source).rstrip('\\/')
            if stored.startswith(prefix + '\\') or stored.startswith(prefix + '/'):
                suffix = stored[len(prefix):].lstrip('\\/').replace('\\', '/')
                return Path(destination) / suffix
        return Path(stored)

    @staticmethod
    def terminal_path(request):
        return request.with_name('connectivity_probe_01.json' if
                                 request.name == 'connectivity_probe_01_request.json'
                                 else 'terminal.json')

    def inspect(self):
        approval = read_json(self.approval)
        limit = approval.get('approved_cumulative_cap')
        if type(limit) is not int or limit < 1:
            raise BudgetBlocked('Invalid explicit budget approval')
        floor = approval.get('historical_attempts_at_approval', 0)
        if type(floor) is not int or floor < 0:
            raise BudgetBlocked('Invalid historical attempt floor')
        slots, requests, pending = {}, set(), []
        for path in self.ledger.glob('*.json'):
            if not path.stem.isdigit():
                raise BudgetBlocked('Unexpected ledger filename')
            number = int(path.stem)
            if number < 1 or number in slots:
                raise BudgetBlocked('Duplicate or invalid slot number')
            record = read_json(path)
            request = self._request(record['request_path']).resolve()
            identity = os.path.normcase(str(request))
            if identity in requests:
                raise BudgetBlocked('A request was charged more than once')
            requests.add(identity)
            if not request.is_file():
                raise BudgetBlocked('Missing reserved request')
            if record.get('request_sha256') and digest(request) != record['request_sha256']:
                raise BudgetBlocked('Reserved request was modified')
            terminal = self.terminal_path(request)
            if terminal.exists():
                result = read_json(terminal)
                if not isinstance(result, dict) or not any(
                    k in result for k in ('http_status', 'transport_error')
                ):
                    raise BudgetBlocked('Malformed terminal; audit before continuing')
            else:
                pending.append(number)
            slots[number] = identity
        count = len(slots)
        if set(slots) != set(range(1, count + 1)) or count < floor:
            raise BudgetBlocked('Ledger history is missing or noncontiguous')
        if count > limit:
            raise BudgetBlocked('Ledger exceeds approved cap')
        return {'reserved': count, 'cap': limit, 'remaining': limit - count,
                'pending': sorted(pending), 'request_identities': requests}

    @contextmanager
    def _lock(self):
        self.ledger.mkdir(parents=True, exist_ok=True)
        lock = self.ledger / '.t1-reservation.lock'
        try:
            descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise BudgetBlocked('Budget lock exists; inspect owner, never auto-clear') from exc
        try:
            with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
                json.dump({'pid': os.getpid()}, stream)
            yield
        finally:
            lock.unlink()

    def reserve(self, request, *, purpose):
        """Reserve once immediately before one physical serial request.

        The request JSON must already exist. Persist its terminal even for a
        timeout/error. A pending reservation blocks dispatch; do not resend it.
        """
        request = Path(request).resolve()
        if not request.is_file() or not purpose.strip():
            raise BudgetBlocked('Existing request and declared purpose required')
        if self.terminal_path(request).exists():
            raise BudgetBlocked('Request already has terminal evidence')
        with self._lock():
            snapshot = self.inspect()
            if snapshot['pending']:
                raise BudgetBlocked('Pending physical attempt; audit before dispatch')
            if os.path.normcase(str(request)) in snapshot['request_identities']:
                raise BudgetBlocked('Request already reserved')
            if not snapshot['remaining']:
                raise BudgetBlocked('Approved cumulative budget exhausted')
            number = snapshot['reserved'] + 1
            path = self.ledger / f'{number:04d}.json'
            record = {'request_path': str(request), 'pid': os.getpid(),
                      'request_sha256': digest(request), 'purpose': purpose,
                      'approval_sha256': digest(self.approval),
                      'approved_cap_at_reservation': snapshot['cap']}
            with path.open('x', encoding='utf-8') as stream:
                json.dump(record, stream, ensure_ascii=False, indent=2)
            return number
