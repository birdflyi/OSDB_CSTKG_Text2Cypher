"""Offline-tested cutover building blocks. NOT an admitted experiment runner.

No network or model calls. A persistent execution lease is intentionally NOT
removed on process exit. Unknown ownership/pending work requires an audit.
The approved cap covers sealed history plus this fragment, never just new work.
"""
import hashlib
import json
import os
from pathlib import Path
import socket


class LeaseBlocked(RuntimeError):
    pass


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def create_json(path, value):
    """Exclusive, durable create. A partial file after a crash blocks reuse."""
    path = Path(path)
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def history_seal(original_budget, destination, *, fence):
    """Must be called only after the local execution fence has been acquired.

    Seals ledger/request/terminal bytes, not credentials. It is a snapshot and
    cannot itself stop another writer. Actual cutover must pin its SHA externally.
    """
    fence.assert_owned()
    if fence.path.resolve() != (original_budget.ledger / '.t1-reservation.lock').resolve():
        raise LeaseBlocked('History must be fenced against original reservations')
    before = original_budget.inspect()
    if before['pending']:
        raise LeaseBlocked('Historical pending request; do not cut over')
    entries = []
    paths = {int(p.stem): p for p in original_budget.ledger.glob('*.json')}
    for path in (paths[n] for n in sorted(paths)):
        request = original_budget._request(read(path)['request_path']).resolve()
        entries.append([int(path.stem), sha(path), sha(request),
                        sha(original_budget.terminal_path(request))])
    after = original_budget.inspect()
    if before != after or len(entries) != before['reserved']:
        raise LeaseBlocked('History changed during sealing')
    # Recheck all three source bytes to detect a same-count modification.
    for number, ledger_sha, request_sha, terminal_sha in entries:
        path = paths[number]
        request = original_budget._request(read(path)['request_path']).resolve()
        if [sha(path), sha(request), sha(original_budget.terminal_path(request))] != [
                ledger_sha, request_sha, terminal_sha]:
            raise LeaseBlocked('Historical evidence changed during sealing')
    fence.assert_owned()
    value = {'kind': 'SEALED_CUMULATIVE_ATTEMPT_BASELINE_V1',
             'reserved': before['reserved'], 'cap': before['cap'],
             'approval_sha256': sha(original_budget.approval), 'pending': [],
             'entries': entries}
    create_json(destination, value)
    return sha(destination)


class ExecutionLease:
    """One process owns the whole execution, beyond per-reservation locks.

    Use one instance for the Windows budget fence (.t1-reservation.lock) and a
    separate linked instance for the remote driver. Cross-host acknowledgement,
    owner admission and final mirror reconciliation remain external gates.
    """
    def __init__(self, path, lease_id, authority):
        self.path = Path(path)
        self.record = {'kind': 'T1_PERSISTENT_EXECUTION_LEASE_V1',
                       'lease_id': lease_id, 'authority': authority,
                       'pid': os.getpid(), 'hostname': socket.gethostname()}
        self.expected = None

    def acquire(self):
        if not self.record['lease_id'] or not self.record['authority']:
            raise LeaseBlocked('Explicit lease identity and authority required')
        try:
            create_json(self.path, self.record)
        except FileExistsError as exc:
            raise LeaseBlocked('Execution lease exists; never auto-clear') from exc
        self.expected = sha(self.path)
        self.assert_owned()
        return self

    def assert_owned(self):
        if (self.expected is None or not self.path.is_file() or
                sha(self.path) != self.expected or read(self.path) != self.record or
                os.getpid() != self.record['pid'] or
                socket.gethostname() != self.record['hostname']):
            raise LeaseBlocked('Execution owner absent, changed or unknown')

    def release(self, *, evidence_path, expected_evidence_sha):
        """Only an independently hash-pinned completed reconciliation releases it."""
        self.assert_owned()
        if sha(evidence_path) != expected_evidence_sha:
            raise LeaseBlocked('Unpinned release evidence')
        proof = read(evidence_path)
        if (proof.get('lease_id') != self.record['lease_id'] or
                proof.get('remote_execution_terminal') is not True or
                proof.get('pending') != [] or proof.get('all_attempts_mirrored') is not True):
            raise LeaseBlocked('Unknown execution or incomplete reconciliation')
        # This is deliberately not an automatic __exit__/finally cleanup.
        self.assert_owned()
        self.path.unlink()
        self.expected = None


class LeasedAttemptFragment:
    """AttemptBudget-compatible counter for NEW attempts above sealed history.

    Requests must be new files under a dedicated root. No terminal means pending,
    even if the owning process has died. Any malformed/modified file blocks use.
    This class does not mirror results, dispatch HTTP, or approve a remote run.
    """
    def __init__(self, ledger, approval, *, baseline, baseline_sha, lease, requests_root):
        self.ledger, self.approval = Path(ledger), Path(approval)
        self.baseline, self.baseline_sha = Path(baseline), baseline_sha
        self.lease, self.requests_root = lease, Path(requests_root).resolve()

    @staticmethod
    def terminal_path(request):
        return Path(request).with_name('terminal.json')

    def inspect(self):
        self.lease.assert_owned()
        if sha(self.baseline) != self.baseline_sha:
            raise LeaseBlocked('Baseline changed')
        base, approval = read(self.baseline), read(self.approval)
        count, cap = base['reserved'], approval.get('approved_cumulative_cap')
        if (type(count) is not int or type(cap) is not int or count < 0 or cap < count or
                count < approval.get('historical_attempts_at_approval', 0) or
                base.get('kind') != 'SEALED_CUMULATIVE_ATTEMPT_BASELINE_V1' or
                base.get('pending') != [] or base.get('cap') != cap or
                base.get('approval_sha256') != sha(self.approval) or
                [e[0] for e in base['entries']] != list(range(1, count + 1))):
            raise LeaseBlocked('Invalid cumulative history or approval')
        slots, identities, pending = [], set(), []
        for path in self.ledger.glob('*.json'):
            if not path.stem.isdigit():
                raise LeaseBlocked('Unexpected fragment slot')
            number, record = int(path.stem), read(path)
            request = Path(record['request_path']).resolve()
            if (self.requests_root not in request.parents or not request.is_file() or
                    record.get('lease_id') != self.lease.record['lease_id'] or
                    record.get('baseline_sha256') != self.baseline_sha or
                    record.get('approval_sha256') != sha(self.approval) or
                    record.get('approved_cap_at_reservation') != cap or
                    sha(request) != record.get('request_sha256')):
                raise LeaseBlocked('Modified, foreign or missing attempt')
            identity = os.path.normcase(str(request))
            if identity in identities or number in slots:
                raise LeaseBlocked('Duplicate physical attempt')
            identities.add(identity)
            terminal = self.terminal_path(request)
            if terminal.exists():
                result = read(terminal)
                if not isinstance(result, dict) or not any(
                        k in result for k in ('http_status', 'transport_error')):
                    raise LeaseBlocked('Malformed terminal; audit before continuing')
            else:
                pending.append(number)
            slots.append(number)
        total = count + len(slots)
        if sorted(slots) != list(range(count + 1, total + 1)) or total > cap:
            raise LeaseBlocked('Reset, gap or approved cap exceeded')
        return {'reserved': total, 'cap': cap, 'remaining': cap - total,
                'pending': sorted(pending), 'request_identities': identities}

    def reserve(self, request, *, purpose):
        self.lease.assert_owned()
        request = Path(request).resolve()
        if (self.requests_root not in request.parents or not request.is_file() or
                not purpose.strip() or self.terminal_path(request).exists()):
            raise LeaseBlocked('Fresh in-root request and purpose required')
        self.ledger.mkdir(parents=True, exist_ok=True)
        lock = self.ledger / '.t1-reservation.lock'
        try:
            create_json(lock, self.lease.record)
        except FileExistsError as exc:
            raise LeaseBlocked('Reservation lock exists; audit owner') from exc
        expected_lock = sha(lock)
        try:
            state = self.inspect()
            if (state['pending'] or not state['remaining'] or
                    os.path.normcase(str(request)) in state['request_identities']):
                raise LeaseBlocked('Pending, duplicate or exhausted attempt')
            number = state['reserved'] + 1
            create_json(self.ledger / f'{number:04d}.json', {
                'request_path': str(request), 'request_sha256': sha(request),
                'purpose': purpose, 'pid': os.getpid(),
                'lease_id': self.lease.record['lease_id'],
                'baseline_sha256': self.baseline_sha,
                'approval_sha256': sha(self.approval),
                'approved_cap_at_reservation': state['cap']})
            return number
        finally:
            if lock.is_file() and sha(lock) == expected_lock:
                lock.unlink()  # Own temporary reservation lock only.
