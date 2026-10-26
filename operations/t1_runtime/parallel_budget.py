"""Exclusive campaign lease with idempotent per-attempt local reservation.

Legacy source remains unchanged. One inspect on entry/exit, O(1) reservations
while holding the existing exclusive lock. Unknown terminals still block reuse.
"""
import hashlib
import json
import os
import socket
import threading
import uuid
from pathlib import Path

from common import immutable


def digest(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class ParallelLease:
    def __init__(self, legacy, directory, max_calls):
        self.legacy = legacy; self.directory = Path(directory)
        self.max_calls = max_calls; self.mutex = threading.RLock()
        self.reserved = {}; self.terminals = {}; self.closed = False

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=False)
        self.legacy.ledger.mkdir(parents=True,exist_ok=True)
        self.lock_path=self.legacy.ledger/'.t1-reservation.lock'
        self.lock_wire=json.dumps({'pid':os.getpid(),'hostname':socket.gethostname(),
                                   'run_id':uuid.uuid4().hex,'scope':'owned_parallel_capacity_lease'}).encode()
        descriptor=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        with os.fdopen(descriptor,'wb') as stream:stream.write(self.lock_wire)
        try:
            snapshot = self.legacy.inspect()
            if snapshot['pending']: raise RuntimeError('Unterminated historical request; audit required')
            if self.max_calls > snapshot['remaining']: raise RuntimeError('Insufficient approved capacity')
            self.before = {k:snapshot[k] for k in ('reserved','cap','remaining','pending')}
            self.base = snapshot['reserved']; self.approval_sha = digest(self.legacy.approval)
            self.owner = {'run_id': uuid.uuid4().hex, 'pid':os.getpid(), 'hostname':socket.gethostname(),
                          'max_physical_calls': self.max_calls, 'budget_before':self.before}
            immutable(self.directory/'lease.json', self.owner)
            return self
        except BaseException:
            self.release_owned_lock()
            raise

    def release_owned_lock(self):
        if self.lock_path.read_bytes()!=self.lock_wire:
            raise RuntimeError('Lock ownership changed; preserve and audit, never remove foreign lock')
        self.lock_path.unlink()

    def reserve(self, trial_id, body):
        with self.mutex:
            if self.closed: raise RuntimeError('Lease closed')
            wire = json.dumps(body,ensure_ascii=False,separators=(',',':')).encode('utf-8')
            fingerprint = hashlib.sha256(wire).hexdigest()
            if trial_id in self.reserved:
                old = self.reserved[trial_id]
                if old['request_sha256'] != fingerprint: raise RuntimeError('Idempotency conflict')
                return old
            if len(self.reserved) >= self.max_calls: raise RuntimeError('Finite campaign allowance exhausted')
            path = self.directory/'attempts'/trial_id/'request.json'
            path.parent.mkdir(parents=True,exist_ok=False); path.write_bytes(wire)
            number = self.base+len(self.reserved)+1
            record = {'request_path':str(path.resolve()), 'pid':os.getpid(),
                      'request_sha256':fingerprint, 'purpose':'CypherBench TRAIN capacity development '+trial_id,
                      'approval_sha256':self.approval_sha,
                      'approved_cap_at_reservation':self.before['cap']}
            immutable(self.legacy.ledger/f'{number:04d}.json', record)
            ack = {'slot':number,'request_sha256':fingerprint}
            self.reserved[trial_id] = ack
            return ack

    def terminal(self, trial_id, terminal):
        with self.mutex:
            if trial_id not in self.reserved: raise RuntimeError('Terminal without reservation')
            if terminal['budget_slot'] != self.reserved[trial_id]['slot']: raise RuntimeError('Wrong slot')
            fingerprint = hashlib.sha256(json.dumps(terminal,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
            if trial_id in self.terminals:
                if fingerprint != self.terminals[trial_id]: raise RuntimeError('Terminal conflict')
                return {'accepted':True,'duplicate':True}
            immutable(self.directory/'attempts'/trial_id/'terminal.json',terminal)
            self.terminals[trial_id] = fingerprint
            return {'accepted':True,'duplicate':False}

    def status(self):
        with self.mutex:
            return {'reserved':self.base+len(self.reserved),'remaining':self.before['cap']-self.base-len(self.reserved),
                    'cap':self.before['cap'],'campaign_reserved':len(self.reserved),
                    'pending':len(self.reserved)-len(self.terminals)}

    def __exit__(self, *exc):
        with self.mutex:
            self.closed = True
            try:
                after = self.legacy.inspect()
                immutable(self.directory/'lease_close.json',
                    {k:after[k] for k in ('reserved','cap','remaining','pending')})
            finally:
                self.release_owned_lock()
