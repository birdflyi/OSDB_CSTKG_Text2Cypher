from __future__ import annotations

import threading
import time

from .config import digest
from .transport import classify


class MemoryLedger:
    """Only for isolated dry runs/tests; never represents actual approval."""

    def __init__(self, cap=100000):
        self.cap, self.records, self.claimed, self.terminals = cap, {}, set(), {}
        self.lock = threading.RLock()

    def reserve(self, rid, key, attempt, wire, path, run_dir, journal, *, scope=None):
        with self.lock:
            if rid in self.records:
                old = self.records[rid]
                if old["request_sha256"] != digest(wire):
                    from .ledger import BudgetError

                    raise BudgetError("Body mismatch")
                return old
            if len(self.records) >= self.cap:
                from .ledger import BudgetExhausted

                raise BudgetExhausted("Simulated allowance exhausted")
            value = dict(
                slot=len(self.records) + 1, request_sha256=digest(wire), run_dir=str(run_dir)
            )
            self.records[rid] = value
            return value

    def claim(self, rid):
        with self.lock:
            if rid in self.claimed:
                raise FileExistsError("Already claimed")
            self.claimed.add(rid)

    def finish(self, rid, terminal):
        with self.lock:
            assert rid not in self.terminals
            self.terminals[rid] = dict(terminal, simulated=True)

    def summary(self):
        return dict(
            legacy_charged=0,
            new_charged=len(self.records),
            total_charged=len(self.records),
            cap=self.cap,
            pending=sorted(set(self.records) - set(self.terminals)),
            simulated=True,
        )

    def run_status(self, run_dir):
        records = {rid: v for rid, v in self.records.items() if v["run_dir"] == str(run_dir)}
        pending = sorted(set(records) - set(self.terminals))
        return dict(reserved=len(records), terminal=len(records) - len(pending), pending=pending)


class FakeTransport:
    """Injectable synthetic latency/result. Never loads keys or connects."""

    def __init__(self, delay=0.02, result=None):
        self.delay, self.result = delay, result
        self.calls = self.active = self.peak = self.connections = 0
        self.lock = threading.Lock()

    def prepare(self, endpoint):
        return FakePrepared(self, endpoint)

    def close(self):
        pass


class FakePrepared:
    def __init__(self, transport, endpoint):
        self.transport, self.endpoint = transport, endpoint

    def send(self, wire, timeout):
        transport = self.transport
        with transport.lock:
            transport.calls += 1
            call = transport.calls
            transport.active += 1
            transport.peak = max(transport.peak, transport.active)
        try:
            time.sleep(transport.delay)
            if transport.result:
                return dict(transport.result(call, wire, self.endpoint), simulated=True)
            result = classify(
                dict(
                    http_status=200,
                    write_started=True,
                    request_write_completed=True,
                    body=dict(
                        model=self.endpoint.model,
                        choices=[dict(message=dict(content="RETURN 1"), finish_reason="stop")],
                        usage=dict(prompt_tokens=10, completion_tokens=3, total_tokens=13),
                    ),
                ),
                self.endpoint.model,
            )
            return dict(result, simulated=True)
        finally:
            with transport.lock:
                transport.active -= 1

    def close(self):
        pass
