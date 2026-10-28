from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from pathlib import Path

from .config import Endpoint, endpoints_from_file


class AdmissionTimeout(Exception):
    """No model request was sent or charged."""


@dataclass
class EndpointState:
    spec: Endpoint
    inflight: int = 0
    failures: int = 0
    quarantined_until: float = 0
    cooldown_until: float = 0
    removed: bool = False
    degraded: bool = False
    recovery_successes: int = 0
    recovery_generation: int = 0


class Permit:
    def __init__(self, pool: DynamicPool, state: EndpointState):
        self.pool, self.state = pool, state
        self.endpoint = state.spec  # Immutable routing snapshot for this attempt.
        self.released = False
        self.timeout_multiplier = 2 if state.degraded else 1
        self.recovery_generation = state.recovery_generation

    async def release(self):
        async with self.pool.condition:
            if self.released:
                raise RuntimeError("Model permit released twice")
            self.released = True
            self.state.inflight -= 1
            self.pool.inflight -= 1
            self.pool.condition.notify_all()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *unused):
        await self.release()


class DynamicPool:
    """One authority for global and endpoint capacity; waiting uses no worker.

    Removing/downscaling endpoints affects future admissions only. The existing
    permit object always returns its slot to the original endpoint generation.
    """

    def __init__(
        self,
        global_cap: int,
        *,
        clock=time.monotonic,
    ):
        if global_cap < 1:
            raise ValueError("Positive global cap required")
        self.global_cap = global_cap
        self.clock = clock
        self.condition = asyncio.Condition()
        self.states = {}
        self.inflight = self.peak = self.waiting = 0
        self.issues = []
        self.closed = False
        self._tickets = []

    async def update(self, endpoints: list, *, global_cap=None):
        if global_cap is not None and global_cap < 1:
            raise ValueError("Positive global cap required")
        if len({e.name for e in endpoints}) != len(endpoints):
            raise ValueError("Duplicate endpoint names")
        async with self.condition:
            if global_cap is not None:
                self.global_cap = global_cap
            identities = {e.identity for e in endpoints}
            for key, state in self.states.items():
                state.removed = key not in identities
            for endpoint in endpoints:
                state = self.states.get(endpoint.identity)
                if state is None:
                    self.states[endpoint.identity] = EndpointState(endpoint)
                else:
                    state.spec, state.removed = endpoint, False
            # Keep removed generations until their in-flight permits drain.
            self.states = {k: s for k, s in self.states.items() if not s.removed or s.inflight}
            self.condition.notify_all()

    def _eligible(self, model: str):
        now = self.clock()
        return [
            s
            for s in self.states.values()
            if s.spec.model == model
            and s.spec.enabled
            and not s.removed
            and self.capacity(s) > s.inflight
            and s.quarantined_until <= now
            and s.cooldown_until <= now
        ]

    async def acquire(self, model: str, timeout: float) -> Permit:
        ticket = object()
        deadline = self.clock() + timeout
        async with self.condition:
            self._tickets.append((ticket, model))
            self.waiting += 1
            try:
                while True:
                    if self.closed:
                        raise AdmissionTimeout("Pool drained; no new admissions")
                    eligible = self._eligible(model)
                    # FIFO within a model; absent modelA does not block modelB.
                    first = next((t for t, m in self._tickets if m == model), None)
                    if ticket is first and self.inflight < self.global_cap and eligible:
                        state = min(
                            eligible,
                            key=lambda s: ((s.inflight + 1) / self.capacity(s), s.spec.name),
                        )
                        state.inflight += 1
                        self.inflight += 1
                        self.peak = max(self.peak, self.inflight)
                        return Permit(self, state)
                    remaining = deadline - self.clock()
                    if remaining <= 0:
                        raise AdmissionTimeout("No matching model capacity before deadline")
                    # Timed wake only for cooldown/quarantine expiry, not polling.
                    wakeups = [
                        max(s.quarantined_until, s.cooldown_until) - self.clock()
                        for s in self.states.values()
                        if s.spec.model == model
                        and max(s.quarantined_until, s.cooldown_until) > self.clock()
                    ]
                    wait_for = min([remaining] + wakeups)
                    try:
                        await asyncio.wait_for(self.condition.wait(), wait_for)
                    except asyncio.TimeoutError:
                        pass
            finally:
                self._tickets = [(t, m) for t, m in self._tickets if t is not ticket]
                self.waiting -= 1
                self.condition.notify_all()

    async def record(self, permit: Permit, status: str, *, retry_after=0):
        async with self.condition:
            state = permit.state
            transition = None
            if status == "model_mismatch":
                state.quarantined_until = float("inf")
                state.recovery_successes = 0
                state.recovery_generation += 1
            elif status in ("infra", "unknown", "throttled"):
                state.recovery_successes = 0
                if not state.degraded:
                    state.degraded = True
                    state.recovery_generation += 1
                    transition = "degraded"
                if status == "throttled":
                    state.cooldown_until = self.clock() + max(0, retry_after)
                else:
                    state.failures += 1
            elif status in ("ok", "length", "empty", "refused"):
                state.failures = 0
                # Completions admitted before degradation do not certify the
                # reduced-capacity / extended-timeout recovery policy.
                if (
                    state.degraded
                    and permit.timeout_multiplier == 2
                    and permit.recovery_generation == state.recovery_generation
                ):
                    state.recovery_successes += 1
                    if state.recovery_successes == 3:
                        state.degraded = False
                        state.recovery_successes = 0
                        state.recovery_generation += 1
                        transition = "restored"
            elif state.degraded:
                state.recovery_successes = 0
            self.condition.notify_all()
            if transition:
                return dict(
                    endpoint=state.spec.name,
                    model=state.spec.model,
                    transition=transition,
                    configured_cap=state.spec.cap,
                    effective_cap=self.capacity(state),
                    timeout_multiplier=2 if state.degraded else 1,
                    required_successes=3,
                    trigger_status=status,
                )

    def capacity(self, state):
        base = min(state.spec.cap, self.global_cap)
        return max(1, base // 2) if state.degraded and base else base

    async def drain(self):
        async with self.condition:
            self.closed = True
            self.condition.notify_all()

    def snapshot(self) -> dict:
        return dict(
            global_cap=self.global_cap,
            inflight=self.inflight,
            peak=self.peak,
            waiting=self.waiting,
            endpoints=[
                dict(
                    name=s.spec.name,
                    model=s.spec.model,
                    cap=s.spec.cap,
                    effective_cap=self.capacity(s),
                    degraded=s.degraded,
                    recovery_successes=s.recovery_successes,
                    timeout_multiplier=2 if s.degraded else 1,
                    enabled=s.spec.enabled,
                    removed=s.removed,
                    inflight=s.inflight,
                    consecutive_failures=s.failures,
                    quarantined=s.quarantined_until > self.clock(),
                    quarantine_seconds_remaining=(
                        None
                        if s.quarantined_until == float("inf")
                        else max(0, s.quarantined_until - self.clock())
                    ),
                    cooldown_seconds_remaining=max(0, s.cooldown_until - self.clock()),
                )
                for s in self.states.values()
            ],
        )

    async def watch(self, path: Path, stop: asyncio.Event, interval=10, on_reload=None):
        last = None
        while not stop.is_set():
            try:
                raw = Path(path).read_bytes()
                if raw != last:
                    endpoints = endpoints_from_file(path)
                    await self.update(endpoints)
                    if on_reload is not None:
                        on_reload(endpoints)
                    last = raw
            except (OSError, ValueError, TypeError, KeyError) as exc:
                issue = dict(kind="invalid_registry_keep_previous", error=type(exc).__name__)
                if not self.issues or self.issues[-1] != issue:
                    self.issues.append(issue)
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except asyncio.TimeoutError:
                pass
