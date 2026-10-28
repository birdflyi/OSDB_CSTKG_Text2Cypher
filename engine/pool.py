from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import random
import time
from pathlib import Path

from .config import Endpoint, EndpointPolicy, registry_from_file


class AdmissionTimeout(Exception):
    """No model request was sent or charged."""


class RouteUnavailable(AdmissionTimeout):
    """Every matching route has a known configuration/authentication failure."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


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
    degraded_until: float = 0
    quarantine_reason: str = None


class Permit:
    def __init__(self, pool: DynamicPool, state: EndpointState):
        self.pool, self.state = pool, state
        self.endpoint = state.spec  # Immutable routing snapshot for this attempt.
        self.released = False
        self.timeout_multiplier = 2 if state.degraded else 1
        self.recovery_generation = state.recovery_generation

    async def release(self):
        # Retain and join the release task even if its owner is cancelled while
        # waiting for the condition lock. Never leave a charged POST's slot lost.
        task = asyncio.create_task(self._release())
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _release(self):
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
        ceiling=None,
        policy=None,
    ):
        if type(global_cap) is not int or global_cap < 1:
            raise ValueError("Positive global cap required")
        if ceiling is not None and (type(ceiling) is not int or ceiling < global_cap):
            raise ValueError("Capacity ceiling must cover the initial global cap")
        self.ceiling = ceiling
        self.policy = (
            policy
            if isinstance(policy, EndpointPolicy)
            else EndpointPolicy(**({} if policy is None else policy))
        )
        self.global_cap = global_cap
        self.clock = clock
        self.condition = asyncio.Condition()
        self.states = {}
        self.inflight = self.peak = self.waiting = 0
        self.issues = []
        self.transitions = []
        self.closed = False
        self._tickets = []

    async def update(self, endpoints: list, *, global_cap=None):
        if global_cap is not None and (
            type(global_cap) is not int
            or global_cap < 1
            or (self.ceiling is not None and global_cap > self.ceiling)
        ):
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
            self.states = {
                k: s for k, s in self.states.items() if not s.removed or s.inflight
            }
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
                    matching = [
                        s
                        for s in self.states.values()
                        if s.spec.model == model and s.spec.enabled and not s.removed
                    ]
                    if matching and all(
                        s.quarantine_reason == "auth_error"
                        and s.quarantined_until > self.clock()
                        for s in matching
                    ):
                        raise RouteUnavailable("auth_error")
                    # FIFO within a model; absent modelA does not block modelB.
                    first = next((t for t, m in self._tickets if m == model), None)
                    if ticket is first and self.inflight < self.global_cap and eligible:
                        state = min(
                            eligible,
                            key=lambda s: (
                                (s.inflight + 1) / self.capacity(s),
                                s.spec.name,
                            ),
                        )
                        state.inflight += 1
                        self.inflight += 1
                        self.peak = max(self.peak, self.inflight)
                        return Permit(self, state)
                    remaining = deadline - self.clock()
                    if remaining <= 0:
                        raise AdmissionTimeout(
                            "No matching model capacity before deadline"
                        )
                    # Timed wake only for cooldown/quarantine expiry, not polling.
                    wakeups = [
                        max(s.quarantined_until, s.cooldown_until) - self.clock()
                        for s in self.states.values()
                        if s.spec.model == model
                        and max(s.quarantined_until, s.cooldown_until) > self.clock()
                    ]
                    wakeups.extend(
                        s.degraded_until - self.clock()
                        for s in self.states.values()
                        if s.spec.model == model
                        and s.degraded
                        and s.degraded_until > self.clock()
                    )
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
            if status in ("model_mismatch", "auth_error"):
                state.quarantined_until = self.clock() + self.policy.quarantine_seconds
                state.quarantine_reason = status
                state.recovery_successes = 0
                state.recovery_generation += 1
                transition = "quarantined"
            elif status == "throttled":
                delay = max(0, retry_after)
                state.cooldown_until = self.clock() + min(
                    300, delay + random.uniform(0, min(1, delay * 0.1))
                )
            elif status in ("infra", "unknown"):
                state.recovery_successes = 0
                state.failures += 1
                if (
                    not state.degraded
                    and state.failures >= self.policy.failures_to_degrade
                ):
                    state.degraded = True
                    state.degraded_until = (
                        self.clock() + self.policy.degraded_ttl_seconds
                    )
                    state.recovery_generation += 1
                    transition = "degraded"
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
                    if state.recovery_successes == self.policy.successes_to_restore:
                        state.degraded = False
                        state.recovery_successes = 0
                        state.recovery_generation += 1
                        transition = "restored"
            elif state.degraded:
                state.recovery_successes = 0
            self.condition.notify_all()
            if transition:
                event = dict(
                    endpoint=state.spec.name,
                    model=state.spec.model,
                    transition=transition,
                    configured_cap=state.spec.cap,
                    effective_cap=(
                        0
                        if state.quarantined_until > self.clock()
                        else self.capacity(state)
                    ),
                    timeout_multiplier=2 if state.degraded else 1,
                    required_successes=self.policy.successes_to_restore,
                    trigger_status=status,
                )
                self.transitions.append(event)
                return event

    def capacity(self, state):
        if state.degraded and state.degraded_until <= self.clock():
            state.degraded = False
            state.failures = state.recovery_successes = 0
            state.recovery_generation += 1
            self.transitions.append(
                dict(
                    endpoint=state.spec.name,
                    transition="restored_by_ttl",
                    certified_by_successes=False,
                )
            )
        base = min(state.spec.cap, self.global_cap)
        return max(1, base // 2) if state.degraded and base else base

    async def reset(self, name):
        """Explicit operator reset; it never sends a model probe or changes caps."""
        async with self.condition:
            matches = [
                s for s in self.states.values() if s.spec.name == name and not s.removed
            ]
            if not matches:
                raise ValueError("Unknown current endpoint: " + name)
            for state in matches:
                state.degraded = False
                state.quarantined_until = state.cooldown_until = 0
                state.quarantine_reason = None
                state.failures = state.recovery_successes = 0
                state.recovery_generation += 1
            self.transitions.append(dict(endpoint=name, transition="operator_reset"))
            self.condition.notify_all()

    async def drain(self):
        async with self.condition:
            self.closed = True
            self.condition.notify_all()

    def snapshot(self) -> dict:
        return dict(
            global_cap=self.global_cap,
            global_cap_ceiling=self.ceiling,
            enabled_capacity_sum=sum(
                s.spec.cap
                for s in self.states.values()
                if s.spec.enabled and not s.removed
            ),
            admission_peak_scope="pool_lifetime",
            endpoint_policy=asdict(self.policy),
            recent_transitions=self.transitions[-20:],
            inflight=self.inflight,
            peak=self.peak,
            waiting=self.waiting,
            endpoints=[
                dict(
                    name=s.spec.name,
                    model=s.spec.model,
                    cap=s.spec.cap,
                    effective_cap=(
                        self.capacity(s)
                        if s.spec.enabled
                        and not s.removed
                        and s.quarantined_until <= self.clock()
                        else 0
                    ),
                    admissible=(
                        s in self._eligible(s.spec.model)
                        and self.inflight < self.global_cap
                    ),
                    degraded=s.degraded,
                    recovery_successes=s.recovery_successes,
                    timeout_multiplier=2 if s.degraded else 1,
                    enabled=s.spec.enabled,
                    removed=s.removed,
                    inflight=s.inflight,
                    consecutive_failures=s.failures,
                    quarantined=s.quarantined_until > self.clock(),
                    quarantine_reason=(
                        s.quarantine_reason
                        if s.quarantined_until > self.clock()
                        else None
                    ),
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

    async def watch(
        self,
        path: Path,
        stop: asyncio.Event,
        interval=10,
        on_reload=None,
        validate=None,
    ):
        last = None
        while not stop.is_set():
            try:
                raw = Path(path).read_bytes()
                if raw != last:
                    registry = registry_from_file(path)
                    endpoints = registry["endpoints"]
                    cap = registry["global_cap"]
                    if (
                        cap is not None
                        and self.ceiling is not None
                        and cap > self.ceiling
                    ):
                        raise ValueError("Registry global_cap exceeds admitted ceiling")
                    if validate is not None:
                        validate(endpoints)
                    if on_reload is not None:
                        on_reload(endpoints)
                    await self.update(endpoints, global_cap=cap)
                    interval = registry["reload_seconds"]
                    last = raw
            except (OSError, ValueError, TypeError, KeyError) as exc:
                issue = dict(
                    kind="invalid_registry_keep_previous", error=type(exc).__name__
                )
                if not self.issues or self.issues[-1] != issue:
                    self.issues.append(issue)
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except asyncio.TimeoutError:
                pass
