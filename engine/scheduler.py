from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
import json
from pathlib import Path
import time

from .config import CampaignSpec, canonical, digest
from .ids import component, logical_key, request_id
from .ledger import BudgetError
from .errors import IntegrityError, PersistenceError
from .pool import AdmissionTimeout, DynamicPool
from .transport import endpoint_health, local_resource_error
from .dispatch import BatchPacer, write_queue
from .journal import write_new


@dataclass(frozen=True)
class Item:
    benchmark: str
    task_id: str
    method: str
    graph: str
    wire: bytes
    model: str
    phase: str = "initial"
    read_timeout_seconds: float = 240

    @property
    def key(self):
        return (
            component(self.model)
            + "__"
            + logical_key(self.benchmark, self.task_id, self.method, self.phase)
        )


class Scheduler:
    """Async admission + bounded blocking HTTP + separately bounded DB execution.

    Model permits never cover database work or retry waits. No graph/batch barrier
    is introduced here. Graph lifecycle and score semantics belong to adapters.
    """

    def __init__(
        self,
        spec: CampaignSpec,
        pool: DynamicPool,
        ledger,
        journal,
        transport,
        *,
        close_transport=True,
    ):
        self.spec, self.pool, self.ledger = spec, pool, ledger
        self.journal, self.transport = journal, transport
        self.close_transport = close_transport
        self.http_workers = ThreadPoolExecutor(
            max_workers=spec.global_cap, thread_name_prefix="engine-http"
        )
        self.db_workers = ThreadPoolExecutor(
            max_workers=spec.db_cap, thread_name_prefix="engine-db"
        )
        self.db_gate = asyncio.Semaphore(spec.db_cap)
        self.draining = asyncio.Event()
        self.stop_reason = None
        self.events, self.paths = [], []
        self.saved_replies = {}
        self.retry_sleep = asyncio.sleep
        self.pacer = BatchPacer(spec.batch_size, spec.batch_interval_seconds)

    async def drain(self, reason="operator_drain"):
        self.stop_reason = self.stop_reason or reason
        self.draining.set()
        await self.pool.drain()

    def _physical(self, item: Item, endpoint, attempt_no: int, timeout_multiplier=1) -> dict:
        """One worker owns one connection and at most one POST. No hidden retry."""
        rid = component(self.spec.name) + "__" + request_id(item.key, attempt_no)
        # Full IDs live in the ledger; bounded directory names also work under
        # Windows'260-character path limit, including atomic-write temp suffixes.
        folder = self.spec.run_dir / "attempts" / digest(rid.encode("utf-8"))[:32]
        started = time.time()
        prepared, value = None, None
        try:
            if not set(json.loads(item.wire)) <= set(endpoint.wire_params):
                return dict(
                    status="not_dispatched",
                    charged=False,
                    request_id=rid,
                    undispatched_reason="invalid_endpoint_capabilities",
                )
            try:
                prepared = self.transport.prepare(endpoint)
            except OSError as exc:
                if local_resource_error(exc) or isinstance(exc, FileNotFoundError):
                    return dict(
                        status="local_resource_error",
                        charged=False,
                        error=type(exc).__name__,
                        errno=getattr(exc, "errno", None),
                        winerror=getattr(exc, "winerror", None),
                        logical_key=item.key,
                        request_id=rid,
                    )
                value = dict(
                    status="infra",
                    http_status=None,
                    transport_error=type(exc).__name__,
                    write_started=False,
                    request_write_completed=False,
                )
            if self.draining.is_set():
                return dict(
                    status="not_dispatched",
                    charged=False,
                    request_id=rid,
                    undispatched_reason="stop",
                )
            reserve_started = time.monotonic()
            slot = self.ledger.reserve(
                rid,
                item.key,
                attempt_no,
                item.wire,
                folder / "request.json",
                self.spec.run_dir,
                self.journal,
                scope=self.spec.budget_scope,
            )
            self.ledger.claim(rid)
            reserve_seconds = time.monotonic() - reserve_started
            if prepared is not None:
                self.journal.append(
                    "send_started",
                    request_id=rid,
                    logical_key=item.key,
                    attempt_no=attempt_no,
                    endpoint=endpoint.name,
                )
                value = prepared.send(item.wire, item.read_timeout_seconds * timeout_multiplier)
            value.update(
                request_id=rid,
                logical_key=item.key,
                attempt_no=attempt_no,
                budget_slot=slot["slot"],
                charged=True,
                endpoint=endpoint.name,
                endpoint_url=endpoint.url,
                model=endpoint.model,
                benchmark=item.benchmark,
                task_id=item.task_id,
                method=item.method,
                graph=item.graph,
                phase=item.phase,
                started_epoch=started,
                ended_epoch=time.time(),
                reserve_and_claim_seconds=reserve_seconds,
                base_read_timeout_seconds=item.read_timeout_seconds,
                effective_read_timeout_seconds=item.read_timeout_seconds * timeout_multiplier,
                timeout_multiplier=timeout_multiplier,
            )
            self.ledger.finish(rid, value)
            return value
        except BudgetError as exc:
            return dict(
                status="not_dispatched",
                request_id=rid,
                charged=False,
                undispatched_reason="budget",
                error=type(exc).__name__,
            )
        finally:
            if prepared is not None:
                prepared.close()

    def _retryable(self, value):
        policy = self.spec.retry
        status = value["status"]
        if status == "unknown":
            return bool(
                policy.retry_unknown
                and value.get("charged") is True
                and not value.get("protocol_error")
                and (value.get("transport_error") or (value.get("http_status") or 0) >= 500)
            )
        return status in ("infra", "throttled") and not (
            policy.before_write_only and value.get("write_started")
        )

    async def _generate_once(self, item, attempt):
        loop = asyncio.get_running_loop()
        waiting = time.monotonic()
        deadline = waiting + self.spec.admission_timeout_seconds
        permit = None
        while True:
            try:
                permit = await self.pool.acquire(
                    item.model, min(60, max(0.001, deadline - time.monotonic()))
                )
                break
            except AdmissionTimeout:
                stopped = self.draining.is_set() or self.pool.closed
                if not stopped:
                    self.journal.append(
                        "queue_deferred",
                        logical_key=item.key,
                        reason="waiting_for_model_capacity",
                        seconds=time.monotonic() - waiting,
                        network_request_sent=False,
                    )
                    if time.monotonic() < deadline:
                        continue
                if self.spec.queue_wait_until_available and not stopped:
                    # Queue time is not a failed model attempt. Keep the real
                    # item queued through temporary capacity/route gaps.
                    deadline = time.monotonic() + self.spec.admission_timeout_seconds
                    continue
                break
        if permit is None:
            value = dict(
                status=(
                    "not_dispatched"
                    if self.draining.is_set() or self.pool.closed
                    else "admission_timeout"
                ),
                charged=False,
                undispatched_reason=(
                    "stop" if self.draining.is_set() or self.pool.closed else "capacity"
                ),
                logical_key=item.key,
                admission_wait_seconds=time.monotonic() - waiting,
            )
            self.events.append(value)
            return value
        admission_wait = time.monotonic() - waiting
        try:
            async with permit:
                if not await self.pacer.wait(self.draining):
                    return dict(
                        status="not_dispatched",
                        charged=False,
                        logical_key=item.key,
                        undispatched_reason="stop",
                    )
                work = loop.run_in_executor(
                    self.http_workers,
                    self._physical,
                    item,
                    permit.endpoint,
                    attempt,
                    permit.timeout_multiplier,
                )
                try:
                    value = await asyncio.shield(work)
                except asyncio.CancelledError:
                    # Python cannot cancel a running HTTP thread. Keep its
                    # permit until it terminates; stop admitting other work.
                    await self.drain("controller_cancelled_drain")
                    value = await work
                transition = await self.pool.record(
                    permit,
                    endpoint_health(value),
                    retry_after=value.get("retry_after_seconds", 0),
                )
                if transition:
                    await loop.run_in_executor(
                        self.http_workers,
                        lambda: self.journal.append("endpoint_recovery", **transition),
                    )
        except BaseException as exc:
            # The worker may already have sent bytes. Drain instead of blindly retrying.
            await self.drain("controller_or_persistence_error")
            value = dict(
                status="unknown",
                logical_key=item.key,
                error=type(exc).__name__,
                charged=None,
                generation_outcome_unknown=True,
            )
        value["admission_wait_seconds"] = admission_wait
        self.events.append(value)
        if value.get("undispatched_reason") == "budget":
            await self.drain("budget")
        # fsync is blocking IO. Keep it off the admission/event loop.
        await loop.run_in_executor(
            self.http_workers,
            lambda: self.journal.append(
                "attempt_terminal",
                logical_key=item.key,
                request_id=value.get("request_id"),
                status=value["status"],
                budget_slot=value.get("budget_slot"),
                endpoint=value.get("endpoint"),
            ),
        )
        return value

    async def generate(self, item: Item):
        saved = self.saved_replies.get(item.key)
        previous, count = None, 0
        if saved is not None:
            if digest(item.wire) != saved["wire_sha256"]:
                raise IntegrityError(
                    "Saved response belongs to different request bytes; never resend"
                )
            history = saved.get("history", [saved])
            for entry in history:
                previous = dict(
                    entry["response"],
                    reused=True,
                    source_terminal=entry["source_terminal"],
                    source_terminal_sha256=entry["source_terminal_sha256"],
                )
                self.events.append(previous)
            count = len(history)
            if not self._retryable(previous) or count >= self.spec.retry.max_attempts:
                return previous
        for attempt in range(count + 1, self.spec.retry.max_attempts + 1):
            if self.draining.is_set():
                return dict(
                    status="not_dispatched",
                    charged=False,
                    undispatched_reason="stop",
                )
            if previous is not None:
                # Includes elapsed time since sealed attempts in prior campaigns.
                remaining = max(
                    self.spec.retry.wait_seconds, previous.get("retry_after_seconds", 0)
                ) - (time.time() - previous.get("ended_epoch", time.time()))
                if remaining > 0:
                    self.journal.append(
                        "retry_scheduled",
                        logical_key=item.key,
                        attempt_no=attempt,
                        not_before_epoch=time.time() + remaining,
                    )
                    await self.retry_sleep(remaining)
                value = await self._generate_once(item, attempt)
            else:
                value = await self._generate_once(item, attempt)
            # Local preparation failed before any HTTP write or reservation.
            # Retry that same physical ordinal, rather than consuming its model
            # attempt number. Exhausted preparation is explicitly undispatched.
            for local_retry in range(2):
                if value["status"] != "local_resource_error" or self.draining.is_set():
                    break
                await self.retry_sleep(self.spec.retry.wait_seconds)
                value = await self._generate_once(item, attempt)
            if value["status"] == "local_resource_error":
                value["undispatched_reason"] = "local_resource"
            if not self._retryable(value) or attempt == self.spec.retry.max_attempts:
                return value
            previous = value

    async def _execute(self, item, reply, execute):
        async with self.db_gate:
            started = time.monotonic()
            try:
                result = await asyncio.get_running_loop().run_in_executor(
                    self.db_workers, execute, item, reply
                )
            except (PersistenceError, FileExistsError, IntegrityError):
                raise
            except Exception as exc:
                await self._case_issue(item, "database", exc)
                return dict(status="case_error", error=type(exc).__name__, complete=False)
            return dict(result, db_client_wall_seconds=time.monotonic() - started)

    async def _path(self, item, execute, feedback):
        reply = await self.generate(item)
        final, history = reply, [reply]
        views = []
        interpretable = ("ok", "length", "empty", "refused")
        if execute is not None and reply["status"] in interpretable:
            final = await self._execute(item, reply, execute)
            views.append(dict(phase=item.phase, outcome=final))
            if (
                feedback is not None
                and final.get("status") == "query_error"
                and not self.draining.is_set()
            ):
                repaired = feedback(item, reply, final)
                if repaired is not None:
                    if (
                        repaired.phase != "feedback"
                        or item.phase != "initial"
                        or repaired.task_id != item.task_id
                        or repaired.method != item.method
                        or repaired.model != item.model
                    ):
                        raise IntegrityError("Only one same-path operational feedback is allowed")
                    await asyncio.get_running_loop().run_in_executor(
                        None,
                        write_queue,
                        self.spec.run_dir
                        / "feedback_queue"
                        / (digest(repaired.key.encode())[:32] + ".jsonl"),
                        [repaired],
                        self.saved_replies,
                    )
                    reply = await self.generate(repaired)
                    history.append(reply)
                    final = (
                        await self._execute(repaired, reply, execute)
                        if reply["status"] in interpretable
                        else reply
                    )
                    views.append(dict(phase=repaired.phase, outcome=final))
        if not views:
            views.append(dict(phase=item.phase, outcome=final))
        value = dict(
            logical_key=item.key,
            task_id=item.task_id,
            graph=item.graph,
            method=item.method,
            model=item.model,
            final=final,
            model_attempts=history,
            views=views,
        )
        return await self._save_path(value)

    async def _save_path(self, value):
        await asyncio.get_running_loop().run_in_executor(
            None,
            write_new,
            self.spec.run_dir / "finals" / (digest(value["logical_key"].encode())[:32] + ".json"),
            canonical(value),
        )
        self.paths.append(value)
        return value

    async def _case_issue(self, item, phase, exc):
        import traceback

        await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: self.journal.append(
                "case_error",
                logical_key=item.key,
                phase=phase,
                error_type=type(exc).__name__,
                traceback="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                automatic_model_retry=False,
            ),
        )

    async def _isolated_path(self, item, execute, feedback):
        try:
            return await self._path(item, execute, feedback)
        except (PersistenceError, FileExistsError, IntegrityError):
            raise
        except Exception as exc:
            await self._case_issue(item, "path", exc)
            final = dict(status="case_error", complete=False, error=type(exc).__name__)
            return await self._save_path(
                dict(
                    logical_key=item.key,
                    task_id=item.task_id,
                    graph=item.graph,
                    method=item.method,
                    model=item.model,
                    final=final,
                    model_attempts=[e for e in self.events if e.get("logical_key") == item.key],
                    views=[dict(phase=item.phase, outcome=final)],
                )
            )

    async def run(self, items, *, execute=None, feedback=None):
        items = list(items)
        if len({item.key for item in items}) != len(items):
            raise ValueError("Duplicate logical paths")
        allowed_models = set(self.spec.models or (self.spec.model,))
        if any(item.model not in allowed_models for item in items):
            raise ValueError("Campaign model cannot be changed by endpoint routing")
        if feedback is not None and any(item.phase != "initial" for item in items):
            raise ValueError("Feedback orchestration starts from initial paths only")
        tasks = [
            asyncio.create_task(self._isolated_path(item, execute, feedback)) for item in items
        ]
        try:
            for task in asyncio.as_completed(tasks):
                try:
                    await task
                except Exception as exc:
                    await self.drain("path_controller_error:" + type(exc).__name__)
            return self.paths
        finally:
            # Drain every submitted coroutine/future even after a stop or exception.
            await asyncio.gather(*tasks, return_exceptions=True)
            self.http_workers.shutdown(wait=True)
            self.db_workers.shutdown(wait=True)
            if self.close_transport:
                self.transport.close()
