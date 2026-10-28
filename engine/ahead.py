"""Ordered model demand and serial DB consumer, joined by durable handoffs.

Only handoff filenames pass through the in-memory queue. Model request evidence
is already durable before the producer moves on. HTTP and DB use their existing
separate worker pools; this module creates no HTTP client and no retry policy.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from .errors import IntegrityError, PersistenceError
from .journal import Journal
from .ledger import BudgetError


def pending_under(ledger, directory):
    directory = os.path.normcase(str(Path(directory).resolve()))
    with ledger.lock:
        records = list(ledger.records.values())
    return [
        r["request_id"]
        for r in records
        if (
            os.path.normcase(str(Path(r["run_dir"]))) == directory
            or os.path.normcase(str(Path(r["run_dir"]))).startswith(directory + os.sep)
        )
        and not Path(r["request_path"]).with_name("terminal.json").exists()
    ]


def local_budget(ledger, directory):
    value = ledger.summary()
    value["global_pending_count"] = len(value["pending"])
    value["pending"] = pending_under(ledger, directory)
    return value


class AheadQueue:
    def __init__(self, stages, directory, dispatch, pool):
        self.stages = [s for s in stages if not s.get("blocked_reason")]
        self.dispatch, self.pool = dispatch, pool
        self.ready = {
            s["name"]: asyncio.get_running_loop().create_future() for s in self.stages
        }
        self.journal = Journal(Path(directory) / "handoffs.jsonl")

    async def produce(self):
        # Enrol all admitted cells in order. The shared pool fills a free slot
        # from the next cell without waiting for the previous cell's last reply.
        async def dispatch_one(stage):
            name = stage["name"]
            if self.pool.closed:
                self.ready[name].set_result(dict(stopped="pool_drained"))
                return
            try:
                self.journal.append("dispatch_started", stage=name)
                value = await self.dispatch(stage)
                self.journal.append(
                    "dispatch_landed",
                    stage=name,
                    handoff=value["path"],
                    handoff_sha256=value["sha256"],
                )
                self.ready[name].set_result(value)
            except Exception as exc:
                # Release the consumer even if the disk that stores events fails.
                # A persistence error must drain admissions, not strand take().
                failure = exc
                try:
                    self.journal.append(
                        "dispatch_failed", stage=name, error_type=type(exc).__name__
                    )
                except Exception as journal_error:
                    failure = PersistenceError(str(journal_error))
                self.ready[name].set_result(dict(error=failure))
                if isinstance(
                    failure,
                    (IntegrityError, PersistenceError, FileExistsError, BudgetError),
                ):
                    await self.pool.drain()

        await asyncio.gather(*(dispatch_one(stage) for stage in self.stages))

    async def take(self, stage):
        value = await self.ready[stage["name"]]
        if value.get("error") is not None:
            raise value["error"]
        return value


async def overlap(stages, directory, dispatch, consume, run_consumer, pool):
    queue = AheadQueue(stages, directory, dispatch, pool)
    producer = asyncio.create_task(queue.produce())

    async def block(stage):
        handoff = await queue.take(stage)
        if handoff.get("stopped"):
            return dict(stop_reason=handoff["stopped"], case_failures=1)
        return await consume(stage, handoff)

    try:
        return await run_consumer(block)
    except BaseException:
        await pool.drain()
        raise
    finally:
        # Never cancel a physical POST merely because the consumer failed.
        # Drain admissions, then let already-started attempts write terminals.
        if not producer.done():
            await pool.drain()
        await producer
