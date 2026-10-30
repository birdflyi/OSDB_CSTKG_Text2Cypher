"""Durable feedback notifications with one model writer and one DB consumer.

The callback never waits for model or database capacity. The model thread owns
one batch at a time; finished responses queue independently for DB consumption.
"""

import asyncio
from collections import Counter, defaultdict
from pathlib import Path
import queue
import threading
import time

from engine.config import digest
from engine.errors import IntegrityError
from engine.ledger import BudgetExhausted
from request_session import RequestSession
from offline_pipeline import read_pin, save
from offline_runtime import OfflineRuntime, positive


END = object()


def pin(path):
    return dict(path=str(path), sha256=digest(path.read_bytes()))


class PhaseAccounting:
    """Keep one audited writer throughout the phase; batches read only its index."""

    def __init__(self, requests, root):
        self.requests, self.root = requests, Path(root).resolve()
        self.session = None

    def start(self, stop):
        self.session = RequestSession(self.requests, self.root, stop)
        self.session.__enter__()

    def count(self):
        return self.session.count() if self.session else 0

    def checkpoint(self, path):
        used = self.count()
        save(
            path,
            dict(
                admission_recovery=self.session.recovery,
                charged_physical_attempts=used,
                audited_slot_head=self.session.seen_slot,
            ),
        )
        return used, self.session.recovery

    async def generate(self, function, config):
        return await function(config, admitted_session=self.session)

    def close(self):
        if self.session is not None and self.session.active:
            self.session.__exit__(None, None, None)


class FeedbackStream:
    def __init__(
        self,
        root,
        offline,
        requests,
        runtime,
        consume,
        generate,
        settings=None,
        accounting=None,
    ):
        self.root, self.offline, self.requests = Path(root), offline, requests
        self.runtime, self.consume, self.generate = runtime, consume, generate
        settings = dict(settings or {})
        if settings.keys() - {"batch_size", "coalesce_seconds"}:
            raise ValueError("Unknown streaming_feedback setting")
        self.batch_size = positive(settings.get("batch_size", 8), "feedback.batch_size")
        self.coalesce = settings.get("coalesce_seconds", 0.05)
        if (
            isinstance(self.coalesce, bool)
            or not isinstance(self.coalesce, (int, float))
            or not 0 <= self.coalesce <= 5
        ):
            raise ValueError("coalesce_seconds must be between zero and five")
        self.cap = positive(requests["physical_attempt_cap"], "physical_attempt_cap")
        self.accounting = accounting or PhaseAccounting(requests, self.root)
        self.incoming, self.responses = queue.Queue(), queue.Queue()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.accepted, self.batches, self.consumed = {}, [], []
        self.pending, self.failed_batches, self.errors = [], [], []
        self.first_error = None
        self.model_thread = threading.Thread(
            target=self._model_loop, name="feedback-model"
        )
        self.db_thread = threading.Thread(target=self._db_loop, name="feedback-db")

    def start(self):
        self.root.mkdir(parents=True, exist_ok=False)
        save(
            self.root / "policy.json",
            dict(
                physical_attempt_cap=self.cap,
                batch_size=self.batch_size,
                coalesce_seconds=self.coalesce,
                scope=self.requests["budget_scope"],
            ),
        )
        self.db_thread.start()
        self.model_thread.start()

    def enqueue(self, receipt):
        """Only queue a durable descriptor. Never acquire a graph/model slot here."""
        receipt = dict(receipt)
        with self.lock:
            previous = self.accepted.get(receipt["path"])
            if previous is not None:
                if previous != receipt:
                    raise IntegrityError("Feedback path changed after notification")
                return
            self.accepted[receipt["path"]] = receipt
        self.incoming.put_nowait(receipt)

    def abort(self, error):
        self.stop.set()
        # Stop dispatch in memory first. A failed DRAIN write must never reopen
        # capacity or leave the active scheduler unaware of the stop signal.
        if not isinstance(error, BudgetExhausted):
            self.runtime.stop.set()
        with self.lock:
            self.first_error = self.first_error or error
            self.errors.append(
                dict(type=type(error).__name__, message=str(error)[:1000])
            )
            drain = self.root / "DRAIN"
            if not drain.exists():
                try:
                    save(
                        drain,
                        dict(
                            reason="stream_hard_stop", error_type=type(error).__name__
                        ),
                    )
                except Exception as write_error:
                    self.errors.append(
                        dict(
                            type=type(write_error).__name__,
                            message=str(write_error)[:1000],
                            stage="drain_persistence",
                        )
                    )
                    self.runtime.stop.set()
                    if isinstance(self.first_error, BudgetExhausted):
                        self.first_error = write_error

    def _batch(self, first):
        batch, ended = [first], False
        deadline = time.monotonic() + self.coalesce
        while len(batch) < self.batch_size:
            try:
                remaining = max(0, deadline - time.monotonic())
                item = self.incoming.get(timeout=remaining)
            except queue.Empty:
                break
            if item is END:
                ended = True
                break
            batch.append(item)
        return batch, ended

    def _prepare(self, batch, number):
        folder = self.root / ("batch-%04d" % number)
        folder.mkdir(parents=True, exist_ok=False)
        queue_pin = save(folder / "queue.json", batch)
        for receipt in batch:
            read_pin(receipt)
        manifest = folder / "recovery.json"
        used, recovery = self.accounting.checkpoint(manifest)
        remaining = self.cap - used
        if remaining <= 0:
            raise BudgetExhausted(
                "Streaming feedback phase physical-attempt cap exhausted"
            )
        request = dict(
            self.requests,
            feedback_queue=queue_pin,
            recovery=recovery,
            run_dir=str(folder / "requests"),
            name=self.requests["name"] + "-feedback-%04d" % number,
            physical_attempt_cap=remaining,
            drain_paths=list(self.requests.get("drain_paths", []))
            + [str(self.root / "DRAIN")],
        )
        save(folder / "request_config.json", request)
        return folder, request, used

    def _model_loop(self):
        ended, number = False, 0
        try:
            self.accounting.start(self.stop)
            while not ended:
                first = self.incoming.get()
                if first is END:
                    break
                batch, ended = self._batch(first)
                if self.stop.is_set() or self.runtime.stop.is_set():
                    self.pending.extend(batch)
                    continue
                number += 1
                dispatched = False
                try:
                    folder, request, before = self._prepare(batch, number)
                    if self.stop.is_set() or self.runtime.stop.is_set():
                        self.pending.extend(batch)
                        continue
                    dispatched = True
                    generated = asyncio.run(
                        self.accounting.generate(self.generate, request)
                    )
                    after = self.accounting.count()
                    if not before <= after <= self.cap:
                        raise IntegrityError(
                            "Feedback charged slots exceed frozen phase cap"
                        )
                    report = dict(
                        batch=number,
                        queue=batch,
                        generated=generated,
                        charged_before=before,
                        charged_after=after,
                        charged_this_batch=after - before,
                    )
                    save(folder / "generation.json", report)
                    self.batches.append(report)
                    if generated.get("stop_reason"):
                        self.failed_batches.append(
                            dict(
                                batch=number,
                                queue=batch,
                                reason=generated["stop_reason"],
                            )
                        )
                        error_type = (
                            BudgetExhausted
                            if generated["stop_reason"] == "budget"
                            else IntegrityError
                        )
                        self.abort(
                            error_type(
                                "Request scheduler stopped: "
                                + str(generated["stop_reason"])
                            )
                        )
                    self.responses.put((folder, batch))
                except BaseException as error:
                    if dispatched:
                        self.failed_batches.append(
                            dict(
                                batch=number,
                                queue=batch,
                                reason=str(error)[:1000],
                                dispatch_state="audit_batch_ledger",
                            )
                        )
                    else:
                        self.pending.extend(batch)
                    self.abort(error)
        except BaseException as error:
            self.abort(error)
        finally:
            try:
                self.accounting.close()
            except BaseException as error:
                self.abort(error)
            self.responses.put(END)

    def _db_loop(self):
        while True:
            item = self.responses.get()
            if item is END:
                return
            folder, batch = item
            try:
                selection = defaultdict(list)
                for receipt in batch:
                    row = read_pin(receipt)["position"]
                    selection[row["cell_id"]].append(row["position_id"])
                followup = dict(
                    self.offline,
                    output=str(folder / "answers"),
                    selected_positions=dict(selection),
                    source_finals=[
                        pin(p)
                        for p in sorted((folder / "requests/finals").glob("*.json"))
                    ],
                )
                save(folder / "consumer_config.json", followup)
                result = self.consume(followup, runtime=self.runtime)
                self.consumed.append(result)
            except BaseException as error:
                self.failed_batches.append(
                    dict(
                        folder=str(folder),
                        queue=batch,
                        reason=str(error)[:1000],
                        stage="consume",
                    )
                )
                self.abort(error)

    def finish(self):
        # Initial production is finished. Drain queued descriptors and both
        # workers; never close graph processes while either worker still uses them.
        self.incoming.put(END)
        self.model_thread.join()
        self.db_thread.join()
        while True:
            try:
                item = self.incoming.get_nowait()
                if item is not END:
                    self.pending.append(item)
            except queue.Empty:
                break
        count_error = None
        try:
            charged = self.accounting.count()
        except Exception as error:
            charged = None
            count_error = dict(type=type(error).__name__, message=str(error)[:1000])
            self.abort(error)
        report = dict(
            accepted=list(self.accepted.values()),
            batches=self.batches,
            consumed=self.consumed,
            undispatched_queue=self.pending,
            incomplete_batches=self.failed_batches,
            errors=self.errors,
            charged_physical_attempts=charged,
            count_error=count_error,
            physical_attempt_cap=self.cap,
        )
        save(self.root / "summary.json", report)
        return report


def run_streaming(
    config,
    output,
    offline,
    requests,
    consume,
    generate,
    backend_factory=None,
    accounting=None,
):
    """One shared runtime outlives initial work and all feedback consumers."""
    if backend_factory is None:
        from offline_backends import open_backend

        backend_factory = open_backend
    settings = config["streaming_feedback"]
    if settings is True:
        settings = {}
    failure, initial, report = None, None, None
    try:
        with OfflineRuntime(
            Path(output) / "runtime", backend_factory, offline.get("parallel")
        ) as runtime:
            stream = FeedbackStream(
                Path(output) / "feedback",
                offline,
                requests,
                runtime,
                consume,
                generate,
                settings,
                accounting,
            )
            stream.start()
            try:
                initial = consume(offline, runtime=runtime, on_feedback=stream.enqueue)
            except BaseException as error:
                failure = error
                stream.abort(error)
            finally:
                report = stream.finish()
                failure = failure or stream.first_error
    except BaseException as error:
        failure = failure or error
    if initial is None:
        partial = Path(offline["output"]) / "partial_summary.json"
        initial = (
            read_pin(pin(partial))
            if partial.is_file()
            else dict(results=[], incomplete=True)
        )
    final = {(r["cell_id"], r["position_id"]): r for r in initial["results"]}
    for result in (report or {}).get("consumed", []):
        for row in result["results"]:
            final[row["cell_id"], row["position_id"]] = row
    summary = dict(
        initial=initial,
        feedback_stream=report,
        final=list(final.values()),
        final_counts=dict(Counter(r["status"] for r in final.values())),
        feedback_positions=len((report or {}).get("accepted", [])),
        hard_stop=(
            dict(type=type(failure).__name__, message=str(failure)[:1000])
            if failure
            else None
        ),
        paper_result=False,
    )
    save(
        Path(output) / ("partial_summary.json" if failure else "summary.json"), summary
    )
    if failure is not None:
        raise failure
    return summary
