"""One audited ledger owner for a bounded Direct-feedback phase.

This session deliberately excludes dynamic multi-stage methods. Those methods
continue to use the full recovery path in request_pipeline.
"""

from pathlib import Path
import threading
import time

from engine.config import canonical, digest
from engine.errors import IntegrityError
from engine.journal import write_new
from engine.ledger import Ledger, scan_legacy
from engine.recovery import checkpoint_replies, merge_saved_reply


VARIABLE_FIELDS = {
    "name",
    "run_dir",
    "feedback_queue",
    "recovery",
    "physical_attempt_cap",
    "drain_paths",
}


def fixed_config(config):
    return {k: v for k, v in config.items() if k not in VARIABLE_FIELDS}


class RequestSession:
    """Never pass this object across model-owner threads or reuse it after close."""

    def __init__(self, config, root, stop=None):
        self.config, self.root = config, Path(root).resolve()
        self.cap = config["physical_attempt_cap"]
        if type(self.cap) is not int or self.cap < 1:
            raise ValueError("Session physical-attempt cap must be positive")
        self.config_sha256 = digest(canonical(fixed_config(config)))
        self.stop = stop if stop is not None else threading.Event()
        self.ledger = None
        self.active = False
        self.saved = {}
        self.seen_slot = 0
        self.owner_thread = None

    def __enter__(self):
        if self.ledger is not None:
            raise IntegrityError("A request session cannot be reopened")
        started = time.monotonic()
        legacy = scan_legacy(
            Path(self.config["legacy_ledger"]), self.config.get("path_rewrites", [])
        )
        legacy_done = time.monotonic()
        self.ledger = Ledger(
            Path(self.config["ledger_dir"]),
            Path(self.config["approval_path"]),
            legacy,
            self.config["budget_scope"],
        )
        self.ledger.__enter__()
        ledger_done = time.monotonic()
        try:
            self.ledger.limit_run(self.root, self.cap)
            checkpoint = self.root / "admission_recovery.json"
            self.saved = checkpoint_replies(checkpoint, self.ledger)
            self.recovery = dict(
                path=str(checkpoint), sha256=digest(checkpoint.read_bytes())
            )
            self.seen_slot = len(self.ledger.records)
            self.owner_thread = threading.get_ident()
            self.active = True
            write_new(
                self.root / "admission.json",
                canonical(
                    dict(
                        legacy_scan_seconds=legacy_done - started,
                        ledger_audit_seconds=ledger_done - legacy_done,
                        recovery_seconds=time.monotonic() - ledger_done,
                        cold_prepare_seconds=time.monotonic() - started,
                        phase_cap=self.cap,
                        scope=self.ledger.scope,
                        historical_slots=self.seen_slot,
                        recovery=self.recovery,
                        continuous_request_measurement=False,
                    )
                ),
            )
            return self
        except BaseException:
            self.active = False
            self.ledger.__exit__(None, None, None)
            raise

    def validate(self, config):
        if not config.get("feedback_queue"):
            raise IntegrityError("RequestSession accepts only Direct-feedback queues")
        ledger = self.ledger
        if (
            not self.active
            or ledger is None
            or not ledger.active
            or threading.get_ident() != self.owner_thread
        ):
            raise IntegrityError("Request session is not active on its owner thread")
        if (
            ledger.scope != config["budget_scope"]
            or ledger.directory.resolve() != Path(config["ledger_dir"]).resolve()
            or ledger.approval_path.resolve() != Path(config["approval_path"]).resolve()
            or digest(ledger.approval_path.read_bytes()) != ledger.approval_sha256
            or ledger.owner_path.read_bytes() != ledger.owner
        ):
            raise IntegrityError("Request session approval/scope/owner differs")
        if digest(canonical(fixed_config(config))) != self.config_sha256:
            raise IntegrityError("Request session frozen configuration changed")
        run_dir = Path(config["run_dir"]).resolve()
        if run_dir == self.root or not run_dir.is_relative_to(self.root):
            raise IntegrityError("Batch output is outside the admitted phase")
        if config.get("recovery") != self.recovery:
            raise IntegrityError("Batch recovery differs from the audited admission")
        cap = config["physical_attempt_cap"]
        if type(cap) is not int or not 0 < cap <= self.cap - self.count():
            raise IntegrityError("Batch allowance exceeds remaining phase cap")

    def count(self):
        if self.ledger is None:
            return 0
        status = self.ledger.phase_status(self.root)
        if status["pending"]:
            raise IntegrityError("Phase has unfinalized paid attempts; no new dispatch")
        return status["reserved"]

    def refresh(self):
        """Only newly charged slots are read; the full historical prefix stays cached."""
        self.count()
        added = self.ledger.records_since(self.seen_slot)
        for record in added:
            if record["scope"] != self.ledger.scope or not Path(
                record["run_dir"]
            ).is_relative_to(self.root):
                raise IntegrityError("Foreign attempts appeared in the owned session")
            merge_saved_reply(self.saved, record)
        self.seen_slot += len(added)

    def replies_for(self, keys):
        self.refresh()
        verified = {}
        for key in set(keys):
            if key not in self.saved:
                continue
            # Recheck only replies about to be reused. This preserves the hash
            # contract without rereading unrelated historical attempts.
            for attempt in self.saved[key]["history"]:
                rid = attempt["response"]["request_id"]
                record = self.ledger.records[rid]
                entry = dict(
                    request_id=rid,
                    request_sha256=record["request_sha256"],
                    terminal_sha256=attempt["source_terminal_sha256"],
                )
                merge_saved_reply(verified, record, entry)
        return verified

    def __exit__(self, *exc):
        started = time.monotonic()
        self.active = False
        try:
            self.ledger.__exit__(*exc)
            if self.ledger.last_close_audit["unknown"]:
                raise IntegrityError(
                    "Final phase audit found unfinalized attempts; owner retained"
                )
        finally:
            write_new(
                self.root / "close_timing.json",
                canonical(
                    dict(
                        final_audit_seconds=time.monotonic() - started,
                        continuous_request_measurement=False,
                    )
                ),
            )
