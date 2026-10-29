from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import os
from pathlib import Path
import socket
import threading
import time
import uuid

from .config import canonical, digest
from .journal import Journal, write_new


class BudgetError(RuntimeError):
    pass


@dataclass(frozen=True)
class LegacySnapshot:
    charged: int
    by_scope: dict
    pending: tuple
    formats: dict
    manifest_sha256: str


def scan_legacy(directory: Path, path_rewrites=()) -> LegacySnapshot:
    """Once at entry, read only. Supports all three historical slot formats."""
    directory = Path(directory)
    slots = sorted(directory.glob("*.json"), key=lambda p: int(p.stem))
    if [int(p.stem) for p in slots] != list(range(1, len(slots) + 1)):
        raise BudgetError("Noncontiguous legacy ledger")
    scopes, formats, pending, seen, hashes = Counter(), Counter(), [], set(), []
    for slot in slots:
        raw = slot.read_bytes()
        record = json.loads(raw)
        stored = record["request_path"]
        request = Path(stored)
        for source, target in path_rewrites:
            prefix = str(source).rstrip("\\/")
            if stored.startswith(prefix + "\\") or stored.startswith(prefix + "/"):
                request = Path(target) / stored[len(prefix) :].lstrip("\\/").replace("\\", "/")
                break
        identity = os.path.normcase(str(request.resolve()))
        if identity in seen or not request.is_file():
            raise BudgetError("Duplicate/missing legacy request")
        seen.add(identity)
        if (
            record.get("request_sha256")
            and digest(request.read_bytes()) != record["request_sha256"]
        ):
            raise BudgetError("Changed legacy request")
        terminal = request.with_name(
            "connectivity_probe_01.json"
            if request.name == "connectivity_probe_01_request.json"
            else "terminal.json"
        )
        if not terminal.exists():
            pending.append(int(slot.stem))
        else:
            value = json.loads(terminal.read_text(encoding="utf-8-sig"))
            if not isinstance(value, dict) or not (
                {"http_status", "transport_error"} & value.keys()
            ):
                raise BudgetError("Invalid legacy terminal")
        formats[
            (
                "legacy_b"
                if set(record) == {"request_path", "pid"}
                else "scoped" if record.get("scope_id") else "standard"
            )
        ] += 1
        scopes[record.get("scope_id") or "CypherBench"] += 1
        hashes.append((slot.name, digest(raw)))
    return LegacySnapshot(
        len(slots), dict(scopes), tuple(pending), dict(formats), digest(canonical(hashes))
    )


def audit_new(directory: Path) -> dict:
    records, unknown, orphan, missing_intents = [], [], [], []
    head, seen, journals = "0" * 64, set(), {}
    for number, path in enumerate(sorted(Path(directory).glob("[0-9]*.json")), 1):
        raw = path.read_bytes()
        row = json.loads(raw)
        if row["slot"] != number or row["prev_sha256"] != head or row["request_id"] in seen:
            raise BudgetError("Changed/noncontiguous ledger chain")
        head = digest(raw)
        seen.add(row["request_id"])
        journal_path = row["journal_path"]
        if journal_path not in journals:
            journals[journal_path] = {
                event.get("request_id"): event.get("request_sha256")
                for event in Journal(Path(journal_path)).read()
                if event["kind"] == "dispatch_intent"
            }
        if journals[journal_path].get(row["request_id"]) != row["request_sha256"]:
            missing_intents.append(row["request_id"])
        request = Path(row["request_path"])
        if not request.exists():
            orphan.append(row["request_id"])
        elif digest(request.read_bytes()) != row["request_sha256"]:
            raise BudgetError("Request bytes changed")
        elif not (request.parent / "terminal.json").exists():
            unknown.append(row["request_id"])
        records.append(row)
    return dict(
        charged=len(records),
        records=records,
        unknown=unknown,
        orphan_slot=orphan,
        missing_dispatch_intent=missing_intents,
        charged_request_ids=sorted(seen),
        head_sha256=head,
        read_only=True,
        automatically_resent=0,
    )


def _owner_alive(path: Path) -> bool:
    """A lock from another host is respected; a lock from a dead local pid is not."""
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if value.get("hostname") != socket.gethostname():
        return True
    pid = value.get("pid")
    if type(pid) is not int or pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


class Ledger:
    """Single-owner append-only slots, indexed once; O(1) reservation thereafter.

    The old ledger is never written. A crash is audited, never repaired by deleting
    charges or automatically resending. The journal records intent before a slot.
    """

    def __init__(self, directory: Path, approval_path: Path, legacy: LegacySnapshot, scope: str):
        self.directory, self.approval_path = Path(directory), Path(approval_path)
        self.legacy, self.scope = legacy, scope
        self.lock = threading.RLock()
        self.records = {}
        self.new_by_scope = Counter()
        self.run_caps = {}
        self.head = "0" * 64
        self.owner_path = self.directory / ".writer.lock"
        self.owner = canonical(
            dict(pid=os.getpid(), hostname=socket.gethostname(), nonce=uuid.uuid4().hex)
        )
        self.active = False

    def __enter__(self):
        raw = self.approval_path.read_bytes()
        approval = json.loads(raw)
        if (
            approval.get("approved") is not True
            or approval.get("active") is not True
            or self.scope not in approval.get("scope_caps", {})
            or type(approval.get("cumulative_cap")) is not int
            or Path(approval.get("ledger_dir", "")).resolve() != self.directory.resolve()
        ):
            raise BudgetError("Separate active approval required")
        if self.legacy.pending:
            raise BudgetError("Legacy pending requests require audit")
        self.cap = approval["cumulative_cap"]
        if self.cap < self.legacy.charged:
            raise BudgetError("Approved total is below already charged history")
        self.scope_caps = approval["scope_caps"]
        self.scope_cap = approval["scope_caps"][self.scope]
        if type(self.scope_cap) is not int or self.scope_cap < 0:
            raise BudgetError("Invalid scope cap")
        self.approval_sha256 = digest(raw)
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            write_new(self.owner_path, self.owner)
        except FileExistsError:
            if _owner_alive(self.owner_path):
                raise BudgetError("Ledger held by a live writer")
            self.owner_path.unlink()
            write_new(self.owner_path, self.owner)
        try:
            audit = audit_new(self.directory)
            if audit["unknown"] or audit["orphan_slot"] or audit["missing_dispatch_intent"]:
                raise BudgetError("Incomplete ledger/journal; verify before explicit resume")
            for value in audit["records"]:
                if value["scope"] not in self.scope_caps:
                    raise BudgetError("Unapproved historical new scope")
                self.records[value["request_id"]] = value
                self.new_by_scope[value["scope"]] += 1
            self.head = audit["head_sha256"]
            if self.legacy.charged + len(self.records) > self.cap or any(
                self.legacy.by_scope.get(scope, 0) + count > self.scope_caps[scope]
                for scope, count in self.new_by_scope.items()
            ):
                raise BudgetError("Already charged usage exceeds current approval")
            self.active = True
            return self
        except BaseException:
            # Existing pending evidence is preserved; only our new owner claim is removed.
            self._release_owner()
            raise

    def _release_owner(self):
        if self.owner_path.read_bytes() != self.owner:
            raise BudgetError("Foreign writer lock; retained")
        self.owner_path.unlink()

    def reserve(
        self,
        request_id: str,
        logical_key: str,
        attempt_no: int,
        wire: bytes,
        request_path: Path,
        run_dir: Path,
        journal,
        *,
        scope=None,
    ) -> dict:
        with self.lock:
            if not self.active:
                raise BudgetError("Ledger closed")
            if (
                digest(self.approval_path.read_bytes()) != self.approval_sha256
                or self.owner_path.read_bytes() != self.owner
            ):
                raise BudgetError("Approval or writer ownership changed")
            scope = scope or self.scope
            if scope not in self.scope_caps:
                raise BudgetError("Unapproved scope")
            fingerprint = digest(wire)
            if request_id in self.records:
                old = self.records[request_id]
                if old["request_sha256"] != fingerprint:
                    raise BudgetError("Same request_id with different body")
                return dict(old)
            if (
                self.legacy.charged + len(self.records) >= self.cap
                or self.legacy.by_scope.get(scope, 0) + self.new_by_scope[scope]
                >= self.scope_caps[scope]
            ):
                raise BudgetError("Cumulative/scope budget exhausted")
            target = Path(run_dir).resolve()
            matching_limits = []
            for root, limit in self.run_caps.items():
                if target == root or target.is_relative_to(root):
                    if limit["used"] >= limit["cap"]:
                        raise BudgetError("Pipeline physical-attempt cap exhausted")
                    matching_limits.append(limit)
            write_new(request_path, wire)
            journal.append(
                "dispatch_intent",
                request_id=request_id,
                logical_key=logical_key,
                request_sha256=fingerprint,
            )
            slot = len(self.records) + 1
            value = dict(
                format=2,
                slot=slot,
                request_id=request_id,
                logical_key=logical_key,
                attempt_no=attempt_no,
                request_path=str(Path(request_path).resolve()),
                request_sha256=fingerprint,
                run_dir=str(Path(run_dir).resolve()),
                scope=scope,
                pid=os.getpid(),
                purpose=scope,
                journal_path=str(journal.path.resolve()),
                approval_sha256=self.approval_sha256,
                approved_cap_at_reservation=self.cap,
                legacy_charged=self.legacy.charged,
                prev_sha256=self.head,
            )
            raw = canonical(value)
            write_new(self.directory / ("%08d.json" % slot), raw)
            self.records[request_id], self.head = value, digest(raw)
            self.new_by_scope[scope] += 1
            for limit in matching_limits:
                limit["used"] += 1
            return dict(value)

    def limit_run(self, root: Path, cap: int):
        """One cap shared by every stage and retry below this pipeline directory."""
        if type(cap) is not int or cap < 0:
            raise BudgetError("A nonnegative physical-attempt limit is required")
        root = Path(root).resolve()
        with self.lock:
            if root in self.run_caps and self.run_caps[root]["cap"] != cap:
                raise BudgetError("Run cap changed inside an active ledger")
            used = sum(Path(v["run_dir"]) == root or Path(v["run_dir"]).is_relative_to(root)
                       for v in self.records.values())
            if used > cap:
                raise BudgetError("Historical attempts already exceed this run cap")
            self.run_caps[root] = dict(cap=cap, used=used)

    def claim(self, request_id: str):
        with self.lock:
            value = self.records[request_id]
        write_new(
            Path(value["request_path"]).parent / "dispatch_claim.json",
            canonical(dict(request_id=request_id, physical_attempt=1, claimed_epoch=time.time())),
        )

    def finish(self, request_id: str, terminal: dict):
        with self.lock:
            value = self.records[request_id]
        if terminal["budget_slot"] != value["slot"]:
            raise BudgetError("Terminal slot mismatch")
        # Each terminal has a unique immutable filename; unrelated completions
        # need not hold up the budget's single slot allocator while fsync runs.
        write_new(Path(value["request_path"]).parent / "terminal.json", canonical(terminal))

    def summary(self) -> dict:
        with self.lock:
            records = list(self.records.items())
        pending = [
            rid
            for rid, value in records
            if not (Path(value["request_path"]).parent / "terminal.json").exists()
        ]
        return dict(
            legacy_charged=self.legacy.charged,
            new_charged=len(records),
            total_charged=self.legacy.charged + len(records),
            cap=self.cap,
            scope=self.scope,
            pending=pending,
        )

    def run_status(self, run_dir: Path) -> dict:
        target = os.path.normcase(str(Path(run_dir).resolve()))
        with self.lock:
            snapshot = list(self.records.values())
        records = [v for v in snapshot if os.path.normcase(str(Path(v["run_dir"]))) == target]
        pending = [
            v["request_id"]
            for v in records
            if not (Path(v["request_path"]).parent / "terminal.json").exists()
        ]
        return dict(reserved=len(records), terminal=len(records) - len(pending), pending=pending)

    def __exit__(self, *unused):
        self.active = False
        # Never remove the writer claim when a physical attempt has no terminal.
        if not self.summary()["pending"]:
            self._release_owner()
