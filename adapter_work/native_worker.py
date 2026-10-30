"""Owned MTQ/ZOGRASCOPE smoke worker; one explicit transaction per case.

A bounded lane keeps each case on one thread from execution through scoring.
Cases share only the driver and pinned reference loader. Model requests run in
a different process and never wait on this worker.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
import threading
from contextlib import nullcontext

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "operations/analysis"),
    str(ROOT / "experiment-harness/public_benchmark_v1"),
]
from native_cases import CaseLanes, instance_settings
from common.methods import CYPHER_METHODS
from engine.config import canonical, digest
from engine.errors import IntegrityError
from engine.journal import write_new
from neo4j import GraphDatabase
from neo4j_native_plan_dev_20261029 import (
    NativePlanChain,
    PlanEvidence,
    PreparedPlan,
    native_set_count_query,
)
from neo4j_native_selection_dev_20261029 import NativeSchema, UnsupportedSelection
from neo4j_native_answer_contract_dev_20261029 import compare_complete_auxiliary
from neo4j_read_capture_dev_20261029 import NativeReadCapture
from neo4j_reference_boundary_dev_20261029 import PinnedReferences


def read(path):
    return json.loads(Path(path).read_bytes())


def save(path, value):
    write_new(path, canonical(value))


class NativeWorker:
    def __init__(self, settings):
        self.settings = settings
        self.capture = self.driver = self.process = None
        self.last = None
        self.reference_lock = threading.Lock()
        self.workers = settings.get("workers", 1)
        if type(self.workers) is not int or not 1 <= self.workers <= 4:
            raise ValueError("Native worker supports one to four case lanes")
        root = Path(settings["owner_root"]).resolve()
        if (
            read(root / "execution_owner.json")["hostname"] != socket.gethostname()
            or socket.gethostname() != settings["hostname"]
        ):
            raise IntegrityError("Execution owner differs")
        self.source, self.folder = (
            Path(settings["source"]).resolve(),
            Path(settings["output"]).resolve(),
        )
        if (
            root not in self.folder.parents
            or root not in self.source.parents
            or self.source == self.folder
        ):
            raise IntegrityError("Runtime copy must stay inside owned root")
        self.snapshot = read(self.source / "source_snapshot.json")
        if self.snapshot["snapshot_sha256"] != settings["snapshot_sha256"]:
            raise IntegrityError("Snapshot identity differs")
        self.verify_source()
        self.folder.mkdir(parents=True, exist_ok=False)
        self.port = settings["port"]
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", self.port))
        for name in ("data", "transactions"):
            if (self.source / name).exists():
                shutil.copytree(self.source / name, self.folder / name)
        for name in ("conf", "logs", "run", "plugins", "cases"):
            (self.folder / name).mkdir()
        options = [
            "server.default_listen_address=127.0.0.1",
            "server.http.enabled=false",
            "server.https.enabled=false",
            "server.bolt.enabled=true",
            f"server.bolt.listen_address=127.0.0.1:{self.port}",
            "dbms.security.auth_enabled=false",
            "initial.dbms.default_database=neo4j",
            "server.databases.default_to_read_only=true",
            "dbms.security.allow_csv_import_from_file_urls=false",
            "server.memory.heap.initial_size=256m",
            "server.memory.heap.max_size=512m",
            "server.memory.pagecache.size=256m",
        ]
        for name in ("data", "logs", "run", "plugins"):
            options.append("server.directories." + name + "=" + str(self.folder / name))
        options.append(
            "server.directories.transaction.logs.root="
            + str(self.folder / "transactions")
        )
        (self.folder / "conf/neo4j.conf").write_text("\n".join(options) + "\n")
        try:
            with (self.folder / "console.log").open("xb") as log:
                self.process = subprocess.Popen(
                    [settings["neo4j_home"] + "/bin/neo4j", "console"],
                    env=dict(
                        os.environ,
                        JAVA_HOME=settings["java_home"],
                        NEO4J_CONF=str(self.folder / "conf"),
                    ),
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                )
            save(
                self.folder / "execution_owner.json",
                dict(
                    hostname=socket.gethostname(),
                    pid=self.process.pid,
                    parent_pid=os.getpid(),
                    source=str(self.source),
                    port=self.port,
                    loopback_only=True,
                    read_only=True,
                ),
            )
            self.driver = GraphDatabase.driver(
                f"bolt://127.0.0.1:{self.port}",
                auth=None,
                connection_timeout=3,
                connection_acquisition_timeout=5,
                max_transaction_retry_time=0,
                max_connection_pool_size=self.workers + 2,
            )
            deadline = time.monotonic() + 90
            while True:
                try:
                    self.driver.verify_connectivity()
                    break
                except Exception:
                    if self.process.poll() is not None or time.monotonic() > deadline:
                        raise
                    time.sleep(1)
            with self.driver.session(
                database="neo4j", default_access_mode="READ"
            ) as session:
                counts = [
                    session.run("MATCH (n) RETURN count(n)").single()[0],
                    session.run("MATCH ()-[r]->() RETURN count(r)").single()[0],
                ]
            # Cold bytes are pinned; graph counts are observed, not invented gold.
            self.ready = dict(
                kind="READY",
                benchmark=settings["benchmark"],
                graph=settings["graph"],
                snapshot_sha256=settings["snapshot_sha256"],
                worker_sha256=digest(Path(__file__).read_bytes()),
                hostname=socket.gethostname(),
                counts=counts,
                read_only_config=True,
                model_calls=0,
                max_case_workers=self.workers,
                case_protocol="thread-affine-v1",
            )
            save(self.folder / "ready.json", self.ready)
            package = Path(settings["reference_package"])
            manifest = read(package / "manifest.json")
            if (
                digest((package / "manifest.json").read_bytes())
                != settings["reference_manifest_sha256"]
            ):
                raise IntegrityError("Reference manifest changed")
            public = package / "public/plain.jsonl"
            if digest(public.read_bytes()) != manifest["files"]["public/plain.jsonl"]:
                raise IntegrityError("Public task population changed")
            ids = [
                json.loads(line)["task_id"] for line in public.read_bytes().splitlines()
            ]
            self.references = PinnedReferences(
                package, settings["reference_manifest_sha256"], ids
            )
        except BaseException:
            self.close()
            raise

    def new_case(self):
        """Each lane gets independent session/transaction state, never a new DB."""
        case = NativeWorker.__new__(NativeWorker)
        for name in ("settings", "folder", "driver", "references", "reference_lock"):
            setattr(case, name, getattr(self, name))
        case.capture = case.last = case.current_identity = None
        return case

    def verify_source(self):
        for name, pin in self.snapshot["files"].items():
            path = (self.source / name).resolve()
            if (
                self.source not in path.parents
                or digest(path.read_bytes()) != pin["sha256"]
            ):
                raise IntegrityError("Cold snapshot bytes changed")

    def end_capture(self):
        if getattr(self, "capture_close_error", False):
            raise IntegrityError("Previous native capture cleanup was not confirmed")
        if self.capture is not None:
            try:
                self.capture.__exit__(None, None, None)
            except BaseException:
                self.capture_close_error = True
                raise
            self.capture = None

    def finish_case(self, reason):
        self.end_capture()
        identity = getattr(self, "current_identity", None)
        if identity:
            path = (
                self.folder
                / "cases"
                / digest(canonical(identity))[:24]
                / "case_closed.json"
            )
            if not path.exists():
                save(
                    path,
                    dict(
                        identity=identity,
                        reason=reason,
                        thread_id=threading.get_ident(),
                        closed_epoch=time.time(),
                    ),
                )

    def execute(self, request):
        self.end_capture()
        self.last = None
        identity = request["identity"]
        if (identity["benchmark"], identity["graph"]) != (
            self.settings["benchmark"],
            self.settings["graph"],
        ):
            raise IntegrityError("Case belongs to a different graph")
        self.current_identity = identity
        folder = self.folder / "cases" / digest(canonical(identity))[:24]
        evidence = PlanEvidence(folder / "capture", max_record_bytes=64000000)
        save(folder / "input.json", request)
        self.capture = NativeReadCapture(
            self.driver,
            snapshot_sha256=self.settings["snapshot_sha256"],
            database="neo4j",
            transaction_timeout=180,
            max_rows=1000000,
            max_answer_bytes=64000000,
            emit=evidence,
        )
        self.capture.__enter__()
        method = identity["method"]
        if method in CYPHER_METHODS:
            answer = self.capture.read(
                request["cypher"], request.get("parameters", {}), purpose="method"
            )
        else:
            schema = NativeSchema(
                request["schema"],
                format=(
                    "MTQ" if identity["benchmark"] == "MindTheQuery" else "ZOGRASCOPE"
                ),
            )
            prepared = PreparedPlan(request["plan"], schema)
            if method == "native-set-count":
                if prepared.operation == "single" or prepared.output != "count":
                    answer = dict(
                        status="not_applicable", complete=False, rows=None, columns=None
                    )
                else:
                    compiled = native_set_count_query(prepared)
                    answer = self.capture.read(
                        compiled["query"], compiled["parameters"], purpose="control"
                    )
            else:
                chain = NativePlanChain(prepared, self.capture, evidence)
                result = chain.run_method(
                    strategy="full_detail" if method == "correct-detail" else "contract"
                )
                answer = result["answer"]
                save(folder / "plan_result.json", result)
        save(folder / "final.json", answer)
        self.last = dict(
            identity=identity,
            folder=folder,
            answer=answer,
            sha256=digest(canonical(answer)),
        )
        if answer.get("complete") is not True:
            self.end_capture()
        return dict(answer=answer, remote_final_sha256=self.last["sha256"])

    def score(self, request):
        if (
            self.last is None
            or self.capture is None
            or self.last["sha256"] != request["answer_sha256"]
            or ("identity" in request and self.last["identity"] != request["identity"])
        ):
            raise IntegrityError("Score requires the last sealed native answer")
        try:
            with getattr(self, "reference_lock", nullcontext()):
                item = self.references.get(self.last["identity"]["task_id"])
            if not item.get("query"):
                result = dict(status="reference_missing", value=None)
            else:
                reference = self.capture.read(item["query"], {}, purpose="evaluator")
                # Auxiliary order policy is declared independently of prediction.
                ordered = "order by" in item["query"].lower()
                exact = compare_complete_auxiliary(
                    reference, self.last["answer"], ordered=ordered
                )
                save(
                    self.last["folder"] / "reference.json",
                    dict(source=item, answer=reference, ordered=ordered),
                )
                result = dict(
                    status="scored_auxiliary",
                    value=exact,
                    primary_score=None,
                    profile="strict-typed-auxiliary-v1",
                    author_comparator_authenticated=False,
                )
            save(self.last["folder"] / "score.json", result)
            return result
        finally:
            self.end_capture()

    def close(self):
        try:
            self.end_capture()
            if self.driver:
                self.driver.close()
        finally:
            if self.process and self.process.poll() is None:
                os.killpg(self.process.pid, signal.SIGTERM)
                try:
                    self.process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=10)
            self.verify_source()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("settings", type=Path)
    parser.add_argument("--instance-id")
    args = parser.parse_args()
    worker = NativeWorker(instance_settings(read(args.settings), args.instance_id))
    print(json.dumps(worker.ready), flush=True)
    lanes = CaseLanes(
        worker.new_case,
        worker.workers,
        idle_seconds=worker.settings.get("case_idle_seconds", 300),
    )
    output_lock = threading.Lock()

    def reply(request, future):
        try:
            result = future.result()
        except (IntegrityError, PermissionError, FileExistsError, OSError) as error:
            result = dict(
                status="integrity_error",
                error_type=type(error).__name__,
                error=str(error)[:1000],
            )
        except UnsupportedSelection as error:
            result = dict(
                status="UNSUPPORTED",
                error_type=type(error).__name__,
                error=str(error)[:1000],
            )
        except Exception as error:
            result = dict(
                status="execution_error",
                error_type=type(error).__name__,
                error=str(error)[:1000],
                database_code=getattr(error, "code", None),
            )
        with output_lock:
            print(
                json.dumps(dict(result, id=request["id"]), ensure_ascii=False),
                flush=True,
            )

    try:
        for line in sys.stdin:
            request = json.loads(line)
            future = lanes.submit(request)
            future.add_done_callback(lambda done, request=request: reply(request, done))
    finally:
        try:
            lanes.close()
        finally:
            worker.close()


if __name__ == "__main__":
    main()
