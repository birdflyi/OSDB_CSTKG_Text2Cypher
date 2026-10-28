from __future__ import annotations

import argparse
from dataclasses import asdict
import importlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from ..config import DEFAULT_TIMEOUTS, canonical, digest, timeouts_from
from ..campaign import code_manifest
from ..execution import serve_stream
from ..journal import Journal, write_new


class ReadHandler:
    """Same read guard, normalization, EXPLAIN, rows and bytes as the old worker.

    Each call creates its own Neo4j session. The immutable candidate index is
    shared; model permits and model credentials never enter this process.
    """

    def __init__(
        self,
        driver,
        resolver,
        read_query,
        normalize,
        query_class,
        timeouts=DEFAULT_TIMEOUTS,
    ):
        self.driver, self.resolver = driver, resolver
        self.timeouts = timeouts_from(timeouts)
        self.read_query, self.normalize, self.query_class = (
            read_query,
            normalize,
            query_class,
        )

    def __call__(self, request):
        started = time.monotonic()
        if request.get("kind") == "candidates":
            candidates = self.resolver.find(request["question"])
            return dict(
                status="CANDIDATES",
                candidates=candidates,
                lookup_seconds=time.monotonic() - started,
                result_bytes=len(json.dumps(candidates, ensure_ascii=False).encode("utf-8")),
            )
        query = self.read_query(request["query"])
        parameters = request.get("parameters", {})
        if not isinstance(parameters, dict):
            raise ValueError("Expected parameter object")
        # These are protocol limits, not arbitrary per-question tuning knobs.
        with self.driver.session(default_access_mode="READ") as session:
            explained = session.run(
                self.query_class("EXPLAIN " + query, timeout=self.timeouts.explain),
                parameters,
            ).consume()
            if explained.query_type != "r":
                raise ValueError("EXPLAIN did not certify read-only query")
            after_explain = time.monotonic()
            cursor = session.run(self.query_class(query, timeout=self.timeouts.query), parameters)
            columns, rows, complete, size = list(cursor.keys()), [], True, 2
            for record in cursor:
                values = [self.normalize(record[c]) for c in columns]
                row_size = len(json.dumps(values, ensure_ascii=False).encode("utf-8")) + (
                    2 if rows else 0
                )
                if len(rows) == 1000000 or size + row_size > 64 * 1024 * 1024:
                    complete = False
                    break
                rows.append(values)
                size += row_size
            cursor.consume()
        return dict(
            status="EXECUTED" if complete else "TRUNCATED",
            complete=complete,
            columns=columns,
            rows=rows,
            result_bytes=size,
            explain_seconds=after_explain - started,
            db_seconds=time.monotonic() - after_explain,
        )


def load_helpers(harness: Path, expected: dict):
    names = ("public_read_guard.py", "entity_candidates.py", "smoke_train_database.py")
    if set(expected) != set(names):
        raise ValueError("Exact hash-pinned read helper set required")
    for name in names:
        if digest((harness / name).read_bytes()) != expected[name]:
            raise ValueError("Database read helper changed")
        module = sys.modules.get(Path(name).stem)
        if module and Path(module.__file__).resolve().parent != harness.resolve():
            raise ValueError("Conflicting helper already imported")
    sys.path.insert(0, str(harness))
    guard = importlib.import_module("public_read_guard")
    candidates = importlib.import_module("entity_candidates")
    normalization = importlib.import_module("smoke_train_database")
    return guard.read_query, candidates.EntityCandidates, normalization.normalize


def check_bolt_available(port=17688):
    # A stopped server can leave TIME_WAIT sockets. These do not represent a
    # listening database; match the server's reuse policy without reusing a live
    # listener. SO_REUSEPORT is deliberately not enabled.
    with socket.socket() as check:
        check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        check.bind(("127.0.0.1", port))


class OwnedRuntime:
    def __init__(self, settings: dict):
        startup_started = time.monotonic()
        diagnostics = None

        def milestone(phase):
            record = dict(
                graph=settings["graph"],
                phase=phase,
                elapsed_seconds=time.monotonic() - startup_started,
                epoch=time.time(),
            )
            if diagnostics is not None:
                Journal(diagnostics / "startup.jsonl").append("worker_startup", **record)
            print(
                json.dumps(dict(kind="worker_startup", **record)),
                file=sys.stderr,
                flush=True,
            )

        milestone("identity_and_source_verification")
        from neo4j import GraphDatabase, Query

        self.process = self.driver = None
        self.lock_bytes = canonical(
            dict(pid=os.getpid(), graph=settings["graph"], kind="ENGINE_OWNED_GRAPH")
        )
        self.root = Path(settings["root"]).resolve()
        owner = json.loads((self.root / "execution_owner.json").read_text())
        if (
            owner["hostname"] != socket.gethostname()
            or owner["hostname"] != settings["expected_hostname"]
        ):
            raise ValueError("Execution owner/hostname mismatch")
        diagnostics = (
            self.root / "worker_diagnostics" / (settings["graph"] + "-" + str(os.getpid()))
        )
        diagnostics.mkdir(parents=True, exist_ok=False)
        self.diagnostics = diagnostics
        milestone("owner_verified")
        self.folder = self.root / "test_runtime_v1" / settings["graph"]
        validation = json.loads((self.folder / "graph_validation.json").read_text())
        if validation.get("all_properties_labels_and_endpoints_match") is not True:
            raise ValueError("Full graph validation is missing")
        source = (
            self.root
            / "cypherbench_data/upstream/graphs/simplekg"
            / (settings["graph"] + "_simplekg.json")
        )
        if digest(source.read_bytes()) != settings["graph_sha256"]:
            raise ValueError("Graph source snapshot differs")
        guard, candidates, normalize = load_helpers(
            Path(settings["harness"]), settings["helper_sha256"]
        )
        index_start = time.monotonic()
        milestone("candidate_index")
        resolver = candidates(json.loads(source.read_text())["entities"])
        index_seconds = time.monotonic() - index_start
        milestone("candidate_index_complete")
        runtime = json.loads((self.root / "runtime_receipt.json").read_text())
        credentials = json.loads((self.folder / "db_connection.json").read_text())
        if credentials["uri"] != "bolt://127.0.0.1:17688":
            raise ValueError("Unexpected database URI")
        check_bolt_available()
        # Share the old ownership lock, so old/new workers cannot open this DB together.
        self.lock_path = self.root / ".public-runtime.lock"
        write_new(self.lock_path, self.lock_bytes)
        try:
            environment = dict(
                os.environ,
                JAVA_HOME=runtime["java_home"],
                NEO4J_CONF=str(self.folder / "conf"),
            )
            log_path = self.folder / ("engine_worker_" + str(time.time_ns()) + ".log")
            with log_path.open("x") as log:
                self.process = subprocess.Popen(
                    [runtime["neo4j_home"] + "/bin/neo4j", "console"],
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            deadline = time.monotonic() + 90
            milestone("database_connectivity")
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError("Owned database process exited")
                try:
                    self.driver = GraphDatabase.driver(
                        credentials["uri"],
                        auth=(credentials["user"], credentials["password"]),
                        connection_timeout=2,
                        max_connection_pool_size=settings["workers"] + 2,
                    )
                    self.driver.verify_connectivity()
                    break
                except Exception:
                    if self.driver:
                        self.driver.close()
                        self.driver = None
                    time.sleep(1)
            if self.driver is None:
                raise RuntimeError("Owned DB startup deadline exceeded")
            milestone("graph_counts")
            with self.driver.session() as session:
                counts = [
                    session.run("MATCH (n) RETURN count(n) AS n").single()["n"],
                    session.run("MATCH ()-[r]->() RETURN count(r) AS n").single()["n"],
                ]
            if counts != [validation["nodes"], validation["edges"]]:
                raise ValueError("Graph identity/counts differ")
            milestone("graph_counts_complete")
            self.handler = ReadHandler(
                self.driver,
                resolver,
                guard,
                normalize,
                Query,
                timeouts=settings.get("timeouts"),
            )
            self.ready = dict(
                kind="READY",
                hostname=socket.gethostname(),
                graph=settings["graph"],
                graph_sha256=settings["graph_sha256"],
                nodes=counts[0],
                edges=counts[1],
                candidate_index_seconds=index_seconds,
                worker_version="engine-neo4j-v1",
                worker_source_sha256=digest(Path(__file__).read_bytes()),
                runtime_code_sha256=digest(canonical(code_manifest())),
                workers=settings["workers"],
                diagnostics_dir=str(diagnostics),
                worker_pid=os.getpid(),
                max_rows=1000000,
                max_result_bytes=64 * 1024 * 1024,
                timeouts=asdict(self.handler.timeouts),
            )
            write_new(diagnostics / "ready.json", canonical(self.ready))
            milestone("ready_persisted")
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.driver:
            self.driver.close()
        if self.process and self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=10)
        # The launcher can exit before Java finishes releasing Bolt. Do not
        # release ownership and start the next graph during that shutdown gap.
        deadline = time.monotonic() + 10
        while True:
            try:
                check_bolt_available()
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("Owned database has not released Bolt; lock retained")
                time.sleep(0.2)
        if self.lock_path.exists():
            if self.lock_path.read_bytes() != self.lock_bytes:
                raise RuntimeError("Foreign runtime lock retained")
            self.lock_path.unlink()


def main():
    parser = argparse.ArgumentParser(
        description="Owned concurrent read-only Neo4j JSON-lines worker"
    )
    parser.add_argument("settings", type=Path)
    args = parser.parse_args()
    settings = json.loads(args.settings.read_text(encoding="utf-8"))
    if type(settings["workers"]) is not int or settings["workers"] < 1:
        raise ValueError("Positive configured DB concurrency required")
    started = time.monotonic()
    runtime = OwnedRuntime(settings)

    def emit(value):
        print(json.dumps(value, ensure_ascii=False), flush=True)

    try:
        emit(dict(runtime.ready, startup_roundtrip_seconds=time.monotonic() - started))
        serve_stream(sys.stdin, emit, runtime.handler, settings["workers"])
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
