from __future__ import annotations

import argparse
from dataclasses import asdict
import importlib
import json
import os
import re
import shutil
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from ..config import DEFAULT_TIMEOUTS, canonical, digest, timeouts_from
from ..campaign import code_manifest
from ..execution import serve_stream
from ..errors import IntegrityError
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
                result_bytes=len(
                    json.dumps(candidates, ensure_ascii=False).encode("utf-8")
                ),
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
            cursor = session.run(
                self.query_class(query, timeout=self.timeouts.query), parameters
            )
            columns, rows, complete, size = list(cursor.keys()), [], True, 2
            for record in cursor:
                values = [self.normalize(record[c]) for c in columns]
                row_size = len(
                    json.dumps(values, ensure_ascii=False).encode("utf-8")
                ) + (2 if rows else 0)
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
        raise IntegrityError("Exact hash-pinned read helper set required")
    for name in names:
        if digest((harness / name).read_bytes()) != expected[name]:
            raise IntegrityError("Database read helper changed")
        module = sys.modules.get(Path(name).stem)
        if module and Path(module.__file__).resolve().parent != harness.resolve():
            raise IntegrityError("Conflicting helper already imported")
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


def instance_paths(settings):
    """Select an already validated owned copy; never clone or rewrite cold data."""
    root = Path(settings["root"]).resolve()
    folder = Path(
        settings.get("instance_dir", root / "test_runtime_v1" / settings["graph"])
    ).resolve()
    if root not in folder.parents:
        raise IntegrityError("Graph instance must be strictly inside owned root")
    port = settings.get("bolt_port", 17688)
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError("Private Bolt port required")
    default_lock = (
        folder / ".engine-instance.lock"
        if "instance_dir" in settings
        else root / ".public-runtime.lock"
    )
    lock = Path(settings.get("instance_lock_path", default_lock)).resolve()
    if root not in lock.parents:
        raise IntegrityError("Instance lock outside owner root")
    return folder, port, lock


def check_instance_conf(folder, port):
    """An explicit copy cannot point its mutable directories at another instance."""
    values = {}
    for line in (folder / "conf/neo4j.conf").read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    if values.get("server.bolt.listen_address") != "127.0.0.1:" + str(port):
        raise IntegrityError("Instance config must bind assigned loopback Bolt port")
    for key in ("data", "logs", "run", "transaction.logs.root"):
        value = values.get("server.directories." + key)
        if value is None:
            if key == "transaction.logs.root":
                continue
            raise IntegrityError("Explicit instance directory missing: " + key)
        target = Path(value)
        if not target.is_absolute() or folder not in target.resolve().parents:
            raise IntegrityError("Mutable Neo4j directory outside instance: " + key)
    for protocol in ("http", "https"):
        if values.get("server." + protocol + ".enabled") != "false":
            raise IntegrityError(
                "Explicit read worker requires HTTP and HTTPS disabled"
            )


def runtime_conf(settings, folder, diagnostics):
    """Resource overrides live in a fresh launch config, not the validated copy."""
    overrides = {}
    cpu = settings.get("cpu_count")
    if cpu is not None:
        if type(cpu) is not int or not 1 <= cpu <= 100:
            raise ValueError("CPU allocation must be in 1..100")
        overrides["server.jvm.additional"] = "-XX:ActiveProcessorCount=" + str(cpu)
    for setting, key in (
        ("heap_initial", "server.memory.heap.initial_size"),
        ("heap_max", "server.memory.heap.max_size"),
        ("pagecache", "server.memory.pagecache.size"),
    ):
        if setting in settings:
            value = settings[setting]
            if not isinstance(value, str) or not re.fullmatch(
                r"[1-9][0-9]*[mMgG]", value
            ):
                raise ValueError("Invalid memory resource setting")
            overrides[key] = value
    if not overrides:
        return folder / "conf"
    target = diagnostics / "launch_conf"
    shutil.copytree(folder / "conf", target)
    lines = (target / "neo4j.conf").read_text().splitlines()
    retained = []
    for line in lines:
        key, _, value = line.partition("=")
        if key.strip() in overrides:
            if key.strip() == "server.jvm.additional" and not value.strip().startswith(
                "-XX:ActiveProcessorCount="
            ):
                retained.append(line)
            continue
        retained.append(line)
    retained.extend(key + "=" + value for key, value in overrides.items())
    (target / "neo4j.conf").write_text("\n".join(retained) + "\n")
    return target


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
                Journal(diagnostics / "startup.jsonl").append(
                    "worker_startup", **record
                )
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
            raise IntegrityError("Execution owner/hostname mismatch")
        diagnostics = (
            self.root
            / "worker_diagnostics"
            / (settings["graph"] + "-" + str(os.getpid()) + "-" + str(time.time_ns()))
        )
        diagnostics.mkdir(parents=True, exist_ok=False)
        self.diagnostics = diagnostics
        milestone("owner_verified")
        self.folder, self.port, self.lock_path = instance_paths(settings)
        if "instance_dir" in settings:
            check_instance_conf(self.folder, self.port)
            identity = json.loads((self.folder / "instance_identity.json").read_bytes())
            if (
                identity.get("graph") != settings["graph"]
                or identity.get("graph_sha256") != settings["graph_sha256"]
            ):
                raise IntegrityError("Explicit instance snapshot identity differs")
        validation = json.loads((self.folder / "graph_validation.json").read_text())
        if validation.get("all_properties_labels_and_endpoints_match") is not True:
            raise IntegrityError("Full graph validation is missing")
        source = (
            self.root
            / "cypherbench_data/upstream/graphs/simplekg"
            / (settings["graph"] + "_simplekg.json")
        )
        if digest(source.read_bytes()) != settings["graph_sha256"]:
            raise IntegrityError("Graph source snapshot differs")
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
        if credentials["uri"] != "bolt://127.0.0.1:" + str(self.port):
            raise IntegrityError("Unexpected database URI")
        check_bolt_available(self.port)
        # Share the old ownership lock, so old/new workers cannot open this DB together.
        self.port_lock = self.root / (".engine-bolt-" + str(self.port) + ".lock")
        write_new(self.lock_path, self.lock_bytes)
        try:
            write_new(self.port_lock, self.lock_bytes)
        except BaseException:
            if self.lock_path.read_bytes() == self.lock_bytes:
                self.lock_path.unlink()
            raise
        try:
            environment = dict(
                os.environ,
                JAVA_HOME=runtime["java_home"],
                NEO4J_CONF=str(runtime_conf(settings, self.folder, diagnostics)),
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
                raise IntegrityError("Graph identity/counts differ")
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
                max_case_workers=settings["workers"],
                instance_dir=str(self.folder),
                bolt_port=self.port,
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
                check_bolt_available(self.port)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        "Owned database has not released Bolt; lock retained"
                    )
                time.sleep(0.2)
        locks = [self.port_lock, self.lock_path]
        if any(
            path.exists() and path.read_bytes() != self.lock_bytes for path in locks
        ):
            raise IntegrityError("Foreign runtime/port lock retained")
        for path in locks:
            if path.exists():
                path.unlink()


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
