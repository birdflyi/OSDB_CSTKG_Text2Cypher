"""One owned local graph copy at a time, with pinned cold bytes and deadlines.

Development only. No model, import, source-store startup, foreign process stop,
lock recovery or automatic database statement replay. Windows Neo4j runtime.
"""

from contextlib import contextmanager
from pathlib import Path
import json, os, re, shutil, socket, subprocess, threading, time, uuid
import requests
from neo4j import GraphDatabase
from osdb_scoped_attempts_dev import read, sha, immutable, wire
from osdb_model_read_executor_dev_v2 import ModelReadExecutor
from accept_osdb_actor_graph_references_20261026 import (
    stores,
    store_directories,
    stop_owned,
    live,
)

VERSION = "osdb-owned-graph-factory-dev-2"


class ClientDeadlineExceeded(RuntimeError):
    pass


def bounded_database_call(function, abort_owned, seconds):
    """A deadline on this known driver's I/O, using its supplied scope cancellation.

    Admission supplies whole-graph stop; method cases do not use this watchdog.
    A timeout is evidence, never permission to replay a statement.
    """
    expired = threading.Event()
    abort_errors = []

    def abort():
        expired.set()
        try:
            abort_owned()
        except BaseException as exc:
            abort_errors.append(type(exc).__name__)

    timer = threading.Timer(seconds, abort)
    timer.daemon = True
    timer.start()
    try:
        try:
            result = function()
        except BaseException as exc:
            if expired.is_set():
                raise ClientDeadlineExceeded(
                    "Owned database client deadline expired"
                ) from exc
            raise
        if expired.is_set():
            raise ClientDeadlineExceeded("Owned database client deadline expired")
        return result
    finally:
        timer.cancel()
        timer.join()
        if abort_errors:
            raise RuntimeError(
                "Owned deadline cleanup failed; preserve graph lock: "
                + ",".join(abort_errors)
            )


class FactoryExecutor(ModelReadExecutor):
    def _statement(self, query, params, kind, consumer=None, timeout=60):
        # Full physical fingerprint is offline admission, not a method query.
        if kind in ("admit_nodes", "admit_edges"):
            if timeout != 240:
                raise RuntimeError("Unexpected inherited offline scan budget")
            timeout = 600
        return super()._statement(query, params, kind, consumer, timeout=timeout)


def case_executor(driver, schema, evidence, admission, graph_stopped):
    executor = FactoryExecutor(driver, schema, evidence)
    executor.receipt = dict(admission["proof"])
    executor.admitted = True
    immutable(Path(evidence) / "shared_admission.json", admission)

    def execute(query, params):
        if graph_stopped():
            raise RuntimeError("Owned graph stopped; no further reads")
        # Sync Neo4j Session has no public thread-safe cancel API. The server
        # enforces EXPLAIN/read deadlines (30/60 s). Never kill the shared JVM
        # on a case watchdog. A still-running client retains its worker slot
        # and the enclosing graph lease until it has actually returned.
        return executor.execute(query, params)

    return {"execute": execute, "physical_statement_count": lambda: executor.number}


def memory_setting(value):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*[mMgG]", value):
        raise ValueError("Memory setting must be a positive MiB/GiB value")
    return value


class OwnedGraphFactory:
    def __init__(
        self,
        directory,
        inventory,
        inventory_sha256,
        profiles,
        home,
        lock_path,
        *,
        http=18531,
        bolt=18532,
        startup_seconds=32,
        workers=1,
        connection_pool_size=None,
        cpu_count=2,
        heap_initial="256m",
        heap_max="512m",
        pagecache="128m",
    ):
        self.directory = Path(directory).resolve()
        self.inventory = Path(inventory)
        if sha(self.inventory) != inventory_sha256:
            raise RuntimeError("Cold inventory changed")
        self.pins = read(self.inventory)["cold_receipts"]
        self.profiles = profiles
        self.home = Path(home).resolve()
        self.lock_path = Path(lock_path)
        if set(self.pins) != {"redis", "duckdb", "iotdb", "elasticsearch"}:
            raise ValueError("Four graph pins required")
        if type(http) is not int or type(bolt) is not int or http == bolt:
            raise ValueError("Distinct ports required")
        self.http, self.bolt = http, bolt
        if not 32 <= startup_seconds <= 600:
            raise ValueError("Bounded startup budget required")
        self.startup_seconds = startup_seconds
        if any(type(n) is not int or not 1 <= n <= 100 for n in (workers, cpu_count)):
            raise ValueError("Worker and CPU limits must be in 1..100")
        connection_pool_size = (
            workers + 2 if connection_pool_size is None else connection_pool_size
        )
        if (
            type(connection_pool_size) is not int
            or not workers <= connection_pool_size <= 102
        ):
            raise ValueError("Pool must cover workers and stay bounded")
        self.workers, self.cpu_count = workers, cpu_count
        self.connection_pool_size = connection_pool_size
        self.heap_initial, self.heap_max, self.pagecache = map(
            memory_setting, (heap_initial, heap_max, pagecache)
        )
        self.active = False
        self.busy = False
        self.unknown = False
        self.used = set()

    def __enter__(self):
        if live(self.http) or live(self.bolt):
            raise RuntimeError("Selected local port occupied; never stop it")
        self.lock_wire = wire(
            {
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "directory": str(self.directory),
                "lease": uuid.uuid4().hex,
                "kind": VERSION,
            }
        )
        self.claimed_locks = []
        lock_paths = [self.lock_path] + [
            self.lock_path.parent / (".osdb-port-" + str(port) + ".lock")
            for port in (self.http, self.bolt)
        ]
        try:
            for path in lock_paths:
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                self.claimed_locks.append(path)
                with os.fdopen(descriptor, "wb") as f:
                    f.write(self.lock_wire)
                    f.flush()
                    os.fsync(f.fileno())
            self.directory.mkdir(parents=True, exist_ok=False)
            immutable(self.directory / "factory_owner.json", json.loads(self.lock_wire))
            self.active = True
            return self
        except BaseException:
            for path in reversed(self.claimed_locks):
                if path.read_bytes() == self.lock_wire:
                    path.unlink()
            raise

    def __exit__(self, *unused):
        self.active = False
        if self.busy or self.unknown:
            raise RuntimeError("Graph ownership or shutdown unresolved; lock retained")
        if any(path.read_bytes() != self.lock_wire for path in self.claimed_locks):
            raise RuntimeError("Changed graph factory lock retained")
        for path in reversed(self.claimed_locks):
            path.unlink()

    @contextmanager
    def __call__(self, graph):
        if (
            not self.active
            or self.busy
            or self.unknown
            or graph not in self.pins
            or graph in self.used
        ):
            raise RuntimeError(
                "Inactive/busy/unknown/repeated graph; no automatic graph recovery"
            )
        if self.lock_path.read_bytes() != self.lock_wire:
            raise RuntimeError("Factory ownership changed")
        if live(self.http) or live(self.bolt):
            raise RuntimeError("Port occupied before boot")
        self.used.add(graph)
        folder = self.directory / graph
        folder.mkdir()
        pin = self.pins[graph]
        source = Path(pin["root"]).resolve()
        profile = self.profiles[graph]
        profile_path = Path(profile["path"])
        if sha(profile_path) != profile["sha256"]:
            raise RuntimeError("Pinned profile changed")
        schema = read(profile_path)
        cold = source / "new_store_manifest_after_shutdown.json"
        process = driver = session = stdout = stderr = None
        stopped = None
        stop_mutex = threading.Lock()
        admitted = None
        source_verified = False

        def stop():
            nonlocal stopped
            with stop_mutex:
                if stopped is None and process is not None:
                    stopped = stop_owned(process, folder / "conf", self.http)
                    stopped["bolt_port_closed"] = not live(self.bolt)
                    if not stopped["bolt_port_closed"]:
                        raise RuntimeError("Owned Bolt port did not close")
                return stopped

        self.busy = True
        try:
            if sha(cold) != pin["cold_manifest_sha256"]:
                raise RuntimeError("Cold manifest changed")
            expected_bytes = read(cold)
            if stores(source) != expected_bytes:
                raise RuntimeError("Actual cold source bytes differ")
            source_verified = True
            immutable(folder / "source_bytes_before.json", expected_bytes)
            if (
                shutil.disk_usage(folder.anchor).free
                < sum(v["bytes"] for v in expected_bytes.values()) + 2 * 1024**3
            ):
                raise RuntimeError("Insufficient space for a new isolated copy")
            for name in store_directories(source):
                shutil.copytree(source / name, folder / name)
            if stores(folder) != expected_bytes:
                raise RuntimeError("Copied bytes differ; do not boot")
            for name in ("conf", "logs", "run", "plugins"):
                (folder / name).mkdir()
            config = [
                "server.default_listen_address=127.0.0.1",
                f"server.http.listen_address=127.0.0.1:{self.http}",
                "server.http.enabled=true",
                "server.https.enabled=false",
                "server.bolt.enabled=true",
                f"server.bolt.listen_address=127.0.0.1:{self.bolt}",
                f"server.bolt.advertised_address=127.0.0.1:{self.bolt}",
                "dbms.security.auth_enabled=false",
                "initial.dbms.default_database=neo4j",
                f"server.memory.heap.initial_size={self.heap_initial}",
                f"server.memory.heap.max_size={self.heap_max}",
                f"server.memory.pagecache.size={self.pagecache}",
                f"server.jvm.additional=-XX:ActiveProcessorCount={self.cpu_count}",
                "dbms.security.allow_csv_import_from_file_urls=false",
            ]
            for name in ("data", "logs", "run", "plugins"):
                config.append(
                    "server.directories."
                    + name
                    + "="
                    + str(folder / name).replace("\\", "/")
                )
            if "transactions" in store_directories(source):
                config.append(
                    "server.directories.transaction.logs.root="
                    + str(folder / "transactions").replace("\\", "/")
                )
            (folder / "conf/neo4j.conf").write_text(
                "\n".join(config) + "\n", encoding="utf-8"
            )
            launch = folder / "launch_owned.ps1"
            launch.write_text(
                "& '" + str(self.home / "bin/neo4j.bat") + "' console\n",
                encoding="utf-8",
            )
            stdout = (folder / "console_stdout.log").open("wb")
            stderr = (folder / "console_stderr.log").open("wb")
            process = subprocess.Popen(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(launch),
                ],
                env=dict(
                    os.environ,
                    NEO4J_CONF=str(folder / "conf"),
                    NEO4J_HOME=str(self.home),
                ),
                stdout=stdout,
                stderr=stderr,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            immutable(
                folder / "execution_owner.json",
                {
                    "hostname": socket.gethostname(),
                    "runner_pid": os.getpid(),
                    "wrapper_pid": process.pid,
                    "conf": str(folder / "conf"),
                    "graph": graph,
                    "source": str(source),
                },
            )
            session = requests.Session()
            session.trust_env = False
            ready = False
            deadline = time.monotonic() + self.startup_seconds
            index = 0
            while time.monotonic() < deadline:
                time.sleep(min(2, max(0, deadline - time.monotonic())))
                index += 1
                receipt = {"attempt": index, "startup_seconds": self.startup_seconds}
                try:
                    response = session.get(
                        f"http://127.0.0.1:{self.http}/", timeout=(2, 3)
                    )
                    receipt["http_status"] = response.status_code
                    ready = response.status_code == 200
                except requests.RequestException as exc:
                    receipt["error_type"] = type(exc).__name__
                immutable(folder / f"readiness_{index}.json", receipt)
                if ready or process.poll() is not None:
                    break
            if not ready:
                raise RuntimeError("Bounded readiness failed; no restart")
            driver = GraphDatabase.driver(
                f"bolt://127.0.0.1:{self.bolt}",
                auth=None,
                connection_timeout=3,
                connection_acquisition_timeout=5,
                max_transaction_retry_time=0,
                max_connection_pool_size=self.connection_pool_size,
            )
            executor = FactoryExecutor(driver, schema, folder / "reads")
            actual = bounded_database_call(executor.admit, stop, 1240)
            if any(
                actual[k] != pin[k]
                for k in ("nodes", "edges", "semantic_snapshot_sha256")
            ):
                raise RuntimeError("Current physical graph fingerprint differs")
            admitted = {
                "graph": graph,
                "admitted": True,
                "model_views_equal": True,
                "current_snapshot_sha256": actual["semantic_snapshot_sha256"],
                "client_and_server_deadlines_accepted": True,
                "factory_version": VERSION,
                "proof": actual,
                "cold_bytes_sha256": pin["cold_manifest_sha256"],
                "source_store_never_started": True,
                "same_executor_for_both_methods": True,
                "offline_scan_seconds": 600,
                "explain_seconds": 30,
                "method_read_seconds": 60,
                "client_answer_call_seconds": None,
                "client_admission_call_seconds": 1240,
                "timeout_action": "Admission timeout stops owned JVM; cases use server30/60 deadlines; no client-wide kill or replay",
                "max_case_workers": self.workers,
                "formal_admission": False,
            }
            immutable(folder / "admission.json", admitted)

            def make_case(evidence):
                return case_executor(
                    driver, schema, evidence, admitted, lambda: stopped is not None
                )

            # The historical single-case callback remains available. New consumers
            # select make_case so counters, deadlines and evidence never mix.
            default_case = make_case(folder / "default_case_reads")
            yield {
                "admission": admitted,
                "schema": schema,
                "execute": default_case["execute"],
                "physical_statement_count": default_case["physical_statement_count"],
                "case_executor": make_case,
                "abort_owned": stop,
            }

        except BaseException as exc:
            immutable(
                folder / "failure.json",
                {
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:1200],
                    "no_model_or_query_replay": True,
                },
            )
            raise
        finally:
            try:
                # Stop on ownership proof before driver teardown can block on I/O.
                receipt = stop()
                if receipt is not None:
                    immutable(folder / "shutdown_receipt.json", receipt)
                if driver:
                    driver.close()
                if session:
                    session.close()
                if stdout:
                    stdout.close()
                if stderr:
                    stderr.close()
                if source_verified:
                    if stores(source) != expected_bytes:
                        raise RuntimeError("Cold source changed during use")
                    immutable(folder / "source_bytes_after.json", expected_bytes)
            except BaseException:
                self.unknown = True
                raise
            finally:
                self.busy = False
