"""One owned TuGraph copy for offline smoke. No model client or references."""

from __future__ import annotations

import argparse
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [
    str(ROOT),
    str(ROOT / "experiment-harness/public_benchmark_v1"),
    str(ROOT / "operations/analysis"),
]
from engine.config import canonical, digest
from engine.errors import IntegrityError
from engine.journal import write_new
from tugraph_execution import (
    LocalRestTransport,
    NativeQueryJournal,
    execute_native_plan,
)


def save(path, value):
    write_new(path, canonical(value))


class TuGraphWorker:
    def __init__(self, settings):
        self.settings = settings
        self.process = None
        root = Path(settings["owner_root"]).resolve()
        self.folder = Path(settings["output"]).resolve()
        if (
            root not in self.folder.parents
            or json.loads((root / "execution_owner.json").read_bytes())["hostname"]
            != socket.gethostname()
        ):
            raise IntegrityError("TuGraph runtime owner differs")
        source = Path(settings["data"]).resolve()
        if root not in source.parents:
            raise IntegrityError("Data outside owned root")
        for name, expected in settings["data_sha256"].items():
            path = (source / name).resolve()
            if source not in path.parents or digest(path.read_bytes()) != expected:
                raise IntegrityError("TuGraph public import data changed")
        runtime = Path(settings["runtime"])
        for name, expected in settings["runtime_sha256"].items():
            if digest((runtime / name).read_bytes()) != expected:
                raise IntegrityError("TuGraph executable changed")
        self.folder.mkdir(parents=True, exist_ok=False)
        port = settings["port"]
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))
        environment = dict(
            os.environ,
            LD_LIBRARY_PATH=str(runtime / "lib"),
            OMP_NUM_THREADS="2",
            OPENBLAS_NUM_THREADS="2",
        )
        config = json.loads((source / "import_config.json").read_bytes())
        for entry in config["files"]:
            name = Path(entry["path"]).name
            if name not in settings["data_sha256"]:
                raise IntegrityError("Undeclared CSV dependency")
            entry["path"] = str(source / name)
        save(self.folder / "import_config.json", config)
        command = [
            str(runtime / "bin/lgraph_import"),
            "--online",
            "false",
            "-c",
            str(self.folder / "import_config.json"),
            "--dir",
            str(self.folder / "db"),
            "--graph",
            settings["graph"],
            "--overwrite",
            "false",
            "--continue_on_error",
            "false",
            "--parse_block_threads",
            "2",
            "--parse_file_threads",
            "2",
            "--generate_sst_threads",
            "2",
            "--read_rocksdb_threads",
            "2",
        ]
        with (self.folder / "import.log").open("xb") as log:
            imported = subprocess.run(
                command,
                cwd=self.folder,
                env=environment,
                stdout=log,
                stderr=log,
                timeout=600,
            )
        save(
            self.folder / "import_result.json",
            dict(returncode=imported.returncode, graph=settings["graph"]),
        )
        if imported.returncode:
            raise RuntimeError("TuGraph import failed; retained import.log")
        server = dict(
            directory=str(self.folder / "db"),
            log_dir=str(self.folder / "logs"),
            host="127.0.0.1",
            port=port,
            bolt_port=0,
            enable_rpc=False,
            enable_ha=False,
            thread_limit=2,
            verbose=1,
            log_file_size=16,
            max_n_log_files=2,
            enable_audit_log=False,
            is_cypher_v2=True,
        )
        save(self.folder / "server.json", server)
        try:
            with (self.folder / "console.log").open("xb") as log:
                self.process = subprocess.Popen(
                    [
                        str(runtime / "bin/lgraph_server"),
                        "--config",
                        str(self.folder / "server.json"),
                        "-d",
                        "run",
                    ],
                    cwd=self.folder,
                    env=environment,
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                )
            save(
                self.folder / "execution_owner.json",
                dict(
                    pid=self.process.pid,
                    hostname=socket.gethostname(),
                    parent_pid=os.getpid(),
                ),
            )
            deadline = time.monotonic() + 90
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=1):
                        break
                except OSError:
                    if self.process.poll() is not None or time.monotonic() > deadline:
                        raise
                    time.sleep(0.5)
            # Fresh offline imports use TuGraph's documented bootstrap account.
            # Keep its short-lived token in memory, never in request journals.
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            try:
                connection.request(
                    "POST",
                    "/login",
                    json.dumps(
                        json.loads(Path(settings["bootstrap_auth_file"]).read_bytes())
                    ),
                    headers={"Content-Type": "application/json"},
                )
                response = connection.getresponse()
                body = json.loads(response.read())
                if response.status != 200 or not body.get("jwt"):
                    raise RuntimeError("Fresh owned TuGraph login failed")
                self.transport = LocalRestTransport(port, body["jwt"])
            finally:
                connection.close()
            counts = NativeQueryJournal(
                self.folder / "count_checks",
                self.transport,
                query_seconds=60,
                wall_seconds=90,
            )
            for kind, pattern in [
                ("vertices", "MATCH (n) RETURN count(n) AS n"),
                ("edges", "MATCH ()-[r]->() RETURN count(r) AS n"),
            ]:
                result = counts.execute(kind, settings["graph"], pattern)
                if result.get("complete") is not True or result.get("rows") != [
                    [settings["expected_" + kind]]
                ]:
                    raise IntegrityError("Imported graph count differs: " + kind)
            self.ready = dict(
                kind="READY",
                hostname=socket.gethostname(),
                graph=settings["graph"],
                benchmark="Text2GQL",
                worker_sha256=digest(Path(__file__).read_bytes()),
                data_sha256=digest(canonical(settings["data_sha256"])),
                model_calls=0,
                loopback_only=True,
                read_guard_and_explain=True,
                server_readonly_role=False,
            )
            save(self.folder / "ready.json", self.ready)
        except BaseException:
            self.close()
            raise

    def execute(self, request):
        identity = request["identity"]
        if (
            identity["benchmark"] != "Text2GQL"
            or identity["graph"] != self.settings["graph"]
        ):
            raise IntegrityError("TuGraph case graph differs")
        folder = self.folder / "cases" / digest(canonical(identity))[:24]
        save(folder / "input.json", request)
        journal = NativeQueryJournal(
            folder / "queries", self.transport, query_seconds=60, wall_seconds=90
        )
        method = identity["method"]
        if method in ("direct", "direct-feedback"):
            result = journal.execute("direct", identity["graph"], request["cypher"])
        elif method == "native-set-count":
            combine = request["plan"].get("combine", {})
            applicable = (
                combine.get("op") in ("union", "intersection", "difference")
                and combine.get("output") == "count"
                and len(request["plan"].get("calls", [])) == 2
            )
            if applicable:
                import schema_services
                from tugraph_dialect import render_compiler_query
                from osdb_native_set_count_control_dev import compile_native_set_count

                compiled = compile_native_set_count(
                    request["plan"],
                    request["schema"],
                    selection_compiler=lambda call, schema: schema_services.compile_selection(
                        call, schema, backend="tugraph-4.5.2"
                    ),
                    set_contract=schema_services._set_contract,
                )
                query = render_compiler_query(compiled["query"], compiled["parameters"])
                result = journal.execute("native-set-count", identity["graph"], query)
            else:
                result = dict(
                    status="not_applicable",
                    complete=False,
                    reason="requires_two_input_set_count",
                )
        else:
            result = execute_native_plan(
                request["plan"],
                request["schema"],
                journal,
                request_prefix=method,
                graph=identity["graph"],
                strategy="full_detail" if method == "correct-detail" else "contract",
            )
            result = dict(result, complete=result.get("status") == "EXECUTED")
        save(folder / "final.json", result)
        return dict(answer=result, remote_final_sha256=digest(canonical(result)))

    def close(self):
        if self.process and self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("settings", type=Path)
    args = parser.parse_args()
    worker = TuGraphWorker(json.loads(args.settings.read_bytes()))
    print(json.dumps(worker.ready), flush=True)
    try:
        for line in sys.stdin:
            request = json.loads(line)
            try:
                if request["kind"] != "execute":
                    raise ValueError("Unknown TuGraph command")
                result = worker.execute(request)
            except (IntegrityError, OSError):
                raise
            except Exception as error:
                result = dict(
                    status="execution_error",
                    error_type=type(error).__name__,
                    error=str(error)[:1000],
                )
            print(
                json.dumps(dict(result, id=request["id"]), ensure_ascii=False),
                flush=True,
            )
    finally:
        worker.close()


if __name__ == "__main__":
    main()
