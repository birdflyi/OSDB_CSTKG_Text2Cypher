"""Small backend registry for the independent smoke consumer.

Reuse the project's compilers, graph owners and scoring code. No HTTP model
client is imported here. References are opened only by score(), after sealing.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sys
import threading
from copy import deepcopy

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [
    str(ROOT / "operations" / "analysis"),
    str(ROOT / "experiment-harness" / "public_benchmark_v1"),
]
from native_cases import case_id, identity_of
from common.methods import CYPHER_METHODS
from engine.config import canonical, digest
from engine.execution import JsonRpcClient, TracedRpcExecutor, worker_command
from engine.errors import IntegrityError
from offline_pipeline import read_pin, save
from common.ri import payload_of


def instance_command(command, output):
    """Only explicit placeholders opt frozen commands into versioned reopens."""
    token = digest(str(output.resolve()).encode("utf-8"))[:20]
    return [part.replace("{instance_id}", token) for part in command]


class SelectionBackend:
    def __init__(self, benchmark, settings, execute_factory):
        self.benchmark, self.settings, self.execute_factory = (
            benchmark,
            settings,
            execute_factory,
        )
        if benchmark == "CypherBench":
            import schema_services as compiler
        else:
            import observed_neo4j_services_dev_v2 as compiler
        self.compiler = compiler
        self.max_case_workers = settings.get("case_workers", 1)
        self._score_lock = threading.Lock()
        self._score_cache = None

    @staticmethod
    def schema(row):
        return payload_of(
            dict(
                public_input_path=row["public_input"]["path"],
                public_input_sha256=row["public_input"]["sha256"],
            )
        )["schema"]

    def execute(self, row, source, directory):
        execute = self.execute_factory(row, directory)
        prediction = source["final"]["prediction"]
        if row["method"] in CYPHER_METHODS:
            return execute(prediction["cypher"], prediction.get("parameters") or {})
        plan = json.loads(source["final"]["raw_prediction"])
        result = self.compiler.execute_plan(
            plan, self.schema(row), execute, strategy="contract"
        )
        return dict(result, complete=result["status"] == "EXECUTED")

    def control(self, row, source, directory):
        plan = json.loads(source["final"]["raw_prediction"])
        schema = self.schema(row)
        execute = self.execute_factory(row, directory)
        if row["method"] == "native-set-count":
            combine = plan.get("combine", {})
            if (
                combine.get("op") not in ("union", "intersection", "difference")
                or combine.get("output") != "count"
                or len(plan.get("calls", [])) != 2
            ):
                return dict(
                    status="not_applicable",
                    reason="requires_two_input_set_count",
                    complete=False,
                )
            from osdb_native_set_count_control_dev import compile_native_set_count

            compiled = compile_native_set_count(
                plan,
                schema,
                selection_compiler=self.compiler.compile_selection,
                set_contract=self.compiler._set_contract,
                identity_kind="str" if self.benchmark == "CypherBench" else None,
            )
            return execute(compiled["query"], compiled["parameters"])
        strategy = "full_detail" if row["method"] == "correct-detail" else "contract"
        result = self.compiler.execute_plan(plan, schema, execute, strategy=strategy)
        return dict(
            result,
            complete=result["status"] == "EXECUTED",
            shared_planner=True,
            independent_model_method=False,
            profile=row["method"] + "-20261027",
        )

    def score(self, row, answer, receipt):
        read_pin(receipt)
        if self.benchmark == "CypherBench":
            import answer_scoring

            with self._score_lock:
                if self._score_cache is None:
                    reference = self.settings["reference"]
                    raw = Path(reference["path"]).read_bytes()
                    if digest(raw) != reference["sha256"]:
                        raise IntegrityError("Reference changed")
                    references = {
                        v["qid"]: v
                        for v in map(json.loads, raw.decode("utf-8").splitlines())
                    }
                    comparator = answer_scoring.load_official_comparator(
                        self.settings["official_source"]
                    )
                    self._score_cache = (references, comparator)
                references, comparator = self._score_cache
                gold = deepcopy(references[row["task_id"]])
            result = answer_scoring.score_executed_answer(
                status=answer["status"],
                predicted_rows=answer["rows"],
                reference_rows=answer_scoring.read_reference_answer(
                    gold["answer_json"]
                ),
                order_matters="order by" in gold["gold_cypher"].lower(),
                qid=row["task_id"],
                compare=comparator,
            )
            return dict(
                status="scored",
                value=result,
                protocol="CypherBench_pinned_result_comparator",
            )
        from osdb_response_boundary_dev import score_numeric

        with self._score_lock:
            if self._score_cache is None:
                self._score_cache = {
                    r["task_id"]: r for r in read_pin(self.settings["reference"])
                }
            gold = deepcopy(self._score_cache[row["task_id"]])
        result = score_numeric(answer, gold["numeric_reference_rows"])
        return dict(
            status="scored_development",
            value=result["numeric_EX"],
            detail=result,
            protocol="exposed_numeric_typed_bag",
            paper_result=False,
        )


class NativeBackend:
    def __init__(self, client):
        self.client = client
        self.answer_hashes = {}
        self._case_lock = threading.Lock()
        self._active = set()
        ready = getattr(client, "ready_value", {})
        self.max_case_workers = ready.get("max_case_workers", 1)
        if type(self.max_case_workers) is not int or self.max_case_workers < 1:
            raise IntegrityError("Invalid backend case capacity")
        self.supports_finish_case = ready.get("case_protocol") == "thread-affine-v1"

    def execute(self, row, source, directory):
        request = dict(
            kind="execute",
            identity=identity_of(row),
        )
        if row["method"] in CYPHER_METHODS:
            request.update(
                cypher=source["final"]["prediction"]["cypher"],
                parameters=source["final"]["prediction"].get("parameters") or {},
            )
        else:
            request.update(
                plan=json.loads(source["final"]["raw_prediction"]),
                schema=SelectionBackend.schema(row),
            )
        save(directory / "rpc_intent.json", request)
        with self._case_lock:
            self._active.add(case_id(row))
        result = self.client.request(request, timeout=200)
        if result.get("status") == "integrity_error":
            raise IntegrityError(result.get("error", "Remote case integrity failure"))
        save(directory / "rpc_result.json", result)
        if "answer" not in result:
            return dict(
                status=result.get("status", "execution_error"),
                complete=False,
                error_type=result.get("error_type"),
                error=result.get("error"),
                database_code=result.get("database_code"),
            )
        answer = result["answer"]
        if digest(canonical(answer)) != result["remote_final_sha256"]:
            raise IntegrityError("Remote sealed answer differs")
        with self._case_lock:
            self.answer_hashes[case_id(row)] = result["remote_final_sha256"]
        return answer

    def control(self, row, source, directory):
        return self.execute(row, source, directory)

    def score(self, row, answer, receipt):
        read_pin(receipt)
        with self._case_lock:
            expected = self.answer_hashes[case_id(row)]
        if digest(canonical(answer)) != expected:
            raise IntegrityError("Local answer differs from remote case seal")
        result = self.client.request(
            dict(kind="score", identity=identity_of(row), answer_sha256=expected),
            timeout=200,
        )
        if result.get("status") == "integrity_error":
            raise IntegrityError(result.get("error", "Remote score identity failure"))
        result.pop("id", None)
        return result

    def finish_case(self, row):
        key = case_id(row)
        with self._case_lock:
            active = key in self._active
        if active and self.supports_finish_case:
            result = self.client.request(
                dict(kind="finish_case", identity=identity_of(row)), timeout=30
            )
            if result.get("status") != "case_closed":
                raise IntegrityError("Remote native case did not confirm closure")
        with self._case_lock:
            self._active.discard(key)
            self.answer_hashes.pop(key, None)


class Text2GQLBackend(NativeBackend):
    def __init__(self, client, settings):
        super().__init__(client)
        self.settings = settings
        self.supports_finish_case = False  # TuGraph has no cross-command transaction.
        self._score_lock = threading.Lock()
        self._score_cache = None

    def score(self, row, answer, receipt):
        read_pin(receipt)
        from text2gql_reference_store_eval_20261027 import SealedReferenceStore
        from text2gql_score_contract_dev_20261027 import FinalAnswerContract
        from audit_text2gql_score_boundaries_20261027 import subset

        with self._score_lock:
            if self._score_cache is None:
                reference = self.settings["reference"]
                store = SealedReferenceStore(
                    reference["path"], expected_seal_sha256=reference["sha256"]
                )
                comparator = self.settings["comparator"]
                path = Path(comparator["path"])
                if digest(path.read_bytes()) != comparator["sha256"]:
                    raise IntegrityError("Text2GQL comparator changed")
                evaluator, _ = subset(path, "ExecutionEvaluator", ["compare_results"])
                contract = FinalAnswerContract(
                    store.cohort(),
                    evaluator.compare_results,
                    comparator_sha256=comparator["sha256"],
                )
                self._score_cache = (store, contract)
            store, contract = self._score_cache
        metadata = store.metadata(row["graph"], row["task_id"])
        gold = store.load(
            row["graph"], row["task_id"], expected_query_sha256=metadata["query_sha256"]
        )
        scored = contract.score_one(
            (row["graph"], row["task_id"]), gold["answer"], answer
        )
        return dict(
            status="scored_development",
            value=scored["primary"],
            detail=scored,
            author_Bolt_REST_equivalence_established=False,
            paper_result=False,
        )


@contextmanager
def open_backend(benchmark, graph, settings, output):
    if not settings:
        raise ValueError("Backend configuration missing: " + benchmark)
    if benchmark == "CypherBench":
        route = settings["graphs"][graph]
        with (output / ("worker-" + graph + ".stderr")).open("xb") as stderr:
            client = JsonRpcClient(
                worker_command(instance_command(route["command"], output)),
                expected_ready=route["expected_ready"],
                stderr=stderr,
            )
            try:
                save(output / ("ready-" + graph + ".json"), client.ready_value)

                def execute_factory(row, directory):
                    return TracedRpcExecutor(
                        client,
                        directory / "reads",
                        identity=dict(
                            task_id=row["task_id"],
                            method=row["method"],
                            benchmark=benchmark,
                        ),
                        graph_sha256=route["expected_ready"]["graph_sha256"],
                        worker_version="engine-neo4j-v1",
                    ).execute

                backend = SelectionBackend(benchmark, settings, execute_factory)
                backend.max_case_workers = client.ready_value.get("max_case_workers", 1)
                yield backend
            finally:
                client.close()
    elif benchmark == "OSDB-STKG":
        from osdb_owned_graph_factory_dev import OwnedGraphFactory

        binding = read_pin(settings["binding"])
        runtime_root = Path(settings.get("runtime_root", output))
        runtime_directory = (
            runtime_root / output.name / ("osdb-" + graph)
            if "runtime_root" in settings
            else output / ("osdb-" + graph)
        )
        route = settings.get("graphs", {}).get(graph, {})
        factory = OwnedGraphFactory(
            runtime_directory,
            binding["inventory"]["path"],
            binding["inventory"]["sha256"],
            binding["profiles"],
            binding["neo4j_home"],
            Path(route.get("lock_path") or settings["lock_path"]),
            http=route.get("http_port", binding["http_port"]),
            bolt=route.get("bolt_port", binding["bolt_port"]),
            workers=settings.get("case_workers", 1),
            connection_pool_size=settings.get("connection_pool_size"),
            cpu_count=settings.get("cpu_count", 2),
            heap_initial=settings.get("heap_initial", "256m"),
            heap_max=settings.get("heap_max", "512m"),
            pagecache=settings.get("pagecache", "128m"),
            startup_seconds=settings.get("startup_seconds", 32),
        )
        with factory, factory(graph) as handle:
            yield SelectionBackend(
                benchmark,
                settings,
                lambda row, directory: handle["case_executor"](directory / "reads")[
                    "execute"
                ],
            )
    elif benchmark in ("MindTheQuery", "ZOGRASCOPE", "Text2GQL"):
        route = settings["graphs"][graph]
        with (output / ("worker-" + benchmark + "-" + graph + ".stderr")).open(
            "xb"
        ) as stderr:
            client = JsonRpcClient(
                worker_command(instance_command(route["command"], output)),
                expected_ready=route["expected_ready"],
                stderr=stderr,
            )
            try:
                save(
                    output / ("ready-" + benchmark + "-" + graph + ".json"),
                    client.ready_value,
                )
                yield (
                    Text2GQLBackend(client, settings)
                    if benchmark == "Text2GQL"
                    else NativeBackend(client)
                )
            finally:
                client.close()
    else:
        raise ValueError("Backend lifecycle not yet integrated: " + benchmark)
