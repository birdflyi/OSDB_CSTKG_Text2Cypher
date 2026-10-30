"""Small backend registry for the independent smoke consumer.

Reuse the project's compilers, graph owners and scoring code. No HTTP model
client is imported here. References are opened only by score(), after sealing.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [
    str(ROOT / "operations" / "analysis"),
    str(ROOT / "experiment-harness" / "public_benchmark_v1"),
]
from engine.config import canonical, digest
from engine.execution import JsonRpcClient, TracedRpcExecutor, worker_command
from engine.errors import IntegrityError
from offline_pipeline import read_pin, save
from common.ri import payload_of


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
        if row["method"] in ("direct", "direct-feedback"):
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

            reference = self.settings["reference"]
            raw = Path(reference["path"]).read_bytes()
            if digest(raw) != reference["sha256"]:
                raise IntegrityError("Reference changed")
            references = {
                v["qid"]: v for v in map(json.loads, raw.decode("utf-8").splitlines())
            }
            gold = references[row["task_id"]]
            comparator = answer_scoring.load_official_comparator(
                self.settings["official_source"]
            )
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

        references = read_pin(self.settings["reference"])
        gold = next(r for r in references if r["task_id"] == row["task_id"])
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

    def execute(self, row, source, directory):
        request = dict(
            kind="execute",
            identity={
                k: row[k]
                for k in ("task_id", "method", "benchmark", "graph", "position_id")
            },
        )
        if row["method"] in ("direct", "direct-feedback"):
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
        result = self.client.request(request, timeout=200)
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
        self.answer_hashes[row["method"], row["position_id"]] = result[
            "remote_final_sha256"
        ]
        return answer

    def control(self, row, source, directory):
        return self.execute(row, source, directory)

    def score(self, row, answer, receipt):
        read_pin(receipt)
        result = self.client.request(
            dict(
                kind="score",
                answer_sha256=self.answer_hashes[row["method"], row["position_id"]],
            ),
            timeout=200,
        )
        result.pop("id", None)
        return result


class Text2GQLBackend(NativeBackend):
    def __init__(self, client, settings):
        super().__init__(client)
        self.settings = settings

    def score(self, row, answer, receipt):
        read_pin(receipt)
        from text2gql_reference_store_eval_20261027 import SealedReferenceStore
        from text2gql_score_contract_dev_20261027 import FinalAnswerContract
        from audit_text2gql_score_boundaries_20261027 import subset

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
                worker_command(route["command"]),
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

                yield SelectionBackend(benchmark, settings, execute_factory)
            finally:
                client.close()
    elif benchmark == "OSDB-STKG":
        from osdb_owned_graph_factory_dev import OwnedGraphFactory

        binding = read_pin(settings["binding"])
        factory = OwnedGraphFactory(
            output / ("osdb-" + graph),
            binding["inventory"]["path"],
            binding["inventory"]["sha256"],
            binding["profiles"],
            binding["neo4j_home"],
            Path(settings["lock_path"]),
            http=binding["http_port"],
            bolt=binding["bolt_port"],
            startup_seconds=settings.get("startup_seconds", 32),
        )
        with factory, factory(graph) as handle:
            yield SelectionBackend(
                benchmark, settings, lambda row, directory: handle["execute"]
            )
    elif benchmark in ("MindTheQuery", "ZOGRASCOPE", "Text2GQL"):
        route = settings["graphs"][graph]
        with (output / ("worker-" + benchmark + "-" + graph + ".stderr")).open(
            "xb"
        ) as stderr:
            client = JsonRpcClient(
                worker_command(route["command"]),
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
