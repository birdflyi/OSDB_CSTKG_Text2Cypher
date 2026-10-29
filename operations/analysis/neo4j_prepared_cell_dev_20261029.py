"""Join prepared public inputs, saved replies and native answer consumers.

This module has no model sender, credentials or graph startup command. A caller
supplies an owned graph lease and independently pinned generation bindings.
It reuses one driver lease per graph, while each answer keeps its own explicit
read transaction. Runtime lifecycle acceptance and primary scoring stay pending.
"""

from collections import Counter
from dataclasses import asdict
import hashlib
from pathlib import Path
import re

from neo4j.exceptions import AuthError, DriverError, Neo4jError

from engine.attempt_costs import summarize_attempts
from audited_model_reply_dev_20261029 import PinnedFile, RecordedReply, ReplyConflict
from neo4j_native_answer_contract_dev_20261029 import encoded
from neo4j_native_selection_dev_20261029 import NativeSchema
from neo4j_reply_consumer_dev_20261029 import consume_recorded_reply, failure, save_new
from property_graph_input_contract_dev_20261029 import iter_public_tasks


BENCHMARKS = {
    "MindTheQuery": ("mindthequery", "MTQ"),
    "ZOGRASCOPE": ("zograscope", "ZOGRASCOPE"),
}


class PreparedNativeCell:
    """One Direct/Ours initial-reply cell, with the whole public denominator.

    bindings is a pinned object with benchmark, method, model, input_variant,
    package_manifest_sha256, graphs and requests. Graph records bind the schema,
    database and expected snapshot supplied by offline graph preparation.
    Each request binds all six logical
    identity fields, the physical request digest and public common-input digest.
    These bindings must be produced at generation time, not inferred from a
    model response or reconstructed from an evaluator reference.
    """

    def __init__(self, package, manifest_sha256, *, variant, method, model, bindings):
        if (method not in ("direct", "ours") or not isinstance(bindings, PinnedFile)
                or type(model) is not str or not model.strip()):
            raise ReplyConflict("Explicit Direct/Ours method and pinned request bindings required")
        manifest = PinnedFile(str(Path(package) / "manifest.json"), manifest_sha256).load()
        if manifest.get("benchmark") not in BENCHMARKS:
            raise ReplyConflict("Only the prepared MTQ/ZOGRASCOPE profiles are supported")
        self.benchmark = manifest["benchmark"]
        self.identity_benchmark, schema_format = BENCHMARKS[self.benchmark]
        self.method, self.model, self.variant = method, model, variant
        self.tasks = dict(iter_public_tasks(package, variant, expected_manifest_sha256=manifest_sha256))
        self.graphs = tuple(dict.fromkeys(task["graph"] for task in self.tasks.values()))
        self.schemas = {}
        if method == "ours":
            for task in self.tasks.values():
                if task["graph"] not in self.schemas:
                    self.schemas[task["graph"]] = NativeSchema(task["schema"], format=schema_format)
        generation = bindings.load()
        expected = dict(benchmark=self.benchmark, method=method, model=model,
                        input_variant=variant, package_manifest_sha256=manifest_sha256)
        if any(generation.get(key) != value for key, value in expected.items()):
            raise ReplyConflict("Generation bindings belong to another cell or public package")
        self.graph_bindings = generation["graphs"]
        if set(self.graph_bindings) != set(self.graphs):
            raise ReplyConflict("Every public graph needs an explicit prepared runtime binding")
        for graph, binding in self.graph_bindings.items():
            if (binding.get("schema_sha256") != manifest["graphs"][graph]["schema_sha256"]
                    or type(binding.get("database")) is not str or not binding["database"]
                    or type(binding.get("snapshot_sha256")) is not str
                    or not re.fullmatch("[0-9a-f]{64}", binding["snapshot_sha256"])):
                raise ReplyConflict("Graph runtime binding lacks its public schema/database/snapshot")
        self.requests = {}
        for row in generation["requests"]:
            identity = row["identity"]
            task = self.tasks.get(identity["task_id"])
            if (task is None or identity.get("benchmark") != self.identity_benchmark
                    or identity.get("method") != method or identity.get("model") != model
                    or identity.get("phase") != "initial" or identity.get("graph") != task["graph"]
                    or row["common_input_sha256"] != hashlib.sha256(encoded(task)).hexdigest()):
                raise ReplyConflict("Generation binding differs from its public task")
            task_id = identity["task_id"]
            if task_id in self.requests:
                raise ReplyConflict("Duplicate initial request position")
            self.requests[task_id] = row
        self.preparation = dict(expected, public_positions=len(self.tasks),
                                bound_initial_positions=len(self.requests),
                                graphs=self.graph_bindings,
                                generation_binding=asdict(bindings),
                                formal_admission=False, live_graph_verified=False)

    def _validate_sources(self, sources):
        seen, slots = set(), {}
        for source in sources:
            if not isinstance(source, RecordedReply):
                raise ReplyConflict("Pinned RecordedReply objects required")
            identity = source.identity
            binding = self.requests.get(identity.task_id)
            if (binding is None or binding["identity"] != asdict(identity)
                    or identity.task_id in seen):
                raise ReplyConflict("Reply identity is extra, duplicate or not bound to this cell")
            link = source.source_link()
            if link["files"]["request"]["sha256"] != binding["request_sha256"]:
                raise ReplyConflict("Physical reply source differs from generation bindings")
            # Existing RecordedReply objects cache parsed values; recheck their
            # pinned evidence before any graph lease or writable output opens.
            for pin in link["files"].values():
                if pin is not None:
                    PinnedFile(**pin).load()
            if link["source_charged"] is True:
                slot = link["source_budget_scope"], link["source_budget_slot"]
                if slot in slots and slots[slot] != link["source_charge_key"]:
                    raise ReplyConflict("One audited source slot names conflicting physical attempts")
                slots[slot] = link["source_charge_key"]
            seen.add(identity.task_id)

    def consume_initial(self, sources, directory, *, lease_factory,
                        run_controls=False, allow_feedback=False, evaluator_factory=None):
        """Consume saved replies only; never resend, charge or infer missing replies.

        lease_factory(graph) returns a context manager yielding (driver,
        capture_settings). It owns live identity/authentication/immutability
        verification and graph cleanup outside this preparation component.
        Ordinary model/query and declared graph-connection errors are recorded;
        integrity, persistence and unexpected implementation errors propagate.
        Feedback remains an unsent draft and requires separate model dispatch.
        """
        if run_controls and self.method != "ours":
            raise ReplyConflict("Shared-plan controls require an Ours cell")
        if type(run_controls) is not bool or type(allow_feedback) is not bool:
            raise ReplyConflict("Explicit boolean control and feedback policies required")
        if evaluator_factory is not None and not callable(evaluator_factory):
            raise ReplyConflict("Explicit post-seal evaluator factory required")
        sources = list(sources)
        self._validate_sources(sources)
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=False)
        save_new(directory / "preparation.json", self.preparation)
        outcomes, costs = [], []
        for source in sources:
            link = source.source_link()
            source_id = ((str(link["source_budget_scope"]) + ":" + link["source_charge_key"])
                         if link["source_charge_key"] else "terminal-sha256:" + link["files"]["terminal"]["sha256"])
            costs.append(dict(source_id=source_id, terminal=source.terminal))
        for graph in self.graphs:
            group = [source for source in sources if source.identity.graph == graph]
            if not group:
                continue
            # Materialize failure/nonterminal records without opening a driver.
            ready = [source for source in group if source.status == "READY"]
            deferred = [source for source in group if source.status != "READY"]
            for source in deferred:
                outcomes.append(self._consume(source, directory, None, {}, run_controls,
                                               allow_feedback, evaluator_factory))
            if not ready:
                continue
            try:
                with lease_factory(graph) as lease:
                    driver, settings = lease
                    expected_graph = self.graph_bindings[graph]
                    if any(settings.get(key) != expected_graph[key]
                           for key in ("database", "snapshot_sha256")):
                        raise ReplyConflict("Leased database/snapshot differs from the prepared graph")
                    for source in ready:
                        outcomes.append(self._consume(source, directory, driver, settings,
                                                       run_controls, allow_feedback, evaluator_factory))
            except (Neo4jError, DriverError) as error:
                if isinstance(error, AuthError):
                    raise  # Authentication is not an ordinary bad task.
                # Preserve any answers already committed before a lease/close
                # failure. The remaining graph positions stay explicit failures.
                completed = {row["task_id"] for row in outcomes}
                for source in ready:
                    if source.identity.task_id not in completed:
                        path = self._case_directory(directory, source)
                        path.mkdir(parents=True, exist_ok=True)
                        final_path = path / "method_final.json"
                        method = failure("INFRASTRUCTURE_ERROR", type(error).__name__)
                        if final_path.exists():
                            raw = final_path.read_bytes()
                            saved = PinnedFile(str(final_path), hashlib.sha256(raw).hexdigest()).load()
                            if saved.get("source") != source.source_link():
                                raise ReplyConflict("Committed method belongs to another source")
                            method = saved["result"]
                        record = dict(task_id=source.identity.task_id, graph=graph,
                                      source=source.source_link(),
                                      method=method,
                                      graph_error_type=type(error).__name__,
                                      post_final_failure=final_path.exists(),
                                      feedback_draft=None, controls=None, evaluation=None)
                        save_new(path / "graph_failure.json", record)
                        outcomes.append(record)
                save_new(directory / ("lease-" + hashlib.sha256(graph.encode()).hexdigest()[:16] + ".json"),
                         dict(graph=graph, error_type=type(error).__name__,
                              method_answers_retained=True, lifecycle_verified=False))
        statuses = Counter(row["method"]["answer"]["status"] for row in outcomes)
        summary = dict(self.preparation, source_positions=len(sources),
                       consumed_positions=len(outcomes), missing_positions=len(self.tasks)-len(outcomes),
                       final_statuses=dict(statuses), actual_answers_saved=statuses["EXECUTED"],
                       pending_operation_feedback=sum(row.get("feedback_draft") is not None for row in outcomes),
                       model_attempt_costs=summarize_attempts(costs), new_model_calls=0,
                       primary_score=None, author_primary_scoring_integrated=False,
                       graph_lifecycle_accepted=False,
                       public_population_recorded=len(outcomes)==len(self.tasks),
                       unresolved_model_outcomes=statuses["UNKNOWN"])
        save_new(directory / "summary.json", summary)
        return summary, outcomes

    @staticmethod
    def _case_directory(directory, source):
        return directory / "cases" / hashlib.sha256(encoded(asdict(source.identity))).hexdigest()[:24]

    def _consume(self, source, directory, driver, settings, run_controls, allow_feedback, evaluator_factory):
        evaluator = evaluator_factory(source.identity) if evaluator_factory is not None else None
        result = consume_recorded_reply(source, self.schemas.get(source.identity.graph),
            driver=driver, directory=self._case_directory(directory, source), capture_settings=settings,
            run_controls=run_controls, allow_feedback=allow_feedback, evaluator=evaluator)
        return dict(result, task_id=source.identity.task_id, graph=source.identity.graph)
