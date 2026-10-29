"""Prepared-cell integration checks with synthetic replies and fake drivers."""

import argparse
from contextlib import contextmanager
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

from neo4j.exceptions import AuthError, ServiceUnavailable

from accept_neo4j_native_plan_20261029 import AUTHOR
from accept_neo4j_read_capture_20261029 import Driver, Result
from accept_neo4j_reply_consumer_20261029 import ConsumerChecks
from audited_model_reply_dev_20261029 import ReplyConflict
from neo4j_native_answer_contract_dev_20261029 import encoded
from neo4j_prepared_cell_dev_20261029 import PreparedNativeCell


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class CellChecks(unittest.TestCase):
    def setUp(self):
        self.fixture = ConsumerChecks(methodName="test_direct_actual_answer_is_saved_with_original_cost_identity")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.package = self.root / "public-package"
        (self.package / "public").mkdir(parents=True)
        (self.package / "schemas").mkdir()
        schema = encoded(AUTHOR)
        (self.package / "schemas/people.json").write_bytes(schema)
        self.common = {}
        rows = []
        for i in range(1, 4):
            identity = "task-" + str(i)
            common = dict(graph="people", question="Public fixture question " + str(i), schema=AUTHOR)
            self.common[identity] = common
            rows.append(dict(task_id=identity, graph="people", question=common["question"],
                             input_variant="plain", schema_file="schemas/people.json",
                             schema_sha256=sha(schema), common_input_sha256=sha(encoded(common))))
        public = b"".join(encoded(row) + b"\n" for row in rows)
        (self.package / "public/plain.jsonl").write_bytes(public)
        manifest = dict(benchmark="MindTheQuery", variants=["plain"], positions_per_variant=3,
                        graphs={"people": dict(schema_file="schemas/people.json", schema_sha256=sha(schema))},
                        files={"schemas/people.json": sha(schema), "public/plain.jsonl": sha(public)})
        self.manifest_sha = sha(encoded(manifest))
        (self.package / "manifest.json").write_bytes(encoded(manifest))
        self.leases = []

    def source(self, task=1, **options):
        identity = replace(self.fixture.identity, task_id="task-" + str(task),
                           method=options.pop("method", "direct"))
        return self.fixture.source(identity=identity,
            change_terminal=lambda row: row.update(budget_slot=task),
            change_slot=lambda row: row.update(slot=task), **options)

    def cell(self, sources, *, method="direct", change=None):
        rows = [dict(identity=asdict(source.identity),
                     request_sha256=source.source_link()["files"]["request"]["sha256"],
                     common_input_sha256=sha(encoded(self.common[source.identity.task_id])))
                for source in sources]
        value = dict(benchmark="MindTheQuery", method=method, model="fixture-model", input_variant="plain",
                     package_manifest_sha256=self.manifest_sha, requests=rows,
                     graphs={"people": dict(schema_sha256=sha(encoded(AUTHOR)),
                                           snapshot_sha256="a" * 64, database="fixture")})
        if change:
            change(value)
        pin = self.fixture.pin("bindings", value)
        return PreparedNativeCell(self.package, self.manifest_sha, variant="plain",
                                  method=method, model="fixture-model", bindings=pin)

    def factory(self, results):
        driver = Driver(results)

        @contextmanager
        def lease(graph):
            self.leases.append(graph)
            yield driver, dict(snapshot_sha256="a" * 64, database="fixture", transaction_timeout=75,
                               max_rows=100, max_answer_bytes=1000000)

        return lease, driver

    def test_same_graph_uses_one_lease_and_keeps_missing_positions(self):
        sources = [self.source(1), self.source(2)]
        cell = self.cell(sources)
        factory, driver = self.factory([Result(), Result([[1]]), Result(), Result([[1]])])
        summary, outcomes = cell.consume_initial(sources, self.root / "cell", lease_factory=factory)
        self.assertEqual(self.leases, ["people"])
        self.assertEqual(len(driver.session_calls), 2)
        self.assertEqual((summary["public_positions"], summary["consumed_positions"], summary["missing_positions"]), (3, 2, 1))
        self.assertEqual(summary["model_attempt_costs"]["total"]["charged"], 2)
        self.assertIsNone(summary["primary_score"])
        self.assertFalse(summary["public_population_recorded"])
        self.assertEqual(len(outcomes), 2)

    def test_bad_query_keeps_its_feedback_draft_and_next_task_runs(self):
        sources = [self.source(1, content="CREATE (n)"), self.source(2)]
        factory, driver = self.factory([Result(), Result([[1]])])
        summary, _ = self.cell(sources).consume_initial(sources, self.root / "cell", lease_factory=factory,
                                                       allow_feedback=True)
        self.assertEqual(summary["final_statuses"], {"GUARD_REJECTED": 1, "EXECUTED": 1})
        self.assertEqual(summary["pending_operation_feedback"], 1)
        self.assertEqual(summary["new_model_calls"], 0)
        self.assertEqual(len(driver.tx.calls), 2)

    def test_changed_public_binding_is_rejected_without_a_graph_lease(self):
        source = self.source()
        with self.assertRaises(ReplyConflict):
            self.cell([source], change=lambda row: row["requests"][0].update(common_input_sha256="b" * 64))
        self.assertEqual(self.leases, [])

    def test_changed_pinned_reply_is_rejected_before_output_creation(self):
        source = self.source()
        cell = self.cell([source])
        Path(source.source_link()["files"]["terminal"]["path"]).write_bytes(b"{}")
        with self.assertRaises(ReplyConflict):
            cell.consume_initial([source], self.root / "cell", lease_factory=lambda graph: None)
        self.assertFalse((self.root / "cell").exists())

    def test_wrong_leased_snapshot_is_rejected_before_a_query(self):
        source = self.source()
        driver = Driver([])

        @contextmanager
        def wrong_graph(graph):
            yield driver, dict(snapshot_sha256="b" * 64, database="fixture")

        with self.assertRaises(ReplyConflict):
            self.cell([source]).consume_initial([source], self.root / "cell", lease_factory=wrong_graph)
        self.assertEqual(driver.session_calls, [])

    def test_zograscope_gold_linked_input_keeps_its_own_schema_profile(self):
        public_schema = dict(classes={"Person": {}}, properties={}, relations={})
        common = dict(graph="people", question="Public linked entity fixture", schema=public_schema)
        schema = encoded(public_schema)
        row = dict(task_id="task-1", graph="people", question=common["question"], input_variant="gold_linked",
                   schema_file="schemas/people.json", schema_sha256=sha(schema), common_input_sha256=sha(encoded(common)))
        public = encoded(row) + b"\n"
        (self.package / "schemas/people.json").write_bytes(schema)
        (self.package / "public/gold_linked.jsonl").write_bytes(public)
        manifest = dict(benchmark="ZOGRASCOPE", variants=["gold_linked"], positions_per_variant=1,
                        graphs={"people": dict(schema_file="schemas/people.json", schema_sha256=sha(schema))},
                        files={"schemas/people.json": sha(schema), "public/gold_linked.jsonl": sha(public)})
        (self.package / "manifest.json").write_bytes(encoded(manifest))
        identity = replace(self.fixture.identity, benchmark="zograscope", method="ours")
        source = self.fixture.source(identity=identity, content='{"status":"UNSUPPORTED","reason":"outside"}')
        value = dict(benchmark="ZOGRASCOPE", method="ours", model="fixture-model", input_variant="gold_linked",
            package_manifest_sha256=sha(encoded(manifest)),
            graphs={"people": dict(schema_sha256=sha(schema), snapshot_sha256="a"*64, database="fixture")},
            requests=[dict(identity=asdict(identity), common_input_sha256=sha(encoded(common)),
                           request_sha256=source.source_link()["files"]["request"]["sha256"])])
        cell = PreparedNativeCell(self.package, sha(encoded(manifest)), variant="gold_linked", method="ours",
                                  model="fixture-model", bindings=self.fixture.pin("zog-bindings", value))
        self.assertEqual(cell.schemas["people"].format, "ZOGRASCOPE")
        factory, _ = self.factory([])
        summary, _ = cell.consume_initial([source], self.root / "cell", lease_factory=factory)
        self.assertEqual(summary["input_variant"], "gold_linked")
        self.assertEqual(summary["final_statuses"], {"UNSUPPORTED": 1})

    def test_duplicate_and_mixed_cell_sources_are_rejected(self):
        source = self.source()
        cell = self.cell([source])
        for sources in ([source, source], [self.source(2)]):
            with self.subTest(), self.assertRaises(ReplyConflict):
                cell.consume_initial(sources, self.root / "cell", lease_factory=lambda graph: None)

    def test_one_audited_slot_cannot_name_two_physical_attempts(self):
        first = self.source(1)
        second = self.fixture.source(identity=replace(self.fixture.identity, task_id="task-2"))
        with self.assertRaises(ReplyConflict):
            self.cell([first, second]).consume_initial([first, second], self.root / "cell",
                                                       lease_factory=lambda graph: None)
        self.assertFalse((self.root / "cell").exists())

    def test_unknown_reply_opens_no_connection_and_keeps_its_charge(self):
        source = self.source(status="unknown")
        summary, _ = self.cell([source]).consume_initial([source], self.root / "cell",
            lease_factory=lambda graph: self.fail("Unknown reply must not open a graph lease"))
        self.assertEqual(summary["final_statuses"], {"UNKNOWN": 1})
        self.assertEqual(summary["model_attempt_costs"]["total"]["charged"], 1)
        self.assertIsNone(summary["model_attempt_costs"]["total"]["monetary_cost"])

    def test_graph_connection_failure_records_every_received_task(self):
        sources = [self.source(1), self.source(2)]

        @contextmanager
        def unavailable(graph):
            raise ServiceUnavailable("Synthetic graph lease failure")
            yield

        summary, _ = self.cell(sources).consume_initial(sources, self.root / "cell", lease_factory=unavailable)
        self.assertEqual(summary["final_statuses"], {"INFRASTRUCTURE_ERROR": 2})
        self.assertEqual(summary["missing_positions"], 1)
        self.assertEqual(summary["pending_operation_feedback"], 0)
        self.assertEqual(len(list((self.root / "cell/cases").glob("*/graph_failure.json"))), 2)

    def test_ours_unsupported_is_not_a_correct_empty_answer(self):
        source = self.source(method="ours", content='{"status":"UNSUPPORTED","reason":"outside profile"}')
        factory, driver = self.factory([])
        summary, _ = self.cell([source], method="ours").consume_initial([source], self.root / "cell",
            lease_factory=factory, run_controls=True)
        self.assertEqual(summary["actual_answers_saved"], 0)
        self.assertEqual(summary["final_statuses"], {"UNSUPPORTED": 1})
        self.assertEqual(driver.session_calls, [])

    def test_post_final_connection_error_preserves_answer_and_remaining_failures(self):
        sources = [self.source(1), self.source(2)]
        original = __import__("neo4j_prepared_cell_dev_20261029").consume_recorded_reply

        def close_failure(*args, **kwargs):
            original(*args, **kwargs)
            raise ServiceUnavailable("Synthetic close failure after saving final")

        factory, _ = self.factory([Result(), Result([[1]])])
        with patch("neo4j_prepared_cell_dev_20261029.consume_recorded_reply", side_effect=close_failure):
            summary, rows = self.cell(sources).consume_initial(sources, self.root / "cell", lease_factory=factory)
        self.assertEqual(summary["final_statuses"], {"EXECUTED": 1, "INFRASTRUCTURE_ERROR": 1})
        self.assertTrue(rows[0]["post_final_failure"])
        self.assertFalse(rows[1]["post_final_failure"])
        self.assertEqual(len(list((self.root / "cell/cases").glob("*/method_final.json"))), 1)

    def test_direct_cannot_request_shared_ours_controls(self):
        source = self.source()
        with self.assertRaises(ReplyConflict):
            self.cell([source]).consume_initial([source], self.root / "cell",
                                               lease_factory=lambda graph: None, run_controls=True)

    def test_authentication_error_is_not_masked_as_a_bad_task(self):
        source = self.source()

        @contextmanager
        def authentication_failure(graph):
            raise AuthError("Synthetic authentication failure")
            yield

        with self.assertRaises(AuthError):
            self.cell([source]).consume_initial([source], self.root / "cell", lease_factory=authentication_failure)

    def test_output_evidence_failure_precedes_graph_open(self):
        source = self.source()
        with patch("neo4j_prepared_cell_dev_20261029.save_new", side_effect=OSError("Synthetic full disk")):
            with self.assertRaises(OSError):
                self.cell([source]).consume_initial([source], self.root / "cell",
                                                   lease_factory=lambda graph: self.fail("No lease before persistence"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CellChecks)
    with patch.object(socket, "create_connection", side_effect=AssertionError("No network")), \
         patch.object(socket.socket, "connect", side_effect=AssertionError("No network")):
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    checked = [Path(__file__), Path(__file__).with_name("neo4j_prepared_cell_dev_20261029.py")]
    (output / "summary.json").write_bytes(encoded(dict(
        kind="PREPARED_NATIVE_CELL_FAKE_DRIVER_NOT_EXPERIMENT", checks=result.testsRun,
        failed=len(result.failures)+len(result.errors), new_model_calls=0, DB_connections=0,
        DB_queries=0, training=0, adapter_registered=False, formal_admission=False,
        checked_files={path.name: sha(path.read_bytes()) for path in checked})))
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
