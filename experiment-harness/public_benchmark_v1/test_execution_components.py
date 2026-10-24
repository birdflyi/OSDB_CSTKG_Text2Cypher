import json
import os
from pathlib import Path
import tempfile
import unittest

from answer_scoring import load_official_comparator, ScoringError, score_executed_answer
from attempt_budget import AttemptBudget, BudgetBlocked


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding='utf-8')


class AttemptAccountingChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.ledger = self.root / 'budget'
        self.ledger.mkdir()
        self.approval = self.root / 'approval.json'
        write(self.approval, {'approved_cumulative_cap': 2,
                              'historical_attempts_at_approval': 0})
        self.manager = AttemptBudget(self.ledger, self.approval)

    def request(self, name):
        path = self.root / name / 'request.json'
        write(path, {'model': 'test', 'messages': []})
        return path

    def finish(self, request, status=200):
        write(request.with_name('terminal.json'), {'http_status': status})

    def test_failed_attempt_charged_and_explicit_cap_enforced(self):
        first = self.request('one')
        self.assertEqual(self.manager.reserve(first, purpose='development'), 1)
        self.finish(first, 500)
        second = self.request('two')
        self.assertEqual(self.manager.reserve(second, purpose='same-body retry'), 2)
        self.finish(second)
        with self.assertRaises(BudgetBlocked):
            self.manager.reserve(self.request('three'), purpose='development')
        self.assertEqual(self.manager.inspect()['remaining'], 0)

    def test_pending_request_blocks_duplicate_and_other_dispatch(self):
        first = self.request('one')
        self.manager.reserve(first, purpose='development')
        for request in [first, self.request('two')]:
            with self.assertRaises(BudgetBlocked):
                self.manager.reserve(request, purpose='development')
        self.assertEqual(self.manager.inspect()['reserved'], 1)

    def test_budget_lock_is_not_silently_removed(self):
        lock = self.ledger / '.t1-reservation.lock'
        lock.write_text('unresolved owner')
        with self.assertRaises(BudgetBlocked):
            self.manager.reserve(self.request('one'), purpose='development')
        self.assertTrue(lock.exists())

    def test_deleted_or_modified_history_blocks_progress(self):
        first = self.request('one')
        self.manager.reserve(first, purpose='development')
        self.finish(first)
        write(first, {'changed': True})
        with self.assertRaises(BudgetBlocked):
            self.manager.inspect()
        (self.ledger / '0001.json').unlink()
        write(self.approval, {'approved_cumulative_cap': 2,
                              'historical_attempts_at_approval': 1})
        with self.assertRaises(BudgetBlocked):
            self.manager.inspect()

    def test_special_legacy_terminal_and_migration_are_read_only(self):
        request = self.root / 'migrated' / 'connectivity_probe_01_request.json'
        write(request, {'model': 'legacy'})
        write(request.with_name('connectivity_probe_01.json'), {'http_status': 500})
        slot = self.ledger / '0001.json'
        write(slot, {'request_path': 'C:\\legacy\\connectivity_probe_01_request.json'})
        before = slot.read_bytes()
        manager = AttemptBudget(self.ledger, self.approval,
                                path_rewrites=[('C:\\legacy', request.parent)])
        snapshot = manager.inspect()
        self.assertEqual(snapshot['reserved'], 1)
        self.assertEqual(snapshot['pending'], [])
        self.assertEqual(slot.read_bytes(), before)

    def test_duplicate_slot_number_or_malformed_terminal_fails_closed(self):
        request = self.request('one')
        self.manager.reserve(request, purpose='development')
        self.finish(request)
        (self.ledger / '1.json').write_bytes((self.ledger / '0001.json').read_bytes())
        with self.assertRaises(BudgetBlocked):
            self.manager.inspect()
        (self.ledger / '1.json').unlink()
        write(request.with_name('terminal.json'), {'not_a_terminal': True})
        with self.assertRaises(BudgetBlocked):
            self.manager.inspect()


class ExecutedAnswerChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        default = Path(__file__).resolve().parents[4] / 'literature/CypherBench/official_source_20261023'
        cls.source = Path(os.environ.get('CH7_CYPHERBENCH_SOURCE', str(default)))
        if not cls.source.exists():
            raise unittest.SkipTest('Set CH7_CYPHERBENCH_SOURCE to pinned official checkout')
        cls.compare = staticmethod(load_official_comparator(cls.source))

    def score(self, prediction, reference, *, ordered=False, status='EXECUTED'):
        return score_executed_answer(status=status, predicted_rows=prediction,
                                     reference_rows=reference, order_matters=ordered,
                                     qid='training-regression', compare=self.compare)['ex']

    def test_bag_multiplicity_not_just_set(self):
        self.assertEqual(self.score([[1], [2], [1]], [[1], [1], [2]]), 1)
        self.assertEqual(self.score([[1], [2], [2]], [[1], [1], [2]]), 0)

    def test_ordered_query_does_not_accept_reversed_rows(self):
        self.assertEqual(self.score([[2], [1]], [[1], [2]], ordered=False), 1)
        self.assertEqual(self.score([[2], [1]], [[1], [2]], ordered=True), 0)

    def test_column_permutation_matches_pinned_official_semantics(self):
        self.assertEqual(self.score([['a', 1], ['b', 2]], [[1, 'a'], [2, 'b']]), 1)
        self.assertEqual(self.score([['a', 2], ['b', 1]], [[1, 'a'], [2, 'b']]), 0)

    def test_empty_answer_not_confused_with_refusal(self):
        self.assertEqual(self.score([], []), 1)
        self.assertEqual(self.score(None, [], status='REFUSED'), 0)
        self.assertEqual(self.score(None, [], status='INFRASTRUCTURE_ERROR'), 0)

    def test_never_grade_plan_or_cypher_as_executed_answer(self):
        for output in [{'plan': []}, 'MATCH (n) RETURN count(n)', [{'count': 1}]]:
            with self.assertRaises(ScoringError):
                self.score(output, [[1]])
        with self.assertRaises(ScoringError):
            self.score([[1]], [[1]], status='STATIC_VALID')

    def test_tampered_upstream_cannot_silently_change_metric(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'cypherbench/metrics/execution_accuracy.py'
            path.parent.mkdir(parents=True)
            path.write_text('def result_eq(*args): return True')
            with self.assertRaises(ScoringError):
                load_official_comparator(tmp)


if __name__ == '__main__':
    unittest.main()
