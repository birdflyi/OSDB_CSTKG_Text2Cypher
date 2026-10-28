"""Offline integration witnesses for the new receipt-to-answer boundary.

Only fabricated model/DB responses are used. Supply a pinned author source;
only its pure compare_results method is loaded, never its driver or runner.
The caller sets PYTHONPATH to the prepared native component bundle.
"""

import argparse
import ast
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

from text2gql_receipt_consumer_dev_20261029 import (
    ReceiptConflict, ReplyIdentity, consume_recorded_reply,
)
from text2gql_score_contract_dev_20261027 import FinalAnswerContract, summarize
from tugraph_execution import NativeQueryJournal, JournalConflict
from tugraph_schema import schema_from_import_config


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def author_comparator(path, expected_sha256):
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('Author comparator source digest mismatch')
    tree = ast.parse(raw.decode('utf-8-sig'))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ExecutionEvaluator')
    method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'compare_results')
    isolated = ast.fix_missing_locations(ast.Module(body=[ast.ClassDef(
        name='ExecutionEvaluator', bases=[], keywords=[], body=[method], decorator_list=[])], type_ignores=[]))
    namespace = {}
    exec(compile(isolated, str(path), 'exec'), namespace)
    return namespace['ExecutionEvaluator']().compare_results


def native(rows, columns=('n',)):
    return {'raw': canonical({'header': [{'name': c} for c in columns],
                              'result': rows, 'size': len(rows)}),
            'http_status': 200, 'transport_complete': True, 'execution_complete': True}


def explain():
    return native([['ReadOnly:1\nExecution Plan:\nSynthetic receipt check\n']], ('@plan',))


def terminal(identity, content='RETURN 1 AS n', *, status='ok', reason='stop', refusal=None):
    body = {'model': identity.model, 'choices': [{'finish_reason': reason,
            'message': {'content': content, 'refusal': refusal}}],
            'usage': {'prompt_tokens': 5, 'completion_tokens': 4}}
    return dict(asdict(identity), request_id='fixture_' + identity.task_id + '_' + identity.phase,
                charged=True, status=status, http_status=200, body=body,
                finish_reason=reason, content=content, usage=deepcopy(body['usage']))


class ConsumerChecks(unittest.TestCase):
    output = None
    compare = None
    comparator_sha256 = None

    def setUp(self):
        self.folder = self.output / self._testMethodName
        self.folder.mkdir()
        self.identity = ReplyIdentity('text2gql', 'synthetic_task', 'direct',
                                      'synthetic_graph', 'synthetic_model', 'initial')
        self.initial = [{'role': 'system', 'content': 'Synthetic format contract'},
                        {'role': 'user', 'content': 'Synthetic public question only'}]
        self.schema = schema_from_import_config({'schema': [
            {'type': 'VERTEX', 'label': 'Person', 'primary': 'id',
             'properties': [{'name': 'id', 'type': 'STRING', 'optional': False}]}]})
        self.wires, self.sent = [], []
        self.journal = NativeQueryJournal(self.folder / 'native', self.transport)

    def transport(self, payload, wall, cap):
        self.sent.append(json.loads(payload))
        if not self.wires:
            raise AssertionError('Unplanned simulated DB request')
        return self.wires.pop(0)

    def consume(self, receipt, identity=None, allow_feedback=True, filename='terminal.json'):
        path = self.folder / filename
        path.write_bytes(canonical(receipt))
        return consume_recorded_reply(path, hashlib.sha256(path.read_bytes()).hexdigest(),
                                      identity or self.identity, self.schema, self.journal,
                                      initial_messages=self.initial, allow_feedback=allow_feedback)

    def score(self, result, rows, *, known=True):
        identity = (self.identity.graph, self.identity.task_id)
        contract = FinalAnswerContract([(identity, {'reference_known': known})],
                                       self.compare, comparator_sha256=self.comparator_sha256)
        ref = {'status': 'EXECUTED', 'complete': True, 'columns': ['n'], 'rows': rows}
        return contract.score_one(identity, ref if known else None, result['final_answer'])

    def test_raw_direct_query_reaches_native_journal_and_score(self):
        text = 'CALL { RETURN 1 AS n UNION RETURN 2 AS n } RETURN n'
        self.wires = [explain(), native([[1], [2]])]
        result = self.consume(terminal(self.identity, text))
        self.assertEqual(self.sent[-1]['script'], text)
        self.assertTrue(self.score(result, [[2], [1]])['primary'])
        self.assertIsNone(result['feedback_messages'])
        self.assertEqual(result['new_model_calls'], 0)
        self.assertIsNone(result['monetary_cost'])

    def test_only_actual_feedback_answer_is_the_final_score(self):
        self.wires = [{'raw': b'{"error_message":"ParserException: synthetic syntax error"}',
                       'http_status': 400, 'transport_complete': True, 'execution_complete': False}]
        first = self.consume(terminal(self.identity, 'RETURN missing'))
        self.assertFalse(self.score(first, [[7]])['primary'])
        self.assertIsNotNone(first['feedback_messages'])
        self.assertEqual(first['feedback_messages'][:2], self.initial)
        self.assertNotIn('reference_rows', json.dumps(first['feedback_messages']))
        self.wires = [explain(), native([[7]])]
        identity = replace(self.identity, phase='feedback')
        final = self.consume(terminal(identity, 'RETURN 7 AS n'), identity, filename='feedback.json')
        self.assertTrue(self.score(final, [[7]])['primary'])
        self.assertIsNone(final['feedback_messages'])
        self.assertFalse(final['feedback_dispatched'])
        self.assertEqual(len(self.sent), 3)

    def test_body_refusal_survives_scheduler_projection(self):
        result = self.consume(terminal(self.identity, None, status='refused', refusal='Synthetic refusal'))
        self.assertEqual(result['final_answer']['status'], 'REFUSED')
        self.assertFalse(self.score(result, [])['primary'])
        self.assertEqual(self.sent, [])

    def test_complete_empty_is_an_answer_without_gold_feedback(self):
        self.wires = [explain(), native([])]
        result = self.consume(terminal(self.identity))
        self.assertTrue(self.score(result, [])['primary'])
        self.assertIsNone(result['feedback_messages'])

    def test_finite_ours_uses_native_primary_property_and_actual_rows(self):
        identity = replace(self.identity, method='ours')
        plan = {'status': 'ANSWER', 'calls': [{'nodes': [{'id': 'p', 'label': 'Person'}],
                'edges': [], 'where': [], 'returns': [{'field': 'p.id', 'alias': 'id'}]}],
                'combine': {'op': 'single'}}
        self.wires = [explain(), native([['p1']], ('id',))]
        result = self.consume(terminal(identity, json.dumps(plan)), identity)
        self.assertEqual(result['final_answer']['rows'], [['p1']])
        self.assertTrue(result['final_answer']['complete'])
        self.assertTrue(self.score(result, [['p1']])['primary'])

    def test_unknown_http200_does_not_execute_present_content(self):
        receipt = terminal(self.identity)
        receipt['status'] = 'unknown'
        receipt['body']['model'] = 'different_model'
        result = self.consume(receipt)
        self.assertEqual(result['final_answer']['status'], 'UNKNOWN')
        self.assertIsNone(self.score(result, [[1]])['primary'])
        self.assertIsNone(result['feedback_messages'])
        self.assertEqual(self.sent, [])

    def test_undispatched_and_charged_infrastructure_stay_distinct(self):
        receipt = terminal(self.identity)
        receipt.update(status='not_dispatched', charged=False, http_status=None, usage=None)
        not_sent = self.consume(receipt)
        self.assertEqual(not_sent['final_answer']['status'], 'UNDISPATCHED')
        self.assertIsNone(self.score(not_sent, [[1]])['primary'])
        receipt.update(status='infra', charged=True, transport_error='OSError')
        failed = self.consume(receipt, filename='infra.json')
        self.assertEqual(failed['final_answer']['status'], 'INFRASTRUCTURE_ERROR')
        self.assertFalse(self.score(failed, [[1]])['primary'])
        self.assertIsNone(failed['source_usage'])
        self.assertEqual(self.sent, [])

    def test_identity_and_projection_conflicts_stop_before_DB(self):
        for field in asdict(self.identity):
            receipt = terminal(self.identity)
            receipt[field] = 'foreign'
            with self.subTest(field=field), self.assertRaises(ReceiptConflict):
                self.consume(receipt)
        for field, value in [('content', 'changed'), ('usage', {}), ('charged', None)]:
            receipt = terminal(self.identity)
            receipt[field] = value
            with self.subTest(field=field), self.assertRaises(ReceiptConflict):
                self.consume(receipt)
        receipt = terminal(self.identity)
        receipt['body']['model'] = 'foreign'
        with self.assertRaises(ReceiptConflict):
            self.consume(receipt)
        self.assertEqual(self.sent, [])

    def test_tampered_bytes_duplicate_keys_and_nonfinite_rejected(self):
        path = self.folder / 'invalid.json'
        raw = canonical(terminal(self.identity))
        for content, expected in [(raw + b' ', hashlib.sha256(raw).hexdigest()),
                                  (b'{"x":1,"x":2}', None), (b'{"x":NaN}', None),
                                  (b'{"x":1e999}', None)]:
            path.write_bytes(content)
            digest = expected or hashlib.sha256(content).hexdigest()
            with self.assertRaises(ReceiptConflict):
                consume_recorded_reply(path, digest, self.identity, self.schema, self.journal,
                                       initial_messages=self.initial, allow_feedback=False)
        self.assertEqual(self.sent, [])

    def test_native_cache_replay_has_no_new_DB_charge(self):
        receipt = terminal(self.identity)
        self.wires = [explain(), native([[1]])]
        first = self.consume(receipt)
        second = self.consume(receipt)
        self.assertEqual(first['native']['new_physical_requests'], 2)
        self.assertEqual(second['native']['new_physical_requests'], 0)
        self.assertEqual(second['native']['physical_database_requests'], 2)
        self.assertEqual(first['source_receipt_sha256'], second['source_receipt_sha256'])
        self.assertEqual(len(self.sent), 2)

    def test_unknown_native_query_is_not_retried_or_answered_empty(self):
        self.wires = [explain(), {'raw': b'{', 'http_status': 200,
                       'transport_complete': False, 'execution_complete': False,
                       'failure_kind': 'TimeoutError'}]
        result = self.consume(terminal(self.identity))
        self.assertEqual(result['final_answer']['status'], 'UNKNOWN')
        self.assertIsNone(self.score(result, [])['primary'])
        self.assertIsNone(result['feedback_messages'])
        self.assertEqual(len(self.sent), 2)

    def test_unknown_reference_stays_in_fixed_population(self):
        self.wires = [explain(), native([[1]])]
        result = self.consume(terminal(self.identity))
        row = self.score(result, [], known=False)
        summary = summarize([row], expected_keys=[row['task_key']])
        self.assertTrue(row['complete_answer'])
        self.assertIsNone(row['primary'])
        self.assertEqual(summary['all_positions'], 1)
        self.assertEqual(summary['reference_unknown'], 1)
        self.assertEqual(summary['positions_excluded'], 0)

    def test_author_row_set_and_strict_bag_are_not_conflated(self):
        self.wires = [explain(), native([[1], [1]])]
        row = self.score(self.consume(terminal(self.identity)), [[1]])
        self.assertTrue(row['primary'])
        self.assertFalse(row['strict_bag'])

    def test_no_feedback_for_direct_view_and_no_second_feedback(self):
        receipt = terminal(self.identity, status='empty', content='')
        first = self.consume(receipt, allow_feedback=False)
        self.assertIsNone(first['feedback_messages'])
        identity = replace(self.identity, phase='feedback')
        final = self.consume(terminal(identity, status='empty', content=''), identity, filename='last.json')
        self.assertIsNone(final['feedback_messages'])
        with self.assertRaises(ValueError):
            self.consume(terminal(identity), identity, allow_feedback=False)

    def test_native_journal_conflict_is_not_zero_cost_failure(self):
        def conflict(*args):
            raise JournalConflict('Synthetic unresolved native request')
        self.journal.execute = conflict
        with self.assertRaises(JournalConflict):
            self.consume(terminal(self.identity))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--author-source', required=True, type=Path)
    parser.add_argument('--author-sha256', required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    ConsumerChecks.output = args.out
    ConsumerChecks.compare = staticmethod(author_comparator(args.author_source, args.author_sha256))
    ConsumerChecks.comparator_sha256 = args.author_sha256
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerChecks)
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('Network prohibited')):
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    summary = {'kind': 'SYNTHETIC_RECEIPT_NATIVE_ANSWER_SCORE_CHECKS_NOT_EXPERIMENT',
               'planned': suite.countTestCases() if not result.testsRun else result.testsRun,
               'completed': result.testsRun, 'failed': len(result.failures) + len(result.errors),
               'skipped': len(result.skipped), 'checks_passed': result.testsRun - len(result.failures) - len(result.errors),
               'author_source_sha256': args.author_sha256,
               'model_calls': 0, 'live_database_requests': 0, 'benchmark_tasks_run': 0,
               'references_to_consumer': False, 'adapter_registered': False, 'formal_admission': False}
    (args.out / 'summary.json').write_bytes(canonical(summary) + b'\n')
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == '__main__':
    main()
