"""Fault injection with fake transport and real SDK values; no DB connection."""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import socket
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from neo4j import Record
from neo4j.graph import Graph, Node

from neo4j_native_answer_contract_dev_20261029 import (
    IdentityScopeConflict, UnsupportedNativeValue, compare_complete_auxiliary,
)
from neo4j_read_capture_dev_20261029 import (
    CaptureStateError, NativeReadCapture, native_identity_projection,
)
from public_read_guard import ReadOnlyViolation


def summary(**overrides):
    fields = dict(query_type='r', database='fixture',
                  counters=SimpleNamespace(contains_updates=False),
                  result_available_after=None, result_consumed_after=None)
    fields.update(overrides)
    return SimpleNamespace(**fields)


class Result:
    def __init__(self, rows=(), columns=('x',), end=None, fetch_error=None, consume_error=None):
        self.rows, self.columns, self.end = rows, columns, end or summary()
        self.fetch_error, self.consume_error = fetch_error, consume_error
        self.consumed = False

    def keys(self):
        return list(self.columns)

    def __iter__(self):
        yield from self.rows
        if self.fetch_error:
            raise self.fetch_error

    def consume(self):
        if self.consume_error:
            raise self.consume_error
        self.consumed = True
        return self.end


class Transaction:
    def __init__(self, results):
        self.results, self.calls, self.closed = iter(results), [], 0
        self.close_error = None

    def run(self, query, parameters):
        if type(query) is not str:
            raise AssertionError('Explicit Transaction.run does not accept Query wrappers')
        self.calls.append((query, deepcopy(parameters)))
        value = next(self.results)
        if isinstance(value, BaseException):
            raise value
        return value

    def close(self):
        self.closed += 1
        if self.close_error:
            raise self.close_error


class Driver:
    def __init__(self, results):
        self.tx = Transaction(results)
        self.session_calls, self.begin_calls, self.closed = [], [], 0
        self.begin_error = None

    def session(self, **options):
        self.session_calls.append(options)
        return self

    def begin_transaction(self, **options):
        self.begin_calls.append(options)
        if self.begin_error:
            raise self.begin_error
        return self.tx

    def close(self):
        self.closed += 1


class CaptureChecks(unittest.TestCase):
    def setUp(self):
        self.events = []

    def capture(self, results=(), **options):
        driver = Driver(results)
        settings = dict(snapshot_sha256='a'*64, database='fixture', transaction_timeout=75,
                        max_rows=10, max_answer_bytes=100000, emit=self.events.append)
        settings.update(options)
        return NativeReadCapture(driver, **settings), driver

    def pair(self, rows, **options):
        return [Result(), Result(rows, **options)]

    def test_one_explicit_transaction_owns_multiple_reads_and_native_identity(self):
        a = Node(Graph(), 'entity_a', 1, ['Person'], {'display': 'Same'})
        b = Node(Graph(), 'entity_a', 1, ['Person'], {'display': 'Same'})
        cap, driver = self.capture(self.pair([Record([('x', a)])])+self.pair([Record([('x', b)])]))
        with cap:
            first = cap.read('MATCH (n) RETURN n', {}, purpose='method')
            second = cap.read('MATCH (m) RETURN m', {}, purpose='evaluator')
        self.assertTrue(compare_complete_auxiliary(first, second, ordered=False))
        self.assertEqual(len(driver.session_calls), 1)
        self.assertEqual(driver.session_calls[0], {'database':'fixture', 'default_access_mode':'READ'})
        self.assertEqual(driver.begin_calls, [{'timeout':75, 'metadata':{'native_capture_id':cap.scope.capture_id}}])
        self.assertEqual((driver.tx.closed, driver.closed), (1, 1))

    def test_native_identity_lowering_uses_real_entity_not_fabricated_eid(self):
        for kind in ('node', 'relationship'):
            item = native_identity_projection('n', 'entity', {'n':kind})
            self.assertEqual(item['projection'], 'n AS `entity`')
            self.assertEqual(item['entity_kind'], kind)
            self.assertNotIn('.eid', item['projection'])
            self.assertFalse(item['cross_transaction_identity_certified'])

    def test_invalid_identity_binding_or_query_injection_is_rejected(self):
        for var, alias, binding in [('n.x','id',{'n.x':'node'}), ('n','x` RETURN 1',{'n':'node'}),
                                    ('n','x',{'n':'str'}), ('n','x',{}), ('n','x',None)]:
            with self.subTest(var=var), self.assertRaises(ValueError):
                native_identity_projection(var, alias, binding)

    def test_cross_transaction_entity_comparison_is_not_certified(self):
        results = []
        for unused in range(2):
            cap, driver = self.capture(self.pair([[Node(Graph(), 'same', 1, [], {})]]))
            with cap:
                results.append(cap.read('RETURN 1', {}, purpose='method'))
        self.assertNotEqual(results[0]['scope']['capture_id'], results[1]['scope']['capture_id'])
        with self.assertRaises(IdentityScopeConflict):
            compare_complete_auxiliary(*results, ordered=False)

    def test_durable_intent_precedes_explain_and_actual_answer_is_saved(self):
        cap, driver = self.capture(self.pair([[1]]))
        with cap:
            answer = cap.read('RETURN $value', {'value':1}, purpose='control')
        kinds = [e['kind'] for e in self.events]
        self.assertEqual(kinds, ['capture_open_intent','capture_opened','read_intent',
                                'read_explained','answer_captured','capture_closed'])
        saved = self.events[4]
        self.assertEqual(saved['answer'], answer)
        self.assertEqual(saved['purpose'], 'control')
        self.assertIsNone(saved['server_consumed_after_ms'])
        self.assertEqual(driver.tx.calls[0][0], 'EXPLAIN RETURN $value')
        self.assertEqual(driver.tx.calls[1][0], 'RETURN $value')

    def test_open_intent_persistence_failure_opens_no_connection(self):
        def broken(event):
            raise OSError('audit storage unavailable')
        cap, driver = self.capture(emit=broken)
        with self.assertRaises(OSError):
            with cap:
                self.fail('Entered capture without durable intent')
        self.assertFalse(driver.session_calls)

    def test_read_intent_persistence_failure_sends_no_statement(self):
        def broken(event):
            if event['kind'] == 'read_intent':
                raise OSError('full disk')
            self.events.append(event)
        cap, driver = self.capture(emit=broken)
        with self.assertRaises(OSError):
            with cap:
                cap.read('RETURN 1', {}, purpose='method')
        self.assertEqual(driver.tx.calls, [])
        self.assertEqual(driver.tx.closed, 1)

    def test_answer_persistence_failure_is_not_returned_as_success(self):
        def broken(event):
            if event['kind'] == 'answer_captured':
                raise OSError('cannot persist answer')
            self.events.append(event)
        cap, driver = self.capture(self.pair([[1]]), emit=broken)
        with self.assertRaises(OSError):
            with cap:
                cap.read('RETURN 1', {}, purpose='method')
        self.assertEqual(driver.tx.closed, 1)
        self.assertIn('read_failed', [e['kind'] for e in self.events])

    def test_write_or_procedure_rejected_before_any_statement(self):
        for query in ('CREATE (n) RETURN n', 'CALL db.labels() RETURN 1', 'RETURN 1; RETURN 2'):
            cap, driver = self.capture()
            with self.subTest(query=query), self.assertRaises(ReadOnlyViolation):
                with cap:
                    cap.read(query, {}, purpose='method')
            self.assertEqual(driver.tx.calls, [])

    def test_explain_wrong_database_or_write_summary_prevents_execution(self):
        for end in (summary(database='other'), summary(query_type='rw'),
                    summary(counters=SimpleNamespace(contains_updates=True))):
            cap, driver = self.capture([Result(end=end)])
            with self.assertRaises(CaptureStateError):
                with cap:
                    cap.read('RETURN 1', {}, purpose='method')
            self.assertEqual(len(driver.tx.calls), 1)

    def test_final_summary_must_certify_read_and_database(self):
        cap, driver = self.capture(self.pair([[1]], end=summary(database='other')))
        with self.assertRaises(CaptureStateError):
            with cap:
                cap.read('RETURN 1', {}, purpose='method')
        self.assertNotIn('answer_captured', [e['kind'] for e in self.events])

    def test_transport_failure_never_retries_or_allows_more_reads(self):
        cap, driver = self.capture([ConnectionResetError('gateway disconnected')])
        with cap:
            with self.assertRaises(ConnectionResetError):
                cap.read('RETURN 1', {}, purpose='method')
            with self.assertRaises(CaptureStateError):
                cap.read('RETURN 2', {}, purpose='method')
        self.assertEqual(len(driver.tx.calls), 1)
        self.assertTrue(self.events[-1]['failed'])

    def test_mid_stream_failure_never_saves_partial_success(self):
        cap, driver = self.capture(self.pair([[1]], fetch_error=ConnectionResetError()))
        with self.assertRaises(ConnectionResetError):
            with cap:
                cap.read('RETURN 1', {}, purpose='method')
        self.assertNotIn('answer_captured', [e['kind'] for e in self.events])

    def test_consume_failure_does_not_certify_already_fetched_rows(self):
        cap, driver = self.capture(self.pair([[1]], consume_error=TimeoutError()))
        with self.assertRaises(TimeoutError):
            with cap:
                cap.read('RETURN 1', {}, purpose='method')
        self.assertNotIn('answer_captured', [e['kind'] for e in self.events])

    def test_row_limit_keeps_exact_limit_but_never_truncates_extra_row(self):
        for rows, fails in (([[1],[2]],False), ([[1],[2],[3]],True)):
            cap, driver = self.capture(self.pair(rows), max_rows=2)
            if fails:
                with self.assertRaises(UnsupportedNativeValue):
                    with cap:
                        cap.read('RETURN 1', {}, purpose='method')
            else:
                with cap:
                    self.assertEqual(len(cap.read('RETURN 1', {}, purpose='method')['rows']), 2)

    def test_byte_overflow_is_failure_not_successful_short_answer(self):
        cap, driver = self.capture(self.pair([['x'*2000]]), max_answer_bytes=900)
        with self.assertRaises(UnsupportedNativeValue):
            with cap:
                cap.read('RETURN 1', {}, purpose='method')
        self.assertNotIn('answer_captured', [e['kind'] for e in self.events])

    def test_duplicate_columns_are_kept_positionally(self):
        cap, driver = self.capture(self.pair([Record([('x',1),('x',2)])], columns=['x','x']))
        with cap:
            answer = cap.read('RETURN 1 AS x, 2 AS x', {}, purpose='method')
        self.assertEqual(answer['columns'], ['x','x'])
        self.assertEqual([v['v'] for v in answer['rows'][0]], ['1','2'])

    def test_empty_complete_result_is_not_a_failed_read(self):
        cap, driver = self.capture(self.pair([]))
        with cap:
            answer = cap.read('MATCH (n) WHERE false RETURN n', {}, purpose='method')
        self.assertTrue(answer['complete'])
        self.assertEqual(answer['rows'], [])

    def test_unknown_native_value_fails_without_string_fallback(self):
        cap, driver = self.capture(self.pair([[object()]]))
        with self.assertRaises(UnsupportedNativeValue):
            with cap:
                cap.read('RETURN 1', {}, purpose='method')

    def test_capture_cannot_be_reentered_or_used_after_close(self):
        cap, driver = self.capture()
        with cap:
            pass
        with self.assertRaises(CaptureStateError):
            cap.read('RETURN 1', {}, purpose='method')
        with self.assertRaises(CaptureStateError):
            cap.__enter__()
        self.assertEqual(len(driver.begin_calls), 1)

    def test_other_thread_cannot_run_on_shared_transaction(self):
        cap, driver = self.capture()
        errors = []
        def other_thread():
            try:
                cap.read('RETURN 1', {}, purpose='method')
            except CaptureStateError as error:
                errors.append(error)
        with cap:
            thread = threading.Thread(target=other_thread)
            thread.start(); thread.join()
        self.assertEqual(len(errors), 1)
        self.assertEqual(driver.tx.calls, [])

    def test_begin_failure_closes_session_without_automatic_retry(self):
        cap, driver = self.capture()
        driver.begin_error = ConnectionError('begin failed')
        with self.assertRaises(ConnectionError):
            with cap:
                self.fail('Unexpected transaction')
        self.assertEqual(len(driver.begin_calls), 1)
        self.assertEqual(driver.closed, 1)

    def test_rollback_failure_still_closes_session_and_is_reported(self):
        cap, driver = self.capture()
        driver.tx.close_error = ConnectionError('rollback failed')
        with self.assertRaises(ConnectionError):
            with cap:
                pass
        self.assertEqual(driver.closed, 1)
        self.assertEqual(self.events[-1]['kind'], 'capture_close_failed')

    def test_timeout_and_limits_are_explicit_and_not_unbounded(self):
        for overrides in ({'transaction_timeout':0}, {'transaction_timeout':True},
                          {'transaction_timeout':float('nan')}, {'max_rows':0},
                          {'max_answer_bytes':True}, {'emit':None}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.capture(**overrides)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CaptureChecks)
    planned = suite.countTestCases()
    with patch.object(socket, 'create_connection', side_effect=AssertionError('No network permitted')), \
         patch.object(socket.socket, 'connect', side_effect=AssertionError('No network permitted')):
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    receipt = dict(kind='SYNTHETIC_NATIVE_TRANSACTION_FAULT_INJECTION_NOT_DB_ACCEPTANCE',
                   planned=planned, completed=result.testsRun,
                   checks_passed=result.testsRun-len(result.failures)-len(result.errors),
                   failed=len(result.failures)+len(result.errors), model_calls=0,
                   DB_connections=0, DB_queries=0, real_transaction_certified=False,
                   full_compiler_implemented=False, formal_admission=False)
    target = Path(args.output)
    target.mkdir(parents=True, exist_ok=False)
    (target/'summary.json').write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n', encoding='utf-8')
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == '__main__':
    main()
