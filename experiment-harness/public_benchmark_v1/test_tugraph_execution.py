"""Failure/recovery invariants; fabricated responses are not benchmark scores."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tugraph_execution import (NativeQueryJournal, JournalConflict, LocalRestTransport,
                               canonical, non_gold_feedback, execute_native_plan)
from tugraph_schema import schema_from_import_config
from test_tugraph_schema import CONFIG


def wire(rows=None, columns=None, **extra):
    raw = canonical({'header': [{'name': c, 'type': 0} for c in (columns or ['n'])],
                     'result': [[1]] if rows is None else rows,
                     'size': 1 if rows is None else len(rows)})
    return {'raw': raw, 'http_status': 200, 'transport_complete': True,
            'execution_complete': True, **extra}


def plan(readonly=1):
    return wire([['ReadOnly:%s\nExecution Plan:\nProject\n' % readonly]], ['@plan'])


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.calls = []

    def executor(self, replies):
        def transport(payload, wall, cap):
            self.calls.append(json.loads(payload))
            reply = replies.pop(0)
            if isinstance(reply, BaseException):
                raise reply
            return reply
        return NativeQueryJournal(self.temp.name, transport)

    def test_complete_replay_is_no_new_request_and_keeps_cost(self):
        run = self.executor([plan(), wire()])
        first = run.execute('task0_direct_0', 'fixture', 'RETURN 1 AS n')
        cached = run.execute('task0_direct_0', 'fixture', 'RETURN 1 AS n')
        self.assertEqual(first['rows'], [[1]])
        self.assertEqual(first['physical_database_requests'], 2)
        self.assertEqual(cached['new_physical_requests'], 0)
        self.assertEqual(cached['sql_wall_seconds'], first['sql_wall_seconds'])
        self.assertIsNone(cached['monetary_cost'])
        self.assertEqual(len(self.calls), 2)

    def test_identity_conflict_cannot_send_changed_query(self):
        run = self.executor([plan(), wire()])
        run.execute('same', 'fixture', 'RETURN 1 AS n')
        with self.assertRaises(JournalConflict):
            run.execute('same', 'fixture', 'RETURN 2 AS n')
        self.assertEqual(len(self.calls), 2)

    def test_process_interruption_stays_pending_without_replay(self):
        run = self.executor([KeyboardInterrupt()])
        with self.assertRaises(KeyboardInterrupt):
            run.execute('pending', 'fixture', 'RETURN 1 AS n')
        with self.assertRaises(JournalConflict):
            run.execute('pending', 'fixture', 'RETURN 1 AS n')
        self.assertEqual(len(self.calls), 1)
        self.assertTrue((Path(self.temp.name)/'pending/explain/reserved.json').exists())

    def test_partial_response_is_saved_and_never_empty_success(self):
        run = self.executor([plan(), {'raw': b'{"header":', 'http_status': 200,
            'transport_complete': False, 'execution_complete': False,
            'failure_kind': 'TimeoutError'}])
        result = run.execute('partial', 'fixture', 'RETURN 1 AS n')
        self.assertEqual(result['status'], 'UNKNOWN_EXECUTION')
        self.assertNotIn('rows', result)
        self.assertEqual((Path(self.temp.name)/'partial/query/response.bin').read_bytes(), b'{"header":')
        self.assertIsNone(non_gold_feedback(result, previous_feedback_count=0))
        run.execute('partial', 'fixture', 'RETURN 1 AS n')
        self.assertEqual(len(self.calls), 2)

    def test_readonly_not_attested_stops_before_query(self):
        run = self.executor([plan(0)])
        self.assertEqual(run.execute('writeplan', 'fixture', 'RETURN 1 AS n')['status'],
                         'READ_ONLY_NOT_CONFIRMED')
        self.assertEqual(len(self.calls), 1)

    def test_write_and_procedure_guard_are_durable_zero_request(self):
        run = self.executor([])
        for i, query in enumerate(('CREATE (p) RETURN p', 'CALL dbms.list() RETURN 1',
                                   'RETURN 1; RETURN 2', 'EXPLAIN RETURN 1')):
            result = run.execute('bad'+str(i), 'fixture', query)
            self.assertEqual(result['status'], 'GUARD_REJECTED')
            self.assertEqual(result['physical_database_requests'], 0)
            self.assertTrue((Path(self.temp.name)/('bad'+str(i))/'complete.json').exists())
        self.assertEqual(self.calls, [])

    def test_literal_write_word_is_not_a_write(self):
        run = self.executor([plan(), wire([['CREATE']])])
        self.assertEqual(run.execute('literal', 'fixture', 'RETURN "CREATE" AS n')['rows'], [['CREATE']])

    def test_engine_syntax_diagnostic_stops_and_is_feedback_eligible(self):
        errors = ('CypherException: bad syntax', '[ParserException] ParserException: bad syntax')
        for i, error in enumerate(errors):
            run = self.executor([{'raw': canonical({'error_message': error}),
                'http_status': 500, 'transport_complete': True, 'execution_complete': False}])
            result = run.execute('syntax'+str(i), 'fixture', 'MATCH RETURN 1')
            self.assertEqual(result['status'], 'EXPLAIN_ERROR')
            feedback = non_gold_feedback(result, previous_feedback_count=0)
            self.assertIn('bad syntax', feedback['content'])
            self.assertIsNone(non_gold_feedback(result, previous_feedback_count=1))
        self.assertEqual(len(self.calls), 2)

    def test_server_auth_and_unknown_errors_are_not_semantic_feedback(self):
        for status in (401, 429, 500, 503):
            run = self.executor([{'raw': canonical({'error_message': 'private server failure'}),
                'http_status': status, 'transport_complete': True, 'execution_complete': False}])
            outcome = run.execute('infra'+str(status), 'fixture', 'RETURN 1 AS n')
            self.assertEqual(outcome['status'], 'INFRASTRUCTURE_ERROR')
            self.assertIsNone(non_gold_feedback(outcome, previous_feedback_count=0))

    def test_bad_shape_and_unknown_execution_do_not_parse_success(self):
        for i, response in enumerate((wire(execution_complete=False),
                {'raw': b'{}', 'http_status': 200, 'transport_complete': True,
                 'execution_complete': True})):
            run = self.executor([plan(), response])
            result = run.execute('badshape'+str(i), 'fixture', 'RETURN 1 AS n')
            self.assertIn(result['status'], ('UNKNOWN_EXECUTION', 'MALFORMED_RESULT'))
            self.assertNotIn('rows', result)

    def test_valid_empty_or_wrong_answer_has_no_gold_repair(self):
        for i, rows in enumerate(([], [[999]])):
            run = self.executor([plan(), wire(rows)])
            result = run.execute('valid'+str(i), 'fixture', 'RETURN 1 AS n')
            self.assertEqual(result['status'], 'EXECUTED')
            self.assertIsNone(non_gold_feedback({**result, 'gold': [[1]]}, previous_feedback_count=0))

    def test_response_tamper_fails_cached_integrity_check(self):
        run = self.executor([plan(), wire()])
        run.execute('tamper', 'fixture', 'RETURN 1 AS n')
        (Path(self.temp.name)/'tamper/query/response.bin').write_bytes(b'{}')
        with self.assertRaises(JournalConflict):
            run.execute('tamper', 'fixture', 'RETURN 1 AS n')
        self.assertEqual(len(self.calls), 2)

    def test_concurrent_same_id_is_not_dispatched_twice(self):
        entered, release = threading.Event(), threading.Event()
        def transport(payload, wall, cap):
            self.calls.append(payload)
            entered.set()
            release.wait(2)
            return plan() if len(self.calls) == 1 else wire()
        run = NativeQueryJournal(self.temp.name, transport)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(run.execute, 'concurrent', 'fixture', 'RETURN 1 AS n')
            self.assertTrue(entered.wait(2))
            try:
                with self.assertRaises(JournalConflict):
                    run.execute('concurrent', 'fixture', 'RETURN 1 AS n')
            finally:
                release.set()
            self.assertEqual(future.result()['status'], 'EXECUTED')
        self.assertEqual(len(self.calls), 2)

    def test_byte_and_row_caps_are_incomplete(self):
        for i, response in enumerate(({'raw': b'x'*100, 'http_status': 200,
                'transport_complete': False, 'execution_complete': False,
                'failure_kind': 'BYTE_CAP'}, wire([[1], [2]]))):
            run = self.executor([plan(), response])
            with patch('tugraph_results.MAX_ROWS', 2):
                result = run.execute('cap'+str(i), 'fixture', 'RETURN 1 AS n')
            self.assertEqual(result['status'], 'TRUNCATED')
            self.assertIsNone(non_gold_feedback(result, previous_feedback_count=0))

    def test_native_plan_preserves_unknown_sql_status_and_cost(self):
        run = self.executor([plan(), {'raw': b'', 'http_status': None,
            'transport_complete': False, 'execution_complete': False,
            'failure_kind': 'TimeoutError'}])
        selection = {'nodes': [{'id': 'p', 'label': 'Person'}], 'edges': [],
                     'where': [], 'returns': [{'field': 'p.id', 'alias': 'id'}]}
        result = execute_native_plan({'status': 'ANSWER', 'calls': [selection],
            'combine': {'op': 'single'}}, schema_from_import_config(CONFIG), run,
            request_prefix='path', graph='fixture')
        self.assertEqual(result['status'], 'UNKNOWN_EXECUTION')
        self.assertEqual(result['physical_database_requests'], 2)
        self.assertIsNone(non_gold_feedback(result, previous_feedback_count=0))

    def test_native_plan_unsupported_is_zero_sql_no_direct_fallback(self):
        result = execute_native_plan({'status': 'ANSWER', 'calls': [],
            'combine': {'op': 'single'}}, schema_from_import_config(CONFIG), self.executor([]),
            request_prefix='unsupported', graph='fixture')
        self.assertEqual(result['status'], 'UNSUPPORTED')
        self.assertEqual(result['physical_database_requests'], 0)
        self.assertEqual(self.calls, [])


class HttpTransportTests(unittest.TestCase):
    def test_actual_http_content_length_and_short_body(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                raw = b'{"ok":true}'
                self.send_response(200)
                self.send_header('Content-Length', str(len(raw)+(10 if request['short'] else 0)))
                self.send_header('Connection', 'close')
                self.end_headers()
                self.wfile.write(raw)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            transport = LocalRestTransport(server.server_port, 'synthetic-test-token')
            good = transport(canonical({'short': False}), 2, 1024)
            bad = transport(canonical({'short': True}), 2, 1024)
            self.assertTrue(good['transport_complete'])
            self.assertFalse(bad['transport_complete'])
            self.assertEqual(good['raw'], bad['raw'])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)


if __name__ == '__main__':
    unittest.main()
