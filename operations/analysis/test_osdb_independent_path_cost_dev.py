"""Deterministic synthetic client/read timings, not live model/DB tests."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import json
import tempfile
import unittest
from osdb_independent_path_cost_dev import PathCosts, timed_scheduled_path


class Clock:
    def __init__(self): self.now = 0.0
    def __call__(self): return self.now
    def advance(self, seconds): self.now += seconds


class Lock:
    def __init__(self, clock): self.clock, self.held, self.releases = clock, False, 0
    def acquire(self):
        assert not self.held
        self.clock.advance(7)
        self.held = True
        return True
    def release(self):
        assert self.held
        self.held = False
        self.releases += 1


class Client:
    def __init__(self, directory, clock, arm='ours'):
        self.lease = SimpleNamespace(directory=directory, stopped=False, reserved={})
        self.key = 'synthetic__' + arm
        self.initial = {self.key: [{'role': 'system', 'content': 'synthetic'},
                                   {'role': 'user', 'content': 'synthetic'}]}
        self.clock, self.calls, self.stops = clock, [], []
    def generate(self, key, phase, result=None):
        self.calls.append(phase)
        self.clock.advance(4 if phase == 'initial' else 6)
        return {'content': 'SYNTHETIC_NOT_MODEL_REPLY', 'finish_reason': 'stop',
                'transport_error': None}
    def stop_for_unknown_read(self, key):
        self.stops.append(key)
        self.lease.stopped = True


class CostTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.clock = Clock()
        self.client = Client(self.root, self.clock)
        self.costs = PathCosts(self.client.key, self.clock)
        self.lock = Lock(self.clock)
        self.counter = 0
        self.socket_guard = patch('socket.socket', side_effect=AssertionError('No network in cost checks'))
        self.socket = self.socket_guard.start()
    def tearDown(self):
        self.assertEqual(self.socket.call_count, 0)
        self.socket_guard.stop()
        self.folder.cleanup()
    def physical(self, query, params):
        self.assertTrue(self.lock.held)
        self.counter += 2  # synthetic EXPLAIN + read; never inferred from callback count
        self.clock.advance(11)
        return {'status': 'EXECUTED', 'complete': True, 'rows': [[1]],
                'columns': ['n'], 'result_bytes': 5}
    def read(self, q, p):
        return self.costs.read(self.physical, q, p, self.lock, lambda: self.counter)
    def answer(self, arm, record, schema, execute):
        self.clock.advance(2)
        result = execute('SYNTHETIC_QUERY', {})
        self.clock.advance(3)
        return result
    def control(self, arm, text, schema, execute, strategy):
        self.assertEqual(strategy, 'full_detail')
        self.clock.advance(5)
        result = execute('SYNTHETIC_CONTROL_QUERY', {})
        self.clock.advance(17)
        return result
    def run_path(self, answer=None, control=None):
        with patch('osdb_independent_path_cost_dev.outcome', answer or self.answer), \
             patch('osdb_independent_path_cost_dev.execute_response', control or self.control):
            return timed_scheduled_path(self.client, self.client.key, 'ours', {}, self.read, self.costs)

    def test_control_time_excluded_and_nested_components_not_double_counted(self):
        saved = self.run_path()
        groups = saved['costs']['groups']
        self.assertEqual(groups['method']['root_wall_seconds'], 27)
        self.assertEqual(groups['full_detail_control']['root_wall_seconds'], 40)
        method = groups['method']
        self.assertEqual(method['model_client_generate_wall_seconds'], 4)
        self.assertEqual(method['read_lock_wait_seconds'], 7)
        self.assertEqual(method['db_executor_client_wall_seconds'], 11)
        self.assertEqual(method['answer_processing_excluding_read_seconds'], 5)
        self.assertEqual(method['exclusive_accounted_seconds'], 27)
        self.assertEqual(method['physical_statements'], 2)
        self.assertEqual(method['read_callbacks'], 1)
        self.assertEqual(method['returned_rows'], 1)
        self.assertEqual(method['result_bytes'], 5)
        self.assertEqual(self.client.calls, ['initial'])
        self.assertIsNone(saved['costs']['pure_decode_tokens_per_second'])

    def test_feedback_time_is_in_method_not_control(self):
        def answer(arm, record, schema, execute):
            if len(self.client.calls) == 1:
                self.clock.advance(1)
                return {'status': 'EXECUTION_ERROR', 'complete': False,
                        'rows': None, 'columns': None, 'operational_error': 'synthetic'}
            return self.answer(arm, record, schema, execute)
        saved = self.run_path(answer=answer)
        self.assertEqual(self.client.calls, ['initial', 'feedback'])
        self.assertEqual(saved['costs']['method_path_wall_seconds'], 34)
        self.assertEqual(saved['costs']['groups']['method']['model_client_generate_wall_seconds'], 10)

    def test_stopped_batch_withholds_feedback_without_fake_failure_or_second_call(self):
        def answer(*args):
            self.client.lease.stopped = True
            return {'status': 'EXECUTION_ERROR', 'complete': False,
                    'rows': None, 'columns': None, 'operational_error': 'synthetic'}
        saved = self.run_path(answer=answer)
        self.assertTrue(saved['feedback_withheld_due_to_batch_stop'])
        self.assertEqual(self.client.calls, ['initial'])
        self.assertEqual(saved['final']['status'], 'EXECUTION_ERROR')
        self.assertNotIn('full_detail_control', saved['costs']['groups'])
        self.assertEqual(saved['costs']['groups']['method']['physical_statements'], 0)

    def test_control_crash_keeps_method_final_and_separate_error_spans(self):
        def broken(*args, **kwargs):
            self.clock.advance(100)
            raise RuntimeError('synthetic control failure')
        with self.assertRaisesRegex(RuntimeError, 'synthetic control'):
            self.run_path(control=broken)
        final = json.loads((self.root/'method_finals'/(self.client.key+'.json')).read_text())
        costs = json.loads((self.root/'costs'/(self.client.key+'.json')).read_text())
        self.assertEqual(final['final']['status'], 'EXECUTED')
        self.assertEqual(final['costs']['method_path_wall_seconds'], 27)
        self.assertEqual(costs['method_path_wall_seconds'], 27)
        self.assertEqual(costs['groups']['full_detail_control']['root_wall_seconds'], 100)
        self.assertTrue(any(s['status'] == 'ERROR' and s['group'] == 'full_detail_control'
                            for s in costs['spans']))
        self.assertFalse((self.root/'paths'/(self.client.key+'.json')).exists())
        self.assertEqual(self.client.calls, ['initial'])

    def test_missing_statement_and_byte_observations_stay_unknown(self):
        with self.costs.span('method_path', 'method'):
            self.costs.read(lambda q, p: {'rows': [[1]], 'complete': True}, '', {}, self.lock)
        group = self.costs.snapshot()['groups']['method']
        self.assertIsNone(group['physical_statements'])
        self.assertIsNone(group['result_bytes'])
        self.assertEqual(group['returned_rows'], 1)

    def test_failed_read_releases_lock_and_keeps_unknown_statement_count(self):
        def broken(*args):
            self.clock.advance(3)
            raise OSError('synthetic read failure')
        with self.assertRaises(OSError):
            with self.costs.span('method_path', 'method'):
                self.costs.read(broken, '', {}, self.lock)
        group = self.costs.snapshot()['groups']['method']
        self.assertFalse(self.lock.held)
        self.assertEqual(self.lock.releases, 1)
        self.assertIsNone(group['physical_statements'])
        self.assertIsNone(group['returned_rows'])
        self.assertEqual(group['db_executor_client_wall_seconds'], 3)

    def test_broken_optional_counter_does_not_block_or_retry_the_read(self):
        def broken_counter(): raise ValueError('synthetic counter observation unavailable')
        with self.costs.span('method_path', 'method'):
            result = self.costs.read(self.physical, '', {}, self.lock, broken_counter)
        self.assertTrue(result['complete'])
        self.assertEqual(self.counter, 2)
        self.assertEqual(self.lock.releases, 1)
        self.assertIsNone(self.costs.snapshot()['groups']['method']['physical_statements'])

    def test_open_measurement_is_not_a_zero_duration(self):
        with self.costs.span('method_path', 'method'):
            self.clock.advance(2)
            snap = self.costs.snapshot()
            self.assertIsNone(snap['method_path_wall_seconds'])
            self.assertIsNone(snap['groups']['method']['exclusive_accounted_seconds'])
        self.assertEqual(self.costs.snapshot()['method_path_wall_seconds'], 2)

    def test_unexpected_method_error_keeps_costs_and_never_runs_control(self):
        def broken(*args):
            raise AssertionError('synthetic adapter bug')
        with patch('osdb_independent_path_cost_dev.execute_response') as control, \
             patch('osdb_independent_path_cost_dev.outcome', broken):
            with self.assertRaisesRegex(AssertionError, 'synthetic adapter'):
                timed_scheduled_path(self.client, self.client.key, 'ours', {}, self.read, self.costs)
        control.assert_not_called()
        costs = json.loads((self.root/'costs'/(self.client.key+'.json')).read_text())
        self.assertTrue(any(s['status'] == 'ERROR' and s['group'] == 'method' for s in costs['spans']))
        self.assertEqual(self.client.calls, ['initial'])


if __name__ == '__main__':
    unittest.main()
