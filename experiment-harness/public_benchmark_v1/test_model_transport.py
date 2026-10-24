import json
from pathlib import Path
import tempfile
import unittest

from attempt_budget import AttemptBudget
from model_transport import ModelTransport, BatchStopped


class TransportChecks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        approval = self.root / 'approval.json'
        approval.write_text(json.dumps({'approved_cumulative_cap': 30}))
        self.budget = AttemptBudget(self.root / 'budget', approval)
        self.config = {'endpoint': 'http://unused.invalid/v1/chat/completions',
                       'api_key': 'unit-test-only-not-a-real-key', 'model': 'stub',
                       'temperature': 0, 'top_p': 0.95, 'reasoning_effort': 'low'}

    def test_three_failures_charge_each_attempt_and_stop_same_body(self):
        wires, sleeps = [], []

        def send(config, wire):
            wires.append(wire)
            return {'http_status': 500, 'body': {}}
        client = ModelTransport(self.config, self.budget, self.root / 'run',
                                send=send, sleep=sleeps.append)
        with self.assertRaises(BatchStopped):
            client.complete([{'role': 'user', 'content': '中文任务'}], purpose='development')
        self.assertEqual(len(wires), 3)
        self.assertEqual(len(set(wires)), 1)
        self.assertIn('中文任务'.encode(), wires[0])
        self.assertEqual(sleeps, [10, 10])
        self.assertEqual(self.budget.inspect()['reserved'], 3)
        self.assertEqual(self.budget.inspect()['pending'], [])
        with self.assertRaises(BatchStopped):
            client.complete([], purpose='must not restart')
        self.assertEqual(len(wires), 3)

    def test_valid_but_truncated_model_output_is_not_transport_retry(self):
        calls = []

        def send(config, wire):
            calls.append(wire)
            return {'http_status': 200, 'body': {'choices': [
                {'finish_reason': 'length', 'message': {'content': '{partial'}}]}}
        client = ModelTransport(self.config, self.budget, self.root / 'run', send=send)
        result = client.complete([], purpose='development')
        self.assertFalse(result['usable'])
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.budget.inspect()['reserved'], 1)

    def test_transport_recovery_then_success_preserves_all_attempts(self):
        calls, sleeps = [], []

        def send(config, wire):
            calls.append(wire)
            if len(calls) == 1:
                raise TimeoutError('injected unit-test timeout')
            return {'http_status': 200, 'body': {'choices': [
                {'finish_reason': 'stop', 'message': {'content': 'RETURN 1'}}],
                'usage': {'total_tokens': 3}}}
        client = ModelTransport(self.config, self.budget, self.root / 'run',
                                send=send, sleep=sleeps.append)
        result = client.complete([], purpose='development')
        self.assertTrue(result['usable'])
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(sleeps, [10])
        self.assertEqual(self.budget.inspect()['reserved'], 2)
        self.assertEqual(len(result['attempt_folders']), 2)

    def test_authentication_failure_and_call_allowance_do_not_loop(self):
        client = ModelTransport(self.config, self.budget, self.root / 'auth',
                                send=lambda c, w: {'http_status': 401, 'body': {}})
        with self.assertRaises(BatchStopped):
            client.complete([], purpose='development')
        with self.assertRaises(BatchStopped):
            client.complete([], purpose='development')
        self.assertEqual(client.physical_calls, 1)
        success = lambda c, w: {'http_status': 200, 'body': {'choices': [
            {'finish_reason': 'stop', 'message': {'content': 'RETURN 1'}}]}}
        other = ModelTransport(self.config, self.budget, self.root / 'finite',
                               max_physical_calls=1, send=success)
        other.complete([], purpose='development')
        with self.assertRaises(BatchStopped):
            other.complete([], purpose='development')
        self.assertEqual(other.physical_calls, 1)

    def test_credentials_are_not_persisted(self):
        client = ModelTransport(self.config, self.budget, self.root / 'run',
                                send=lambda c, w: {'http_status': 500}, sleep=lambda s: None)
        with self.assertRaises(BatchStopped):
            client.complete([], purpose='development')
        for file in self.root.rglob('*.json'):
            self.assertNotIn(self.config['api_key'], file.read_text(encoding='utf-8'))


if __name__ == '__main__':
    unittest.main()
