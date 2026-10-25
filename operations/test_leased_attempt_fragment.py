"""Synthetic offline crash/concurrency/cap checks; never touches actual budget."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from leased_attempt_fragment import (ExecutionLease, LeaseBlocked,
                                     LeasedAttemptFragment, create_json, sha, history_seal)
CANONICAL = Path('D:/ch7/code/OSDB_CSTKG_Text2Cypher_publication_t1')
sys.path.insert(0, str(CANONICAL / 'experiment-harness/public_benchmark_v1'))


class CutoverBudgetChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='t1-offline-lease-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.approval = self.root / 'approval.json'
        create_json(self.approval, {'approved_cumulative_cap': 12000,
                                   'historical_attempts_at_approval': 364})
        self.baseline = self.root / 'baseline.json'
        create_json(self.baseline, {
            'kind': 'SEALED_CUMULATIVE_ATTEMPT_BASELINE_V1',
            'reserved': 3266, 'cap': 12000, 'pending': [],
            'approval_sha256': sha(self.approval),
            'entries': [[n, 'synthetic-ledger', 'synthetic-request', 'synthetic-terminal']
                        for n in range(1, 3267)]})
        self.baseline_sha = sha(self.baseline)
        self.lease_path = self.root / 'execution.lock'
        self.lease = ExecutionLease(self.lease_path, 'test-lease', 'offline-fixture').acquire()
        self.calls = self.root / 'new_calls'
        self.calls.mkdir()
        self.budget = LeasedAttemptFragment(
            self.root / 'fragment', self.approval,
            baseline=self.baseline, baseline_sha=self.baseline_sha,
            lease=self.lease, requests_root=self.calls)

    def request(self, name):
        parent = self.calls / name
        parent.mkdir()
        path = parent / 'request.json'
        create_json(path, {'messages': ['synthetic fixture only']})
        return path

    def terminal(self, request, *, failed=False):
        create_json(request.with_name('terminal.json'),
                    {'transport_error': 'synthetic timeout'} if failed else
                    {'http_status': 200, 'body': {'usage': {'total_tokens': 7}}})

    def test_sealed_history_carries_forward_not_zero(self):
        self.assertEqual(self.budget.inspect()['reserved'], 3266)
        self.assertEqual(self.budget.reserve(self.request('one'), purpose='test'), 3267)
        self.assertEqual(self.budget.inspect()['remaining'], 8733)

    def test_unknown_result_blocks_next_and_no_automatic_replay(self):
        request = self.request('one')
        self.budget.reserve(request, purpose='test')
        self.assertEqual(self.budget.inspect()['pending'], [3267])
        with self.assertRaises(LeaseBlocked):
            self.budget.reserve(self.request('two'), purpose='test')
        with self.assertRaises(LeaseBlocked):
            self.budget.reserve(request, purpose='test')
        self.assertEqual(len(list(self.budget.ledger.glob('*.json'))), 1)

    def test_failed_physical_attempt_consumes_a_slot_and_retry_is_new_slot(self):
        one = self.request('one')
        self.budget.reserve(one, purpose='test')
        self.terminal(one, failed=True)
        self.assertEqual(self.budget.reserve(self.request('retry'), purpose='test'), 3268)
        self.assertEqual(self.budget.inspect()['reserved'], 3268)

    def test_budget_exhaustion_does_not_allow_one_extra(self):
        # Independent miniature cap boundary, with the same cumulative algorithm.
        self.approval.write_text(json.dumps({'approved_cumulative_cap': 3267,
                                            'historical_attempts_at_approval': 364}))
        base = json.loads(self.baseline.read_text())
        base.update(cap=3267, approval_sha256=sha(self.approval))
        self.baseline.write_text(json.dumps(base))
        self.budget.baseline_sha = sha(self.baseline)
        request = self.request('last')
        self.budget.reserve(request, purpose='test')
        self.terminal(request)
        self.assertEqual(self.budget.inspect()['remaining'], 0)
        with self.assertRaises(LeaseBlocked):
            self.budget.reserve(self.request('over-cap'), purpose='test')

    def test_baseline_and_approval_tampering_blocks(self):
        self.baseline.write_text('{}')
        with self.assertRaises(LeaseBlocked):
            self.budget.inspect()

    def test_approval_change_is_not_automatic_budget_increase(self):
        self.approval.write_text(json.dumps({'approved_cumulative_cap': 99999}))
        with self.assertRaises(LeaseBlocked):
            self.budget.inspect()

    def test_request_modified_after_reservation_blocks(self):
        request = self.request('one')
        self.budget.reserve(request, purpose='test')
        request.write_text('{"changed":true}')
        self.terminal(request)
        with self.assertRaises(LeaseBlocked):
            self.budget.inspect()

    def test_noncontiguous_or_reset_fragment_rejected(self):
        self.budget.reserve(self.request('one'), purpose='test')
        (self.budget.ledger / '3267.json').rename(self.budget.ledger / '0001.json')
        with self.assertRaises(LeaseBlocked):
            self.budget.inspect()

    def test_malformed_terminal_is_not_success_or_retriable(self):
        request = self.request('one')
        self.budget.reserve(request, purpose='test')
        create_json(request.with_name('terminal.json'), {'incomplete': True})
        with self.assertRaises(LeaseBlocked):
            self.budget.inspect()

    def test_outside_root_cannot_be_charged(self):
        path = self.root / 'foreign-request.json'
        create_json(path, {})
        with self.assertRaises(LeaseBlocked):
            self.budget.reserve(path, purpose='test')

    def test_unknown_execution_lock_is_preserved(self):
        expected = sha(self.lease_path)
        with self.assertRaises(LeaseBlocked):
            ExecutionLease(self.lease_path, 'second', 'other').acquire()
        self.assertEqual(sha(self.lease_path), expected)
        self.lease_path.write_text('{"unknown-owner":true}')
        with self.assertRaises(LeaseBlocked):
            self.budget.reserve(self.request('one'), purpose='test')
        self.assertTrue(self.lease_path.exists())

    def test_real_second_process_cannot_acquire_and_crash_does_not_clear(self):
        path = self.root / 'crashed-execution.lock'
        script = ('from leased_attempt_fragment import ExecutionLease;'
                  'import os,sys;'
                  'ExecutionLease(sys.argv[1],"child","fixture").acquire();'
                  'os._exit(23)')
        child = subprocess.run([sys.executable, '-c', script, str(path)],
                               cwd=Path(__file__).parent, capture_output=True)
        self.assertEqual(child.returncode, 23, child.stderr.decode(errors='replace'))
        self.assertTrue(path.exists())
        with self.assertRaises(LeaseBlocked):
            ExecutionLease(path, 'replacement', 'fixture').acquire()

    def test_original_windows_budget_lock_interoperability(self):
        from attempt_budget import AttemptBudget, BudgetBlocked
        ledger = self.root / 'original_ledger'
        ledger.mkdir()
        fence = ExecutionLease(ledger / '.t1-reservation.lock', 'fence', 'remote').acquire()
        original = AttemptBudget(ledger, self.approval)
        request = self.request('local-old-runner')
        with self.assertRaises(BudgetBlocked):
            original.reserve(request, purpose='old runner must be blocked')
        fence.assert_owned()
        self.assertEqual(list(ledger.glob('*.json')), [])

    def test_history_seal_requires_fence_and_rejects_pending(self):
        from attempt_budget import AttemptBudget
        local_approval = self.root / 'small_approval.json'
        create_json(local_approval, {'approved_cumulative_cap': 12000,
                                    'historical_attempts_at_approval': 0})
        ledger = self.root / 'original_ledger'
        ledger.mkdir()
        original = AttemptBudget(ledger, local_approval)
        request = self.request('historical')
        self.assertEqual(original.reserve(request, purpose='history fixture'), 1)
        fence = ExecutionLease(ledger / '.t1-reservation.lock', 'fence', 'offline').acquire()
        with self.assertRaises(LeaseBlocked):
            history_seal(original, self.root / 'pending_seal.json', fence=fence)
        self.terminal(request)
        destination = self.root / 'sealed_real_fixture.json'
        pinned = history_seal(original, destination, fence=fence)
        self.assertEqual(pinned, sha(destination))
        self.assertEqual(json.loads(destination.read_text())['reserved'], 1)
        with self.assertRaises(LeaseBlocked):
            history_seal(original, self.root / 'bad_fence.json', fence=self.lease)

    def test_frozen_transport_three_failures_counted_and_stopped_without_network(self):
        from model_transport import ModelTransport, BatchStopped
        sends, waits = [], []
        def no_network_send(config, wire):
            sends.append(wire)
            raise TimeoutError('synthetic fixture; no socket opened')
        config = {'endpoint': 'http://unused.invalid/chat', 'api_key': 'synthetic-not-a-key',
                  'model': 'synthetic', 'temperature': 0.2, 'top_p': 0.95,
                  'reasoning_effort': 'high', 'max_tokens': 8192,
                  'read_timeout_seconds': 240}
        transport = ModelTransport(config, self.budget, self.calls / 'transport',
                                   send=no_network_send, sleep=waits.append)
        with self.assertRaises(BatchStopped):
            transport.complete([{'role': 'user', 'content': 'offline fixture'}], purpose='test')
        self.assertEqual(len(sends), 3)
        self.assertEqual(len(set(sends)), 1)
        self.assertEqual(waits, [10, 10])  # Synthetic sleep, not real formal timing.
        self.assertEqual(self.budget.inspect()['reserved'], 3269)
        self.assertEqual(self.budget.inspect()['pending'], [])
        with self.assertRaises(BatchStopped):
            transport.complete([{'role': 'user', 'content': 'do not send'}], purpose='test')
        self.assertEqual(len(sends), 3)

    def test_pending_budget_stops_frozen_transport_before_any_send(self):
        from model_transport import ModelTransport
        self.budget.reserve(self.request('unknown-result'), purpose='test')
        sends = []
        config = {'endpoint': 'http://unused.invalid/chat', 'api_key': 'synthetic-not-a-key',
                  'model': 'synthetic', 'temperature': 0.2, 'top_p': 0.95,
                  'reasoning_effort': 'high'}
        transport = ModelTransport(config, self.budget, self.calls / 'transport',
                                   send=lambda *args: sends.append(args))
        with self.assertRaises(LeaseBlocked):
            transport.complete([{'role': 'user', 'content': 'offline fixture'}], purpose='test')
        self.assertEqual(sends, [])
        self.assertEqual(self.budget.inspect()['reserved'], 3267)

    def test_incomplete_release_refused_and_completed_release_explicit(self):
        proof = self.root / 'release.json'
        value = {'lease_id': 'test-lease', 'remote_execution_terminal': False,
                 'pending': [], 'all_attempts_mirrored': True}
        create_json(proof, value)
        with self.assertRaises(LeaseBlocked):
            self.lease.release(evidence_path=proof, expected_evidence_sha=sha(proof))
        self.assertTrue(self.lease_path.exists())
        value['remote_execution_terminal'] = True
        proof.write_text(json.dumps(value))
        self.lease.release(evidence_path=proof, expected_evidence_sha=sha(proof))
        self.assertFalse(self.lease_path.exists())
        with self.assertRaises(LeaseBlocked):
            self.budget.inspect()


if __name__ == '__main__':
    unittest.main(verbosity=2)
