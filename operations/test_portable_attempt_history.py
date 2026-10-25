"""Real file transfers and original counter; isolated synthetic attempts only."""
from pathlib import Path
import json
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from portable_attempt_history import (HistoryBlocked, clone_history, open_clone,
                                      prepare_delta, apply_delta, create_json, sha, native_path)
sys.path.insert(0, 'D:/ch7/code/OSDB_CSTKG_Text2Cypher_publication_t1/experiment-harness/public_benchmark_v1')
from attempt_budget import AttemptBudget, BudgetBlocked


class PortableHistoryChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='t1-history-mirror-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ledger = self.root / 'original/ledger'
        self.ledger.mkdir(parents=True)
        self.approval = self.root / 'original/approval.json'
        create_json(self.approval, {'approved_cumulative_cap': 3,
                                   'historical_attempts_at_approval': 0})
        self.original = AttemptBudget(self.ledger, self.approval)
        self.old = self.root / 'original/calls/old/request.json'
        self.old.parent.mkdir(parents=True)
        create_json(self.old, {'messages': ['synthetic history']})
        self.original.reserve(self.old, purpose='synthetic history')
        create_json(self.old.with_name('terminal.json'), {'http_status': 200})
        self.source = self.root / 'remote_simulation'
        self.mirror = self.root / 'local_mirror'
        clone_history(self.original, self.source)
        clone_history(self.original, self.mirror)
        self.source_sha = sha(self.source / 'manifest.json')
        self.mirror_sha = sha(self.mirror / 'manifest.json')
        self.calls = self.source / 'new_calls'
        self.calls.mkdir()
        # New native Windows paths must take precedence over historical D: mapping.
        # Linux-native requests do not share a drive prefix, but test both safely.
        self.source_budget = open_clone(AttemptBudget, self.source, self.source_sha,
                                       new_rewrites=[(str(self.calls), str(self.calls))])

    def new_attempt(self, number, terminal=True):
        request = self.calls / ('physical_' + str(number)) / 'request.json'
        request.parent.mkdir()
        create_json(request, {'messages': ['same request may be a physical retry']})
        slot = self.source_budget.reserve(request, purpose='synthetic physical attempt')
        if terminal:
            create_json(request.with_name('terminal.json'), {'transport_error': 'synthetic timeout',
                                                           'budget_slot': slot})
        return request

    def delta(self):
        target = self.root / 'delta'
        prepare_delta(self.source_budget, 1, self.calls, target)
        return target

    def apply(self, delta):
        return apply_delta(AttemptBudget, self.mirror, self.mirror_sha,
                           delta, sha(delta / 'manifest.json'))

    def test_raw_historical_slots_and_counter_are_preserved(self):
        self.assertEqual(sha(self.ledger / '0001.json'), sha(self.source / 'ledger/0001.json'))
        self.assertEqual(self.source_budget.inspect()['reserved'], 1)
        self.assertEqual(self.source_budget.inspect()['cap'], 3)
        self.assertEqual(self.new_attempt(2).name, 'request.json')
        self.assertEqual(self.source_budget.inspect()['reserved'], 2)
        self.assertEqual(self.original.inspect()['reserved'], 1)

    def test_complete_mirror_idempotent_and_cap_still_cumulative(self):
        self.new_attempt(2)
        self.new_attempt(3)
        delta = self.delta()
        result = self.apply(delta)
        self.assertEqual(result.inspect()['reserved'], 3)
        self.assertEqual(result.inspect()['remaining'], 0)
        self.assertEqual(result.inspect()['pending'], [])
        self.assertEqual(self.apply(delta).inspect()['reserved'], 3)
        self.assertEqual(sha(self.source / 'ledger/0002.json'), sha(self.mirror / 'ledger/0002.json'))
        request = self.mirror / 'extra/request.json'
        request.parent.mkdir()
        create_json(request, {})
        with self.assertRaises(BudgetBlocked):
            result.reserve(request, purpose='over cumulative cap')
        self.assertEqual(self.original.inspect()['reserved'], 1)

    def test_pending_source_is_not_mirrored_or_resent(self):
        self.new_attempt(2, terminal=False)
        with self.assertRaises(HistoryBlocked):
            self.delta()
        self.assertFalse((self.mirror / 'ledger/0002.json').exists())
        self.assertEqual(self.source_budget.inspect()['pending'], [2])

    def test_missing_transferred_terminal_blocks_before_slot_copy(self):
        self.new_attempt(2)
        delta = self.delta()
        terminal = next((delta / 'requests').rglob('terminal.json'))
        terminal.unlink()
        with self.assertRaises((HistoryBlocked, FileNotFoundError)):
            self.apply(delta)
        self.assertFalse((self.mirror / 'ledger/0002.json').exists())

    def test_mirror_conflict_preserves_existing_bytes(self):
        self.new_attempt(2)
        delta = self.delta()
        conflict = self.mirror / 'incoming_attempts/physical_2/request.json'
        conflict.parent.mkdir(parents=True)
        create_json(conflict, {'wrong': True})
        expected = sha(conflict)
        with self.assertRaises(HistoryBlocked):
            self.apply(delta)
        self.assertEqual(sha(conflict), expected)
        self.assertFalse((self.mirror / 'ledger/0002.json').exists())

    def test_altered_delta_slot_rejected_before_append(self):
        self.new_attempt(2)
        delta = self.delta()
        (delta / 'ledger/0002.json').write_text('{}')
        with self.assertRaises(HistoryBlocked):
            self.apply(delta)
        self.assertFalse((self.mirror / 'ledger/0002.json').exists())

    def test_historical_missing_evidence_is_not_an_empty_new_budget(self):
        request = next((self.source / 'history').rglob('request.json'))
        request.unlink()
        with self.assertRaises((HistoryBlocked, FileNotFoundError)):
            open_clone(AttemptBudget, self.source, self.source_sha)

    def test_legacy_c_drive_paths_translate_without_modifying_slots(self):
        slot_path = self.ledger / '0001.json'
        record = json.loads(slot_path.read_text())
        record['request_path'] = r'C:\legacy\old\request.json'
        slot_path.write_text(json.dumps(record))
        original = AttemptBudget(self.ledger, self.approval,
                                 path_rewrites=[(r'C:\legacy\old', str(self.old.parent))])
        target = self.root / 'c_drive_clone'
        clone_history(original, target)
        cloned = open_clone(AttemptBudget, target, sha(target / 'manifest.json'))
        self.assertEqual(cloned.inspect()['reserved'], 1)
        self.assertEqual(sha(slot_path), sha(target / 'ledger/0001.json'))

    def test_nested_historical_directories_use_longest_prefix_first(self):
        request = self.old.parent / 'nested/request.json'
        request.parent.mkdir()
        create_json(request, {'messages': ['nested history fixture']})
        self.original.reserve(request, purpose='nested synthetic history')
        create_json(request.with_name('terminal.json'), {'http_status': 200})
        target = self.root / 'nested_clone'
        clone_history(self.original, target)
        cloned = open_clone(AttemptBudget, target, sha(target / 'manifest.json'))
        self.assertEqual(cloned.inspect()['reserved'], 2)
        self.assertEqual(cloned.inspect()['pending'], [])

    def test_long_windows_destination_rejected_before_copy(self):
        target = self.root / ('long_component_' * 7) / ('long_component_' * 7) / 'clone'
        self.addCleanup(lambda: shutil.rmtree(native_path(self.root / ('long_component_' * 7)), ignore_errors=True))
        if sys.platform == 'win32':
            with self.assertRaises(HistoryBlocked):
                clone_history(self.original, target)
            self.assertFalse(native_path(target).exists())
        else:
            self.skipTest('Windows-specific path limit')


if __name__ == '__main__':
    unittest.main(verbosity=2)
