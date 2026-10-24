import json
from pathlib import Path
import tempfile
import unittest

from import_recovery import recovery_evidence


class ImportRecoveryChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name) / 'owned_graph'
        self.prior = self.folder / 'reference_preflight_v1'
        self.prior.mkdir(parents=True)
        self.graph = {'entities': [{}], 'relations': [{}, {}]}
        self.write(self.folder / 'import_started.json', {'nodes': 1, 'edges': 2})
        self.write(self.prior / 'failure.json', {'error': 'Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration'})
        self.write(self.prior / 'process_stopped.json', {'owned_process_stopped': True})

    @staticmethod
    def write(path, data):
        path.write_text(json.dumps(data), encoding='utf-8')

    def admit(self):
        return recovery_evidence(self.folder, self.prior, self.graph, 'Exhaustive scan exceeded offline timeout')

    def test_admits_only_documented_read_only_reverification(self):
        result = self.admit()
        self.assertTrue(result['no_import_or_repair_writes'])
        self.assertEqual(result['formal_query_timeout_seconds'], 60)
        self.assertEqual(len(result['prior_failure_sha256']), 64)

    def test_wrong_graph_or_uncertified_shutdown_blocks(self):
        self.write(self.folder / 'import_started.json', {'nodes': 1, 'edges': 1})
        with self.assertRaises(ValueError):
            self.admit()
        self.write(self.folder / 'import_started.json', {'nodes': 1, 'edges': 2})
        self.write(self.prior / 'process_stopped.json', {'owned_process_stopped': False})
        with self.assertRaises(ValueError):
            self.admit()

    def test_outside_failure_or_other_error_cannot_resume_import(self):
        with self.assertRaises(ValueError):
            recovery_evidence(self.folder, self.folder.parent, self.graph, 'reason')
        with self.assertRaises(ValueError):
            recovery_evidence(self.folder, self.prior, self.graph, '')
        self.write(self.prior / 'failure.json', {'error': 'Node import mismatch'})
        with self.assertRaises(ValueError):
            self.admit()
