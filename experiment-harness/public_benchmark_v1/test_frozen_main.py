import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from public_read_guard import read_query, ReadOnlyViolation
from run_frozen_main import check_admission


class PublicGuardChecks(unittest.TestCase):
    def test_ordinary_scoped_subqueries_are_allowed(self):
        for scope in ['', '*', 'n', 'n, m', '`variable with spaces`']:
            query = 'CALL (' + scope + ') { RETURN 1 AS value } RETURN value'
            self.assertEqual(read_query(query), query)
        with self.assertRaises(ReadOnlyViolation):
            read_query('CALL (n) { DELETE n RETURN 1 AS value } RETURN value')
        with self.assertRaises(ReadOnlyViolation):
            read_query('CALL db.labels() YIELD label RETURN label')


class AdmissionChecks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.code, self.data = self.root/'code', self.root/'data'
        self.code.mkdir(); self.data.mkdir()
        (self.code/'worker.py').write_text('pinned source')
        (self.data/'runtime').mkdir()
        rows = [{'qid': str(i), 'graph': 'example', 'nl_question': 'question'} for i in range(2348)]
        (self.data/'runtime/test.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
        self.freeze = {'graphs': {'example': 2348},
            'code_files': {'worker.py': hashlib.sha256((self.code/'worker.py').read_bytes()).hexdigest()},
            'data_files': {'runtime/test.jsonl': hashlib.sha256((self.data/'runtime/test.jsonl').read_bytes()).hexdigest()}}
        self.fpath, self.apath = self.root/'freeze.json', self.root/'admission.json'
        self.fpath.write_text(json.dumps(self.freeze))
        self.admission = {'formal_run_ready': True,
            'freeze_sha256': hashlib.sha256(self.fpath.read_bytes()).hexdigest(),
            'admitted_graphs': {'example': {}}}
        self.apath.write_text(json.dumps(self.admission))

    def call(self):
        return check_admission(self.fpath, self.apath, self.data, self.code, 'example')

    def test_correct_freeze_passes_and_source_mutation_blocks(self):
        self.assertEqual(len(self.call()[2]), 2348)
        (self.code/'worker.py').write_text('changed')
        with self.assertRaises(ValueError):
            self.call()

    def test_missing_admission_or_wrong_freeze_blocks_before_network(self):
        for change in [{'formal_run_ready': False}, {'freeze_sha256': 'wrong'}, {'admitted_graphs': {}}]:
            self.apath.write_text(json.dumps({**self.admission, **change}))
            with self.assertRaises(ValueError):
                self.call()

    def test_task_tampering_blocks_even_if_questions_look_similar(self):
        with (self.data/'runtime/test.jsonl').open('a') as stream:
            stream.write(json.dumps({'qid': 'gold', 'gold_cypher': 'RETURN 1'})+'\n')
        with self.assertRaises(ValueError):
            self.call()


if __name__ == '__main__':
    unittest.main()
