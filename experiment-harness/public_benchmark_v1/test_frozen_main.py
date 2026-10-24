import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from public_read_guard import read_query, ReadOnlyViolation
from run_frozen_main import (check_admission, GRAPHS, REQUIRED_CODE, REQUIRED_DATA,
                             SOURCE_FOLDER, DIRECT_INSTRUCTIONS, PLAN_INSTRUCTIONS, TracedExecutor)


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
        for relative in REQUIRED_CODE:
            path = self.code / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('pinned source')
        for relative in REQUIRED_DATA:
            path = self.data / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{}')
        rows = [{'qid': graph + str(i), 'graph': graph, 'nl_question': 'question'}
                for graph, count in GRAPHS.items() for i in range(count)]
        (self.data/'runtime/test.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
        self.freeze = {'graphs': GRAPHS,
            'code_files': {r: hashlib.sha256((self.code/r).read_bytes()).hexdigest() for r in REQUIRED_CODE},
            'data_files': {r: hashlib.sha256((self.data/r).read_bytes()).hexdigest() for r in REQUIRED_DATA},
            'prompt_sha256': {'direct': hashlib.sha256(DIRECT_INSTRUCTIONS.encode()).hexdigest(),
                              'ours': hashlib.sha256(PLAN_INSTRUCTIONS.encode()).hexdigest()},
            'remote_worker': {'code_folder': 'public_query_test',
                              'files': {r[len(SOURCE_FOLDER):]: hashlib.sha256((self.code/r).read_bytes()).hexdigest()
                                        for r in REQUIRED_CODE}}}
        self.fpath, self.apath = self.root/'freeze.json', self.root/'admission.json'
        self.fpath.write_text(json.dumps(self.freeze))
        self.admission = {'formal_run_ready': True,
            'freeze_sha256': hashlib.sha256(self.fpath.read_bytes()).hexdigest(),
            'admitted_graphs': {'flight_accident': {}}}
        self.apath.write_text(json.dumps(self.admission))

    def call(self):
        return check_admission(self.fpath, self.apath, self.data, self.code, 'flight_accident')

    def test_correct_freeze_passes_and_source_mutation_blocks(self):
        self.assertEqual(len(self.call()[2]), 189)
        (self.code / SOURCE_FOLDER / 'schema_services.py').write_text('changed')
        with self.assertRaises(ValueError):
            self.call()

    def test_omitted_dependency_or_unpinned_worker_blocks(self):
        for change in ['dependency', 'remote', 'prompt']:
            freeze = json.loads(json.dumps(self.freeze))
            if change == 'dependency':
                del freeze['code_files'][SOURCE_FOLDER + 'schema_services.py']
            elif change == 'remote':
                freeze['remote_worker']['files']['public_query_worker.py'] = 'wrong'
            else:
                freeze['prompt_sha256']['ours'] = 'wrong'
            self.fpath.write_text(json.dumps(freeze))
            self.apath.write_text(json.dumps({**self.admission,
                'freeze_sha256': hashlib.sha256(self.fpath.read_bytes()).hexdigest()}))
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


class QueryTraceChecks(unittest.TestCase):
    def test_prior_cost_survives_later_query_failure(self):
        class Worker:
            count = 0
            def execute(self, query, params):
                self.count += 1
                if self.count == 2:
                    raise RuntimeError('connection interrupted')
                return {'status': 'EXECUTED', 'rows': [[1]], 'result_bytes': 5}
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            traced = TracedExecutor(Worker(), folder)
            traced.execute('RETURN 1', {})
            with self.assertRaises(RuntimeError):
                traced.execute('RETURN 2', {})
            self.assertEqual(json.loads((folder/'000/result.json').read_text())['result']['result_bytes'], 5)
            self.assertTrue(json.loads((folder/'001/transport_error.json').read_text())['completion_unknown'])
            self.assertTrue((folder/'001/intent.json').is_file())



if __name__ == '__main__':
    unittest.main()
