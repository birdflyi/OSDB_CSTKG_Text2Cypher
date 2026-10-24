import hashlib
from pathlib import Path
import tempfile
import unittest

from prepare_cypherbench import separate_records, validate_splits, verify_asset, save_once


def sample(qid='q1', graph='dev'):
    return {'qid': qid, 'graph': graph, 'nl_question': 'How many records?',
            'gold_cypher': 'MATCH (n) RETURN count(n)', 'answer_json': '[3]',
            'from_template': {'gold_only': 'hidden'}, 'future_gold_field': 'hidden'}


class PreparationChecks(unittest.TestCase):
    def test_runtime_allowlist_excludes_gold_and_future_fields(self):
        runtime, references = separate_records([sample()])
        self.assertEqual(set(runtime[0]), {'qid', 'graph', 'nl_question'})
        self.assertIn('gold_cypher', references[0])
        self.assertNotIn('nl_question', references[0])

    def test_reject_duplicate_or_invalid_tasks(self):
        with self.assertRaises(ValueError):
            separate_records([sample(), sample()])
        row = sample()
        row['nl_question'] = ' '
        with self.assertRaises(ValueError):
            separate_records([row])

    def test_reject_split_leakage(self):
        for other in [sample(graph='test'), sample(qid='q2')]:
            with self.assertRaises(ValueError):
                validate_splits([sample()], [other])
        validate_splits([sample()], [sample(qid='q2', graph='test')])

    def test_reject_tampered_lfs_or_git_blob(self):
        data = b'original'
        for entry in [
            {'path': 'x', 'size': 8, 'lfs': {'oid': hashlib.sha256(data).hexdigest()}},
            {'path': 'x', 'size': 8, 'oid': hashlib.sha1(b'blob 8\0' + data).hexdigest()},
        ]:
            verify_asset(data, entry)
            with self.assertRaises(ValueError):
                verify_asset(b'tampered', entry)

    def test_no_silent_artifact_replacement(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'a.json'
            save_once(path, b'first')
            save_once(path, b'first')
            with self.assertRaises(ValueError):
                save_once(path, b'changed')


if __name__ == '__main__':
    unittest.main()
