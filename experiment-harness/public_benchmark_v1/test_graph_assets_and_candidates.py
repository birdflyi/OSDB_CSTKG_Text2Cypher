from io import BytesIO
import hashlib
from pathlib import Path
import tempfile
import unittest

from fetch_graph_assets import fetch_one, verify_file
from entity_candidates import EntityCandidates


class GraphAssetChecks(unittest.TestCase):
    def test_lfs_stream_verification_and_cache_reuse(self):
        content = b'{"entities":[],"relations":[]}'
        entry = {'path': 'graphs/simplekg/example.json', 'size': len(content),
                 'lfs': {'oid': hashlib.sha256(content).hexdigest()}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'graph.json'
            first = fetch_one(entry, path, opener=lambda *a, **k: BytesIO(content))
            self.assertTrue(first['downloaded'])
            self.assertFalse(fetch_one(entry, path, opener=lambda *a, **k: self.fail('redownload'))['downloaded'])
            path.write_bytes(b'x' * len(content))
            with self.assertRaises(ValueError):
                verify_file(path, entry)

    def test_failed_hash_leaves_partial_never_promoted(self):
        entry = {'path': 'graph.json', 'size': 3, 'lfs': {'oid': '0' * 64}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'graph.json'
            with self.assertRaises(ValueError):
                fetch_one(entry, path, opener=lambda *a, **k: BytesIO(b'bad'))
            self.assertFalse(path.exists())
            self.assertTrue(path.with_name('graph.json.partial').exists())
            with self.assertRaises(RuntimeError):
                fetch_one(entry, path)


class CandidateChecks(unittest.TestCase):
    def test_plural_ranking_returns_canonical_name_without_rewriting_question(self):
        resolver = EntityCandidates([{'eid': 'w1', 'label': 'Weapon', 'name': 'explosive belt'},
                                     {'eid': 'w2', 'label': 'Weapon', 'name': 'rifle'}])
        self.assertEqual(resolver.find('attacks using explosive belts')[0]['name'], 'explosive belt')
        self.assertEqual(resolver.find('nothing matches'), [])

    def test_aliases_ambiguity_and_stable_limit(self):
        rows = [{'eid': 'b', 'label': 'Place', 'name': 'Second place', 'aliases': ['shared']},
                {'eid': 'a', 'label': 'Place', 'name': 'First place', 'aliases': ['shared']}]
        resolver = EntityCandidates(rows)
        self.assertEqual([r['eid'] for r in resolver.find('shared')], ['a', 'b'])
        self.assertEqual(len(resolver.find('shared', limit=1)), 1)


if __name__ == '__main__':
    unittest.main()
