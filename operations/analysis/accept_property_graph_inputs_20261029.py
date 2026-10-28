"""Synthetic input/variant/population checks. No model, graph or benchmark run."""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

from property_graph_input_contract_dev_20261029 import (
    digest, encoded, iter_public_tasks, prepare_package, public_input,
)


class InputChecks(unittest.TestCase):
    output = None

    def setUp(self):
        self.root = self.output / self._testMethodName
        self.root.mkdir()
        self.source = self.root / 'source'
        self.source.mkdir()
        self.schema = {'node_props': {'Person': [{'property': 'name', 'datatype': 'STRING'}]}}
        self.schema_path = self.root / 'author_schema.json'
        self.schema_path.write_bytes(encoded(self.schema))
        self.rows = [
            {'task_id': 'synthetic_a', 'graph': 'alias', 'question': '  Original question A?  ',
             'gold_linked_question': 'Linked question A?', 'split': 'iid', 'track': 'Manual',
             'category': 'Complex_Retrieval_test', 'source_file': 'file.csv', 'source_position': 0,
             'reference_query': 'FORBIDDEN_REFERENCE_IN_PUBLIC_RECORD',
             'return_count': 99, 'validation_label': 'FORBIDDEN_LABEL'},
            {'task_id': 'synthetic_b', 'graph': 'alias', 'question': 'Original question B?',
             'gold_linked_question': 'Linked question B?', 'split': 'length', 'track': 'Automated',
             'category': 'Simple_Aggregation_test', 'source_file': 'file.csv', 'source_position': 1},
        ]
        self.refs = [{'task_id': r['task_id'], 'reference_query': 'FORBIDDEN_REFERENCE_QUERY',
                      'query_type': 'count', 'template_id': 'FORBIDDEN_TEMPLATE', 'return_count': 123,
                      'validation_labels': {'logical': 0}} for r in self.rows]
        self.binding = {'benchmark': 'ZOGRASCOPE', 'task_graph': 'canonical',
                         'task_aliases': ['alias', 'canonical'],
                         'schema_path': str(self.schema_path),
                         'schema_sha256': digest(self.schema_path.read_bytes()),
                         'offline_store': '/owned/prepared/store'}
        self.write_source()

    def write_source(self):
        manifest = {'N': len(self.rows)}
        for name, values in [('public_inputs.jsonl', self.rows), ('evaluator_references.jsonl', self.refs)]:
            path = self.source / name
            path.write_bytes(b''.join(encoded(row) + b'\n' for row in values))
            manifest[name] = {'sha256': digest(path.read_bytes())}
        (self.source / 'manifest.json').write_bytes(encoded(manifest))

    def prepare(self, benchmark='ZOGRASCOPE'):
        binding = dict(self.binding, benchmark=benchmark)
        self.package = self.root / 'package'
        result = prepare_package(benchmark, self.source, [binding], self.package)
        self.manifest_sha = digest((self.package / 'manifest.json').read_bytes())
        return result

    def load(self, variant='plain'):
        return list(iter_public_tasks(self.package, variant, expected_manifest_sha256=self.manifest_sha))

    def test_model_input_is_an_explicit_projection(self):
        self.prepare()
        tasks = self.load()
        for identity, common in tasks:
            self.assertEqual(set(common), {'graph', 'question', 'schema'})
            self.assertNotIn('FORBIDDEN', json.dumps(common))
        self.assertEqual(tasks[0][1]['question'], self.rows[0]['question'])
        self.assertEqual(tasks[0][1]['graph'], 'canonical')

    def test_plain_and_gold_linked_are_distinct_explicit_variants(self):
        manifest = self.prepare()
        plain, linked = self.load('plain'), self.load('gold_linked')
        self.assertEqual([p[0] for p in plain], [p[0] for p in linked])
        self.assertNotEqual(plain[0][1]['question'], linked[0][1]['question'])
        self.assertEqual(len(plain), manifest['positions_per_variant'])
        self.assertEqual(len(linked), manifest['positions_per_variant'])
        with self.assertRaises(ValueError):
            self.load('automatic_fallback')

    def test_missing_gold_linked_text_never_falls_back_to_plain(self):
        self.rows[1]['gold_linked_question'] = ''
        self.write_source()
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertFalse(self.package.exists())

    def test_duplicate_task_or_missing_reference_rejected_before_write(self):
        self.rows.append(deepcopy(self.rows[0]))
        self.write_source()
        with self.assertRaises(ValueError):
            self.prepare()
        self.rows.pop()
        self.refs.pop()
        self.write_source()
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertFalse(self.package.exists())

    def test_source_validation_labels_only_in_evaluation(self):
        self.prepare()
        cohorts = [json.loads(line) for line in (self.package / 'evaluation/cohorts.jsonl').read_text().splitlines()]
        self.assertEqual(cohorts[0]['source_validation_labels'], {'logical': 0})
        self.assertEqual(cohorts[0]['query_type'], 'count')
        for path in (self.package / 'public').glob('*.jsonl'):
            self.assertNotIn(b'FORBIDDEN', path.read_bytes())
        self.assertNotIn('source_validation_labels', self.load()[0][1])

    def test_public_loader_never_opens_evaluation_files(self):
        self.prepare()
        original_open = Path.open
        def guarded(path, *args, **kwargs):
            if 'evaluation' in path.parts:
                raise AssertionError('Public loader tried to read evaluator material')
            return original_open(path, *args, **kwargs)
        with patch.object(Path, 'open', guarded):
            self.assertEqual(len(self.load()), 2)

    def test_changed_public_bytes_or_schema_blocks_all_tasks(self):
        self.prepare()
        path = self.package / 'public/plain.jsonl'
        raw = path.read_bytes()
        path.write_bytes(raw + b' ')
        with self.assertRaises(ValueError):
            self.load()
        path.write_bytes(raw)
        schema = next((self.package / 'schemas').glob('*.json'))
        schema.write_bytes(schema.read_bytes() + b' ')
        with self.assertRaises(ValueError):
            next(iter_public_tasks(self.package, 'plain', expected_manifest_sha256=self.manifest_sha))

    def test_changed_source_bytes_rejected(self):
        path = self.source / 'public_inputs.jsonl'
        path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertFalse(self.package.exists())

    def test_manifest_digest_is_supplied_externally(self):
        self.prepare()
        path = self.package / 'manifest.json'
        path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaises(ValueError):
            self.load()

    def test_schema_cannot_point_into_evaluation_even_in_a_new_manifest(self):
        self.prepare()
        manifest_path = self.package / 'manifest.json'
        manifest = json.loads(manifest_path.read_bytes())
        public_path = self.package / 'public/plain.jsonl'
        rows = [json.loads(line) for line in public_path.read_text().splitlines()]
        forbidden = 'evaluation/cohorts.jsonl'
        sha = digest((self.package / forbidden).read_bytes())
        for row in rows:
            row.update(schema_file=forbidden, schema_sha256=sha)
        manifest['graphs']['canonical'].update(schema_file=forbidden, schema_sha256=sha)
        public_path.write_bytes(b''.join(encoded(row) + b'\n' for row in rows))
        manifest['files']['public/plain.jsonl'] = digest(public_path.read_bytes())
        manifest_path.write_bytes(encoded(manifest))
        self.manifest_sha = digest(manifest_path.read_bytes())
        original_open = Path.open
        def guarded(path, *args, **kwargs):
            if 'evaluation' in path.parts:
                raise AssertionError('Public loader followed an evaluator pointer')
            return original_open(path, *args, **kwargs)
        with patch.object(Path, 'open', guarded), self.assertRaises(ValueError):
            self.load()

    def test_graph_aliases_do_not_invent_properties(self):
        self.prepare()
        common = self.load()[0][1]
        self.assertEqual(common['schema'], self.schema)
        self.assertNotIn('eid', json.dumps(common['schema']))
        common['schema']['node_props']['Person'].append({'property': 'changed'})
        self.assertEqual(self.load()[0][1]['schema'], self.schema)

    def test_MTQ_preserves_both_tracks_and_all_categories(self):
        manifest = self.prepare('MindTheQuery')
        self.assertEqual(manifest['variants'], ['plain'])
        self.assertEqual(manifest['splits'], {'Manual': 1, 'Automated': 1})
        self.assertEqual(len(self.load()), 2)
        self.assertFalse(manifest['experiment_enabled'])
        self.assertTrue(manifest['common_input_hash_is_not_a_prediction_cache_key'])

    def test_common_object_cannot_mutate_source_schema(self):
        value = public_input(self.rows[0], self.schema, 'canonical', 'question')
        value['schema']['node_props'].clear()
        self.assertIn('Person', self.schema['node_props'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    InputChecks.output = args.out
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(InputChecks)
    planned = suite.countTestCases()
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('Network prohibited')):
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    summary = {'kind': 'SYNTHETIC_PUBLIC_INPUT_BOUNDARY_CHECKS_NOT_EXPERIMENT',
               'planned': planned, 'completed': result.testsRun,
               'failed': len(result.failures) + len(result.errors),
               'checks_passed': result.testsRun - len(result.failures) - len(result.errors),
               'model_calls': 0, 'live_database_requests': 0, 'benchmark_queries': 0,
               'experiment_enabled': False}
    (args.out / 'summary.json').write_bytes(encoded(summary) + b'\n')
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == '__main__':
    main()
