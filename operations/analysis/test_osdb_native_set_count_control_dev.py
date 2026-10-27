"""Offline compiler checks; SQLite algebra check is NOT Neo4j execution."""
from copy import deepcopy
from itertools import product
from unittest.mock import patch
import sqlite3
import unittest
from osdb_native_set_count_control_dev import compile_native_set_count, namespace_parameters
from observed_neo4j_services_dev_v2 import UnsupportedPlan


SCHEMA = {'entities': [{'label': 'Person', 'identity_property': 'key',
                       'properties': {'key': 'str', 'display': 'str', 'clock': 'str'},
                       'temporal_properties': ['clock']}], 'relations': []}


def selection():
    return {'nodes': [{'id': 'n', 'label': 'Person'}], 'edges': [], 'where': [],
            'returns': [{'field': 'n.key', 'alias': 'id'}]}


def plan(op='union'):
    return {'status': 'ANSWER', 'calls': [selection(), selection()],
            'combine': {'op': op, 'key': 'id', 'output': 'count', 'alias': 'answer'}}


class NativeControlTests(unittest.TestCase):
    def setUp(self):
        self.guard = patch('socket.socket', side_effect=AssertionError('Offline compilation only'))
        self.socket = self.guard.start()

    def tearDown(self):
        self.assertEqual(self.socket.call_count, 0)
        self.guard.stop()

    def test_branches_keep_conflicting_parameter_values_separate(self):
        p = plan()
        for call, value in zip(p['calls'], ['left', 'right']):
            call['where'] = [{'field': 'n.display', 'op': 'eq', 'value': value}]
        before = deepcopy(p)
        out = compile_native_set_count(p, SCHEMA)
        self.assertEqual(out['parameters'], {'t1Branch0_p0': 'left', 't1Branch1_p0': 'right'})
        self.assertIn('$t1Branch0_p0', out['query'])
        self.assertIn('$t1Branch1_p0', out['query'])
        self.assertEqual(p, before)
        self.assertFalse(out['engine_acceptance'])
        self.assertEqual(out['required_physical_queries'], 1)

    def test_timestamp_expression_and_original_literal_bytes_preserved(self):
        p = plan()
        stamp = '2023-07-01T01:00:00.000000001+01:00'
        p['calls'][0]['where'] = [{'field': 'n.clock', 'op': 'ge', 'value': stamp}]
        out = compile_native_set_count(p, SCHEMA)
        self.assertEqual(out['parameters']['t1Branch0_p0'], stamp)
        self.assertIn('datetime($t1Branch0_p0).nanosecond', out['query'])

    def test_parameter_names_inside_quoted_content_are_not_rewritten(self):
        query = "RETURN `$p0``x`, '$p0', \"$p0\", $p0, $p01"
        got, params = namespace_parameters(query, {'p0': 'a', 'p01': 'b'}, 'branch_')
        self.assertEqual(got, "RETURN `$p0``x`, '$p0', \"$p0\", $branch_p0, $branch_p01")
        self.assertEqual(params, {'branch_p0': 'a', 'branch_p01': 'b'})

    def test_bad_parameter_or_quotation_rejected(self):
        for query, params in [('RETURN $absent', {}), ('RETURN 1', {'p0': 1}),
                              ("RETURN 'open", {}), ('RETURN $p0 //x', {'p0': 1})]:
            with self.subTest(query=query), self.assertRaises(UnsupportedPlan):
                namespace_parameters(query, params, 'branch_')

    def test_set_count_shapes_and_duplicate_safe_source_tags(self):
        for op in ('union', 'intersection', 'difference'):
            out = compile_native_set_count(plan(op), SCHEMA)
            self.assertEqual(out['query'].count('RETURN DISTINCT n.`key` AS `id`'), 2)
            self.assertIn('count(DISTINCT t1ControlSource)', out['query'])
            self.assertTrue(out['query'].endswith('RETURN count(*) AS `answer`'))
            self.assertEqual(out['columns'], ['answer'])
            self.assertEqual(out['extra_model_calls'], 0)
        self.assertIn('t1FirstSource = 1', compile_native_set_count(plan('difference'), SCHEMA)['query'])

    def test_non_identity_payload_aggregation_limit_order_and_wrong_output_rejected(self):
        variants = []
        p = plan(); p['calls'][0]['returns'][0]['field'] = 'n.display'; variants.append(p)
        p = plan(); p['calls'][0]['returns'].append({'field': 'n.display', 'alias': 'text'}); variants.append(p)
        p = plan(); p['calls'][0]['returns'][0]['agg'] = 'count'; variants.append(p)
        p = plan(); p['calls'][0]['limit'] = 5; variants.append(p)
        p = plan(); p['calls'][0]['order_by'] = [{'column': 'id', 'direction': 'ASC'}]; variants.append(p)
        p = plan(); p['combine']['output'] = 'rows'; variants.append(p)
        p = plan(); p['status'] = 'UNSUPPORTED'; variants.append(p)
        for p in variants:
            with self.subTest(plan=p), self.assertRaises(UnsupportedPlan):
                compile_native_set_count(p, SCHEMA)

    def test_integer_identity_supported_without_string_conversion(self):
        schema = deepcopy(SCHEMA); schema['entities'][0]['properties']['key'] = 'int'
        out = compile_native_set_count(plan(), schema)
        self.assertEqual(out['identity_contract']['kind'], 'int')
        self.assertNotIn('toString', out['query'])

    def test_tag_grouping_algebra_against_independent_python_sets(self):
        # Exhaust all small bags, including duplicates, empty sides and overlap.
        # Uses independent SQL, not execution/translation of emitted Cypher.
        bags = [list(x) for n in range(4) for x in product(('a', 'b'), repeat=n)]
        connection = sqlite3.connect(':memory:')
        try:
            connection.execute('CREATE TABLE evidence(identity TEXT, source INTEGER)')
            for a, b in product(bags, repeat=2):
                connection.execute('DELETE FROM evidence')
                connection.executemany('INSERT INTO evidence VALUES (?,?)',
                                       [(x, 1) for x in a] + [(x, 2) for x in b])
                clauses = {'union': '1=1', 'intersection': 'sources=2',
                           'difference': 'sources=1 AND first_source=1'}
                expected = {'union': len(set(a) | set(b)), 'intersection': len(set(a) & set(b)),
                            'difference': len(set(a) - set(b))}
                for op, clause in clauses.items():
                    value = connection.execute('SELECT COUNT(*) FROM (SELECT identity, '
                        'COUNT(DISTINCT source) AS sources, MIN(source) AS first_source '
                        'FROM evidence GROUP BY identity) WHERE ' + clause).fetchone()[0]
                    self.assertEqual(value, expected[op], (a, b, op))
        finally:
            connection.close()


if __name__ == '__main__':
    unittest.main()
