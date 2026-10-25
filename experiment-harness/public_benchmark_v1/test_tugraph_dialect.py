"""Compiler contracts; actual engine fixtures are a separate measured artifact."""
from copy import deepcopy
import unittest

from schema_services import compile_selection, UnsupportedPlan
from tugraph_dialect import compile_rest_selection, literal


SCHEMA = {'entities': [{'label': 'Person', 'properties': {
    'id': 'str', 'name': 'str', 'optional_value': 'str', 'birthday': 'date'}}],
    'relations': [{'label': 'LINK', 'subj_label': 'Person', 'obj_label': 'Person',
                   'properties': {'weight': 'int'}}]}


def selection():
    return {'nodes': [{'id': 'p', 'label': 'Person'}], 'edges': [], 'where': [],
            'returns': [{'field': 'p.id', 'alias': 'id'}],
            'order_by': [{'column': 'id', 'direction': 'ASC'}]}


class NativeCompilerContract(unittest.TestCase):
    def test_default_preserves_original_query(self):
        call = selection()
        self.assertEqual(compile_selection(call, SCHEMA)['query'],
                         'MATCH (p:`Person`) RETURN p.`id` AS `id` ORDER BY `id` ASC')

    def test_native_only_admits_declared_properties(self):
        for prop in ('eid', 'aliases', 'description'):
            call = selection()
            call['returns'][0]['field'] = 'p.' + prop
            with self.subTest(prop=prop), self.assertRaises(UnsupportedPlan):
                compile_rest_selection(call, SCHEMA)

    def test_native_does_not_reinterpret_unverified_identifiers(self):
        for label in ('Needs Space', 'MATCH', '注释', 'x`'):
            schema = deepcopy(SCHEMA)
            schema['entities'][0]['label'] = label
            call = selection()
            call['nodes'][0]['label'] = label
            with self.subTest(label=label), self.assertRaises(UnsupportedPlan):
                compile_rest_selection(call, schema)

    def test_unverified_optional_is_explicitly_unsupported(self):
        call = selection()
        call['nodes'].append({'id': 'q', 'label': 'Person'})
        call['edges'] = [{'id': 'r', 'label': 'LINK', 'source': 'p', 'target': 'q',
                          'optional': True}]
        with self.assertRaises(UnsupportedPlan):
            compile_rest_selection(call, SCHEMA)

    def test_literals_are_not_rescanned_as_parameters(self):
        hostile = 'x" RETURN 1 // $p1 \\ \n 测试'
        call = selection()
        call['where'] = [{'field': 'p.name', 'op': 'eq', 'value': hostile}]
        rendered = compile_rest_selection(call, SCHEMA)
        self.assertEqual(rendered['parameters'], {'p0': hostile})
        self.assertIn(literal(hostile), rendered['script'])
        self.assertFalse(rendered['formal_admission'])

    def test_literal_domains_reject_coercion(self):
        for value in (None, 2**63, -(2**63)-1, float('inf'), float('nan'), [[1]], {}):
            with self.subTest(value=repr(value)), self.assertRaises(UnsupportedPlan):
                literal(value)
        self.assertEqual(literal([1, 1.5, True, '1']), '[1,1.5,true,"1"]')

    def test_negated_property_comparison_guards_both_sides(self):
        call = selection()
        call['where'] = [{'field': 'p.optional_value', 'op': 'eq',
                          'other_field': 'p.name', 'negate': True}]
        query = compile_rest_selection(call, SCHEMA)['script']
        self.assertIn('p.optional_value IS NOT NULL AND p.name IS NOT NULL', query)
        self.assertIn('NOT (p.optional_value=p.name)', query)

    def test_parallel_edges_keep_distinct_bindings_in_one_trail(self):
        call = selection()
        call['nodes'].append({'id': 'q', 'label': 'Person'})
        call['edges'] = [{'id': key, 'label': 'LINK', 'source': 'p', 'target': 'q'}
                         for key in ('r', 's')]
        query = compile_rest_selection(call, SCHEMA)['query']
        self.assertIn('(p:Person)-[r:LINK]->(q:Person)<-[s:LINK]-(p)', query)
        self.assertEqual(query.count('MATCH'), 1)
        self.assertEqual(query.count('[r:'), 1)
        self.assertEqual(query.count('[s:'), 1)

    def test_native_node_count_uses_physical_identity(self):
        call = selection()
        call['returns'] = [{'field': 'p', 'agg': 'count', 'distinct': True, 'alias': 'n'}]
        call['order_by'] = []
        self.assertIn('count(DISTINCT id(p))', compile_rest_selection(call, SCHEMA)['script'])
        self.assertIn('count(DISTINCT p)', compile_selection(call, SCHEMA)['query'])


if __name__ == '__main__':
    unittest.main()
