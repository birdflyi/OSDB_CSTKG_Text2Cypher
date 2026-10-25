from copy import deepcopy
import unittest

from schema_services import compile_selection, execute_plan, UnsupportedPlan
from tugraph_schema import schema_from_import_config
from test_tugraph_dialect import selection


CONFIG = {'schema': [
    {'type': 'VERTEX', 'label': 'Person', 'primary': 'id', 'properties': [
        {'name': 'id', 'type': 'STRING', 'optional': False},
        {'name': 'name', 'type': 'STRING', 'optional': True},
        {'name': 'capacity', 'type': 'STRING', 'optional': True}]},
    {'type': 'EDGE', 'label': 'LINK', 'constraints': [['Person', 'Person']],
     'properties': [{'name': 'weight', 'type': 'INT32', 'optional': True}]}]}


def call(values):
    s = selection()
    s['where'] = [{'field': 'p.id', 'op': 'in', 'value': values}]
    s['returns'].append({'field': 'p.name', 'alias': 'name'})
    s.pop('order_by')
    return s


def plan(op='union', left=None, right=None):
    return {'status': 'ANSWER', 'calls': [call(left or ['p1','p2']), call(right or ['p2','p3'])],
            'combine': {'op': op, 'key': 'id', 'output': 'count', 'alias': 'n'}}


def executor(results):
    queue = iter(results)
    return lambda q, p: {'columns': ['n'] if 'count(DISTINCT' in q else ['id','name'],
                         'rows': next(queue), 'complete': True}


class NativeSchemaIdentity(unittest.TestCase):
    def setUp(self):
        self.schema = schema_from_import_config(CONFIG)

    def test_schema_keeps_authoritative_types_and_properties(self):
        row = self.schema['entities'][0]
        self.assertEqual(row['identity_property'], 'id')
        self.assertEqual(set(row['properties']), {'id','name','capacity'})
        self.assertEqual(row['properties']['capacity'], 'str')
        self.assertNotIn('rid', self.schema['relations'][0]['properties'])
        self.assertFalse(self.schema['formal_admission'])

    def test_duplicate_or_nullable_primary_is_not_silently_repaired(self):
        for mode in ('nullable','duplicate','missing'):
            c = deepcopy(CONFIG)
            if mode == 'nullable': c['schema'][0]['properties'][0]['optional'] = True
            if mode == 'duplicate': c['schema'][0]['properties'].append(c['schema'][0]['properties'][0])
            if mode == 'missing': c['schema'][0]['primary'] = 'eid'
            with self.subTest(mode=mode), self.assertRaises(UnsupportedPlan):
                schema_from_import_config(c)

    def test_unsupported_native_type_is_preserved(self):
        c = deepcopy(CONFIG)
        c['schema'][0]['properties'][1]['type'] = 'FLOAT_VECTOR'
        self.assertEqual(schema_from_import_config(c)['entities'][0]['properties']['name'],
                         'native:FLOAT_VECTOR')

    def test_edge_constraints_and_duplicate_signatures_are_checked(self):
        for pair in (['Person','Unknown'], ['Person'], ['Person','Person']):
            c = deepcopy(CONFIG)
            c['schema'][1]['constraints'].append(pair)
            with self.subTest(pair=pair), self.assertRaises(UnsupportedPlan):
                schema_from_import_config(c)

    def test_overlap_union_intersection_difference_use_primary(self):
        for op, expected in [('union',3),('intersection',1),('difference',1)]:
            result = execute_plan(plan(op), self.schema,
                executor([[['p1','Twin'],['p2','Twin']], [['p2','Twin'],['p3','Other']]]),
                backend='tugraph-4.5.2')
            self.assertEqual(result['rows'], [[expected]])
            self.assertEqual(result['representation'], 'entity_records')

    def test_positive_disjoint_count_and_full_control_match(self):
        p = plan()
        for s, value in zip(p['calls'], ['Twin','Other']):
            s['where'] = [{'field':'p.name','op':'eq','value':value}]
        count = execute_plan(p, self.schema, executor([[[2]],[[1]]]), backend='tugraph-4.5.2')
        full = execute_plan(p, self.schema,
            executor([[['p1','Twin'],['p2','Twin']],[['p3','Other']]]),
            backend='tugraph-4.5.2', strategy='full_detail')
        self.assertEqual(count['rows'], full['rows'])
        self.assertEqual(count['representation'], 'certified_disjoint_counts')
        self.assertEqual(full['representation'], 'entity_records')

    def test_name_is_not_an_entity_identity(self):
        p = plan()
        for s in p['calls']: s['returns'][0]['field'] = 'p.name'
        with self.assertRaises(UnsupportedPlan):
            execute_plan(p, self.schema, executor([]), backend='tugraph-4.5.2')

    def test_integer_primary_preserves_type_and_rejects_boolean(self):
        c = deepcopy(CONFIG);c['schema'][0]['properties'][0]['type'] = 'INT64'
        schema = schema_from_import_config(c)
        p = plan(left=[1,2], right=[2,3])
        result = execute_plan(p, schema, executor([[[1,'A'],[2,'B']],[[2,'B'],[3,'C']]]),
                              backend='tugraph-4.5.2')
        self.assertEqual(result['rows'], [[3]])
        for bad in (True,'1',None,1.0):
            with self.subTest(bad=bad), self.assertRaises(UnsupportedPlan):
                execute_plan(p, schema, executor([[[bad,'A']],[[2,'B']]]),backend='tugraph-4.5.2')

    def test_grouping_by_primary_determines_display_payload(self):
        c = call(['p1','p2'])
        c['returns'] = [{'field':'p.name','alias':'name'},
                        {'field':'*','agg':'count','alias':'n'}]
        c['group_by'] = ['p.id']
        compiled = compile_selection(c, self.schema, backend='tugraph-4.5.2')
        self.assertEqual(compiled['contract']['identity_fields']['p']['field'],'p.id')


if __name__ == '__main__': unittest.main()
