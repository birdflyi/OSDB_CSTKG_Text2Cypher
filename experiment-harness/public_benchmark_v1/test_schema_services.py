from copy import deepcopy
import unittest

from cypher_read_guard import read_query, ReadOnlyViolation
from schema_services import compile_selection, execute_plan, UnsupportedPlan, IncompleteEvidence


SCHEMA = {'entities': [{'label': 'Person', 'properties': {'born': 'date', 'age': 'int'}},
                       {'label': 'Company', 'properties': {}}],
          'relations': [{'label': 'worksAt', 'subj_label': 'Person',
                         'obj_label': 'Company', 'properties': {'since': 'date'}}]}


def selection(value=None):
    return {'nodes': [{'id': 'n', 'label': 'Person'}], 'edges': [],
            'where': [] if value is None else [{'field': 'n.name', 'op': 'eq', 'value': value}],
            'returns': [{'field': 'n.eid', 'alias': 'id'},
                        {'field': 'n.name', 'alias': 'name'}]}


def plan(left=None, right=None):
    return {'status': 'ANSWER', 'calls': [selection(left), selection(right)],
            'combine': {'op': 'union', 'key': 'id', 'output': 'count', 'alias': 'count'}}


def queued_results(results):
    index = 0

    def execute(query, parameters):
        nonlocal index
        rows = results[index]
        index += 1
        return {'columns': ['count'] if 'count(DISTINCT' in query else ['id', 'name'],
                'rows': rows, 'complete': True, 'result_bytes': 10}
    return execute


class SchemaSelectionChecks(unittest.TestCase):
    def test_literals_are_parameters_and_schema_is_enforced(self):
        malicious = "x' DELETE n //"
        call = selection(malicious)
        compiled = compile_selection(call, SCHEMA)
        self.assertNotIn(malicious, compiled['query'])
        self.assertEqual(compiled['parameters']['p0'], malicious)
        call['returns'][0]['field'] = 'n.not_a_property'
        with self.assertRaises(UnsupportedPlan):
            compile_selection(call, SCHEMA)

    def test_dates_and_aggregation_are_typed(self):
        call = selection()
        call['where'] = [{'field': 'n.born', 'op': 'ge', 'value': '2020-01-01'}]
        call['returns'] = [{'field': 'n', 'agg': 'count', 'distinct': True, 'alias': 'count'}]
        compiled = compile_selection(call, SCHEMA)
        self.assertIn('date($p0)', compiled['query'])
        self.assertIn('count(DISTINCT n)', compiled['query'])
        call['returns'] = [{'field': 'n.name', 'agg': 'avg', 'alias': 'average'}]
        with self.assertRaises(UnsupportedPlan):
            compile_selection(call, SCHEMA)

    def test_edge_direction_and_connectedness(self):
        call = selection()
        call['nodes'].append({'id': 'c', 'label': 'Company'})
        with self.assertRaises(UnsupportedPlan):
            compile_selection(call, SCHEMA)
        call['edges'] = [{'id': 'r', 'source': 'c', 'target': 'n', 'label': 'worksAt'}]
        with self.assertRaises(UnsupportedPlan):
            compile_selection(call, SCHEMA)
        call['edges'][0].update(source='n', target='c')
        self.assertIn('worksAt', compile_selection(call, SCHEMA)['query'])

    def test_disjoint_counts_and_correct_record_control_agree(self):
        candidate = plan('Alice', 'Bob')
        count = execute_plan(candidate, SCHEMA, queued_results([[[2]], [[1]]]))
        records = execute_plan(candidate, SCHEMA,
                  queued_results([[['a1', 'Alice'], ['a2', 'Alice']], [['b', 'Bob']]]),
                  strategy='full_detail')
        self.assertEqual(count['rows'], [[3]])
        self.assertEqual(records['rows'], count['rows'])
        self.assertEqual(count['representation'], 'certified_disjoint_counts')
        self.assertEqual(records['representation'], 'entity_records')

    def test_overlap_requires_keys_not_addition(self):
        candidate = plan()
        outcome = execute_plan(candidate, SCHEMA,
                 queued_results([[['a', 'A'], ['b', 'B'], ['b', 'B']],
                                 [['b', 'B'], ['c', 'C']]]))
        self.assertEqual(outcome['rows'], [[3]])
        self.assertEqual(outcome['representation'], 'entity_records')

    def test_intersection_and_difference_follow_entity_keys(self):
        for op, expected in [('intersection', 1), ('difference', 1)]:
            candidate = plan()
            candidate['combine']['op'] = op
            result = execute_plan(candidate, SCHEMA,
                     queued_results([[['a', 'A'], ['b', 'B']], [['b', 'B'], ['c', 'C']]]))
            self.assertEqual(result['rows'], [[expected]])

    def test_truncation_and_lost_keys_not_accepted(self):
        candidate = plan()
        with self.assertRaises(IncompleteEvidence):
            execute_plan(candidate, SCHEMA, lambda q, p: {'complete': False})
        candidate['calls'][0]['limit'] = 1
        with self.assertRaises(UnsupportedPlan):
            execute_plan(candidate, SCHEMA, queued_results([]))
        candidate = plan()
        candidate['calls'][0]['returns'][0]['field'] = 'n.name'
        with self.assertRaises(UnsupportedPlan):
            execute_plan(candidate, SCHEMA, queued_results([]))

    def test_conflicting_payloads_not_silently_joined(self):
        with self.assertRaises(UnsupportedPlan):
            execute_plan(plan(), SCHEMA,
                         queued_results([[['a', 'A']], [['a', 'changed']]]))


class ReadGuardChecks(unittest.TestCase):
    def test_literals_comments_and_quoted_properties_are_not_write_clauses(self):
        query = "MATCH (n) WHERE n.name='DELETE; SET n.x=1' RETURN n.`create` // DELETE\n"
        self.assertEqual(read_query(query), query.strip())
        read_query('MATCH (n) RETURN n.set;')

    def test_writes_procedures_and_multiple_statements_rejected(self):
        for query in ['MATCH (n) DELETE n RETURN n',
                      'CALL dbms.listConfig() YIELD name RETURN name',
                      'RETURN 1; RETURN 2', 'MATCH (n) SET n.x=1 RETURN n']:
            with self.assertRaises(ReadOnlyViolation):
                read_query(query)
        read_query('CALL { MATCH (n) RETURN count(n) AS c } RETURN c')


if __name__ == '__main__':
    unittest.main()
