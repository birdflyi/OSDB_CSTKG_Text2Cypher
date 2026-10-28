"""Synthetic compilation and public-schema inventory checks; no DB/model I/O."""

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

from neo4j_native_selection_dev_20261029 import NativeSchema, UnsupportedSelection, compile_selection
from public_read_guard import read_query


AUTHOR = {'node_props':{'Person':[
    {'property':key,'datatype':kind} for key,kind in {
        'display':'STRING','age':'INTEGER','alive':'BOOLEAN','born':'DATE',
        'clock':'DATE_TIME','local':'LOCAL_DATE_TIME','tags':'LIST',
        'location':'POINT','span':'DURATION','amount':'FLOAT'}.items()]},
    'rel_props':{'KNOWS':[{'property':'weight','datatype':'FLOAT'}]},
    'relationships':[{'start':'Person','type':'KNOWS','end':'Person'}]}


def call():
    return dict(nodes=[dict(id='n',label='Person')], edges=[], where=[],
                returns=[dict(field='n',alias='entity')])


def with_edge(*, optional=False):
    value = call()
    value['nodes'].append(dict(id='m',label='Person'))
    value['edges'] = [dict(id='r',source='n',target='m',label='KNOWS',optional=optional)]
    return value


class SelectionChecks(unittest.TestCase):
    def compile(self, value, schema=None):
        result = compile_selection(value, schema or NativeSchema(AUTHOR,format='MTQ'))
        self.assertEqual(read_query(result['query']), result['query'])
        self.assertFalse(result['contract']['formal_admission'])
        return result

    def test_source_schema_and_plan_are_not_modified(self):
        raw = deepcopy(AUTHOR); value = with_edge(); before = deepcopy(value)
        schema = NativeSchema(raw,format='MTQ'); self.compile(value,schema)
        self.assertEqual(raw,AUTHOR); self.assertEqual(value,before)

    def test_native_entity_projection_requires_no_property_key(self):
        result = self.compile(call())
        self.assertIn('RETURN `n` AS `entity`',result['query'])
        self.assertEqual(result['contract']['projected']['entity'],{'field':'n','kind':'node'})
        self.assertNotIn('eid',result['query'])
        self.assertNotIn('identity_property',result['contract']['bindings']['n'])

    def test_undeclared_metadata_is_not_injected(self):
        for prop in ('eid','rid','name','aliases','description'):
            value = call(); value['returns'][0]['field'] = 'n.'+prop
            with self.subTest(prop=prop), self.assertRaises(UnsupportedSelection): self.compile(value)

    def test_values_remain_parameters_even_with_cypher_text(self):
        value = call(); literal = "x') DELETE n //"
        value['where'] = [dict(field='n.display',op='eq',value=literal)]
        result = self.compile(value)
        self.assertEqual(result['parameters']['p0'],literal)
        self.assertNotIn('DELETE',result['query'])

    def test_parameter_prefix_covers_literals_membership_and_limit_without_text_rewrite(self):
        value=call();value['where']=[dict(field='n.display',op='eq',value='$p0'),
            dict(field='n.tags',op='contains',value=1),dict(field='n.age',op='in',value=[2,3])]
        value['limit']=5
        schema=NativeSchema(AUTHOR,format='MTQ')
        result=compile_selection(value,schema,parameter_prefix='left_')
        self.assertEqual(result['parameters'],{'left_p0':'$p0','left_p1':1,'left_p2_0':2,
                                             'left_p2_1':3,'left_requested_limit':5})
        self.assertIn('LIMIT $left_requested_limit',result['query'])
        self.assertEqual(read_query(result['query']),result['query'])
        with self.assertRaises(UnsupportedSelection):compile_selection(value,schema,parameter_prefix='x;RETURN ')

    def test_quoted_author_identifiers_are_not_query_fragments(self):
        schema = {'node_props':{'A`Label':[{'property':'odd.key`','datatype':'STRING'}]},
                  'rel_props':{},'relationships':[]}
        value = call(); value['nodes'][0]['label']='A`Label'; value['returns'][0]['field']='n.odd.key`'
        result = self.compile(value,NativeSchema(schema,format='MTQ'))
        self.assertIn('`A``Label`',result['query']); self.assertIn('`odd.key```',result['query'])

    def test_boolean_is_not_integer_and_nonfinite_literal_is_rejected(self):
        for field,literal in (('n.age',True),('n.alive',1),('n.amount',float('inf')),('n.amount',float('nan'))):
            value=call();value['where']=[dict(field=field,op='eq',value=literal)]
            with self.subTest(field=field),self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_null_and_negation_preserve_native_three_valued_logic(self):
        value=call();value['where']=[dict(field='n.alive',op='eq',value=False,negate=True)]
        result=self.compile(value)
        self.assertIn('NOT (`n`.`alive`=$p0)',result['query'])
        self.assertNotIn('coalesce',result['query'].lower())
        value['where']=[dict(field='n.alive',op='is_null',negate=True)]
        self.assertIn('NOT (`n`.`alive` IS NULL)',self.compile(value)['query'])
        value['where'][0]['value']=None
        with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_temporal_constructor_keeps_raw_nanoseconds_and_zone(self):
        value=call();clock='2026-01-01T00:00:00.000000001+05:30'
        value['where']=[dict(field='n.clock',op='ge',value=clock)]
        result=self.compile(value)
        self.assertIn('`n`.`clock`>=datetime($p0)',result['query'])
        self.assertEqual(result['parameters']['p0'],clock)
        value['where']=[dict(field='n.local',op='eq',value='2026-01-01T00:00:00.123456789')]
        self.assertIn('localdatetime($p0)',self.compile(value)['query'])

    def test_date_membership_converts_each_parameter_not_the_property(self):
        value=call();value['where']=[dict(field='n.born',op='in',value=['2024-02-29','2025-03-01'])]
        result=self.compile(value)
        self.assertIn('`n`.`born` IN [date($p0_0),date($p0_1)]',result['query'])
        value['where'][0]['value']=['2023-02-29']
        with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_compact_or_week_date_spellings_do_not_depend_on_python_version(self):
        for literal in ('20260203','2026-W05-2','0000-01-01'):
            value=call();value['where']=[dict(field='n.born',op='eq',value=literal)]
            with self.subTest(literal=literal),self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_integer_wire_bounds_apply_without_float_overflow(self):
        for literal in (-(2**63),2**63-1):
            value=call();value['where']=[dict(field='n.age',op='eq',value=literal)]
            self.assertEqual(self.compile(value)['parameters']['p0'],literal)
        for literal in (2**63,-(2**63)-1,10**1000):
            value=call();value['where']=[dict(field='n.amount',op='eq',value=literal)]
            with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_list_membership_does_not_claim_a_string_subtype(self):
        value=call();value['where']=[dict(field='n.tags',op='contains',value=3)]
        result=self.compile(value)
        self.assertIn('$p0 IN `n`.`tags`',result['query'])
        self.assertEqual(result['contract']['bindings']['n']['properties']['tags'],'LIST')

    def test_native_duration_and_point_are_not_string_normalized(self):
        value=call();value['where']=[dict(field='n.span',op='eq',value='P1M')]
        result=self.compile(value)
        self.assertIn('duration($p0)',result['query']);self.assertEqual(result['parameters']['p0'],'P1M')
        value['where']=[dict(field='n.location',op='eq',value={'x':1.5,'y':2,'srid':7203})]
        self.assertIn('point($p0)',self.compile(value)['query'])
        value['where'][0]['value']={'x':float('inf'),'y':2}
        with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_relation_type_and_direction_use_public_signature(self):
        schema=deepcopy(AUTHOR);schema['node_props']['Other']=[]
        schema['relationships']=[dict(start='Person',type='KNOWS',end='Other')]
        value=with_edge();value['nodes'][1]['label']='Other'
        self.compile(value,NativeSchema(schema,format='MTQ'))
        value['edges'][0].update(source='m',target='n')
        with self.assertRaises(UnsupportedSelection):self.compile(value,NativeSchema(schema,format='MTQ'))

    def test_parallel_relationships_share_one_required_match_clause(self):
        value=with_edge();value['edges'].append(dict(id='s',source='n',target='m',label='KNOWS'))
        value['returns']=[dict(field='r',alias='a'),dict(field='s',alias='b')]
        result=self.compile(value)
        self.assertEqual(result['query'].count('MATCH '),1)
        self.assertEqual(result['contract']['projected']['a']['kind'],'relationship')

    def test_optional_zero_association_uses_nullable_entity_and_count_edge(self):
        value=with_edge(optional=True)
        value['returns']=[dict(field='n.display',alias='label'),dict(field='r',agg='count',alias='n')]
        value['group_by']=['n']
        result=self.compile(value)
        self.assertIn('OPTIONAL MATCH',result['query']);self.assertIn('count(`r`)',result['query'])
        self.assertTrue(result['contract']['bindings']['m']['nullable'])
        self.assertTrue(result['contract']['bindings']['r']['nullable'])
        self.assertFalse(result['contract']['bindings']['n']['nullable'])

    def test_optional_post_filter_does_not_become_optional_match_filter(self):
        value=with_edge(optional=True);value['where']=[dict(field='m',op='is_null')]
        result=self.compile(value)
        self.assertIn('WITH * WHERE (`m` IS NULL)',result['query'])

    def test_entity_grouping_does_not_collapse_same_display_entities(self):
        value=with_edge();value['group_by']=['n']
        value['returns']=[dict(field='n.display',alias='name'),dict(field='r',agg='count',alias='n')]
        result=self.compile(value)
        self.assertIn('WITH `n` AS `__group_0`,',result['query'])
        self.assertIn('`n`.`display` AS `name`',result['query'])
        self.assertNotIn('.`eid`',result['query'])

    def test_property_group_does_not_prove_entity_or_other_property(self):
        for projected in ('n','n.display'):
            value=call();value['group_by']=['n.age']
            value['returns']=[dict(field=projected,alias='x'),dict(field='*',agg='count',alias='n')]
            with self.subTest(projected=projected),self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_explicit_grouping_rejects_undetermined_neighbour_payload(self):
        value=with_edge();value['group_by']=['n']
        value['returns']=[dict(field='m.display',alias='x'),dict(field='r',agg='count',alias='n')]
        with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_disconnected_and_optional_only_components_are_rejected(self):
        value=call();value['nodes'].append(dict(id='m',label='Person'))
        with self.assertRaises(UnsupportedSelection):self.compile(value)
        value=with_edge();value['nodes'].append(dict(id='k',label='Person'))
        value['edges'].append(dict(id='s',source='m',target='k',label='KNOWS',optional=True))
        self.compile(value)
        value['edges'][0]['optional']=True
        with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_bounded_variables_and_unknown_raw_query_fields_are_rejected(self):
        for mutate in (lambda v:v.update(query='RETURN 1'),
                       lambda v:v['nodes'][0].update(id='n) DELETE n'),
                       lambda v:v['nodes'][0].update(label='Unknown'),
                       lambda v:v['returns'][0].update(alias='bad`alias')):
            value=call();mutate(value)
            with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_distinct_and_aggregate_rules_are_explicit(self):
        value=call();value['returns']=[dict(field='n',agg='collect',distinct=True,alias='items')]
        self.assertIn('collect(DISTINCT `n`)',self.compile(value)['query'])
        value['returns']=[dict(field='*',agg='count',distinct=True,alias='n')]
        with self.assertRaises(UnsupportedSelection):self.compile(value)
        value['returns']=[dict(field='n.display',agg='sum',alias='n')]
        with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_order_and_limit_never_appear_implicitly(self):
        result=self.compile(call())
        self.assertNotIn('LIMIT',result['query']);self.assertNotIn('ORDER BY',result['query'])
        value=call();value.update(order_by=[dict(column='entity',direction='DESC')],limit=17)
        result=self.compile(value)
        self.assertTrue(result['query'].endswith('ORDER BY `entity` DESC LIMIT $requested_limit'))
        self.assertEqual(result['parameters']['requested_limit'],17)
        value['limit']=True
        with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_output_aliases_are_unique_and_ordering_cannot_inject_text(self):
        value=call();value['returns'].append(dict(field='n.age',alias='entity'))
        with self.assertRaises(UnsupportedSelection):self.compile(value)
        value=call();value['order_by']=[dict(column='entity',direction='DESC DELETE n')]
        with self.assertRaises(UnsupportedSelection):self.compile(value)

    def test_zog_global_vocabulary_is_not_per_label_existence(self):
        raw=dict(classes={'Person':{},'Vehicle':{}},properties={'name':{'type':'STRING'}},
                 relations={'OWNS':dict(domain='Person',range='Vehicle')})
        schema=NativeSchema(raw,format='ZOGRASCOPE')
        value=call();value['returns']=[dict(field='n.name',alias='name')]
        result=self.compile(value,schema)
        self.assertEqual(schema.nodes,{'Person':{},'Vehicle':{}})
        self.assertEqual(result['contract']['property_scope'],'author_global_vocabulary_not_per_label_presence')
        self.assertFalse(result['contract']['actual_schema_storage_types_verified'])

    def test_endpoint_only_label_is_retained_without_invented_properties(self):
        raw=deepcopy(AUTHOR);raw['relationships']=[dict(start='Person',type='KNOWS',end='MissingProps')]
        schema=NativeSchema(raw,format='MTQ')
        self.assertEqual(schema.endpoint_only_nodes,{'MissingProps'})
        value=with_edge();value['nodes'][1]['label']='MissingProps'
        self.compile(value,schema)
        value['returns']=[dict(field='m.display',alias='x')]
        with self.assertRaises(UnsupportedSelection):self.compile(value,schema)

    def test_unknown_datatype_and_duplicate_schema_declaration_are_rejected(self):
        for mutate in (lambda r:r['node_props']['Person'].append(dict(property='display',datatype='STRING')),
                       lambda r:r['node_props']['Person'][0].update(datatype='UNKNOWN'),
                       lambda r:r['relationships'].extend(deepcopy(r['relationships']))):
            raw=deepcopy(AUTHOR);mutate(raw)
            with self.assertRaises(UnsupportedSelection):NativeSchema(raw,format='MTQ')


def audit_public_schemas(root):
    inventory=[]
    for benchmark,format in (('MindTheQuery','MTQ'),('ZOGRASCOPE','ZOGRASCOPE')):
        for path in sorted((root/benchmark/'schemas').glob('*.json')):
            raw=path.read_bytes(); schema=NativeSchema(json.loads(raw),format=format)
            checks=0
            for label in schema.nodes:
                value=call();value['nodes'][0]['label']=label
                read_query(compile_selection(value,schema)['query']);checks+=1
                for prop in schema.properties('node',label):
                    value['returns']=[dict(field='n.'+prop,alias='value')]
                    read_query(compile_selection(value,schema)['query']);checks+=1
            for relation,start,end in schema.relations:
                value=with_edge();value['nodes'][0]['label']=start;value['nodes'][1]['label']=end
                value['edges'][0]['label']=relation;value['returns']=[dict(field='r',alias='edge')]
                read_query(compile_selection(value,schema)['query']);checks+=1
            inventory.append(dict(benchmark=benchmark,file=path.name,sha256=hashlib.sha256(raw).hexdigest(),
                node_labels=len(schema.nodes),relations=len(schema.relations),property_scope=schema.property_scope,
                endpoint_only_labels=sorted(schema.endpoint_only_nodes),
                schema_only_synthetic_compilations=checks,DB_execution_verified=False))
    if len(inventory)!=12:raise ValueError('Expected the pinned eleven MTQ and one ZOG schema files')
    return inventory


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True)
    parser.add_argument('--schema-root',required=True);args=parser.parse_args()
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(SelectionChecks);planned=suite.countTestCases()
    with patch.object(socket,'create_connection',side_effect=AssertionError('Network disabled')), \
         patch.object(socket.socket,'connect',side_effect=AssertionError('Network disabled')):
        result=unittest.TextTestRunner(verbosity=2).run(suite)
        inventory=audit_public_schemas(Path(args.schema_root)) if result.wasSuccessful() else []
    receipt=dict(kind='NATIVE_SELECTION_COMPILATION_NOT_DATABASE_ACCEPTANCE',planned=planned,
        completed=result.testsRun,checks_passed=result.testsRun-len(result.failures)-len(result.errors),
        failed=len(result.failures)+len(result.errors),author_schemas=len(inventory),
        schema_only_synthetic_compilations=sum(r['schema_only_synthetic_compilations'] for r in inventory),
        actual_benchmark_questions_used=0,reference_queries_used=0,model_calls=0,DB_connections=0,DB_queries=0,
        full_Cypher_coverage=False,formal_admission=False)
    output=Path(args.output);output.mkdir(parents=True,exist_ok=False)
    for name,value in (('summary.json',receipt),('schema_inventory.json',inventory)):
        (output/name).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    if not result.wasSuccessful():raise SystemExit(1)


if __name__=='__main__':main()
