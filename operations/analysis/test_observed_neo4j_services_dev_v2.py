from copy import deepcopy
import unittest
from observed_neo4j_services_dev_v2 import compile_selection,execute_plan,UnsupportedPlan,literal_matches

SCHEMA={'entities':[{'label':'Person','identity_property':'key',
    'properties':{'key':'str','display':'str','eligible':'bool','clock':'str'},
    'temporal_properties':['clock']}],
    'relations':[{'label':'LINK','subj_label':'Person','obj_label':'Person',
        'identity_property':'physical_key','properties':{'physical_key':'str','clock':'str'},
        'temporal_properties':['clock']}]}


def call():
    return {'nodes':[{'id':'n','label':'Person'}],'edges':[],'where':[],
        'returns':[{'field':'n.key','alias':'id'}]}


def plan(a,b,op='difference',output='count'):
    return {'status':'ANSWER','calls':[a,b],
        'combine':{'op':op,'key':'id','output':output,'alias':'n'}}


def queued(records):
    items=iter(records)
    def execute(query,params):return {'rows':next(items),'columns':['id'],'complete':True}
    return execute


class ProfileTests(unittest.TestCase):
    def test_missing_instant_operands_are_guarded_even_when_negated(self):
        for op in ('eq','ne','lt','le','gt','ge'):
            x=call();x['where']=[{'field':'n.clock','op':op,'value':'2023-07-01T00:00:00Z','negate':True}]
            c=compile_selection(x,SCHEMA)
            self.assertIn('n.`clock` IS NOT NULL',c['query'])
            self.assertIn('NOT (',c['query'])

    def test_instant_null_check_uses_original_property_not_pair_container(self):
        x=call();x['where']=[{'field':'n.clock','op':'is_null'}]
        c=compile_selection(x,SCHEMA)
        self.assertIn('n.`clock` IS NULL',c['query']);self.assertNotIn('epochSeconds',c['query'])

    def test_actual_identity_composes_duplicate_records(self):
        a=call();b=call()
        for op,expected in [('difference',1),('intersection',1),('union',3)]:
            result=execute_plan(plan(a,b,op),SCHEMA,queued([[['a'],['a'],['b']],[['b'],['c']]]))
            self.assertEqual(result['rows'],[[expected]])

    def test_unknown_metadata_and_wrong_key_rejected(self):
        for prop in ('eid','name','aliases','description','__pilot_uid'):
            x=call();x['returns'][0]['field']='n.'+prop
            with self.subTest(prop=prop),self.assertRaises(UnsupportedPlan):compile_selection(x,SCHEMA)
        a=call();a['returns'][0]['field']='n.display'
        with self.assertRaises(UnsupportedPlan):execute_plan(plan(a,a),SCHEMA,lambda *args:self.fail('Dispatched'))

    def test_edge_identity_not_synthetic_rid_and_single_match(self):
        x=call();x['nodes'].append({'id':'m','label':'Person'})
        x['edges']=[{'id':v,'source':'n','target':'m','label':'LINK'} for v in ('r','s')]
        x['returns']=[{'field':'r.physical_key','alias':'physical'}]
        self.assertEqual(compile_selection(x,SCHEMA)['query'].count('MATCH '),1)
        x['returns'][0]['field']='r.rid'
        with self.assertRaises(UnsupportedPlan):compile_selection(x,SCHEMA)

    def test_boolean_literal_is_not_integer_and_null_negation_retained(self):
        x=call();x['where']=[{'field':'n.eligible','op':'eq','value':False,'negate':True}]
        compiled=compile_selection(x,SCHEMA)
        self.assertIs(compiled['parameters']['p0'],False);self.assertIn('NOT (',compiled['query'])
        x['where'][0]['value']=0
        with self.assertRaises(UnsupportedPlan):compile_selection(x,SCHEMA)
        x['where'][0].update(op='lt',value=True)
        with self.assertRaises(UnsupportedPlan):compile_selection(x,SCHEMA)

    def test_timestamp_storage_kept_raw_projection_predicates_are_instants(self):
        x=call();value='2023-07-01T00:00:00.000000001+00:00'
        x['where']=[{'field':'n.clock','op':'ge','value':value}]
        x['returns']=[{'field':'n.clock','alias':'raw'}]
        c=compile_selection(x,SCHEMA)
        self.assertIn('[datetime(n.`clock`).epochSeconds,datetime(n.`clock`).nanosecond]>=',c['query'])
        self.assertIn('[datetime($p0).epochSeconds,datetime($p0).nanosecond]',c['query'])
        self.assertIn('RETURN n.`clock` AS `raw`',c['query'])
        self.assertEqual(c['parameters']['p0'],value)
        for bad in ('2023-02-30T00:00:00Z','2023-07-01T00:00:00','2023-07-01T00:00:00.1234567890Z','2023-07-01T00:00:00+00:60'):
            self.assertFalse(literal_matches(bad,'datetime'),bad)

    def test_instant_spelling_cannot_certify_disjoint_count(self):
        a=call();b=call()
        a['where']=[{'field':'n.clock','op':'eq','value':'2023-07-01T00:00:00Z'}]
        b['where']=[{'field':'n.clock','op':'eq','value':'2023-07-01T01:00:00+01:00'}]
        result=execute_plan(plan(a,b,'union'),SCHEMA,queued([[['same']],[['same']]]))
        self.assertEqual(result['rows'],[[1]])
        self.assertEqual(result['representation'],'entity_records')

    def test_explicit_grouping_uses_declared_entity_identity(self):
        x=call();x['returns']=[{'field':'n.display','alias':'display'},{'field':'n','agg':'count','alias':'n'}]
        x['group_by']=['n.key']
        self.assertIn('n.`key` AS `__group_0`',compile_selection(x,SCHEMA)['query'])
        x['group_by']=['n.eligible']
        with self.assertRaises(UnsupportedPlan):compile_selection(x,SCHEMA)

    def test_optional_zero_count_and_post_join_filter_shape(self):
        x=call();x['nodes'].append({'id':'m','label':'Person'})
        x['edges']=[{'id':'r','source':'n','target':'m','label':'LINK','optional':True}]
        x['where']=[{'field':'m.key','op':'is_null'}]
        x['group_by']=['n.key'];x['returns']=[{'field':'n.key','alias':'id'},{'field':'m.key','agg':'count','alias':'n'}]
        c=compile_selection(x,SCHEMA)
        self.assertIn('WITH * WHERE',c['query']);self.assertIn('count(m.`key`)',c['query'])

    def test_missing_identity_duplicate_schema_and_false_temporal_annotation(self):
        for mutate in (lambda s:s['entities'][0].pop('identity_property'),
            lambda s:s['entities'].append(deepcopy(s['entities'][0])),
            lambda s:s['entities'][0].update(temporal_properties=['eligible'])):
            s=deepcopy(SCHEMA);mutate(s)
            with self.assertRaises(UnsupportedPlan):compile_selection(call(),s)

    def test_integer_keys_enforce_exact_type_and_payload_consistency(self):
        s=deepcopy(SCHEMA);s['entities'][0]['properties']['key']='int'
        self.assertEqual(execute_plan(plan(call(),call()),s,queued([[[1],[2]],[[2]]]))['rows'],[[1]])
        for rows in ([[True]],[[None]],[['1']]):
            with self.subTest(rows=rows),self.assertRaises(UnsupportedPlan):execute_plan(plan(call(),call()),s,queued([rows,[]]))

    def test_call_bound_and_backend_not_relaxed(self):
        with self.assertRaises(UnsupportedPlan):execute_plan({'status':'ANSWER','calls':[call()]*4,'combine':{'op':'union'}},SCHEMA,lambda *args:self.fail('Dispatched'))
        with self.assertRaises(UnsupportedPlan):compile_selection(call(),SCHEMA,backend='tugraph-4.5.2')


if __name__=='__main__':unittest.main()
