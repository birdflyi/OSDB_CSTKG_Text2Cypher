from copy import deepcopy
import unittest

from schema_services import compile_selection, execute_plan, UnsupportedPlan, native_datetime_literal
from tugraph_dialect import compile_rest_selection
from tugraph_schema import schema_from_import_config
from test_tugraph_schema import CONFIG


def config():
    c = deepcopy(CONFIG)
    c['schema'][0]['properties'] += [
        {'name':'active','type':'BOOL','optional':True},
        {'name':'created','type':'DATETIME','optional':True}]
    return c


def call(field, value, op='eq'):
    return {'nodes':[{'id':'p','label':'Person'}], 'edges':[],
            'where':[{'field':field,'op':op,'value':value}],
            'returns':[{'field':'p.id','alias':'id'}]}


class NativeTemporalContract(unittest.TestCase):
    def setUp(self):self.schema = schema_from_import_config(config())

    def test_iso_separator_normalizes_without_offset_or_precision_loss(self):
        self.assertEqual(native_datetime_literal('2020-02-29T01:02:03.1'),
                         '2020-02-29 01:02:03.100000')
        c = compile_rest_selection(call('p.created','2020-02-29T01:02:03.123456'), self.schema)
        self.assertEqual(c['parameters']['p0'],'2020-02-29 01:02:03.123456')
        self.assertIn('datetime("2020-02-29 01:02:03.123456")', c['script'])

    def test_ambiguous_or_unrepresentable_datetime_is_rejected(self):
        for value in ('2020-01-01','2020-01-01T00:00:00Z','2020-01-01T00:00:00+08:00',
                      '2020-01-01T00:00:00.1234567','2020-02-30T00:00:00',0,True):
            with self.subTest(value=value), self.assertRaises(UnsupportedPlan):
                compile_rest_selection(call('p.created',value),self.schema)

    def test_bool_is_not_an_integer_or_string(self):
        for value in (0,1,'true','false',None):
            with self.subTest(value=value), self.assertRaises(UnsupportedPlan):
                compile_rest_selection(call('p.active',value),self.schema)
        self.assertIn('p.active=true',compile_rest_selection(call('p.active',True),self.schema)['script'])
        self.assertIn('p.active IN [true,false]',compile_rest_selection(call('p.active',[True,False],'in'),self.schema)['script'])

    def test_bool_and_datetime_do_not_expand_the_original_default(self):
        for c in (call('p.active',True),call('p.created','2020-01-01T00:00:00')):
            with self.assertRaises(UnsupportedPlan):compile_selection(c,self.schema)

    def test_bool_ordering_is_not_admitted_for_property_comparisons(self):
        c=call('p.active',True)
        c['where']=[{'field':'p.active','other_field':'p.active','op':'gt'}]
        with self.assertRaises(UnsupportedPlan):compile_rest_selection(c,self.schema)

    def test_equal_timestamps_with_different_text_do_not_certify_disjointness(self):
        p={'status':'ANSWER','calls':[call('p.created','2020-01-01T00:00:00'),
                                     call('p.created','2020-01-01 00:00:00')],
           'combine':{'op':'union','key':'id','output':'count','alias':'n'}}
        def execute(q, params):
            self.assertNotIn('count(DISTINCT',q)
            return {'columns':['id'],'rows':[['p1']],'complete':True}
        result=execute_plan(p,self.schema,execute,backend='tugraph-4.5.2')
        self.assertEqual(result['rows'],[[1]])
        self.assertEqual(result['representation'],'entity_records')


if __name__=='__main__':unittest.main()
