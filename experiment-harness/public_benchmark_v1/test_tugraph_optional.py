from copy import deepcopy
import unittest

from tugraph_dialect import compile_rest_selection
from tugraph_schema import schema_from_import_config
from test_tugraph_schema import CONFIG


def call():
    return {'nodes':[{'id':'p','label':'Person'},{'id':'q','label':'Person'}],
            'edges':[{'id':'r','source':'p','target':'q','label':'LINK','optional':True}],
            'where':[], 'returns':[{'field':'p.id','alias':'id'},
                                   {'field':'q','agg':'count','alias':'degree'}]}


class NativeOptionalLowering(unittest.TestCase):
    def setUp(self):self.schema=schema_from_import_config(CONFIG)

    def test_zero_neighbor_count_uses_nullable_primary(self):
        q=compile_rest_selection(call(),self.schema)['script']
        self.assertIn('MATCH (p:Person) WITH p OPTIONAL MATCH',q)
        self.assertIn('count(q.id)',q)
        self.assertNotIn('count(id(q))',q)

    def test_global_predicate_runs_after_optional_query_part(self):
        c=call();c['where']=[{'field':'q.id','op':'is_null'}]
        q=compile_rest_selection(c,self.schema)['script']
        self.assertIn('WITH p,q WHERE (q.id IS NULL)',q)

    def test_explicit_entity_grouping_preserves_identity_key(self):
        c=call();c['returns'][0]={'field':'p.name','alias':'name'};c['group_by']=['p.id']
        q=compile_rest_selection(c,self.schema)['script']
        self.assertIn('WITH p.id AS __group_0,p.name AS name,count(q.id) AS degree',q)

    def test_required_anchor_bindings_are_retained(self):
        c=call();c['nodes'].insert(1,{'id':'x','label':'Person'})
        c['edges'].insert(0,{'id':'a','source':'p','target':'x','label':'LINK'})
        c['where']=[{'field':'a.weight','op':'eq','value':2}]
        q=compile_rest_selection(c,self.schema)['script']
        self.assertIn('WITH p,x,a OPTIONAL MATCH',q)
        self.assertIn('WITH p,q,a.weight AS __native_property_0 WHERE',q)

    def test_nullable_edge_properties_are_projected_without_edge_object(self):
        c=call();c['where']=[{'field':'r.weight','op':'ge','value':3}]
        q=compile_rest_selection(c,self.schema)['script']
        self.assertIn('WITH p,q,r.weight AS __native_property_0 WHERE',q)
        self.assertIn('__native_property_0 IS NOT NULL',q)

    def test_both_optional_endpoints_bound_keep_separate_match_scope(self):
        c=call();c['edges'].insert(0,{'id':'a','source':'p','target':'q','label':'LINK'})
        q=compile_rest_selection(c,self.schema)['script']
        self.assertIn('WITH p,q,a OPTIONAL MATCH (p)-[r:LINK]->(q)',q)


if __name__=='__main__':unittest.main()
