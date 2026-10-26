from copy import deepcopy
import unittest

from tugraph_common_input import NativeEntityCandidates, common_input, messages, PLAN_INSTRUCTIONS
from tugraph_schema import schema_from_import_config
from test_tugraph_schema import CONFIG
from schema_services import UnsupportedPlan


class CommonNativeInput(unittest.TestCase):
    def setUp(self):
        self.schema=schema_from_import_config(CONFIG)
        self.rows=[{'label':'Person','properties':{'id':'p1','name':'Twin','capacity':'01'}},
                   {'label':'Person','properties':{'id':'p2','name':'Twin','capacity':None}}]

    def test_same_named_entities_keep_distinct_actual_identities(self):
        candidates=NativeEntityCandidates(self.schema,self.rows).find('Find Twin')
        self.assertEqual([r['primary_value'] for r in candidates],['p1','p2'])
        self.assertTrue(all(r['primary_property']=='id' for r in candidates))
        self.assertNotIn('eid',str(candidates))

    def test_both_arms_receive_identical_user_bytes_and_common_notices(self):
        shared=common_input(task_id='dev1',graph='fixture',question='Find Twin',schema=self.schema,
                            candidates=NativeEntityCandidates(self.schema,self.rows).find('Find Twin'))
        a,b=messages(shared,'direct'),messages(shared,'ours')
        self.assertEqual(a[1],b[1])
        self.assertEqual(a[0]['content'].split('\nTranslate')[0],b[0]['content'].split('\nReturn one')[0])
        self.assertFalse(shared['formal_admission'])
        self.assertNotIn('n.eid',PLAN_INSTRUCTIONS)

    def test_builder_has_no_implicit_reference_channel(self):
        with self.assertRaises(TypeError):
            common_input(task_id='dev',graph='fixture',question='q',schema=self.schema,
                         candidates=[],initial_gql='RETURN 1')

    def test_candidate_data_is_copied_without_numeric_coercion(self):
        shared=common_input(task_id='dev',graph='fixture',question='01',schema=self.schema,
            candidates=NativeEntityCandidates(self.schema,self.rows).find('01'))
        self.assertEqual(shared['payload']['entity_candidates'][0]['matched_properties']['capacity'],'01')
        self.schema['entities'].clear()
        self.assertEqual(len(shared['payload']['schema']['entities']),1)

    def test_duplicate_identity_is_rejected_not_silently_collapsed(self):
        with self.assertRaises(UnsupportedPlan):NativeEntityCandidates(self.schema,self.rows+[self.rows[0]])

    def test_integer_identity_is_not_exposed_as_a_string_literal(self):
        schema=deepcopy(self.schema);schema['entities'][0]['properties']['id']='int'
        index=NativeEntityCandidates(schema,[{'label':'Person','properties':{'id':101,'name':'A'}}])
        row=index.find('101')[0]
        self.assertIs(type(row['primary_value']),int)
        self.assertIs(type(row['matched_properties']['id']),int)

    def test_limits_and_arm_are_explicit(self):
        index=NativeEntityCandidates(self.schema,self.rows)
        for limit in (0,True,13,101):
            with self.assertRaises(UnsupportedPlan):index.find('Twin',limit)
        with self.assertRaises(UnsupportedPlan):
            common_input(task_id='dev',graph='g',question='q',schema=self.schema,candidates=[{}]*13)
        self.assertEqual(index.find('absent'),[])
        with self.assertRaises(UnsupportedPlan):messages({'serialized':'{}'},'unknown')


if __name__=='__main__':unittest.main()
