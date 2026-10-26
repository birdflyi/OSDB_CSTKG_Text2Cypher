from copy import deepcopy
import unittest

from osdb_common_input import common_input, observed_type, property_kind


class CommonInputTests(unittest.TestCase):
    def setUp(self):
        self.schema = {'entities': [{'label': 'Actor', 'identity_property': 'entity_id',
            'properties': {'entity_id': 'str'}, 'gold': 99, 'population': 10}],
            'relations': [], 'reference_answer': 99}

    def make(self, **kw):
        return common_input(task_id='x', graph='g', question='original',
                            schema=kw.get('schema', self.schema))

    def test_private_metadata_not_serialized(self):
        value = self.make()
        self.assertNotIn('gold', value['serialized'])
        self.assertNotIn('"population":', value['serialized'])
        self.assertNotIn('reference_answer', value['serialized'])
        self.assertEqual(value['payload']['question'], 'original')

    def test_same_user_bytes_for_both_arms(self):
        left, right = self.make(), self.make(schema=deepcopy(self.schema))
        self.assertEqual(left['serialized'], right['serialized'])
        self.assertEqual(left['sha256'], right['sha256'])
        self.assertFalse(left['formal_admission'])

    def test_deep_copy_and_no_candidates_invention(self):
        result = self.make()
        result['payload']['schema']['entities'][0]['properties']['invented'] = 'str'
        self.assertNotIn('invented', self.schema['entities'][0]['properties'])
        self.assertEqual(result['payload']['entity_candidates'], [])

    def test_invalid_identity_and_duplicate_fail(self):
        value = deepcopy(self.schema)
        value['entities'][0]['identity_property'] = 'eid'
        with self.assertRaises(ValueError): self.make(schema=value)
        value = deepcopy(self.schema)
        value['entities'] *= 2
        with self.assertRaises(ValueError): self.make(schema=value)

    def test_endpoint_and_whole_task_record_fail(self):
        value = deepcopy(self.schema)
        value['relations'] = [{'label': 'EDGE', 'subj_label': 'Missing',
                              'obj_label': 'Actor', 'properties': {}}]
        with self.assertRaises(ValueError): self.make(schema=value)
        with self.assertRaises(TypeError):
            common_input(task_id='x', graph='g', question='q', schema=self.schema, gold=99)

    def test_types_preserve_bool_null_and_mixed(self):
        self.assertEqual(observed_type(True), 'bool')
        self.assertEqual(observed_type(1), 'int')
        self.assertEqual(property_kind(['null', 'str']), 'str')
        self.assertEqual(property_kind(['int', 'str']), 'mixed[int|str]')
        self.assertEqual(property_kind(['null']), 'null_only')
        with self.assertRaises(ValueError): observed_type(float('nan'))
        with self.assertRaises(ValueError): observed_type({'nested': 'map'})


if __name__ == '__main__': unittest.main()
