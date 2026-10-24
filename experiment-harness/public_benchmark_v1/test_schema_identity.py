from copy import deepcopy
import unittest
from schema_identity import canonical_schema


class SchemaIdentityChecks(unittest.TestCase):
    def setUp(self):
        self.schema = {'name': 'example', 'entities': [
            {'label': 'A', 'properties': {'value': 'int'}}, {'label': 'B', 'properties': {}}],
            'relations': [{'label': 'edge', 'subj_label': 'A', 'obj_label': 'B', 'properties': {}}]}

    def test_declaration_order_does_not_change_schema(self):
        other = deepcopy(self.schema)
        other['entities'].reverse()
        self.assertEqual(canonical_schema(other), canonical_schema(self.schema))
        self.assertNotEqual(other, self.schema)

    def test_types_directions_and_duplicate_signatures_remain_checked(self):
        for change in ['type', 'direction']:
            other = deepcopy(self.schema)
            if change == 'type':
                other['entities'][0]['properties']['value'] = 'str'
            else:
                other['relations'][0]['obj_label'] = 'A'
            self.assertNotEqual(canonical_schema(other), canonical_schema(self.schema))
        other = deepcopy(self.schema)
        other['entities'].append(other['entities'][0])
        with self.assertRaises(ValueError):
            canonical_schema(other)
