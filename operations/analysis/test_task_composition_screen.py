import unittest
from task_composition_screen import screen

class CompositionScreenTests(unittest.TestCase):
    def test_node_or_actual_primary_count(self):
        for expr in ('count(DISTINCT p)','COUNT(DISTINCT p.id) AS n'):
            self.assertTrue(screen('MATCH (p:Person) RETURN '+expr,{'Person':'id'})['one_stage_entity_count'])

    def test_display_property_count_is_not_entity_count(self):
        self.assertFalse(screen('MATCH (p:Person) RETURN count(DISTINCT p.name)',{'Person':'id'})['recognized_distinct_entity_count'])

    def test_non_distinct_count_and_extra_return_are_unrecognized(self):
        for expr in ('count(p)','count(DISTINCT p),p.name'):
            self.assertFalse(screen('MATCH (p:Person) RETURN '+expr,{'Person':'id'})['recognized_distinct_entity_count'])

    def test_partition_is_shape_not_disjointness_certificate(self):
        for predicate in ("p.name='same' OR p.name='same'", "(p.name='a') OR (p.name='b')"):
            self.assertTrue(screen('MATCH (p:Person) WHERE '+predicate+' RETURN count(DISTINCT p)',{'Person':'id'})['equality_partition_shape_only'])

    def test_unknown_binding_and_different_partition_fields(self):
        self.assertFalse(screen('MATCH (p:Other) RETURN count(DISTINCT p)',{'Person':'id'})['recognized_distinct_entity_count'])
        result=screen("MATCH (p:Person) WHERE p.name='a' OR p.id='b' RETURN count(DISTINCT p)",{'Person':'id'})
        self.assertTrue(result['one_stage_entity_count_with_OR']);self.assertFalse(result['equality_partition_shape_only'])

    def test_with_optional_union_remain_outside_one_stage(self):
        for query in ('MATCH (p:Person) WITH p RETURN count(DISTINCT p)',
                      'MATCH (p:Person) OPTIONAL MATCH (p)-[]->(x) RETURN count(DISTINCT p)',
                      'MATCH (p:Person) RETURN count(DISTINCT p) UNION RETURN 1'):
            self.assertFalse(screen(query,{'Person':'id'})['one_stage_entity_count'])

    def test_literals_comments_quoted_names_do_not_inflate_keywords(self):
        query='MATCH (`p`:Person) WHERE p.name="OR WITH UNION" /* OPTIONAL */ RETURN COUNT(DISTINCT `p`.`id`) AS n LIMIT 2'
        result=screen(query,{'Person':'id'})
        self.assertTrue(result['one_stage_entity_count']);self.assertFalse(result['flags']['OR'])

    def test_unbalanced_syntax_remains_recordable_error(self):
        for query in ('MATCH (p:Person RETURN p','RETURN "unterminated','RETURN 1 /*'):
            with self.assertRaises(ValueError):screen(query,{'Person':'id'})

    def test_rebound_variable_is_not_assumed_to_keep_entity_identity(self):
        result=screen('MATCH (p:Person) WITH 1 AS p RETURN count(DISTINCT p)',{'Person':'id'})
        self.assertFalse(result['recognized_distinct_entity_count'])

if __name__=='__main__':unittest.main()
