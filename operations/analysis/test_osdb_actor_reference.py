import unittest
from osdb_actor_reference import python_answers,sqlite_answers,utc_microseconds


class ActorReferenceSemantics(unittest.TestCase):
    def check(self,rows,expected):
        values={'union_openers':expected[0],'both_opening_domains':expected[1],
            'observed_opening_gap':expected[2],'first_observed_opening':expected[3]}
        self.assertEqual(python_answers(rows,0,10,20),values)
        self.assertEqual(sqlite_answers(rows,0,10,20),values)

    def test_empty_is_valid_zero(self):self.check([], (0,0,0,0))
    def test_cross_domain_shared_actor_not_double_counted(self):
        self.check([('a','Issue',11),('a','PullRequest',12),('b','Issue',12)],(2,1,0,2))
    def test_physical_duplicate_and_multiple_events(self):
        self.check([('a','Issue',1),('a','Issue',1),('a','Issue',2),('b','PullRequest',15)],(1,0,1,1))
    def test_half_open_history_cutoff_end_and_outside(self):
        self.check([('before','Issue',-1),('early','Issue',0),('atcut','Issue',10),
            ('atend','Issue',20)],(1,0,1,1))
    def test_old_then_returned_is_neither_new_nor_gap(self):
        self.check([('a','Issue',1),('a','PullRequest',11),('a','Issue',21)],(1,0,0,0))
    def test_typed_actor_keys_stay_distinct(self):
        self.check([('["str","1"]','Issue',11),('["int",1]','PullRequest',11)],(2,0,0,2))
    def test_invalid_records_and_boundaries_abort(self):
        for function in (python_answers,sqlite_answers):
            for rows in ([('a','Commit',1)],[('a','Issue',True)],[('', 'Issue',1)]):
                with self.assertRaises(ValueError):function(rows,0,10,20)
            with self.assertRaises(ValueError):function([],10,0,20)

    def test_utc_fraction_precision_and_no_offset_or_missing_fallback(self):
        self.assertEqual(utc_microseconds('1970-01-01T00:00:00.1Z'),100000)
        self.assertEqual(utc_microseconds('1970-01-01T00:00:00.000001Z'),1)
        for value in (None,'2023-07-01','2023-07-01T00:00:00+00:00','2023-02-30T00:00:00Z'):
            with self.assertRaises(ValueError):utc_microseconds(value)


if __name__=='__main__':unittest.main()
