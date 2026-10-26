import unittest
from osdb_window_census import WindowCensus,canonical_utc,pair_counts


def add(c,source='PullRequest:PR_a',action='CLOSED_BY',time='2023-01-01T00:00:00Z',eid='event',label='PullRequest'):
    c.add(label=label,source_uid=source,action=action,event_time=time,event_id=eid)


class WindowCensusTests(unittest.TestCase):
    def test_half_open_month_and_year_boundaries(self):
        c=WindowCensus();add(c,time='2023-01-31T23:59:59.999999Z');add(c,time='2023-02-01T00:00:00Z',eid='new')
        add(c,time='2024-01-01T00:00:00Z',eid='outside')
        r=c.result();p=next(p for p in r['pairs'] if p['kind']=='same_action_adjacent_months' and p['label']=='PullRequest' and p['left_action']=='CLOSED_BY' and p['left_month']==1)
        self.assertEqual(p['intersection_entities'],1);self.assertEqual(p['sum_minus_union'],1)
        self.assertEqual(r['counts']['outside_declared_year_rows'],1)

    def test_different_actions_can_share_the_same_entity(self):
        c=WindowCensus();add(c,action='OPENED_BY');add(c,action='CLOSED_BY',eid='close')
        p=next(p for p in c.result()['pairs'] if p['kind']=='same_month_different_actions' and p['label']=='PullRequest' and p['left_month']==1 and p['left_action']=='OPENED_BY' and p['right_action']=='CLOSED_BY')
        self.assertEqual(p['sum_entity_counts'],2);self.assertEqual(p['union_entities'],1)

    def test_physical_rows_event_keys_and_entities_are_separate(self):
        c=WindowCensus();add(c);add(c);add(c,eid='another')
        cohort=next(x for x in c.result()['cohorts'] if x['label']=='PullRequest' and x['action']=='CLOSED_BY' and x['month']==1)
        self.assertEqual((cohort['physical_edge_rows'],cohort['distinct_entity_event_id_keys'],cohort['distinct_entities']),(3,2,1))

    def test_missing_event_id_does_not_drop_valid_entity_membership(self):
        c=WindowCensus();add(c,eid=None)
        self.assertEqual(c.result()['counts']['declared_action_rows_without_usable_event_id'],1)
        self.assertEqual(len(c.entities[('PullRequest','CLOSED_BY',1)]),1)

    def test_event_id_types_are_not_silently_coerced(self):
        c=WindowCensus();add(c,eid=1);add(c,eid='1');add(c,eid=True)
        self.assertEqual(len(c.events[('PullRequest','CLOSED_BY',1)]),2)
        self.assertEqual(c.result()['counts']['declared_action_rows_without_usable_event_id'],1)

    def test_noncanonical_offsets_or_invalid_dates_are_not_rewritten(self):
        c=WindowCensus()
        for value in (None,'2023-01-01','2023-01-01T00:00:00+00:00','2023-02-30T00:00:00Z'):
            add(c,time=value)
        self.assertEqual(c.result()['counts']['missing_or_noncanonical_or_invalid_source_time'],4)
        self.assertIsNotNone(canonical_utc('2023-01-01T00:00:00Z'))

    def test_all_empty_and_disjoint_controls_are_retained(self):
        c=WindowCensus();r=c.result()
        self.assertEqual((len(r['cohorts']),len(r['pairs'])),(84,185))
        self.assertTrue(all(p['overlap_fraction_of_union'] is None for p in r['pairs']))
        self.assertEqual(pair_counts({'a'},{'b'})['sum_minus_union'],0)

    def test_same_event_multiple_times_is_reported_without_rewriting(self):
        c=WindowCensus();add(c);add(c,time='2023-02-01T00:00:00Z')
        self.assertEqual(c.result()['distinct_entity_event_keys_with_multiple_observed_times'],1)

    def test_typed_subject_scope_and_unsupported_action_are_explicit(self):
        c=WindowCensus()
        with self.assertRaises(ValueError):add(c,source='Issue:I_a')
        add(c,action='ASSIGNED_BY')
        self.assertEqual(c.result()['counts']['in_year_action_outside_declared_service_subset_rows'],1)
        self.assertFalse(any(x['distinct_entities'] for x in c.result()['cohorts']))

    def test_short_fraction_precision_is_preserved_on_python39(self):
        for fraction in ('1','12','123','1234','12345','123456'):
            a=canonical_utc('2023-01-01T00:00:00.'+fraction+'Z')
            b=canonical_utc('2023-01-01T00:00:00.'+fraction.ljust(6,'0')+'Z')
            self.assertIsNotNone(a);self.assertEqual(a,b)
        self.assertIsNone(canonical_utc('2023-01-01T00:00:00.1234567Z'))


if __name__=='__main__':unittest.main()
