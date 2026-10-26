"""Observed CSV workload census; no query planner, graph server or model.

Membership uses explicit canonical UTC source_event_time only. Physical rows,
typed source-event keys and typed source-node identities remain separate.
All declared 2023 window/action pairs, including empty pairs, are retained.
"""
from collections import Counter,defaultdict
from datetime import datetime,timezone
from itertools import combinations
import re

VERSION='osdb-window-census-2'
ACTIONS={'Issue':('OPENED_BY','CLOSED_BY','REOPENED_BY'),
         'PullRequest':('OPENED_BY','CLOSED_BY','REOPENED_BY','MERGED_BY')}


def canonical_utc(value):
    if type(value) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z',value):
        return None
    # Python 3.9 accepts only 3/6 fractional digits. Right-padding preserves
    # the exact represented instant for the declared 1..6 digit input range.
    body=value[:-1]
    if '.' in body:
        head,fraction=body.split('.')
        body=head+'.'+fraction.ljust(6,'0')
    try:return datetime.fromisoformat(body+'+00:00')
    except ValueError:return None


def typed_event(value):
    if type(value) is str and value:return ('str',value)
    if type(value) is int:return ('int',str(value))
    return None


def pair_counts(left,right):
    a,b=set(left),set(right)
    overlap=len(a&b);union=len(a|b)
    assert len(a)+len(b)-union==overlap
    return {'left_entities':len(a),'right_entities':len(b),'intersection_entities':overlap,
            'union_entities':union,'sum_entity_counts':len(a)+len(b),
            'sum_minus_union':overlap,'both_nonempty':bool(a and b),
            'any_nonempty':bool(a or b),'overlap_fraction_of_union':overlap/union if union else None}


class WindowCensus:
    def __init__(self,year=2023):
        if type(year) is not int:raise ValueError('Explicit calendar year required')
        self.year=year;self.counts=Counter();self.types=Counter()
        self.entities=defaultdict(set);self.rows=Counter();self.events=defaultdict(set)
        self.event_times=defaultdict(set)

    def add(self,*,label,source_uid,action,event_time,event_id):
        if label not in ACTIONS:raise ValueError('Unexpected cohort label')
        if type(source_uid) is not str or not source_uid.startswith(label+':'):
            raise ValueError('Typed source identity required')
        self.counts['all_issue_pr_actor_event_action_rows']+=1
        self.types[type(event_id).__name__]+=1
        when=canonical_utc(event_time)
        if when is None:
            self.counts['missing_or_noncanonical_or_invalid_source_time']+=1;return
        if when.year!=self.year:
            self.counts['outside_declared_year_rows']+=1;return
        self.counts['canonical_in_year_rows']+=1
        if action not in ACTIONS[label]:
            self.counts['in_year_action_outside_declared_service_subset_rows']+=1;return
        self.counts['declared_action_in_year_rows']+=1
        key=(label,action,when.month)
        self.rows[key]+=1;self.entities[key].add(source_uid)
        event=typed_event(event_id)
        if event is None:self.counts['declared_action_rows_without_usable_event_id']+=1
        else:
            self.events[key].add((source_uid,event))
            self.event_times[(label,source_uid,event)].add(when)

    def result(self):
        cohorts=[];pairs=[]
        for label,actions in ACTIONS.items():
            for action in actions:
                for month in range(1,13):
                    key=(label,action,month)
                    cohorts.append({'label':label,'action':action,'month':month,
                        'physical_edge_rows':self.rows[key],
                        'distinct_entity_event_id_keys':len(self.events[key]),
                        'distinct_entities':len(self.entities[key])})
                for month in range(1,12):
                    pairs.append({'kind':'same_action_adjacent_months','label':label,
                        'left_action':action,'right_action':action,'left_month':month,'right_month':month+1,
                        **pair_counts(self.entities[(label,action,month)],self.entities[(label,action,month+1)])})
            for month in range(1,13):
                for left,right in combinations(actions,2):
                    pairs.append({'kind':'same_month_different_actions','label':label,
                        'left_action':left,'right_action':right,'left_month':month,'right_month':month,
                        **pair_counts(self.entities[(label,left,month)],self.entities[(label,right,month)])})
        assert len(cohorts)==84 and len(pairs)==185
        summaries=[]
        for kind in ('same_action_adjacent_months','same_month_different_actions'):
            for label in ACTIONS:
                items=[p for p in pairs if p['kind']==kind and p['label']==label]
                summaries.append({'kind':kind,'label':label,'fixed_pairs':len(items),
                    'both_nonempty_pairs':sum(p['both_nonempty'] for p in items),
                    'any_nonempty_pairs':sum(p['any_nonempty'] for p in items),
                    'empty_both_pairs':sum(not p['any_nonempty'] for p in items),
                    'overlapping_pairs':sum(p['intersection_entities']>0 for p in items),
                    'disjoint_both_nonempty_pairs':sum(p['both_nonempty'] and p['intersection_entities']==0 for p in items),
                    'sum_overlap_entity_pair_incidents':sum(p['intersection_entities'] for p in items),
                    'maximum_overlap_entity_count_in_pair':max(p['intersection_entities'] for p in items)})
        return {'year':self.year,'counts':dict(self.counts),'source_event_id_types':dict(self.types),
                'distinct_entity_event_keys_with_multiple_observed_times':sum(len(ts)>1 for ts in self.event_times.values()),
                'cohorts':cohorts,'pairs':pairs,'pair_summary':summaries}
