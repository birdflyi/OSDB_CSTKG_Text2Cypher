"""Independent raw-endpoint CSV reference functions for the S5 contract.

The caller verifies source hashes and chooses the declared source layer.
Counts of records count CSV relationship records unless distinct event_id is
explicitly specified. Graph projection equivalence is a separate check.
"""
from collections import Counter, defaultdict


def solve(rows, contract, family):
    rows = list(rows)
    w = contract['window']
    def inside(row):
        value = row['event_time']
        lower = value >= w['start'] if w.get('start_inclusive', True) else value > w['start']
        upper = value <= w['end'] if w.get('end_inclusive', True) else value < w['end']
        return bool(value) and lower and upper
    active = [r for r in rows if inside(r)]
    def targets(anchor, relation, data=active, target_type=None):
        return {r['tar_entity_id'] for r in data if r['src_entity_id']==anchor
                and r['relation_label_repr']==relation and r['tar_entity_id']
                and (target_type is None or r['tar_entity_type']==target_type)}
    def entities(row, kind):
        return {row[side+'_entity_id'] for side in ('src','tar')
                if row[side+'_entity_type']==kind and row[side+'_entity_id']}
    if family in ('single_entity_lookup','single_hop_neighbors'):
        # Single lookup's revised public question explicitly scopes the snapshot,
        # whereas the one-hop question explicitly supplies the time window.
        data = rows if family=='single_entity_lookup' and contract.get('snapshot_scoped') else active
        return sorted(targets(contract['anchor'],contract['relation'],data,contract['target_type']))
    if family=='multi_hop_path':
        a,b=contract['path']
        middle=targets(contract['anchor'],a,target_type=contract['intermediate_type'])
        return sorted({r['tar_entity_id'] for r in active if r['src_entity_id'] in middle
                       and r['src_entity_type']==contract['intermediate_type']
                       and r['relation_label_repr']==b and r['tar_entity_type']==contract['target_type']
                       and r['tar_entity_id']})
    if family=='branching_two_relations':
        return [[len(targets(contract['anchor'],rel)) for rel in contract['relations']]]
    if family in ('set_intersection','set_difference'):
        a=targets(**contract['set_a']);b=targets(**contract['set_b'])
        return sorted(a & b if family=='set_intersection' else a-b)
    if family=='aggregation_group':
        counts=Counter(r['event_type'] for r in active if r['src_entity_id']==contract['anchor']
                       and r['relation_label_repr']==contract['relation'])
        return [[k,counts[k]] for k in sorted(counts)]
    if family=='top_k_order':
        counts=Counter(r['tar_entity_id'] for r in active if r['relation_label_repr']==contract['relation']
                       and r['tar_entity_type']=='Actor' and r['tar_entity_id'])
        return [list(pair) for pair in sorted(counts.items(),key=lambda x:(-x[1],x[0]))[:contract['k']]]
    if family=='absence_negation':
        if contract['universe']!='all Issue nodes observed in the snapshot':
            raise ValueError('Unsupported explicit universe; do not guess entity type')
        universe=set().union(*(entities(r,'Issue') for r in rows))
        linked={r['src_entity_id'] for r in active if r['src_entity_type']=='Issue'
                and r['relation_label_repr']==contract['relation']}
        return sorted(universe-linked)
    if family=='temporal_before_after':
        return sorted(targets(contract['anchor'],contract['relation'],
                      [r for r in active if r['event_time']>contract['boundary']['value']], 'Actor'))
    if family in ('event_sequence','duplicate_shared_identity'):
        seen=defaultdict(set)
        for row in active:
            for entity in entities(row,contract['entity_type']):
                seen[entity].add((row['event_time'],row['event_type'],row['event_id']))
        if family=='duplicate_shared_identity':
            counts={k:len({e[2] for e in v}) for k,v in seen.items()}
            return sorted([[k,v] for k,v in counts.items() if v>=2],key=lambda x:(-x[1],x[0]))
        answer=[]
        for entity,events in seen.items():
            a=[t for t,e,_ in events if e==contract['event_a']]
            b=[t for t,e,_ in events if e==contract['event_b']]
            if a and b and min(a)<max(b):answer.append(entity)
        return sorted(answer)
    if family=='zero_boundary':
        # Missing anchor is a real contract gap, never an always-false filter.
        anchor=contract['anchor']
        counts=Counter(r['event_type'] for r in active if r['src_entity_id']==anchor
                       and r['relation_label_repr']==contract['relation'])
        return [[t,counts[t]] for t in sorted({r['event_type'] for r in rows})]
    if family=='time_window_boundary':
        return sorted({r['event_id'] for r in rows if r['event_time'] in (w['start'],w['end'])})
    raise ValueError('No reference implementation for '+family)
