"""Independent references over the public, complete CSV record layer."""
import re


def query(c, family):
    p=dict(lo=c['window']['start'],hi=c['window']['end'])
    def window(v):
        lo='>=' if c['window'].get('start_inclusive',True) else '>'
        hi='<=' if c['window'].get('end_inclusive',True) else '<'
        return f'{v}.event_time {lo} $lo AND {v}.event_time {hi} $hi'
    def column(index=0):
        value=c['output']['columns'][index]
        if not re.fullmatch('[A-Za-z][A-Za-z0-9_]*',value):raise ValueError('Invalid output column')
        return value
    if family in ('single_entity_lookup','single_hop_neighbors','temporal_before_after'):
        p.update(anchor=c['anchor'],relation=c['relation'],kind=c.get('target_type','Actor'))
        q='MATCH (r:OSDBRawRecord) WHERE r.src_entity_id=$anchor AND r.relation_label_repr=$relation AND r.tar_entity_type=$kind AND r.tar_entity_id <> ""'
        if family!='single_entity_lookup' or not c.get('snapshot_scoped'):q+=' AND '+window('r')
        if family=='temporal_before_after':
            p['boundary']=c['boundary']['value'];q+=' AND r.event_time > $boundary'
        return q+f' RETURN DISTINCT r.tar_entity_id AS {column()} ORDER BY {column()}',p
    if family=='multi_hop_path':
        p.update(anchor=c['anchor'],a=c['path'][0],b=c['path'][1],middle=c['intermediate_type'],target=c['target_type'])
        return ('MATCH (a:OSDBRawRecord),(b:OSDBRawRecord) WHERE a.src_entity_id=$anchor AND a.relation_label_repr=$a '
                'AND a.tar_entity_type=$middle AND a.tar_entity_id <> "" AND b.src_entity_type=$middle '
                'AND b.src_entity_id=a.tar_entity_id AND b.relation_label_repr=$b AND b.tar_entity_type=$target '
                'AND b.tar_entity_id <> "" AND '+window('a')+' AND '+window('b')+
                ' RETURN DISTINCT b.tar_entity_id AS target_id ORDER BY target_id'),p
    if family=='branching_two_relations':
        p.update(anchor=c['anchor'],a=c['relations'][0],b=c['relations'][1])
        return ('MATCH (r:OSDBRawRecord) WHERE r.src_entity_id=$anchor AND '+window('r')+
                ' RETURN count(DISTINCT CASE WHEN r.relation_label_repr=$a AND r.tar_entity_id <> "" THEN r.tar_entity_id END) AS rel_a_targets,'
                'count(DISTINCT CASE WHEN r.relation_label_repr=$b AND r.tar_entity_id <> "" THEN r.tar_entity_id END) AS rel_b_targets'),p
    if family in ('set_intersection','set_difference'):
        p.update(aa=c['set_a']['anchor'],ar=c['set_a']['relation'],ba=c['set_b']['anchor'],br=c['set_b']['relation'])
        exists='EXISTS' if family=='set_intersection' else 'NOT EXISTS'
        return ('MATCH (a:OSDBRawRecord) WHERE a.src_entity_id=$aa AND a.relation_label_repr=$ar AND a.tar_entity_id <> "" AND '+window('a')+
                f' AND {exists} {{ MATCH (b:OSDBRawRecord) WHERE b.src_entity_id=$ba AND b.relation_label_repr=$br '
                'AND b.tar_entity_id=a.tar_entity_id AND '+window('b')+' } RETURN DISTINCT a.tar_entity_id AS target_id ORDER BY target_id'),p
    if family=='aggregation_group':
        p.update(anchor=c['anchor'],relation=c['relation'])
        return ('MATCH (r:OSDBRawRecord) WHERE r.src_entity_id=$anchor AND r.relation_label_repr=$relation AND '+window('r')+
                ' RETURN r.event_type AS event_type,count(r) AS event_count ORDER BY event_type'),p
    if family=='top_k_order':
        p.update(relation=c['relation'],k=c['k'])
        return ('MATCH (r:OSDBRawRecord) WHERE r.relation_label_repr=$relation AND r.tar_entity_type="Actor" '
                'AND r.tar_entity_id <> "" AND '+window('r')+' RETURN r.tar_entity_id AS actor_id,count(r) AS event_count '
                'ORDER BY event_count DESC,actor_id ASC LIMIT $k'),p
    if family=='absence_negation':
        if c['universe']!='all Issue nodes observed in the snapshot':raise ValueError('Unknown universe')
        p['relation']=c['relation']
        return ('MATCH (r:OSDBRawRecord) UNWIND [CASE WHEN r.src_entity_type="Issue" THEN r.src_entity_id END,'
                'CASE WHEN r.tar_entity_type="Issue" THEN r.tar_entity_id END] AS entity_id WITH DISTINCT entity_id '
                'WHERE entity_id IS NOT NULL AND entity_id <> "" AND NOT EXISTS { MATCH (e:OSDBRawRecord) '
                'WHERE e.src_entity_type="Issue" AND e.src_entity_id=entity_id AND e.relation_label_repr=$relation AND '+window('e')+
                ' } RETURN entity_id ORDER BY entity_id'),p
    if family in ('event_sequence','duplicate_shared_identity'):
        p['kind']=c['entity_type']
        q=('MATCH (r:OSDBRawRecord) WHERE '+window('r')+' UNWIND [CASE WHEN r.src_entity_type=$kind THEN r.src_entity_id END,'
           'CASE WHEN r.tar_entity_type=$kind THEN r.tar_entity_id END] AS entity_id WITH entity_id,r '
           'WHERE entity_id IS NOT NULL AND entity_id <> "" ')
        if family=='duplicate_shared_identity':
            return q+('WITH entity_id,count(DISTINCT r.event_id) AS distinct_event_count WHERE distinct_event_count >= 2 '
                      'RETURN entity_id,distinct_event_count ORDER BY distinct_event_count DESC,entity_id ASC'),p
        p.update(a=c['event_a'],b=c['event_b'])
        return q+('WITH entity_id,min(CASE WHEN r.event_type=$a THEN r.event_time END) AS first_a,'
                  'max(CASE WHEN r.event_type=$b THEN r.event_time END) AS last_b '
                  'WHERE first_a < last_b RETURN entity_id ORDER BY entity_id'),p
    if family=='zero_boundary':
        p.update(anchor=c['anchor'],relation=c['relation'])
        return ('MATCH (u:OSDBRawRecord) WITH DISTINCT u.event_type AS event_type OPTIONAL MATCH (r:OSDBRawRecord) '
                'WHERE r.event_type=event_type AND r.src_entity_id=$anchor AND r.relation_label_repr=$relation AND '+window('r')+
                ' RETURN event_type,count(r) AS match_count ORDER BY event_type'),p
    if family=='time_window_boundary':
        return ('MATCH (r:OSDBRawRecord) WHERE r.event_time=$lo OR r.event_time=$hi '
                'RETURN DISTINCT r.event_id AS event_id ORDER BY event_id'),p
    raise ValueError('Unknown reference family '+family)
