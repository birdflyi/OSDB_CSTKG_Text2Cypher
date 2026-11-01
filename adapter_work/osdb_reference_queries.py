"""Read-only Cypher references, independently expressed from raw CSV evaluation."""
import re


def query(contract, family):
    c = contract
    p = dict(lo=c['window']['start'], hi=c['window']['end'])
    lower = '>=' if c['window'].get('start_inclusive', True) else '>'
    upper = '<=' if c['window'].get('end_inclusive', True) else '<'
    def window(edge):
        return f'{edge}.event_time {lower} $lo AND {edge}.event_time {upper} $hi'
    def label(value):
        if not re.fullmatch('[A-Za-z][A-Za-z0-9_]*', value):
            raise ValueError('Invalid entity type')
        return value
    if family in ('single_entity_lookup','single_hop_neighbors','temporal_before_after'):
        p.update(anchor=c['anchor'], relation=c['relation'])
        q=f'MATCH (s)-[e]->(t:{label(c.get("target_type","Actor"))}) WHERE s.entity_id=$anchor AND e.raw_relation_label_repr=$relation'
        if family!='single_entity_lookup' or not c.get('snapshot_scoped'):
            q+=' AND '+window('e')
        if family=='temporal_before_after':
            p['boundary']=c['boundary']['value'];q+=' AND e.event_time > $boundary'
        column=c['output']['columns'][0]
        return q+f' RETURN DISTINCT t.entity_id AS {label(column)} ORDER BY {label(column)}',p
    if family=='multi_hop_path':
        p.update(anchor=c['anchor'],a=c['path'][0],b=c['path'][1])
        return (f'MATCH (s)-[a]->(m:{label(c["intermediate_type"])})-[b]->(t:{label(c["target_type"])}) '
                'WHERE s.entity_id=$anchor AND a.raw_relation_label_repr=$a AND b.raw_relation_label_repr=$b AND '
                +window('a')+' AND '+window('b')+' RETURN DISTINCT t.entity_id AS target_id ORDER BY target_id'),p
    if family=='branching_two_relations':
        p.update(anchor=c['anchor'],a=c['relations'][0],b=c['relations'][1])
        return ('MATCH (s)-[e]->(t) WHERE s.entity_id=$anchor AND '+window('e')+
                ' RETURN count(DISTINCT CASE WHEN e.raw_relation_label_repr=$a AND t.raw_entity_id <> "" THEN t.raw_entity_id END) AS rel_a_targets,'
                ' count(DISTINCT CASE WHEN e.raw_relation_label_repr=$b AND t.raw_entity_id <> "" THEN t.raw_entity_id END) AS rel_b_targets'),p
    if family in ('set_intersection','set_difference'):
        p.update(aa=c['set_a']['anchor'],ar=c['set_a']['relation'],ba=c['set_b']['anchor'],br=c['set_b']['relation'])
        existence='EXISTS' if family=='set_intersection' else 'NOT EXISTS'
        return ('MATCH (s)-[a]->(t) WHERE s.entity_id=$aa AND a.raw_relation_label_repr=$ar AND '+window('a')+
                f' AND {existence} {{ MATCH (u)-[b]->(v) WHERE u.entity_id=$ba AND b.raw_relation_label_repr=$br '
                'AND v.entity_id=t.entity_id AND '+window('b')+' } RETURN DISTINCT t.entity_id AS target_id ORDER BY target_id'),p
    if family=='aggregation_group':
        p.update(anchor=c['anchor'],relation=c['relation'])
        return ('MATCH (s)-[e]->() WHERE s.entity_id=$anchor AND e.raw_relation_label_repr=$relation AND '+window('e')+
                ' RETURN e.raw_event_type AS event_type,count(e) AS event_count ORDER BY event_type'),p
    if family=='top_k_order':
        p.update(relation=c['relation'],k=c['k'])
        return ('MATCH ()-[e]->(t:Actor) WHERE e.raw_relation_label_repr=$relation AND '+window('e')+
                ' RETURN t.entity_id AS actor_id,count(e) AS event_count ORDER BY event_count DESC,actor_id ASC LIMIT $k'),p
    if family=='absence_negation':
        if c['universe']!='all Issue nodes observed in the snapshot':raise ValueError('Unknown universe')
        p['relation']=c['relation']
        return ('MATCH (n:Issue) WHERE NOT EXISTS { MATCH (n)-[e]->() WHERE e.raw_relation_label_repr=$relation AND '+window('e')+
                ' } RETURN DISTINCT n.entity_id AS entity_id ORDER BY entity_id'),p
    if family=='event_sequence':
        p.update(a=c['event_a'],b=c['event_b'])
        return (f'MATCH (n:{label(c["entity_type"])})-[a]-(), (n)-[b]-() WHERE '
                'a.raw_event_type=$a AND b.raw_event_type=$b AND a.event_time < b.event_time AND '+window('a')+' AND '+window('b')+
                ' RETURN DISTINCT n.entity_id AS entity_id ORDER BY entity_id'),p
    if family=='duplicate_shared_identity':
        return (f'MATCH (n:{label(c["entity_type"])})-[e]-() WHERE '+window('e')+
                ' WITH n.entity_id AS entity_id,count(DISTINCT e.source_event_id) AS distinct_event_count'
                ' WHERE distinct_event_count >= 2 RETURN entity_id,distinct_event_count ORDER BY distinct_event_count DESC,entity_id ASC'),p
    if family=='zero_boundary':
        p.update(anchor=c['anchor'],relation=c['relation'])
        return ('MATCH ()-[u]->() WITH DISTINCT u.raw_event_type AS event_type'
                ' OPTIONAL MATCH (s)-[e]->() WHERE e.raw_event_type=event_type AND s.entity_id=$anchor'
                ' AND e.raw_relation_label_repr=$relation AND '+window('e')+
                ' RETURN event_type,count(e) AS match_count ORDER BY event_type'),p
    if family=='time_window_boundary':
        return ('MATCH ()-[e]->() WHERE e.event_time=$lo OR e.event_time=$hi'
                ' RETURN DISTINCT e.source_event_id AS event_id ORDER BY event_id'),p
    raise ValueError('No reference query for '+family)
