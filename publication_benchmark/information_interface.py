"""Minimal observed-event interface with explicit filters before aggregation.

The six legacy services are byte-for-byte unchanged. One additional generic
event relation supports fact projection and deterministic aggregation, not
full repository history, current state, bug causality or arbitrary Cypher.
"""
from collections import Counter
from datetime import date
import csv
import json
from pathlib import Path
import re

import mechanism_core as legacy


EVENT_CONTRACT = {
    'operation':'observed_events',
    'arguments':{
        'entity_label':'Issue|PullRequest',
        'entity_prefix':'literal entity_id prefix, beginning I_ or PR_ for the chosen label',
        'start_date':'YYYY-MM-DD inclusive UTC', 'end_date':'YYYY-MM-DD exclusive UTC',
        'events':'array of 1 or 2 objects with exactly action and explicit_decline; action OPENED_BY|CLOSED_BY|REOPENED_BY|MERGED_BY (MERGED_BY only for PullRequest); explicit_decline boolean, true only for PullRequest CLOSED_BY',
        'mode':'records|entity_ids|actor_counts|entity_count|entity_count_ratio',
        'fields':'for records, ordered nonempty subset of entity_id,actor_id,event_time,edge_id,action; otherwise []',
        'output_columns':'requested output names in their requested order: records one per field; entity_ids one; actor_counts actor ID then count; entity_count one; ratio numerator count then denominator count then ratio'},
    'meaning':'Read observed entity -> EVENT_ACTION -> Actor event records in the half-open window and literal identifier scope. OPENED_BY excludes reopening. Explicit decline additionally requires raw_event_trigger CONTAINS pull_merged=False; never infer decline from missing MERGED_BY. records retains duplicates, entity_ids and entity_count use distinct entity IDs; actor_counts counts distinct entities per actor. entity_count_ratio requires exactly two event filters and returns distinct entity counts for the first and second, then their ratio rounded to 6 decimals, null if denominator zero. All other modes require one filter. These are observed event facts, not complete GitHub activity or current lifecycle state. Apply time/trigger filters before reducing.',
    'columns':'output_columns',
}
CATALOG=legacy.CATALOG+[EVENT_CONTRACT]
FIELD_EXPR={'entity_id':'n.entity_id','actor_id':'a.entity_id','event_time':'r.source_event_time',
            'edge_id':'r.edge_uid','action':'r.service_rel_type'}


def compile_call(call):
    if isinstance(call,dict) and call.get('operation')!='observed_events':
        return legacy.compile_call(call)
    if not isinstance(call,dict) or set(call)!={'operation','arguments'}:
        raise ValueError('Expected operation and arguments')
    args=call['arguments']
    if not isinstance(args,dict) or set(args)!=set(EVENT_CONTRACT['arguments']):
        raise ValueError('Missing or extra observed_events arguments')
    label=args['entity_label']
    if label not in ('Issue','PullRequest'):
        raise ValueError('Unsupported source label')
    prefix=args['entity_prefix']
    if not isinstance(prefix,str) or not prefix.startswith('I_' if label=='Issue' else 'PR_') or len(prefix)>200:
        raise ValueError('Invalid literal entity scope')
    for name in ('start_date','end_date'):
        value=args[name]
        if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):
            raise ValueError('Invalid date')
        date.fromisoformat(value)
    if args['start_date']>=args['end_date']:
        raise ValueError('Invalid half-open window')
    mode=args['mode']
    sizes={'records':None,'entity_ids':1,'actor_counts':2,'entity_count':1,'entity_count_ratio':3}
    if mode not in sizes:
        raise ValueError('Unsupported reduction')
    events=args['events']
    if not isinstance(events,list) or len(events)!=(2 if mode=='entity_count_ratio' else 1):
        raise ValueError('Wrong number of event filters')
    params={k:args[k] for k in ('entity_prefix','start_date','end_date')}
    params['start_date']+='T00:00:00Z'
    params['end_date']+='T00:00:00Z'
    predicates=[]
    for index,event in enumerate(events):
        if not isinstance(event,dict) or set(event)!={'action','explicit_decline'}:
            raise ValueError('Expected action and explicit_decline')
        action=event['action']
        if action not in ('OPENED_BY','CLOSED_BY','REOPENED_BY','MERGED_BY') or (action=='MERGED_BY' and label!='PullRequest'):
            raise ValueError('Unsupported event action for entity')
        decline=event['explicit_decline']
        if type(decline) is not bool or (decline and (label!='PullRequest' or action!='CLOSED_BY')):
            raise ValueError('Explicit decline is only a PR close predicate')
        params['action'+str(index)]=action
        predicate='r.service_rel_type=$action'+str(index)
        if decline:
            predicate+=" AND r.raw_event_trigger CONTAINS 'pull_merged=False'"
        predicates.append('('+predicate+')')
    fields=args['fields']
    if not isinstance(fields,list) or (mode=='records' and (not fields or any(not isinstance(f,str) or f not in FIELD_EXPR for f in fields) or len(set(fields))!=len(fields))) or (mode!='records' and fields):
        raise ValueError('Invalid event projection')
    columns=args['output_columns']
    count=len(fields) if mode=='records' else sizes[mode]
    if not isinstance(columns,list) or len(columns)!=count or any(not isinstance(c,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,47}',c) for c in columns) or len(set(columns))!=len(columns):
        raise ValueError('Invalid output column aliases')
    aliases=['`'+c+'`' for c in columns]
    query=f'MATCH (n:{label})-[r:EVENT_ACTION]->(a:Actor) WHERE n.entity_id STARTS WITH $entity_prefix AND r.source_event_time >= $start_date AND r.source_event_time < $end_date AND ('+' OR '.join(predicates)+')'
    if mode=='records':
        query+=' RETURN '+','.join(FIELD_EXPR[f]+' AS '+alias for f,alias in zip(fields,aliases))
    elif mode=='entity_ids':
        query+=' RETURN DISTINCT n.entity_id AS '+aliases[0]
    elif mode=='actor_counts':
        query+=' RETURN a.entity_id AS '+aliases[0]+',count(DISTINCT n.entity_id) AS '+aliases[1]
    elif mode=='entity_count':
        query+=' RETURN count(DISTINCT n.entity_id) AS '+aliases[0]
    else:
        query+=' WITH count(DISTINCT CASE WHEN '+predicates[0]+' THEN n.entity_id END) AS numerator,count(DISTINCT CASE WHEN '+predicates[1]+' THEN n.entity_id END) AS denominator RETURN numerator AS '+aliases[0]+',denominator AS '+aliases[1]+',CASE WHEN denominator=0 THEN null ELSE round(1.0*numerator/denominator,6) END AS '+aliases[2]
    return {'query':query,'parameters':params,'columns':columns}


class EventOracle:
    """CSV-only reference, independent of the compiler/query strings."""
    def __init__(self,data):
        root=Path(data)
        with (root/'nodes.csv').open(encoding='utf-8-sig',newline='') as f:
            nodes={x['node_uid']:(x['label'],x['entity_id']) for x in csv.DictReader(f)}
        self.events=[]
        with (root/'edges.csv').open(encoding='utf-8-sig',newline='') as f:
            for row in csv.DictReader(f):
                source,target=nodes[row['source_node_uid']],nodes[row['target_node_uid']]
                if source[0] not in ('Issue','PullRequest') or target[0]!='Actor' or row['native_rel_type']!='EVENT_ACTION':
                    continue
                props=json.loads(row['properties_json'])
                self.events.append({'label':source[0],'entity_id':source[1],'actor_id':target[1],
                    'event_time':props.get('source_event_time'),'edge_id':props['edge_uid'],
                    'action':row['service_rel_type'],'trigger':props.get('raw_event_trigger','')})

    def expected(self,args):
        lo,hi=args['start_date']+'T00:00:00Z',args['end_date']+'T00:00:00Z'
        scoped=[x for x in self.events if x['label']==args['entity_label'] and x['entity_id'].startswith(args['entity_prefix']) and x['event_time'] and lo<=x['event_time']<hi]
        subsets=[]
        for event in args['events']:
            subsets.append([x for x in scoped if x['action']==event['action'] and (not event['explicit_decline'] or 'pull_merged=False' in x['trigger'])])
        if args['mode']=='entity_count_ratio':
            numerator,denominator=(len({x['entity_id'] for x in items}) for items in subsets)
            return [[numerator,denominator,round(numerator/denominator,6) if denominator else None]]
        rows=subsets[0]
        mode=args['mode']
        if mode=='entity_count':
            return [[len({x['entity_id'] for x in rows})]]
        if mode=='entity_ids':
            return [[x] for x in sorted({x['entity_id'] for x in rows})]
        if mode=='records':
            return [[x[f] for f in args['fields']] for x in rows]
        if mode=='actor_counts':
            pairs={(x['actor_id'],x['entity_id']) for x in rows}
            return [[actor,n] for actor,n in sorted(Counter(x[0] for x in pairs).items())]
        raise ValueError(mode)
