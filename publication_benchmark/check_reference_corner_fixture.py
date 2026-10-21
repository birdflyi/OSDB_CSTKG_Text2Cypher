"""Real query execution on owned tiny synthetic graph; no model dispatch."""
import argparse
import json
from pathlib import Path
import requests
import grounded_references as g
import source_snapshot_references as s


def bag(rows):return sorted(json.dumps(row,sort_keys=True) for row in rows)


def check(a):
    root=Path(a.fixture);out=Path(a.output)
    if out.exists():raise FileExistsError('Immutable check receipt')
    manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    receipt=json.loads(Path(a.receipt).read_text(encoding='utf-8'))
    session=requests.Session();session.trust_env=False
    def execute(query,parameters):
        response=session.post('http://127.0.0.1:17479/db/neo4j/tx/commit',json={'statements':[
            {'statement':query,'parameters':parameters,'resultDataContents':['row']}]},timeout=(3,30))
        response.raise_for_status();body=response.json()
        if body.get('errors'):raise ValueError(body['errors'])
        return [x['row'] for x in body['results'][0]['data']]
    identity=execute('CALL db.info() YIELD id,name RETURN id,name',{})
    if identity!=[x['row'] for x in receipt['database_identity']]:raise ValueError('Wrong owned fixture DB')
    records=[];mutants=[]
    for world in manifest['worlds']:
        queries={**s.queries(world['repo_id']),**g.queries(world['binding'])}
        gold={**world['manual_source'],**world['manual_grounded']}
        for goal,q in queries.items():
            rows=execute(q['query'],q['parameters'])
            records.append({'world':world['repo_id'],'goal':goal,'matches_manual_and_csv':bag(rows)==bag(gold[goal])})
        mutations={
            'branch_events':('count(DISTINCT CASE','count(CASE','count_records_instead_of_branches'),
            'pushed_commits':('count(DISTINCT c.entity_id)','count(c.entity_id)','count_records_instead_of_commits'),
            'reviewed_prs':('count(DISTINCT p.entity_id)','count(p.entity_id)','count_reviews_instead_of_prs'),
            'opening_actor_attribution':('count(DISTINCT CASE','count(CASE','count_opening_records_instead_of_entities'),
            'pr_observed_duration':('min(r.source_event_time) AS closed','max(r.source_event_time) AS closed','last_terminal_instead_of_first'),
            'issue_observed_duration':('min(r.source_event_time) AS closed','max(r.source_event_time) AS closed','last_close_instead_of_first'),
            'issue_observed_response':('NOT responder.entity_id IN opening_actors','true','include_self_response'),
            'issues_observed_active':('direct OR commented','direct','drop_comment_only_activity'),
            g.IDS[0]:('count(r)','count(DISTINCT endNode(r).entity_id)','distinct_targets_instead_of_reference_records'),
            g.IDS[1]:(')-[r:EVENT_ACTION]->()',')<-[r:EVENT_ACTION]-()','reverse_literal_actor_direction'),
            g.IDS[2]:('min(r.source_event_time)','max(r.source_event_time)','last_comment_time_instead_of_first'),
            g.IDS[3]:('min(r.source_event_time)','max(r.source_event_time)','last_push_reference_time_instead_of_first')}
        for goal,(old,new,name) in mutations.items():
            query=queries[goal]
            if old not in query['query']:raise ValueError('Mutation not applied: '+name)
            mutant=query['query'].replace(old,new)
            rows=execute(mutant,query['parameters'])
            mutants.append({'world':world['repo_id'],'goal':goal,'mutation':name,'detected':bag(rows)!=bag(gold[goal])})
        goal='pushed_commits';q=queries[goal]
        rows=execute(q['query'].replace(' AND r.source_event_time < $end',''),q['parameters'])
        mutants.append({'world':world['repo_id'],'goal':goal,'mutation':'drop_exclusive_upper_time_bound',
            'detected':bag(rows)!=bag(gold[goal])})
    result={'mode':'REFERENCE_CORNER_CASE_EXECUTION_NOT_MODEL_EFFECT','reference_checks':records,
        'mutant_checks':mutants,'manual_csv_goal_world_checks':manifest['manual_csv_goal_world_checks'],
        'source_hashes':manifest['source_hashes'],'database_identity':receipt['database_identity'],
        'limits':'24 goal-world reference checks and26 declared mutant instances over13 mutation definitions. Not new model predictions, independent useful tasks or a proof of general query equivalence.'}
    with out.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2)
    print(json.dumps({'references_pass':sum(r['matches_manual_and_csv'] for r in records),'references':len(records),
        'mutants_detected':sum(m['detected'] for m in mutants),'mutants':len(mutants)}))


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for k in ('fixture','receipt','output'):p.add_argument('--'+k,required=True)
    check(p.parse_args())
