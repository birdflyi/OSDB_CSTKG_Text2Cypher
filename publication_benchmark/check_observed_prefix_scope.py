"""Post-hoc witness for a delimiter omitted by an already returned query.

Use only the owned synthetic DB and roll back temporary witness records.
No model call, request change, or regrading of the original snapshot result.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse
import requests
import source_snapshot_references as references


def load(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check(a):
    output=Path(a.output)
    if output.exists():raise FileExistsError('Immutable witness receipt')
    record=load(a.prediction)
    if record['id']!='redis_SOURCE_branch_events' or record['arm']!='G1' or not record['grade']['correct']:
        raise ValueError('Unexpected original observed query')
    spec=next(x for x in load(a.specifications) if x['id']==record['id'])
    prediction=record['prediction']
    expected_prefix=spec['reference_parameters']['branch_prefix']
    if expected_prefix!='B_156018:' or prediction['parameters'].get('branch_prefix')!='B_156018':
        raise ValueError('Not the declared omitted-delimiter case')
    receipt=load(a.fixture_receipt)
    base='http://127.0.0.1:17479/db/neo4j'
    session=requests.Session();session.trust_env=False
    def call(url,statements):
        response=session.post(url,json={'statements':statements},timeout=(3,30))
        response.raise_for_status();body=response.json()
        if body.get('errors'):raise ValueError(body['errors'])
        return response,body
    _,identity=call(base+'/tx/commit',[{'statement':'CALL db.info() YIELD id,name RETURN id,name'}])
    if identity['results'][0]['data']!=receipt['database_identity']:raise ValueError('Wrong synthetic DB')
    count_query='MATCH (n) OPTIONAL MATCH ()-[r]->() RETURN count(DISTINCT n) AS nodes,count(DISTINCT r) AS edges'
    _,before=call(base+'/tx/commit',[{'statement':count_query}])
    tx_url=None
    try:
        response,created=call(base+'/tx',[{'statement':
            "CREATE (a:Actor {entity_id:'A_15601899'}),(b:Branch {entity_id:'B_156018:main'}),(c:Branch {entity_id:'B_1560189:foreign'}) "
            "CREATE (b)-[:EVENT_ACTION {service_rel_type:'CREATED_BY',source_event_time:'2023-04-01T00:00:00Z'}]->(a),"
            "(c)-[:EVENT_ACTION {service_rel_type:'CREATED_BY',source_event_time:'2023-04-01T00:00:00Z'}]->(a)"}])
        location=urlparse(response.headers['Location'])
        if (location.scheme!='http' or location.hostname not in ('localhost','127.0.0.1')
            or location.port!=17479 or not re.fullmatch(r'/db/neo4j/tx/\d+',location.path)):
            raise ValueError('Unexpected transaction location')
        # Normalize to the verified loopback endpoint; never follow arbitrary URLs.
        tx_url='http://127.0.0.1:17479'+location.path
        gold=references.queries('156018')['branch_events']
        _,body=call(tx_url,[{'statement':prediction['cypher'],'parameters':prediction['parameters']},
            {'statement':gold['query'],'parameters':gold['parameters']}])
        actual=[[x['row'] for x in result['data']] for result in body['results']]
        if actual!=[[[2,0]],[[1,0]]]:raise ValueError('Unexpected witness result')
    finally:
        if tx_url:
            response=session.delete(tx_url,timeout=(3,30));response.raise_for_status()
    _,after=call(base+'/tx/commit',[{'statement':count_query}])
    if before['results'][0]['data']!=after['results'][0]['data']:raise ValueError('Fixture not restored after rollback')
    result={'mode':'POSTHOC_OBSERVED_QUERY_SCOPE_WITNESS_NOT_INDEPENDENT_EFFECT',
        'prediction_record_sha256':sha(a.prediction),'task_sha256':sha(a.specifications),
        'original_snapshot_grade_correct':True,'requested_prefix':expected_prefix,
        'predicted_prefix':prediction['parameters']['branch_prefix'],
        'temporary_synthetic_branches':['B_156018:main','B_1560189:foreign'],
        'observed_query_rows':actual[0],'literal_contract_rows':actual[1],
        'semantic_scope_failure_witnessed':True,'temporary_transaction_rolled_back':True,
        'before_after_counts':after['results'][0]['data'],'fixture_database_identity':receipt['database_identity'],
        'limits':'Constructed after inspecting one returned direct query; not prospective error-rate evidence. No model rerun or original task/score change. Service catalogs cannot answer this task, so this is not a service win.'}
    with output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2)
    print(json.dumps({'predicted_rows':actual[0],'contract_rows':actual[1],'rolled_back':True}))


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for k in ('prediction','specifications','fixture-receipt','output'):p.add_argument('--'+k,required=True)
    check(p.parse_args())
