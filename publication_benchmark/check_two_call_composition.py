"""Offline composition witness using unchanged services and existing tasks.

Not a new tested NL planner, new operation, or independent effect estimate.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys


def load(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def bag(rows):return sorted(json.dumps(x,sort_keys=True) for x in rows)


def check(a):
    root=Path(a.root).resolve();private=root.parent;projects=private.parent
    engine=Path(a.engine_root).resolve()
    output=Path(a.output)
    if output.exists():raise FileExistsError('Immutable composition witness')
    sys.path.insert(0,str(engine/'publication_benchmark'))
    import run_baselines as b
    import run_information_comparison as comparison
    import information_interface as info
    data={'redis':projects/'OSDB_CSTKG_Text2Cypher/data_real/d1_typed_projection_staging_v1_2/execution_csv',
        'duckdb':private/'duckdb_projection_publication_v1_20261021/execution_csv',
        'iotdb':root/'e8_iotdb_projection/execution_csv','elasticsearch':root/'e8_elasticsearch_projection/execution_csv'}
    receipts={'redis':private/'automated_dev_pilot_v1_20261021/neo4j_runtime/import_receipt.json',
        'duckdb':private/'publication_duckdb_runtime_v1_20261021/import_receipt.json',
        'iotdb':root/'e8_iotdb_runtime_v1/import_receipt.json','elasticsearch':root/'e8_elasticsearch_runtime_v1/import_receipt.json'}
    records=[]
    for project,port in [('redis',17475),('duckdb',17476),('iotdb',17477),('elasticsearch',17478)]:
        tasks=root/'e7_source_tasks_v1'/project
        manifest=load(tasks/'manifest.json');receipt=load(receipts[project])
        if {name:sha(data[project]/name) for name in ('nodes.csv','edges.csv')}!=manifest['source_hashes']:
            raise ValueError('Source changed')
        if receipt['source_hashes']!=manifest['source_hashes']:raise ValueError('Wrong import receipt')
        b.DB_URL=f'http://127.0.0.1:{port}/db/neo4j/tx/commit'
        if b.db('CALL db.info() YIELD id,name RETURN id,name')['data']!=receipt['database_identity']:
            raise ValueError('Wrong project database')
        spec=next(x for x in load(tasks/'specifications.json') if x['family']=='opening_actor_attribution')
        oracle=info.EventOracle(data[project]);calls=[];parts=[]
        for label,prefix in [('Issue','I_'),('PullRequest','PR_')]:
            call={'operation':'observed_events','arguments':{'entity_label':label,
                'entity_prefix':prefix+manifest['repo_id']+'#',
                'start_date':'2023-04-01','end_date':'2023-04-08',
                'events':[{'action':'OPENED_BY','explicit_decline':False}],
                'mode':'actor_counts','fields':[],'output_columns':['actor_id','count']}}
            # Same compiler, read guard, EXPLAIN, execution and500row gate as I.
            observation=comparison.observe({'prediction':{'status':'ANSWER',**call}},'I')
            execution=observation['execution']
            if execution['status']!='EXECUTED':raise ValueError('Composition call not legal under existing budget: '+project+' '+label)
            rows=execution['rows']
            if bag(rows)!=bag(oracle.expected(call['arguments'])):raise ValueError('Service/CSV disagreement')
            parts.append({actor:count for actor,count in rows})
            calls.append({'call':call,'returned_rows':execution['returned_rows'],'columns':execution['columns'],
                'execution_seconds':execution['seconds'],'execution_bytes':execution['bytes'],
                'explain_seconds':observation['check']['explain_seconds'],
                'explain_bytes':observation['check']['explain_bytes']})
        joined=[[actor,parts[0].get(actor,0),parts[1].get(actor,0)] for actor in sorted(set(parts[0])|set(parts[1]))]
        correct=bag(joined)==bag(spec['expected_rows'])
        if not correct:raise ValueError('Two-call composition mismatch: '+project)
        records.append({'project':project,'goal':spec['family'],'task_sha256':sha(tasks/'specifications.json'),
            'calls':calls,'composition':'full outer join on actor_id; fill absent count with0',
            'joined_rows':len(joined),'matches_independent_source_csv_gold':correct,
            'all_call_and_final_rows_within500':all(x['returned_rows']<=500 for x in calls) and len(joined)<=500,
            'database_identity':receipt['database_identity']})
    result={'mode':'OFFLINE_TWO_CALL_SERVICE_COMPOSITION_WITNESS_NOT_NL_PLANNER_TEST',
        'frozen_information_compiler_sha256':sha(Path(info.__file__)),'records':records,
        'new_model_calls':0,'new_operations':0,'legal_service_call_instances':8,
        'limits':'One pre-existing source goal, four project bindings; eight calls are not eight independent tasks. No NL planner generated these calls. This only demonstrates composability of the existing lower interface within observed row limits. Frozen single-call predictions/scores/tasks remain unchanged. Not an accuracy, latency or LLM-cost superiority result.'}
    with output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2)
    print(json.dumps({'projects_correct':sum(x['matches_independent_source_csv_gold'] for x in records),
        'calls':8,'model_calls':0,'rows':{x['project']:[c['returned_rows'] for c in x['calls']]+[x['joined_rows']] for x in records}}))


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ('root','engine-root','output'):p.add_argument('--'+key,required=True)
    check(p.parse_args())
