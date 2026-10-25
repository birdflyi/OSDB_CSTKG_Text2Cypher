"""Verify complete native reference receipts; never compute method accuracy."""
from pathlib import Path
from datetime import datetime
from collections import Counter
import argparse,hashlib,json

parser=argparse.ArgumentParser();parser.add_argument('--index',type=int,required=True)
parser.add_argument('--job-version',default='v1',choices=['v1','v2','v3']);args=parser.parse_args()
suffix='' if args.job_version=='v1' else '_'+args.job_version
base=Path('D:/ch7/reports/text2gql_native_reference_graph_'+str(args.index).zfill(2)+suffix+'_20261026')
dest=base/'evidence';read=lambda p:json.loads(p.read_text(encoding='utf-8'))
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
manifest=read(base/'evidence_manifest.json')
for item in manifest['files']:
    path=dest/item['path'];assert path.stat().st_size==item['bytes'] and sha(path)==item['sha256']
receipt=read(dest/'receipt.json');spec=read(dest/'spec.json')
assert receipt['phase']=='GRAPH_REFERENCE_AUDIT_FINISHED'
assert receipt['identity_pass'] and all(receipt['child_pids_absent'].values())
assert all(x['matches'] for x in receipt['data_checks']+receipt['metadata_checks'])
assert spec['formal_model_prediction_admitted'] is False
assert receipt['new_model_calls']==0
assert receipt['planned_references']==len(spec['references'])==receipt['completed_reference_receipts']
assert sha(dest/'spec.json')==receipt['source_spec_sha256']
local_spec=read(base/'spec.json');assert local_spec==spec
plan=read(Path('D:/ch7/reports/text2gql_native_engine_20261026/six_graph_input_plan.json'))
expected=plan['graphs'][args.index]
assert spec['graph']==expected['graph'] and spec['source_task_file_sha256']==expected['query_file_sha256']
assert [r['id'] for r in spec['references']]==expected['reference_id_order']
rows=[];statuses=Counter();issued=0
for index,task in enumerate(spec['references']):
    row=read(dest/'reference_records'/(str(index).zfill(5)+'.json'))
    assert row['index']==index and row['id']==task['id']
    assert row['query_sha256']==hashlib.sha256(task['query'].encode()).hexdigest()
    if row['status']!='NOT_DISPATCHED_READONLY_GATE':
        issued+=1
        response=dest/'responses'/('reference_'+str(index).zfill(5)+'.json')
        assert sha(response)==row['response_sha256']
        body=read(response)
        if row['status'].startswith('EXECUTED_'):
            assert row['http_status']==200 and row['size']==body['size']==len(body['result'])
    statuses[row['status']]+=1;rows.append(row)
assert dict(statuses)==receipt['reference_status_counts'] and issued==receipt['dispatched_references']
assert len(list((dest/'reference_records').glob('*.json')))==len(rows)
record={'kind':'ACCEPTED_NATIVE_REFERENCE_AUDIT_NOT_METHOD_RESULT',
    'time_local':datetime.now().astimezone().isoformat(),'graph':spec['graph'],'index':args.index,
    'job_version':args.job_version,'planned':len(rows),'dispatched':issued,'completed_receipts':len(rows),
    'status_counts':dict(statuses),'metadata_checks':len(receipt['metadata_checks']),
    'configured_data_checks':len(receipt['data_checks']),'native_parser_identity_pass':True,
    'all_correctly_rounded_decimal_equal':all(c['matches_correctly_rounded_decimal'] for c in receipt['data_checks']),
    'correctly_rounded_decimal_difference_rows':sum(c['decimal_rounding_difference_rows'] for c in receipt['data_checks']),
    'original_decimal_identity_differences_retained':True,'method_EX':None,
    'reference_correctness_against_independent_human_interpretation_established':False,
    'REST_Bolt_equivalence_established':False,'engine_fixture_semantics_fully_matched':False,
    'formal_model_prediction_admitted':False,'new_model_calls':0,
    'receipt_sha256':sha(dest/'receipt.json'),'manifest_sha256':sha(base/'evidence_manifest.json'),
    'worker_sha256':sha(dest/'worker.py'),'source_task_file_sha256':spec['source_task_file_sha256'],
    'query_response_hash_seal':hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
    'child_pids_absent':receipt['child_pids_absent']}
(base/'acceptance.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:record[k] for k in ['graph','planned','dispatched','status_counts','native_parser_identity_pass','correctly_rounded_decimal_difference_rows','method_EX']}))
