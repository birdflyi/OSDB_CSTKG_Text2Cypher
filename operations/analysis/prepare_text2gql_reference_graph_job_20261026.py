"""Prepare one fixed graph of the previously sealed six-graph native audit."""
from pathlib import Path
from datetime import datetime
import argparse,base64,hashlib,json,re,subprocess
from audit_text2gql_query_features_20261025 import tokens

parser=argparse.ArgumentParser();parser.add_argument('--index',type=int,choices=range(6),required=True)
parser.add_argument('--job-version',choices=['v1','v2','v3'],default='v1');args=parser.parse_args()
BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
plan=json.loads((BASE/'six_graph_input_plan.json').read_text(encoding='utf-8'))
assert plan['graph_input_seal_sha256']=='6f2040bf0f3d758110cc17bc371dd9a5417aa82fdc273a959f43daff73b23a30'
graph=plan['graphs'][args.index];name=graph['graph']
SOURCE=Path('D:/ch7/literature/public_candidate_metadata_20261024/Text2GQL-Bench/selected_zip_assets_20261024/dataset/test')/name/'Cypher'
config=SOURCE/'TuGraph-DB_Instance/import_config.json';assert hashlib.sha256(config.read_bytes()).hexdigest()==graph['config_sha256']
query_file=next(SOURCE.glob('*_cypher.json'));assert hashlib.sha256(query_file.read_bytes()).hexdigest()==graph['query_file_sha256']
rows=json.loads(query_file.read_text(encoding='utf-8'))
assert [str(r['id']) for r in rows]==graph['reference_id_order']
cfg=json.loads(config.read_text(encoding='utf-8'));files={'import_config.json':config.read_bytes()}
for item in cfg['files']:
    raw=(config.parent/item['path']).read_bytes()
    original=next(x for x in graph['graph_data_assets'] if x['member'].rsplit('/',1)[-1]==item['path'])
    assert hashlib.sha256(raw).hexdigest()==original['sha256'];files[item['path']]=raw
def readonly_gate(query):
    ts=tokens(query)
    dangerous={'CREATE','MERGE','SET','REMOVE','DELETE','DETACH','DROP','LOAD','INSERT','UPDATE','ALTER','CALL'}
    return not any(kind=='word' and value.upper() in dangerous and (i==0 or ts[i-1][1] not in ('.',':','$'))
                   for i,(kind,value) in enumerate(ts))
spec={'kind':'SEALED_NATIVE_REFERENCE_AUDIT_NOT_MODEL_PREDICTION','time_local':datetime.now().astimezone().isoformat(),
 'graph':name,'index':args.index,'database_alias':'tgq_'+str(args.index).zfill(2),'http_port':18870,
 'engine_version':'4.5.2','engine_profile':'official package default is_cypher_v2=true, fixed before this reference run',
 'source_plan_sha256':hashlib.sha256((BASE/'six_graph_input_plan.json').read_bytes()).hexdigest(),
 'source_files':{n:{'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in files.items()},
 'source_task_file_sha256':graph['query_file_sha256'],
 'references':[{'id':str(r['id']),'query':r['initial_gql'],'readonly_lexical_gate':readonly_gate(r['initial_gql'])} for r in rows],
 'references_timeout_seconds':60,'response_cap_bytes':64*1024**2,
 'null_and_date_transport_policy':'CSV empty optional values -> None; typed schema canonicalization, no answer tolerance changes.',
 'model_calls':0,'formal_model_prediction_admitted':False,
 'REST_vs_Bolt_answer_representation_equivalence_established':False,
 'fixture_semantic_differences_resolved':False}
if args.job_version in ['v2','v3']:spec['existing_import_job']='graph_'+str(args.index).zfill(2)+'_native_v1'
spec['numeric_identity_policy']='Exact pinned native parser, with correctly-rounded decimal comparison retained separately; no gold scoring tolerance.'
spec['native_numeric_parser_sha256']=hashlib.sha256((BASE/'upstream/include/fma-common/text_parser.h').read_bytes()).hexdigest()
suffix='' if args.job_version=='v1' else '_'+args.job_version
out=Path('D:/ch7/reports/text2gql_native_reference_graph_'+str(args.index).zfill(2)+suffix+'_20261026');out.mkdir(exist_ok=True)
assert not (out/'spec.json').exists(),'Existing preparation needs audit, not automatic replay'
(out/'spec.json').write_text(json.dumps(spec,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
worker=Path('D:/ch7/reports/text2gql_reference_graph_worker_20261026.py').read_bytes()
(out/'worker.py').write_bytes(worker)
doc=(BASE/'upstream/docs/en-US/source/7.client-tools/5.bolt-client.md').read_text(encoding='utf-8')
m=re.search(r'AuthTokens.basic\("([^"\n]+)",\s*"([^"\n]+)"\)',doc);assert m
bundle={name:base64.b64encode(raw).decode() for name,raw in files.items()}
others={'worker.py':base64.b64encode(worker).decode(),
 'spec.json':base64.b64encode(json.dumps(spec,ensure_ascii=False).encode()).decode(),
 '.client-private.json':base64.b64encode(json.dumps({'user':m[1],'password':m[2]}).encode()).decode()}
code=r'''
from pathlib import Path
import json,base64,platform,os,subprocess
root=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']
assert not (root/'.public-runtime.lock').exists()
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  args=(p/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
  assert not (str(root) in args and any(z in args for z in ['/bin/lgraph_server','/bin/lgraph_import','text2gql_reference_graph_worker'])),'Existing engine/audit still active'
 except (FileNotFoundError,PermissionError):pass
base=root/'text2gql_reference_audit_20261026';assert base.is_dir()
job=base/JOBNAME;assert not job.exists(),'Do not replay existing graph job'
job.mkdir(mode=0o700);(job/'inputs').mkdir()
for name,encoded in INPUTS.items():
 path=job/'inputs'/name;assert path.parent==job/'inputs'
 with path.open('xb') as f:f.write(base64.b64decode(encoded))
for name,encoded in OTHERS.items():
 path=job/name;assert path.parent==job
 fd=os.open(str(path),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 with os.fdopen(fd,'wb') as f:f.write(base64.b64decode(encoded))
run=subprocess.run(['python3',str(job/'worker.py'),str(job)],capture_output=True,text=True,timeout=3000)
print(json.dumps({'returncode':run.returncode,'stdout':run.stdout,'stderr':run.stderr[:500]}))
'''.replace('JOBNAME',repr('graph_'+str(args.index).zfill(2)+'_native_'+args.job_version),1).replace('INPUTS',repr(bundle),1).replace('OTHERS',repr(others),1)
# The generated wrapper includes a private credential: send on stdin, do not save it.
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key','-p','2491',
 '-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
 input=code,capture_output=True,text=True,timeout=3030)
result={'ssh_returncode':run.returncode,'stdout':run.stdout,'stderr':run.stderr[:500]}
(out/'client_result.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result,ensure_ascii=True))
