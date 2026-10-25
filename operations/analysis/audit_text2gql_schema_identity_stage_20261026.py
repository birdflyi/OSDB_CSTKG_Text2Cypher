"""Static six-schema bridge and independent measured composition acceptance."""
from pathlib import Path
from datetime import datetime
import hashlib,json,subprocess,sys

ROOT=Path('D:/ch7');WORK=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
SOURCE=WORK/'experiment-harness/public_benchmark_v1';sys.path.insert(0,str(SOURCE))
from tugraph_schema import schema_from_import_config

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
base=ROOT/'reports/text2gql_schema_identity_bridge_20261026';base.mkdir(exist_ok=True)
plan=read(ROOT/'reports/text2gql_native_engine_20261026/six_graph_input_plan.json')
data=ROOT/'literature/public_candidate_metadata_20261024/Text2GQL-Bench/selected_zip_assets_20261024/dataset/test'
rows=[]
for entry in plan['graphs']:
    configpath=data/entry['graph']/'Cypher/TuGraph-DB_Instance/import_config.json'
    assert sha(configpath)==entry['config_sha256']
    config=read(configpath);schema=schema_from_import_config(config)
    original={e['label']:e for e in config['schema'] if e['type']=='VERTEX'}
    for entity in schema['entities']:
        old=original[entity['label']]
        assert entity['identity_property']==old['primary']
        assert set(entity['properties'])=={p['name'] for p in old['properties']}
        assert entity['native_types']=={p['name']:p['type'] for p in old['properties']}
    path=base/(str(len(rows)).zfill(2)+'_normalized_schema.json')
    path.write_text(json.dumps(schema,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    rows.append({'graph':entry['graph'],'import_config_sha256':sha(configpath),
      'normalized_schema_sha256':sha(path),'vertex_labels':len(schema['entities']),
      'directed_edge_signatures':len(schema['relations']),
      'primary_kinds':sorted({e['properties'][e['identity_property']] for e in schema['entities']}),
      'source_types_preserved':True,'phantom_metadata_added':False})
fixture=ROOT/'reports/text2gql_identity_fixture_v1_20261026';spec=read(fixture/'spec.json');r=read(fixture/'receipt.json')
assert spec['compiler_source_sha256']=={name:sha(SOURCE/name) for name in ('schema_services.py','tugraph_dialect.py','tugraph_schema.py')}
assert r['matches_expected']==r['fixed_check_denominator']==16 and len(r['database_calls'])==32
assert all(r['child_pids_absent'].values()) and r['own_server_exitcode']==0
for item in r['database_calls']:
    files=list((fixture/'responses').glob(str(item['id']).zfill(3)+'.*'))
    assert len(files)==1 and sha(files[0])==item['response_sha256']
for case in spec['cases']:
    outcomes=[x for x in r['cases'] if x['id']==case['id']]
    assert len(outcomes)==2 and {x['strategy'] for x in outcomes}=={'contract','full_detail'}
    assert all(x['rows']==case['expected_rows'] for x in outcomes)
disjoint=[x for x in r['cases'] if x['id']=='disjoint_name_summary']
assert disjoint[0]['representation']=='certified_disjoint_counts' and disjoint[1]['representation']=='entity_records'
ids=sorted(int(p.stem) for p in (ROOT/'experiments/ch7_private/continuation_20261021/budget').glob('*.json') if p.stem.isdigit())
assert ids==list(range(1,5193))
f=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
canon=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
assert all(sha(canon/p)==digest for p,digest in f['code_files'].items())
code=r'''
from pathlib import Path
import json,platform
r=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
assert platform.node()==json.loads((r/'execution_owner.json').read_text())['hostname']=='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
processes=[]
for p in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  args=p.read_bytes().split(b'\0')
  if str(r).encode() in b' '.join(args) and any(Path(a.decode(errors='ignore')).name in ('worker.py','lgraph_server','lgraph_import') for a in args):processes.append(int(p.parent.name))
 except (OSError,ValueError):pass
listeners=[]
for f in ['/proc/net/tcp','/proc/net/tcp6']:
 for line in Path(f).read_text().splitlines()[1:]:
  fields=line.split()
  if int(fields[1].split(':')[1],16)==18874 and fields[3]=='0A':listeners.append(fields[1])
print(json.dumps({'hostname':platform.node(),'main_runtime_lock':(r/'.public-runtime.lock').exists(),
 'relevant_processes':processes,'private_port_listeners':listeners,'read_only':True}))
'''
remote=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
 '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
 input=code,text=True,encoding='utf-8',capture_output=True,timeout=30)
assert remote.returncode==0,remote.stderr[:200]
closure=json.loads(remote.stdout)
assert not closure['main_runtime_lock'] and not closure['relevant_processes'] and not closure['private_port_listeners']
closure['time_local']=datetime.now().astimezone().isoformat()
(base/'remote_closure.json').write_text(json.dumps(closure,indent=2)+'\n',encoding='utf-8')
summary={'kind':'NATIVE_SCHEMA_IDENTITY_AND_SET_DEVELOPMENT_NOT_BENCHMARK_EX',
 'time_local':datetime.now().astimezone().isoformat(),'schemas':rows,
 'vertex_labels':sum(x['vertex_labels'] for x in rows),
 'directed_edge_signatures':sum(x['directed_edge_signatures'] for x in rows),
 'source_files_only':'import_config.json; no benchmark question/reference/model configuration read',
 'native_fixed_plans':8,'strategies_each':2,'native_outcomes':16,'native_expected_matches':16,
 'physical_native_queries':32,'model_calls':0,'cumulative_model_attempts':5192,
 'original_cap':12000,'remaining_original_CypherBench_scope':6808,
 'new_model_budget_approved':False,'new_model_budget_question_pending':True,
 'string_primary_actual_native_fixture':True,'integer_primary_unit_only':True,
 'schema_identity_adapter_complete':True,'full_backend_adapted':False,
 'formal_prediction_admitted':False,'entire_T1_complete':False,
 'local_checks_passed':32,'original_frozen_core_files_unchanged':32,
 'fixture_spec_sha256':sha(fixture/'spec.json'),'fixture_receipt_sha256':sha(fixture/'receipt.json'),
 'executed_source_sha256':spec['compiler_source_sha256'],
 'current_source_sha256':{name:sha(SOURCE/name) for name in ('schema_services.py','tugraph_dialect.py','tugraph_schema.py')},
 'executed_source_matches_current':True,
 'remote_closure':closure,
 'pending':['OPTIONAL/multi-stage and DATETIME/BOOL literal admission',
  'equal native Direct/Ours schema/entity/dialect inputs and training-only model development',
  'REST/Bolt typed-answer/scorer/author-cohort contract and full formal freeze',
  'explicit new benchmark model budget']}
p=ROOT/'reports/t1_text2gql_schema_identity_stage_summary_20261026.json'
p.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'schemas':len(rows),'labels':summary['vertex_labels'],
 'edge_signatures':summary['directed_edge_signatures'],'native_checks':16,'matched':16,
 'queries':32,'new_model_calls':0,'remote_closed':True}))
