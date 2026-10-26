from pathlib import Path
from datetime import datetime
import hashlib,json,subprocess,sys
ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
SOURCE=REPO/'experiment-harness/public_benchmark_v1'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
jobs=['text2gql_explain_capability_20261026','text2gql_execution_integration_20261026',
 'text2gql_execution_integration_v2_20261026','text2gql_execution_plan_bridge_20261026']
summary={'kind':'NATIVE_EXECUTION_JOURNAL_DEVELOPMENT_NOT_BENCHMARK_EX',
 'time_local':datetime.now().astimezone().isoformat(),'version':'native-execution-dev-2',
 'formal_prediction_admitted':False,'new_model_calls':0,'new_benchmark_queries':0,
 'new_native_development_requests':0,'jobs':[], 'reference_or_TEST_material_used':False,
 'model_config_read':False,'new_model_health_probes':0,'monetary_cost':None}
pids=[]
for name in jobs:
 folder=ROOT/'reports'/name;r=json.loads((folder/'receipt.json').read_bytes())
 assert all(r['child_pids_absent'].values())
 pids.extend(r['child_pids'])
 n=r['fixture_queries'];summary['new_native_development_requests']+=n
 physical=[]
 for receipt in folder.glob('journal/*/*/receipt.json'):
  c=json.loads(receipt.read_bytes());parent=receipt.parent
  assert sha(parent/'response.bin')==c['response_sha256']
  assert sha(parent/'request.json')==c['request_sha256']
  physical.append(c)
 for sealpath in folder.glob('journal/*/complete.json'):
  seal=json.loads(sealpath.read_bytes())
  for rel,digest in seal['files'].items():assert sha(sealpath.parent/rel)==digest
  assert sha(sealpath.parent/'outcome.json')==seal['outcome_sha256']
 if name!=jobs[0]:assert n==len(physical)
 if name in (jobs[2],jobs[3]):
  assert r['phase']=='FIXTURE_CHECKS_FINISHED' and r['matches_expected']==r['fixed_check_denominator']
 if name==jobs[1]:
  assert r['phase']=='STOPPED' and r['fixture_queries']==7
  assert r['cases'][-1]['outcome']['status']=='INFRASTRUCTURE_ERROR'
  raw=(folder/'journal/bad0/explain/response.bin').read_bytes()
  assert b'[ParserException] ParserException:' in raw
 summary['jobs'].append({'name':name,'phase':r['phase'],'native_requests':n,
  'checks_recorded':len(r['cases']) if name!=jobs[0] else 3,
  'matched_expected':r.get('matches_expected'),'receipt_sha256':sha(folder/'receipt.json'),
  'physical_receipts_verified':len(physical),'children_closed':r['child_pids_absent'],
  'request_response_bytes':sum(c['response_bytes'] for c in physical),
  'physical_wall_seconds':sum(c['wall_seconds'] for c in physical)})
assert summary['new_native_development_requests']==35
# No process enumeration contents or private values are published.
remote=r"""
from pathlib import Path
import json,platform,socket
root=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
hostname=platform.node();assert hostname==json.loads((root/'execution_owner.json').read_text())['hostname']=='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
active=[]
for path in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  parts=path.read_bytes().split(b'\0')
  if str(root).encode() in b' '.join(parts) and any(Path(a.decode(errors='ignore')).name in ('worker.py','lgraph_server','lgraph_import') for a in parts):active.append(int(path.parent.name))
 except OSError:pass
ports={}
for port in (18880,18881,18882,18883):
 s=socket.socket();ports[str(port)]=s.connect_ex(('127.0.0.1',port))==0;s.close()
print(json.dumps({'hostname':hostname,'main_lock':(root/'.public-runtime.lock').exists(),
 'native_processes':active,'ports_listening':ports,'children_absent':{str(p):not Path('/proc',str(p)).exists() for p in PIDS}}))
""".replace('PIDS',repr(pids))
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
 '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
 input=remote,capture_output=True,text=True,encoding='utf-8',timeout=30)
assert run.returncode==0,'Read-only closure failed; no automatic retry'
closure=json.loads(run.stdout)
assert not closure['main_lock'] and not closure['native_processes']
assert not any(closure['ports_listening'].values()) and all(closure['children_absent'].values())
summary['remote_closure']=closure
budget=ROOT/'experiments/ch7_private/continuation_20261021/budget'
ids=sorted(int(p.stem) for p in budget.glob('*.json'))
assert ids==list(range(1,5193))
summary['budget']={'physical_attempts':len(ids),'approved_cap':12000,'remaining':6808,
 'scope':'CypherBench only','Text2GQL_expansion_active':False,
 'latest_decision':'Human conditional on demonstrated large claim impact; deferred impact gate, not unconditional approval'}
freeze=json.loads((ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json').read_bytes())
canonical=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
for rel,digest in freeze['code_files'].items():assert sha(canonical/rel)==digest
summary['canonical_frozen_source_hashes_unchanged']=len(freeze['code_files'])
for key,repo in [('canonical',canonical),('recovery',ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_recovery')]:
 assert not subprocess.check_output(['git','-C',str(repo),'status','--porcelain'],text=True).strip()
 summary[key+'_head']=subprocess.check_output(['git','-C',str(repo),'rev-parse','HEAD'],text=True).strip()
tests=['test_schema_services','test_tugraph_dialect','test_tugraph_schema','test_tugraph_temporal',
 'test_tugraph_optional','test_tugraph_common_input','test_tugraph_results','test_tugraph_execution']
run=subprocess.run([sys.executable,'-m','unittest']+tests,cwd=SOURCE,capture_output=True,text=True)
assert run.returncode==0,run.stderr
assert 'Ran 76 tests' in run.stderr
summary['local_checks_passed']=76
summary['local_check_output']=run.stderr.strip()
summary['final_execution_source_sha256']={n:sha(SOURCE/n) for n in ('tugraph_execution.py','test_tugraph_execution.py')}
# Final bridge executes exactly the final transport/journal/plan implementation.
assert sha(SOURCE/'tugraph_execution.py')==sha(ROOT/'reports'/jobs[3]/'tugraph_execution.py')
summary['remaining_gates']=['whole-path SQL limits and batch infrastructure stop controller',
 'durable model/one-feedback attempt ledger integration and budget scope gate',
 'author EA/REST-Bolt/cohort/order/paired-statistics freeze',
 'demonstrate complementary evidence impact before activating conditional model budget',
 'TRAIN-only model validation then formal source/input/model/metric/resource freeze']
(ROOT/'reports/t1_text2gql_execution_stage_summary_20261026.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'native_requests':35,'models':0,'checks':76,'frozen_hashes':32,
 'remote_children_closed':True,'budget_cap_unchanged':12000}))
