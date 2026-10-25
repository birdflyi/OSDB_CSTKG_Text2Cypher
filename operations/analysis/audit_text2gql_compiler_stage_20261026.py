"""Summarize measured synthetic compiler checks without any model prediction."""
from pathlib import Path
from datetime import datetime
import hashlib, json, subprocess

ROOT=Path('D:/ch7')
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
WORK=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path):return json.loads(path.read_text(encoding='utf-8'))
freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
assert len(freeze['code_files'])==32
assert all(sha(CANON/p)==digest for p,digest in freeze['code_files'].items())
ledger=ROOT/'experiments/ch7_private/continuation_20261021/budget'
ids=sorted(int(p.stem) for p in ledger.glob('*.json') if p.stem.isdigit())
assert ids==list(range(1,5193)), 'No model dispatcher is authorized for this stage'
stages=[]
original=None
for folder in ['text2gql_compiler_fixture_v1_20261026',
               'text2gql_pattern_diagnostic_v2_20261026',
               'text2gql_compiler_fixture_v2_20261026',
               'text2gql_compiler_fixture_v3_20261026']:
    base=ROOT/'reports'/folder
    spec=read(base/'spec.json');r=read(base/'receipt.json')
    assert r['phase']=='FIXTURE_CHECKS_FINISHED'
    assert all(r['child_pids_absent'].values()) and r['own_server_exitcode']==0
    assert len(spec['cases'])==len(r['cases'])==r['fixture_queries']
    for index,(a,b) in enumerate(zip(spec['cases'],r['cases']),1):
        assert a['query']==b['query'] and a['expected_rows']==b['expected_rows']
        paths=list((base/'responses').glob(str(index).zfill(3)+'.*'))
        assert len(paths)==1 and sha(paths[0])==b['response_sha256']
    primary=[{'id':x['id'],'selection':x['selection'],'expected_rows':x['expected_rows']}
             for x in spec['cases'] if not x.get('diagnostic_only')]
    if primary:
        if original is None:original=primary
        else:assert primary==original, 'Expected cases/answers cannot be changed to mask failures'
    stages.append({'folder':folder,'spec_sha256':sha(base/'spec.json'),
       'receipt_sha256':sha(base/'receipt.json'),'queries':r['fixture_queries'],
       'fixed_check_denominator':r['fixed_check_denominator'],
       'fixed_matches':r['matches_expected'],
       'diagnostic_queries':sum(x['diagnostic_only'] for x in r['cases']),
       'diagnostic_matches':sum(x['diagnostic_only'] and x['matches_expected'] for x in r['cases']),
       'retained_mismatches':[x['id'] for x in r['cases'] if not x['matches_expected']],
       'children_closed':True})
assert stages[-1]['fixed_check_denominator']==stages[-1]['fixed_matches']==18
source=WORK/'experiment-harness/public_benchmark_v1'
assert read(ROOT/'reports/text2gql_compiler_fixture_v3_20261026/spec.json')['compiler_source_sha256']=={
    name:sha(source/name) for name in ('schema_services.py','tugraph_dialect.py','test_tugraph_dialect.py')}
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
  x=line.split()
  if int(x[1].split(':')[1],16) in (18870,18871,18872,18873) and x[3]=='0A':listeners.append(x[1])
partial=r/'text2gql_reference_audit_20261026/pattern_diagnostic_v1'
print(json.dumps({'hostname':platform.node(),'runtime_lock_present':(r/'.public-runtime.lock').exists(),
 'processes':processes,'listeners':listeners,'partial_diagnostic_has_receipt':(partial/'receipt.json').exists(),
 'partial_diagnostic_files':[p.name for p in partial.iterdir()],'read_only':True}))
'''
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
 '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
 input=code,text=True,encoding='utf-8',capture_output=True,timeout=30)
assert run.returncode==0,run.stderr[:300]
closure=json.loads(run.stdout)
assert not closure['runtime_lock_present'] and not closure['processes'] and not closure['listeners']
assert not closure['partial_diagnostic_has_receipt']
assert set(closure['partial_diagnostic_files'])=={'spec.json','worker.py','.client-private.json'}
closure['time_local']=datetime.now().astimezone().isoformat()
(ROOT/'reports/text2gql_compiler_remote_closure_20261026.json').write_text(json.dumps(closure,indent=2)+'\n',encoding='utf-8')
summary={'kind':'OFFLINE_NATIVE_COMPILER_DEVELOPMENT_NOT_T1_EX',
 'time_local':datetime.now().astimezone().isoformat(),
 'worktree':str(WORK),'branch':'codex/ch7-t1-tugraph-adapter','base_commit':'c614367',
 'source_sha256':{name:sha(source/name) for name in ('schema_services.py','tugraph_dialect.py','test_tugraph_dialect.py')},
 'original_core_source_hashes_unchanged':32,'model_attempts':5192,'new_model_calls':0,
 'original_cumulative_cap':12000,'remaining_original_scope':6808,
 'new_budget_approved':False,'new_budget_question_pending':True,
 'native_queries_this_stage':sum(s['queries'] for s in stages),
 'stages':stages,'partial_diagnostic_v1':{'phase':'BEFORE_FIRST_RECEIPT','native_queries':0,
   'unknown_worker_start_error':True,'launcher_error_retained':True,
   'blind_replay':False,'job_preserved':True},
 'final_fixed_selection_denominator':18,'final_fixed_selection_matches':18,
 'local_unittest_checks':23,'formal_benchmark_predictions':0,
 'backend_fully_adapted':False,'formal_prediction_admitted':False,
 'newly_observed_engine_differences':['comma-separated parallel patterns suppress permutations',
     'node-object COUNT DISTINCT includes isolated vertex in this fixture'],
 'closures':closure,'entire_T1_complete':False,
 'pending':['native schema/primary-key identity mapping and set composition',
   'OPTIONAL multi-stage lowering and DATETIME profile',
   'equal Direct/Ours dialect/schema/entity access and training development',
   'REST/Bolt typed-answer contract and exact author cohort/scorer audit',
   'fixed task/metric/statistics/fairness/budget freeze and explicit new model scope']}
target=ROOT/'reports/t1_text2gql_compiler_stage_summary_20261026.json'
target.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'native_queries':summary['native_queries_this_stage'],'fixed_matches':18,
 'local_checks':23,'core_hashes_unchanged':32,'model_calls':0,'remote_closed':True}))
