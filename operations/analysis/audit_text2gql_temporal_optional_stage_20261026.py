"""Accept finite native development evidence; never run a model or TEST task."""
from pathlib import Path
from datetime import datetime
import hashlib,json,subprocess,sys

ROOT=Path('D:/ch7');SOURCE=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph/experiment-harness/public_benchmark_v1'
sys.path.insert(0,str(SOURCE))
from schema_services import execute_plan, VERSION
from tugraph_dialect import compile_rest_selection, render_compiler_query
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
rows=[]
names=['text2gql_temporal_fixture_v1_20261026','text2gql_optional_with_probe_v1_20261026',
 'text2gql_optional_compiler_fixture_v1_20261026','text2gql_optional_filter_probe_v1_20261026',
 'text2gql_optional_compiler_fixture_v2_20261026']
for name in names:
    p=ROOT/'reports'/name;spec=read(p/'spec.json');r=read(p/'receipt.json')
    assert r['phase']=='FIXTURE_CHECKS_FINISHED' and all(r['child_pids_absent'].values())
    assert r['own_server_exitcode']==0 and len(r['cases'])==r['fixture_queries']==len(spec['cases'])
    for index,(sealed,result) in enumerate(zip(spec['cases'],r['cases']),1):
        assert sealed['id']==result['id'] and sealed['query']==result['query']
        assert sealed['expected_rows']==result['expected_rows']
        files=list((p/'responses').glob(str(index).zfill(3)+'.*'))
        assert len(files)==1 and sha(files[0])==result['response_sha256']
    rows.append({'folder':name,'spec_sha256':sha(p/'spec.json'),'receipt_sha256':sha(p/'receipt.json'),
       'queries':r['fixture_queries'],'fixed_denominator':r['fixed_check_denominator'],
       'fixed_matches':r['matches_expected'],
       'diagnostics':sum(x['diagnostic_only'] for x in r['cases']),
       'diagnostics_matched':sum(x['diagnostic_only'] and x['matches_expected'] is True for x in r['cases']),
       'mismatched_ids':[x['id'] for x in r['cases'] if x['matches_expected'] is False]})
assert rows[0]['fixed_denominator']==rows[0]['fixed_matches']==20
assert rows[2]['fixed_denominator']==18 and rows[2]['fixed_matches']==12
assert rows[-1]['fixed_denominator']==rows[-1]['fixed_matches']==18
old=read(ROOT/'reports/text2gql_optional_compiler_fixture_v1_20261026/spec.json')
new=read(ROOT/'reports/text2gql_optional_compiler_fixture_v2_20261026/spec.json')
assert [{k:x[k] for k in ('id','selection','expected_rows')} for x in old['cases']]==[
    {k:x[k] for k in ('id','selection','expected_rows')} for x in new['cases'] if not x.get('diagnostic_only')]
assert new['compiler_source_sha256']=={name:sha(SOURCE/name) for name in new['compiler_source_sha256']}
# Temporal stage ran v0.5. Prove current v0.7 emits byte-identical scripts and
# parameter maps for all original twenty cases; this is not another DB run.
temporal=read(ROOT/'reports/text2gql_temporal_fixture_v1_20261026/spec.json')
for item in temporal['cases']:
    if item.get('diagnostic_only'):continue
    c=compile_rest_selection(item['selection'],temporal['schema'])
    assert c['script']==item['query'] and c['parameters']==item['parameters']
# Replay prior independently measured set-service responses only after exact
# query/parameter/script equality, to verify the retained native identity path.
prior=ROOT/'reports/text2gql_identity_fixture_v1_20261026';ps=read(prior/'spec.json');pr=read(prior/'receipt.json')
index=0;replay_matches=0
for case in ps['cases']:
    for strategy in ['contract','full_detail']:
        def execute(query,parameters):
            global index
            physical=pr['database_calls'][index];index+=1
            assert physical['query']==query and physical['parameters']==parameters
            assert physical['script']==render_compiler_query(query,parameters)
            raw=prior/'responses'/(str(physical['id']).zfill(3)+'.json')
            assert sha(raw)==physical['response_sha256']
            body=read(raw)
            return {'rows':body['result'],'columns':[h['name'] for h in body['header']],
                    'complete':True,'status':'EXECUTED'}
        outcome=execute_plan(case['plan'],ps['schema'],execute,backend='tugraph-4.5.2',strategy=strategy)
        assert outcome['rows']==case['expected_rows'];replay_matches+=1
assert index==32 and replay_matches==16
ids=sorted(int(p.stem) for p in (ROOT/'experiments/ch7_private/continuation_20261021/budget').glob('*.json') if p.stem.isdigit())
assert ids==list(range(1,5193))
freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
canon=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
assert all(sha(canon/name)==digest for name,digest in freeze['code_files'].items())
code=r'''
from pathlib import Path
import json,platform
root=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']=='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
processes=[]
for p in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  args=p.read_bytes().split(b'\0')
  if str(root).encode() in b' '.join(args) and any(Path(a.decode(errors='ignore')).name in ('worker.py','lgraph_server','lgraph_import') for a in args):processes.append(int(p.parent.name))
 except (OSError,ValueError):pass
listeners=[]
for p in ['/proc/net/tcp','/proc/net/tcp6']:
 for line in Path(p).read_text().splitlines()[1:]:
  fields=line.split()
  if int(fields[1].split(':')[1],16) in range(18875,18880) and fields[3]=='0A':listeners.append(fields[1])
print(json.dumps({'hostname':platform.node(),'runtime_lock_present':(root/'.public-runtime.lock').exists(),
 'relevant_processes':processes,'private_port_listeners':listeners,'read_only':True}))
'''
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
 '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
 input=code,text=True,encoding='utf-8',capture_output=True,timeout=30)
assert run.returncode==0,run.stderr[:200]
closure=json.loads(run.stdout)
assert not closure['runtime_lock_present'] and not closure['relevant_processes'] and not closure['private_port_listeners']
closure['time_local']=datetime.now().astimezone().isoformat()
(ROOT/'reports/text2gql_temporal_optional_remote_closure_20261026.json').write_text(json.dumps(closure,indent=2)+'\n',encoding='utf-8')
summary={'kind':'NATIVE_TEMPORAL_OPTIONAL_DEVELOPMENT_NOT_BENCHMARK_EX',
 'time_local':datetime.now().astimezone().isoformat(),'version':VERSION,
 'stages':rows,'native_queries_this_stage':sum(r['queries'] for r in rows),
 'final_temporal_cases':20,'final_temporal_matches':20,'current_temporal_scripts_identical':20,
 'final_optional_cases':18,'final_optional_matches':18,'optional_cases_unchanged_across_versions':True,
 'current_identity_offline_replay_outcomes':16,'identity_raw_query_parameter_equalities':32,
 'identity_replay_new_database_queries':0,'current_local_checks_passed':44,
 'local_development_failure_retained':'First 37-check run: 1 Python3.9 one-digit fraction parsing error; canonical right-padding fixed it before native execution.',
 'source_sha256':{n:sha(SOURCE/n) for n in ['schema_services.py','tugraph_dialect.py','tugraph_schema.py',
   'test_tugraph_dialect.py','test_tugraph_temporal.py','test_tugraph_optional.py']},
 'zero_new_model_calls':True,'cumulative_model_attempts':5192,'cumulative_cap':12000,
 'remaining_original_CypherBench_scope':6808,'new_model_budget_approved':False,
 'new_model_budget_question_pending':True,'canonical_core_files_unchanged':32,
 'temporal_profile':'naive ISO datetime, 0..6 fractional digits; timezone and precision loss rejected',
 'bool_profile':'actual booleans, equality/membership/null tests; no integer/string coercion',
 'optional_profile':'one optional edge, explicit WITH query parts, bag-preserving carriers/scalar edge property projections',
 'raw_return_date_datetime':'REST strings in the independent fixture; not proven author Bolt equivalence',
 'formal_prediction_admitted':False,'entire_T1_complete':False,
 'pending':['common Direct/Ours native schema/identity/entity/dialect/feedback inputs',
 'typed REST/Bolt representation/author scorer and cohort/fixed reference denominator/statistics/freeze',
 'explicit model budget then TRAIN/independent development model validation',
 'remaining necessary named methods/other public candidates/OSDB independent tasks'],
 'remote_closure':closure}
p=ROOT/'reports/t1_text2gql_temporal_optional_stage_summary_20261026.json'
p.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'native_queries':summary['native_queries_this_stage'],'temporal':20,'optional':18,
 'identity_offline_replay':16,'core_hashes_unchanged':32,'new_model_calls':0,'remote_closed':True}))
