"""Offline reference-program census: no NL input, predictions, DB or model.

Rules/source hashes are sealed before this run reads the per-task reference
programs. This is not preregistration before all historical data exposure.
"""
from pathlib import Path
from collections import Counter
from datetime import datetime,timezone,timedelta
import hashlib,json,subprocess,sys,zlib

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
ANALYSIS=REPO/'operations/analysis';sys.path.insert(0,str(ANALYSIS))
from task_composition_screen import screen,VERSION
OUT=ROOT/'reports/text2gql_composition_evidence_v2_20261026'
assert not OUT.exists(),'Preserve the sealed census instead of overwriting it'
OUT.mkdir()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
stamp=lambda:datetime.now(timezone(timedelta(hours=8))).isoformat()
source=ANALYSIS/'task_composition_screen.py'
tests=subprocess.run([sys.executable,'-m','unittest','test_task_composition_screen'],cwd=ANALYSIS,capture_output=True,text=True)
assert tests.returncode==0 and 'Ran 9 tests' in tests.stderr
protocol={'kind':'COMPOSITION_EVIDENCE_MEASUREMENT_RULES_NOT_FORMAL_LABELS',
 'version':VERSION,'sealed_local':stamp(),'screen_source_sha256':sha(source),
 'local_check_output':tests.stderr.strip(),
 'scope':'All six fixed Text2GQL Cypher TEST program positions; no task removed',
 'before_this_scan':True,'before_all_historical_reference_exposure':False,
 'inputs':'initial_gql and original declared schema/primary only; NL and model outcomes not consumed',
 'labels':['recognized_distinct_entity_count','one_stage_entity_count',
 'one_stage_entity_count_with_OR','equality_partition_shape_only'],
 'unknown_policy':'Retain all other/unrecognized/parser-error records; zero screen hits do not establish semantic absence',
 'partition_interpretation':'Two-to-eight OR branches of same counted variable/same-property literal equality shape; literal values and exclusivity unverified',
 'not_proved':['Cypher grammar/type correctness','method support or plan quality',
 'safe summary applicability','actual mechanism activation or gain','task natural-language fidelity'],
 'method_adaptation_from_this_TEST_census_prohibited':True,'new_model_calls':0,'new_database_queries':0}
write(OUT/'protocol.json',protocol)
runtime=REPO/'experiment-harness/public_benchmark_v1'
runtime_before={p.name:sha(p) for p in runtime.glob('*.py')}
base=ROOT/'literature/public_candidate_metadata_20261024/Text2GQL-Bench'
snapshot=base/'cypher_test_snapshot_inventory_20261024.json'
inventory={x['member']:x for x in json.loads(snapshot.read_bytes())['assets']}
oldprofile=ROOT/'reports/text2gql_query_feature_profile_20261025.json'
oldsummary=json.loads((ROOT/'reports/text2gql_query_feature_profile_summary_20261025.json').read_bytes())
assert sha(oldprofile)==oldsummary['full_local_report_sha256']
old=json.loads(oldprofile.read_bytes())
oldmap={(r['graph'],str(r['id'])):r for r in old['task_receipts']}
cohortfile=ROOT/'reports/text2gql_reference_cohorts_provisional_20261026.json'
cohort=json.loads(cohortfile.read_bytes())
reference={(r['graph'],str(r['id'])):r for group in ('native_reference_executable','native_reference_unscoreable') for r in cohort[group]}
def asset(member):
 raw=(base/'selected_zip_assets_20261024'/member).read_bytes();item=inventory[member]
 assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
 assert format(zlib.crc32(raw)&0xffffffff,'08x')==item['crc32']
 return json.loads(raw)
query_files=sorted(m for m in inventory if m.endswith('.json') and m.count('/')==4)
assert len(query_files)==6
records=[];graphs={};unrecognized=Counter()
for member in query_files:
 graph=member.split('/')[2];parent=member.rsplit('/',1)[0]
 configs=[m for m in inventory if m.startswith(parent+'/') and m.endswith('/import_config.json')]
 assert len(configs)==1
 config=asset(configs[0]);primary={r['label']:r['primary'] for r in config['schema'] if r['type']=='VERTEX'}
 counts=Counter({k:0 for k in protocol['labels']});n=0
 for item in asset(member):
  n+=1;key=(graph,str(item['id']));query=item['initial_gql'];qsha=hashlib.sha256(query.encode()).hexdigest()
  assert key in oldmap and qsha==oldmap[key]['query_sha256']
  assert reference[key]['source_query_sha256']==qsha
  try:result=screen(query,primary)
  except ValueError as exc:result={'recognition':'SCREEN_ERROR_RETAINED','error_type':type(exc).__name__}
  counts.update(k for k in protocol['labels'] if result.get(k) is True)
  counts['lexical_OR']+=oldmap[key]['flags']['OR']
  counts['lexical_UNION']+=oldmap[key]['flags']['UNION']
  counts['native_reference_executable']+=reference[key]['status'] in ('EXECUTED_NONEMPTY','EXECUTED_EMPTY')
  counts['native_reference_unknown']+=reference[key]['status']=='GOLD_EXECUTION_ERROR'
  if 'error_type' in result:counts['screen_errors']+=1
  unrecognized[result['recognition']]+=1
  records.append({'graph':graph,'id':key[1],'reference_query_sha256':qsha,
    'native_reference_status':reference[key]['status'],'screen':result})
 graphs[graph]={'n':n,'counts':dict(counts),'query_asset_sha256':inventory[member]['sha256'],
                'config_asset_sha256':inventory[configs[0]]['sha256']}
assert sum(g['counts']['native_reference_executable'] for g in graphs.values())==3815
assert sum(g['counts']['native_reference_unknown'] for g in graphs.values())==11
assert len(records)==3826 and len({(r['graph'],r['id']) for r in records})==3826
assert set(reference)=={(r['graph'],r['id']) for r in records}
write(OUT/'task_screen_records.json',records)
assert runtime_before=={p.name:sha(p) for p in runtime.glob('*.py')}
assert sha(source)==protocol['screen_source_sha256']
freeze=json.loads((ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json').read_bytes())
canonical=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
for rel,digest in freeze['code_files'].items():assert sha(canonical/rel)==digest
ids=sorted(int(p.stem) for p in (ROOT/'experiments/ch7_private/continuation_20261021/budget').glob('*.json'))
assert ids==list(range(1,5193))
summary={'kind':'TASK_REFERENCE_PROGRAM_COMPOSITION_SCREEN_NOT_METHOD_RESULTS','time_local':stamp(),
 'protocol_sha256':sha(OUT/'protocol.json'),'screen_source_sha256':sha(source),
 'old_lexical_profile_sha256':sha(oldprofile),'native_reference_cohort_sha256':sha(cohortfile),
 'asset_inventory_sha256':sha(snapshot),'task_screen_records_sha256':sha(OUT/'task_screen_records.json'),
 'full_N':3826,'graphs':graphs,'recognition_counts':dict(unrecognized),
 'totals':dict(sum((Counter(g['counts']) for g in graphs.values()),Counter())),
 'initial_status_join_counter_error_preserved':'reports/text2gql_composition_evidence_20261026/summary_initial_status_join_error.json; original status used EXECUTED while cohort uses EXECUTED_NONEMPTY/EXECUTED_EMPTY; screen labels unchanged',
 'local_measurement_checks_passed':9,'unknown_records_retained':True,
 'test_reference_programs_read_for_characterization':True,'test_NL_read_for_model_input_or_method_development':False,
 'model_outputs_read':False,'method_runtime_files_unchanged':len(runtime_before),
 'original_frozen_source_hashes_unchanged':len(freeze['code_files']),
 'native_reference_read':'Cached cohort status/hash only, not new DB/reference execution or result comparison',
 'new_model_calls':0,'new_database_queries':0,'network_checks':0,'new_attempt_reservations':0,
 'actual_cumulative_model_attempts':5192,'active_cumulative_cap':12000,
 'Text2GQL_conditional_budget_activated':False,'formal_admission':False,
 'limits':protocol['not_proved']+[protocol['unknown_policy'],protocol['partition_interpretation']]}
for label in protocol['labels']:summary['totals'].setdefault(label,0)
write(ROOT/'reports/t1_text2gql_composition_screen_summary_20261026.json',summary)
print(json.dumps({'full_N':3826,'totals':summary['totals'],'recognition':summary['recognition_counts'],
 'models':0,'queries':0,'checks':9},ensure_ascii=True))
