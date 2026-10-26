"""One-shot offline workload census on unchanged historical graph CSVs.

No old runner/author code, database, network or model is invoked. Historical
source exposure is explicit. Calendar/action rules are sealed before scanning.
"""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from collections import Counter
import csv,hashlib,json,subprocess,sys,time

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
ANALYSIS=REPO/'operations/analysis';sys.path.insert(0,str(ANALYSIS))
from osdb_window_census import WindowCensus,VERSION,ACTIONS
OUT=ROOT/'reports/osdb_fixed_windows_v2_20261026'
assert not OUT.exists(),'Audit prior one-shot output; do not overwrite'
OUT.mkdir()
P=ROOT/'experiments/ch7_private';C=P/'continuation_20261021'
GRAPHS={
 'redis':(ROOT/'code/OSDB_CSTKG_Text2Cypher/data_real/d1_typed_projection_staging_v1_2/execution_csv',P/'automated_dev_pilot_v1_20261021/neo4j_runtime/import_receipt.json'),
 'duckdb':(P/'duckdb_projection_publication_v1_20261021/execution_csv',P/'publication_duckdb_runtime_v1_20261021/import_receipt.json'),
 'iotdb':(C/'e8_iotdb_projection/execution_csv',C/'e8_iotdb_runtime_v1/import_receipt.json'),
 'elasticsearch':(C/'e8_elasticsearch_projection/execution_csv',C/'e8_elasticsearch_runtime_v1/import_receipt.json')}
def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for chunk in iter(lambda:f.read(8*1024**2),b''):h.update(chunk)
 return h.hexdigest()
def stamp():return datetime.now(timezone(timedelta(hours=8))).isoformat()
def write(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
check=subprocess.run([sys.executable,'-m','unittest','test_osdb_window_census'],cwd=ANALYSIS,capture_output=True,text=True)
assert check.returncode==0 and 'Ran 10 tests' in check.stderr
protocol={'kind':'FIXED_OBSERVED_OSDB_WORKLOAD_RULES_NOT_NEW_INDEPENDENT_TASKS',
 'version':VERSION,'sealed_local':stamp(),'source_sha256':sha(ANALYSIS/'osdb_window_census.py'),
 'year':2023,'all_actions_by_label':ACTIONS,'all_projects':list(GRAPHS),
 'entity_scope':'All supplied Issue/PullRequest nodes with outgoing EVENT_ACTION to Actor within each snapshot; no inferred main-repo restriction',
 'time_policy':'Only canonical UTC source_event_time. Monthly [start,next_month) UTC, no other time fallback/coercion. Missing/invalid/noncanonical/out-of-year separately reported.',
 'pairs':'All 11 adjacent month pairs per label/action plus all action combinations per label in each of12 months;185/project,740 overall; no outcome-based selection',
 'record_identity':'Physical CSV rows; typed (entity UID, source_event_id) keys; distinct source UID, separately. Missing event ID never drops otherwise valid entity membership.',
 'count_policy':'Keep raw rows/duplicate IDs, count anomalies; do not change source files. Node and endpoint identities must validate against original CSVs.',
 'unknown_actions':'Report all matched rows, separately retain service-outside-action counts; finite subcohorts use declared actions only.',
 'all_projects_historically_exposed':True,'rules_sealed_before_this_scan_not_before_historical_exposure':True,
 'not_claimed':['Independent natural language tasks','Method EX or planner accuracy','Runtime/cost advantage','Complete GitHub history','Four unseen projects'],
 'local_check_output':check.stderr.strip(),'new_model_calls':0,'new_database_queries':0}
write(OUT/'protocol.json',protocol)
runtime=REPO/'experiment-harness/public_benchmark_v1';runtime_before={p.name:sha(p) for p in runtime.glob('*.py')}
csv.field_size_limit(64*1024**2)
projects={}
for project,(folder,receipt_path) in GRAPHS.items():
 started=time.monotonic();receipt=json.loads(receipt_path.read_bytes())
 original_hashes={name:sha(folder/name) for name in ('nodes.csv','edges.csv')}
 assert all(original_hashes[name]==receipt['source_hashes'][name] for name in original_hashes)
 nodes={};labels=Counter()
 with (folder/'nodes.csv').open(encoding='utf-8-sig',newline='') as f:
  for row in csv.DictReader(f):
   uid=row['node_uid'];assert uid not in nodes
   assert uid.startswith(row['label']+':')
   nodes[uid]=(row['label'],row['entity_id']);labels[row['label']]+=1
 assert len(nodes)==receipt['nodes']
 census=WindowCensus(2023);diagnostics=Counter();edge_ids=set();total=0
 with (folder/'edges.csv').open(encoding='utf-8-sig',newline='') as f:
  for row in csv.DictReader(f):
   total+=1
   assert row['source_node_uid'] in nodes and row['target_node_uid'] in nodes
   source,target=nodes[row['source_node_uid']],nodes[row['target_node_uid']]
   if row['native_rel_type']!='EVENT_ACTION' or source[0] not in ACTIONS or target[0]!='Actor':continue
   props=json.loads(row['properties_json'])
   assert props.get('service_rel_type')==row['service_rel_type']
   edge_id=props.get('edge_uid')
   if type(edge_id) is str and edge_id:
    if edge_id in edge_ids:diagnostics['repeated_edge_uid_rows']+=1
    edge_ids.add(edge_id)
   else:diagnostics['rows_without_edge_uid']+=1
   diagnostics['query_eligible_false_rows']+=props.get('query_eligible') is False
   census.add(label=source[0],source_uid=row['source_node_uid'],action=row['service_rel_type'],
       event_time=props.get('source_event_time'),event_id=props.get('source_event_id'))
 assert total==receipt['edges']
 result=census.result()
 assert result['counts'].get('all_issue_pr_actor_event_action_rows',0)==sum(result['source_event_id_types'].values())
 assert result['counts'].get('canonical_in_year_rows',0)+result['counts'].get('outside_declared_year_rows',0)+result['counts'].get('missing_or_noncanonical_or_invalid_source_time',0)==result['counts']['all_issue_pr_actor_event_action_rows']
 assert all(sha(folder/name)==original_hashes[name] for name in original_hashes)
 projects[project]={'project':project,'source_csv_hashes':original_hashes,'source_receipt_sha256':sha(receipt_path),
   'snapshot_nodes':len(nodes),'snapshot_edges':total,'source_label_populations':{label:labels[label] for label in ACTIONS},
   'edge_identity_diagnostics':dict(diagnostics),'source_scan_wall_seconds':time.monotonic()-started,
   'census':result}
 write(OUT/(project+'.json'),projects[project])
 print(json.dumps({'project_completed':project,'node_rows':len(nodes),'edge_rows':total,
   'matched_actor_event_rows':result['counts']['all_issue_pr_actor_event_action_rows'],
   'summaries':result['pair_summary']},ensure_ascii=True),flush=True)
 assert sha(ANALYSIS/'osdb_window_census.py')==protocol['source_sha256']
assert runtime_before=={p.name:sha(p) for p in runtime.glob('*.py')}
freeze=json.loads((ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json').read_bytes())
for rel,digest in freeze['code_files'].items():assert sha(ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'/rel)==digest
ids=sorted(int(p.stem) for p in (C/'budget').glob('*.json'));assert ids==list(range(1,5193))
summary={'kind':'OSDB_OBSERVED_WORKLOAD_CENSUS_NOT_METHOD_EVALUATION','time_local':stamp(),
 'protocol_sha256':sha(OUT/'protocol.json'),'measurement_source_sha256':protocol['source_sha256'],
 'projects':{name:{k:v for k,v in report.items() if k!='census'}|{
   'census_counts':report['census']['counts'],'source_event_id_types':report['census']['source_event_id_types'],
   'inconsistent_entity_event_times':report['census']['distinct_entity_event_keys_with_multiple_observed_times'],
   'pair_summary':report['census']['pair_summary'],'full_report_sha256':sha(OUT/(name+'.json'))}
   for name,report in projects.items()},
 'fixed_pairs':sum(len(p['census']['pairs']) for p in projects.values()),
 'monthly_action_cohorts':sum(len(p['census']['cohorts']) for p in projects.values()),
 'local_checks_passed':10,'all_projects_historically_exposed':True,
 'method_runtime_files_unchanged':len(runtime_before),'original_frozen_source_hashes_unchanged':len(freeze['code_files']),
 'new_model_calls':0,'new_database_queries':0,'new_interface_probes':0,'new_attempt_reservations':0,
 'actual_attempts':5192,'active_budget_cap':12000,'Text2GQL_conditional_budget_activated':False,
 'formal_admission':False,'T1_complete':False,
 'limits':protocol['not_claimed']+['Repeated entity-pair incidents are correlated workload positions, not independent tasks/observations. No p-values or method-gain inference.']}
assert summary['fixed_pairs']==740 and summary['monthly_action_cohorts']==336
previous=json.loads((ROOT/'reports/osdb_fixed_windows_20261026/summary_v1.json').read_bytes())
for name in projects:
 old=json.loads((ROOT/'reports/osdb_fixed_windows_20261026'/(name+'.json')).read_bytes())
 for key in ('counts','source_event_id_types','cohorts','pairs','pair_summary'):
  assert old['census'][key]==projects[name]['census'][key]
summary['prior_version_retained']='reports/osdb_fixed_windows_20261026; extra independent short-fraction check found Python3.9 input limitation; repaired by lossless right-padding. All actual counts/cohorts/pairs unchanged in full re-scan.'
write(ROOT/'reports/t1_osdb_fixed_window_census_summary_20261026.json',summary)
print(json.dumps({'stage_completed':True,'fixed_pairs':740,'cohorts':336,'model_calls':0,'checks':10}),flush=True)
