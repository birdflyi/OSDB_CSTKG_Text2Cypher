"""Four NEW semantic-only copies: full CSV fingerprint + shared read references.

Preserve original stores/configs, predictions and frozen runtime. No model calls,
training, remote service changes, lock clearing or uncertain statement replay.
"""
from collections import Counter
import csv,hashlib,json,math,os,shutil,socket,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,r'D:\ch7\tools\python_deps\neo4j_5_15')
import requests
from neo4j import GraphDatabase
from accept_osdb_actor_graph_references_20261026 import SOURCES,HOME,store_directories,stores,stop_owned,live,budget,runtime,now
from build_osdb_actor_intake_20261026 import GRAPHS
from osdb_shared_read_executor_dev import SharedReadExecutor,digest

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OUT=ROOT/'reports/osdb_semantic_snapshots_v1_20261027'
PROFILE=ROOT/'reports/osdb_observed_profile_dev_v2_20261027'
REGISTRY=ROOT/'reports/osdb_actor_candidate_intake_v1_20261026/candidate_registry.json'
BATCH=2000;HTTP=18501;BOLT=18502


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8*1024**2),b''):h.update(b)
 return h.hexdigest()
def write(p,x):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(x,f,ensure_ascii=False,indent=2);f.write('\n')
def progress(value):
 temp=OUT/'progress.writing';temp.write_bytes((json.dumps(value,ensure_ascii=False,indent=2)+'\n').encode('utf-8'));os.replace(temp,OUT/'progress.json')


def csv_fingerprint(folder):
 """Independent CSV path: typed node keys, directed physical edges, bags.

 Neo4j has no stored null properties: only top-level JSON null entries are
 omitted. No other casting, identity replacement or deduplication is allowed.
 """
 nodes={};keys=set();edge_ids=set();hashes=[];nulls=Counter();counts=Counter()
 def properties(row):
  raw=json.loads(row['properties_json']);assert type(raw) is dict
  for k,v in raw.items():
   if v is None:nulls[k]+=1
  return {k:v for k,v in raw.items() if v is not None}
 with (folder/'nodes.csv').open(encoding='utf-8-sig',newline='') as f:
  for row in csv.DictReader(f):
   props=properties(row);label=row['label'];key=(label,props['entity_id']);uid=row['node_uid']
   assert type(key[1]) is str and key[1] and key[1]==row['entity_id']
   assert uid not in nodes and key not in keys and label!='PilotNode' and '__pilot_uid' not in props
   nodes[uid]=key;keys.add(key);hashes.append(digest(['node',label,props]));counts['nodes']+=1
 with (folder/'edges.csv').open(encoding='utf-8-sig',newline='') as f:
  for row in csv.DictReader(f):
   props=properties(row);key=props['edge_uid'];label=row['native_rel_type']
   assert type(key) is str and key and key not in edge_ids
   assert props['native_rel_type']==label and props['service_rel_type']==row['service_rel_type']
   edge_ids.add(key);a=nodes[row['source_node_uid']];b=nodes[row['target_node_uid']]
   hashes.append(digest(['edge',label,a,b,props]));counts['edges']+=1
 return {'nodes':counts['nodes'],'edges':counts['edges'],
  'semantic_snapshot_sha256':hashlib.sha256(''.join(sorted(hashes)).encode()).hexdigest(),
  'null_property_entries_omitted_by_key':dict(nulls),'typed_entity_keys_unique':True,'global_edge_ids_unique':True}


def main():
 assert not OUT.exists(),'Preserve any prior partial run; audit before another version'
 assert all(not live(p) for p in (17475,17476,17477,17478,18475,18476,18477,18478,HTTP,BOLT))
 assert shutil.disk_usage('D:/').free>6*1024**3
 before_budget=budget();before_runtime=runtime();assert not before_budget['pending']
 assert not (ROOT/'experiments/ch7_private/continuation_20261021/budget/.t1-reservation.lock').exists()
 frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/n)==h for n,h in frozen.items())
 registry=read(REGISTRY);assert len(registry)==24 and sha(REGISTRY)=='26f24882852e446ae0d28957d3f60b0edaf5d2ec68e66d275d0159a83ac183b3'
 tasks=[t for t in registry if t['reference_query'] is not None];assert len(tasks)==16
 assert all('PilotNode' not in t['reference_query'] and '__pilot_uid' not in t['reference_query'] for t in tasks)
 profiles={g:read(PROFILE/(g+'_profile.json')) for g in SOURCES}
 OUT.mkdir();(OUT/'executed_sources').mkdir()
 sources=[Path(__file__)]+[Path(__file__).with_name(n) for n in (
  'osdb_shared_read_executor_dev.py','osdb_common_input.py','accept_osdb_actor_graph_references_20261026.py','build_osdb_actor_intake_20261026.py')]
 for p in sources:shutil.copyfile(p,OUT/'executed_sources'/p.name)
 protocol={'sealed_local':now(),'kind':'REAL_GRAPH_SEMANTIC_SNAPSHOT_PREPARATION_NOT_FORMAL_MODEL_RESULTS',
  'sources':{p.name:sha(p) for p in sources},'source_stores':{g:str(p) for g,p in SOURCES.items()},
  'planned_graphs':4,'planned_reference_checks':16,'planned_full_physical_graph_admissions':4,
  'fixed_graph_order':list(SOURCES),'max_nodes_per_cleanup_transaction':BATCH,'ports':[HTTP,BOLT],
  'registry_sha256':sha(REGISTRY),'profiles_sha256':{g:sha(PROFILE/(g+'_profile.json')) for g in SOURCES},
  'budget_before':before_budget,'runtime_before':before_runtime,'canonical_before':frozen,
  'no_model_requests_training_remote_changes_or_statement_retries':True,
  'null_rule':'Omit only top-level JSON null properties, reflecting physical Neo4j absence. Preserve all non-null types and values.',
  'isolation':'Copy verified stopped stores into new directories; only remove PilotNode/__pilot_uid there. Never start or change legacy config.',
  'offline_audit_budget':'CSV scan and full admission 240s per statement; fixed metadata cleanup 120s each; reference EXPLAIN30/read60s.',
  'readiness':'At most three checks >=10s apart, one owned JVM at a time; first uncertain/error/mismatch stops entire stage.',
  'formal_admission':False,'independent_task_scope_and_model_budget_still_pending':True}
 write(OUT/'protocol.json',protocol)
 session=requests.Session();session.trust_env=False;completed={};attempted=[];failure=None;total_http=0
 progress({'state':'PREPARING','completed_graphs':[],'active_graph':None,'new_model_calls':0})
 try:
  csv.field_size_limit(64*1024**2)
  for graph,source in SOURCES.items():
   attempted.append(graph);started=time.perf_counter();root=OUT/'runtime'/graph;root.mkdir(parents=True)
   progress({'state':'CSV_AND_COPY','completed_graphs':list(completed),'active_graph':graph,'new_model_calls':0})
   csv_folder,receipt_path=GRAPHS[graph];receipt=read(receipt_path)
   csv_hashes={n:sha(csv_folder/n) for n in ('nodes.csv','edges.csv')};assert csv_hashes==receipt['source_hashes']
   expected=csv_fingerprint(csv_folder);assert (expected['nodes'],expected['edges'])==(receipt['nodes'],receipt['edges'])
   write(root/'csv_semantic_fingerprint.json',dict(expected,source_hashes=csv_hashes))
   source_before=stores(source);write(root/'source_store_manifest_before.json',source_before)
   assert shutil.disk_usage('D:/').free>sum(x['bytes'] for x in source_before.values())+4*1024**3
   copy_started=time.perf_counter()
   directories=store_directories(source)
   for directory in directories:shutil.copytree(source/directory,root/directory)
   assert stores(root)==source_before,'Copied bytes differ; never boot'
   copy_seconds=time.perf_counter()-copy_started
   for d in ('conf','logs','run','plugins'):(root/d).mkdir()
   conf=root/'conf'
   config=['server.default_listen_address=127.0.0.1',f'server.http.listen_address=127.0.0.1:{HTTP}',
    'server.http.enabled=true','server.https.enabled=false','server.bolt.enabled=true',
    f'server.bolt.listen_address=127.0.0.1:{BOLT}',f'server.bolt.advertised_address=127.0.0.1:{BOLT}',
    'dbms.security.auth_enabled=false','initial.dbms.default_database=neo4j','server.memory.heap.initial_size=256m',
    'server.memory.heap.max_size=512m','server.memory.pagecache.size=128m',
    'dbms.security.allow_csv_import_from_file_urls=false','server.jvm.additional=-XX:ActiveProcessorCount=2']
   for d in ('data','logs','run','plugins'):config.append('server.directories.'+d+'='+str(root/d).replace('\\','/'))
   if 'transactions' in directories:config.append('server.directories.transaction.logs.root='+str(root/'transactions').replace('\\','/'))
   (conf/'neo4j.conf').write_text('\n'.join(config)+'\n',encoding='utf-8')
   launch=root/'launch_owned.ps1';launch.write_text("& '"+str(HOME/'bin/neo4j.bat')+"' console\n",encoding='utf-8')
   outlog=(root/'console_stdout.log').open('wb');errlog=(root/'console_stderr.log').open('wb')
   process=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(launch)],
    env=dict(os.environ,NEO4J_CONF=str(conf),NEO4J_HOME=str(HOME)),stdout=outlog,stderr=errlog,creationflags=subprocess.CREATE_NO_WINDOW)
   write(root/'execution_owner.json',{'host':socket.gethostname(),'wrapper_pid':process.pid,'config':str(conf),
    'ports':[HTTP,BOLT],'scope':'new local semantic-only copy, not remote owner migration','source_receipt_sha256':sha(receipt_path)})
   posts=0;gets=0;driver=None;cleanup=[];accepted=[];executor=None
   def trusted(query,params,kind):
    nonlocal posts,total_http
    posts+=1;total_http+=1;folder=root/'trusted_requests'/f'{posts:04d}_{kind}';folder.mkdir(parents=True)
    payload={'statements':[{'statement':query,'parameters':params,'resultDataContents':['row']}]}
    write(folder/'request.json',payload);t={'started_local':now()};start=time.perf_counter()
    try:
     r=session.post(f'http://127.0.0.1:{HTTP}/db/neo4j/tx/commit',json=payload,timeout=(3,120))
     t.update(http_status=r.status_code,body=r.json());assert r.status_code==200 and not t['body']['errors']
     result=t['body']['results'][0];return [row['row'] for row in result['data']]
    except Exception as exc:t.update(error_type=type(exc).__name__,error=str(exc)[:1000]);raise
    finally:t['client_seconds']=time.perf_counter()-start;write(folder/'terminal.json',t)
   try:
    ready=False
    for i in range(3):
     time.sleep(12 if i==0 else 10);gets+=1;total_http+=1;t={'attempt':i+1}
     try:r=session.get(f'http://127.0.0.1:{HTTP}/',timeout=(2,3));t['http_status']=r.status_code;ready=r.status_code==200
     except requests.RequestException as exc:t['error']=type(exc).__name__
     write(root/f'readiness_{i+1}.json',t)
     if ready:break
     if process.poll() is not None:break
    assert ready,'Readiness failed; preserve and stop, no auto restart'
    assert trusted('CALL db.info() YIELD id,name RETURN id,name',{},'graph_identity')==[receipt['database_identity'][0]['row']]
    before=trusted('MATCH(n) RETURN count(n),sum(CASE WHEN n:PilotNode THEN 1 ELSE 0 END),sum(CASE WHEN n.__pilot_uid IS NOT NULL THEN 1 ELSE 0 END)',{},'metadata_before')
    assert before==[[expected['nodes']]*3], 'Unexpected partial source metadata, do not repair blindly'
    progress({'state':'OWN_COPY_METADATA_CLEANUP','completed_graphs':list(completed),'active_graph':graph,'new_model_calls':0})
    remaining=expected['nodes']
    for i in range(math.ceil(remaining/BATCH)):
     amount=min(BATCH,remaining)
     actual=trusted('MATCH(n:PilotNode) WITH n LIMIT $batch REMOVE n:PilotNode REMOVE n.__pilot_uid RETURN count(n)',{'batch':BATCH},'cleanup')
     assert actual==[[amount]],'Unexpected cleanup count; stop without replay'
     remaining-=amount;cleanup.append(amount)
    assert remaining==0
    driver=GraphDatabase.driver(f'bolt://127.0.0.1:{BOLT}',auth=None,connection_timeout=3,max_transaction_retry_time=0,max_connection_pool_size=1)
    executor=SharedReadExecutor(driver,profiles[graph],root/'shared_access')
    progress({'state':'FULL_GRAPH_ADMISSION','completed_graphs':list(completed),'active_graph':graph,'new_model_calls':0})
    admitted=executor.admit()
    assert all(admitted[k]==expected[k] for k in ('nodes','edges','semantic_snapshot_sha256')),'Full semantic graph differs from CSV'
    write(root/'full_graph_equivalence.json',{'expected':expected,'admitted':admitted,'matches':True,'formal_admission':False})
    for task in tasks:
     if task['project']!=graph:continue
     result=executor.execute(task['reference_query'],task['parameters'])
     same=result['complete'] and result['rows']==task['csv_expected_rows']
     write(root/'reference_checks'/(task['task_id']+'.json'),{'result':result,'expected_rows':task['csv_expected_rows'],'matches':same})
     assert same,'Reference differs; do not tune method or modify expected rows'
     accepted.append(task['task_id'])
    assert len(accepted)==4
    assert {n:sha(csv_folder/n) for n in csv_hashes}==csv_hashes
    terminals=[read(p) for p in (root/'shared_access').glob('*/terminal.json')]
    completed[graph]={'nodes':expected['nodes'],'edges':expected['edges'],'semantic_snapshot_sha256':admitted['semantic_snapshot_sha256'],
     'full_semantic_CSV_fingerprint_matches':True,'all_storage_metadata_absent':True,
     'physical_graph_access_admitted_for_development':True,'reference_checks_passed':len(accepted),
     'cleanup_transactions':len(cleanup),'cleaned_nodes':sum(cleanup),'readiness_HTTP':gets,'trusted_HTTP_POST':posts,
     'Bolt_Cypher_statements':len(terminals),'Bolt_client_seconds':sum(t['client_seconds'] for t in terminals),
     'source_store_files':len(source_before),'source_store_bytes':sum(x['bytes'] for x in source_before.values()),
     'copy_and_verify_seconds':copy_seconds,'null_property_entries_omitted_by_key':expected['null_property_entries_omitted_by_key'],
     'formal_admission':False}
   finally:
    if driver:driver.close()
    stop=stop_owned(process,conf,HTTP);stop['bolt_port_closed']=not live(BOLT);assert stop['bolt_port_closed']
    write(root/'shutdown_receipt.json',stop);outlog.close();errlog.close()
   assert stores(source)==source_before,'Original store bytes changed'
   write(root/'source_store_manifest_after.json',source_before)
   write(root/'new_store_manifest_after_shutdown.json',stores(root))
   completed[graph]['all_original_store_bytes_unchanged']=True
   completed[graph]['owned_copy_stopped']=True
   completed[graph]['total_project_client_seconds']=time.perf_counter()-started
   write(root/'summary.json',completed[graph]);progress({'state':'GRAPH_DONE','completed_graphs':list(completed),'active_graph':None,'new_model_calls':0})
   print(json.dumps({'project':graph,'full_graph_matches':True,'references':4,'owned_copy_stopped':True}),flush=True)
 except Exception as exc:
  failure={'type':type(exc).__name__,'message':str(exc)[:1600]};write(OUT/'failure.json',failure)
 finally:session.close()
 after_budget=budget();assert before_budget==after_budget
 assert runtime()==before_runtime and all(sha(CANON/n)==h for n,h in frozen.items())
 assert sha(REGISTRY)==protocol['registry_sha256']
 summary={'kind':'OSDB_REAL_SEMANTIC_SNAPSHOT_DEVELOPMENT_ACCESS_NOT_FORMAL_MODEL_RESULTS',
  'planned_graphs':4,'attempted_graphs':len(attempted),'completed_graphs':len(completed),'graphs':completed,
  'failed_graphs':len(attempted)-len(completed),'undispatched_graphs':4-len(attempted),
  'planned_reference_checks':16,'dispatched_reference_checks':sum(len(list((OUT/'runtime'/g/'reference_checks').glob('*.json'))) for g in attempted),
  'accepted_reference_checks':sum(v['reference_checks_passed'] for v in completed.values()),'total_HTTP_requests':total_http,
  'same_body_statement_retries':0,'new_model_calls':0,'training':0,'failure':failure,
  'budget_before':before_budget,'budget_after':after_budget,'canonical32_and_adapter46_unchanged':True,
  'old_source_stores_unchanged':all(v.get('all_original_store_bytes_unchanged') for v in completed.values()),
  'formal_N':None,'formal_model_dispatched':0,'formal_model_completed':0,'formal_model_failed':0,
  'formal_model_undispatched':None,'formal_admission':False,'entire_T1_complete':False}
 write(OUT/'summary.json',summary);progress({'state':'STOPPED' if failure else 'COMPLETED','completed_graphs':list(completed),'active_graph':None,'new_model_calls':0})
 print(json.dumps(summary,ensure_ascii=True),flush=True)
 if failure:raise RuntimeError('Stopped; inspect retained failure, never automatic same-version recovery')


if __name__=='__main__':main()
