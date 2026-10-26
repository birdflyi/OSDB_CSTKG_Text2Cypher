"""One audited new clean-copy attempt with a 600s OFFLINE scan budget.

Preserve the 240s failure and completed cleanup. Do not replay cleanup, change
formal read limits, resume model batches or mutate the stopped source copy.
"""
import hashlib,json,os,shutil,socket,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,r'D:\ch7\tools\python_deps\neo4j_5_15')
import requests
from neo4j import GraphDatabase
from accept_osdb_actor_graph_references_20261026 import HOME,stores,store_directories,stop_owned,live,budget,runtime,now
from osdb_shared_read_executor_dev import SharedReadExecutor

ROOT=Path('D:/ch7');BASE=ROOT/'reports/osdb_semantic_snapshots_v1_20261027'
SOURCE=BASE/'runtime/elasticsearch';OUT=ROOT/'reports/osdb_elasticsearch_offline_scan_v2_20261027'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1';HTTP=18503;BOLT=18504


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,x):
 p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(x,f,ensure_ascii=False,indent=2);f.write('\n')


def offline_timeout(kind,timeout):
 if kind in ('admit_nodes','admit_edges'):
  assert timeout==240,'Only the original fixed offline admission budget can be extended'
  return 600
 return timeout


class OfflineScanExecutor(SharedReadExecutor):
 def _statement(self,query,params,kind,consumer=None,timeout=60):
  return super()._statement(query,params,kind,consumer,timeout=offline_timeout(kind,timeout))


def main():
 assert not OUT.exists() and all(not live(p) for p in (18501,18502,HTTP,BOLT))
 failed=read(BASE/'summary.json');audit=read(BASE/'failed_elasticsearch_audit.json');stop=read(SOURCE/'shutdown_receipt.json')
 assert failed['completed_graphs']==3 and 'TransactionTimedOutClientConfiguration' in failed['failure']['message']
 assert audit['source_original_files_unchanged'] and audit['cleanup_transactions_complete']==66 and audit['cleaned_nodes']==130413
 assert stop['port_closed'] and stop['bolt_port_closed']
 before_budget=budget();before_runtime=runtime();assert before_budget==failed['budget_after'] and not before_budget['pending']
 frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/p)==h for p,h in frozen.items())
 expected=read(SOURCE/'csv_semantic_fingerprint.json');source_before=stores(SOURCE)
 assert shutil.disk_usage('D:/').free>sum(v['bytes'] for v in source_before.values())+4*1024**3
 registry_path=ROOT/'reports/osdb_actor_candidate_intake_v1_20261026/candidate_registry.json'
 assert sha(registry_path)==read(BASE/'protocol.json')['registry_sha256']
 tasks=[t for t in read(registry_path) if t['project']=='elasticsearch' and t['reference_query'] is not None];assert len(tasks)==4
 profile_path=ROOT/'reports/osdb_observed_profile_dev_v2_20261027/elasticsearch_profile.json'
 assert sha(profile_path)==read(BASE/'protocol.json')['profiles_sha256']['elasticsearch']
 schema=read(profile_path);OUT.mkdir();(OUT/'executed_sources').mkdir()
 sources=[Path(__file__),Path(__file__).with_name('osdb_shared_read_executor_dev.py')]
 for p in sources:shutil.copyfile(p,OUT/'executed_sources'/p.name)
 write(OUT/'protocol.json',{'sealed_local':now(),'kind':'ONE_OFFLINE_SCAN_RESOURCE_RECOVERY_NOT_MODEL_OR_METHOD_CHANGE',
  'sources':{p.name:sha(p) for p in sources},'previous_failure_sha256':sha(BASE/'failure.json'),
  'previous_failure_audit_sha256':sha(BASE/'failed_elasticsearch_audit.json'),'source_copy':str(SOURCE),
  'source_snapshot_manifest':source_before,'source_expected_sha256':sha(SOURCE/'csv_semantic_fingerprint.json'),
  'offline_transaction_timeout_seconds':600,'unchanged_explain_read_timeout_seconds':[30,60],
  'planned_reference_checks':4,'cleanup_replay_count':0,'previous_complete_cleanup_retained':True,
  'no_statement_retry_or_future_auto_recovery':True,'budget_before':before_budget,
  'runtime_before':before_runtime,'canonical_before':frozen,'formal_admission':False})
 write(OUT/'source_store_manifest_before.json',source_before)
 for directory in store_directories(SOURCE):shutil.copytree(SOURCE/directory,OUT/directory)
 assert stores(OUT)==source_before
 for d in ('conf','logs','run','plugins'):(OUT/d).mkdir()
 config=['server.default_listen_address=127.0.0.1',f'server.http.listen_address=127.0.0.1:{HTTP}',
  'server.http.enabled=true','server.https.enabled=false','server.bolt.enabled=true',
  f'server.bolt.listen_address=127.0.0.1:{BOLT}',f'server.bolt.advertised_address=127.0.0.1:{BOLT}',
  'dbms.security.auth_enabled=false','initial.dbms.default_database=neo4j','server.memory.heap.initial_size=256m',
  'server.memory.heap.max_size=512m','server.memory.pagecache.size=128m',
  'dbms.security.allow_csv_import_from_file_urls=false','server.jvm.additional=-XX:ActiveProcessorCount=2']
 for d in ('data','logs','run','plugins'):config.append('server.directories.'+d+'='+str(OUT/d).replace('\\','/'))
 if 'transactions' in store_directories(SOURCE):config.append('server.directories.transaction.logs.root='+str(OUT/'transactions').replace('\\','/'))
 (OUT/'conf/neo4j.conf').write_text('\n'.join(config)+'\n',encoding='utf-8')
 launch=OUT/'launch_owned.ps1';launch.write_text("& '"+str(HOME/'bin/neo4j.bat')+"' console\n",encoding='utf-8')
 stdout=(OUT/'console_stdout.log').open('wb');stderr=(OUT/'console_stderr.log').open('wb')
 process=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(launch)],
  env=dict(os.environ,NEO4J_CONF=str(OUT/'conf'),NEO4J_HOME=str(HOME)),stdout=stdout,stderr=stderr,creationflags=subprocess.CREATE_NO_WINDOW)
 write(OUT/'execution_owner.json',{'hostname':socket.gethostname(),'runner_pid':os.getpid(),'wrapper_pid':process.pid,'config':str(OUT/'conf'),'ports':[HTTP,BOLT]})
 session=requests.Session();session.trust_env=False;driver=None;failure=None;accepted=[];dispatched=0;gets=0;executor=None;admitted=None
 try:
  ready=False
  for i in range(3):
   time.sleep(12 if i==0 else 10);gets+=1;t={'attempt':i+1}
   try:r=session.get(f'http://127.0.0.1:{HTTP}/',timeout=(2,3));t['http_status']=r.status_code;ready=r.status_code==200
   except requests.RequestException as exc:t['error']=type(exc).__name__
   write(OUT/f'readiness_{i+1}.json',t)
   if ready:break
   if process.poll() is not None:break
  assert ready,'Readiness failed, no further automatic recovery'
  driver=GraphDatabase.driver(f'bolt://127.0.0.1:{BOLT}',auth=None,connection_timeout=3,max_transaction_retry_time=0,max_connection_pool_size=1)
  executor=OfflineScanExecutor(driver,schema,OUT/'shared_access');admitted=executor.admit()
  assert all(admitted[k]==expected[k] for k in ('nodes','edges','semantic_snapshot_sha256'))
  write(OUT/'full_graph_equivalence.json',{'expected':expected,'admitted':admitted,'matches':True})
  for t in tasks:
   dispatched+=1;result=executor.execute(t['reference_query'],t['parameters'])
   matches=result['complete'] and result['rows']==t['csv_expected_rows']
   write(OUT/'reference_checks'/(t['task_id']+'.json'),{'result':result,'expected_rows':t['csv_expected_rows'],'matches':matches})
   assert matches,'Reference differs; stop and preserve'
   accepted.append(t['task_id'])
 except Exception as exc:failure={'type':type(exc).__name__,'message':str(exc)[:1500]};write(OUT/'failure.json',failure)
 finally:
  if driver:driver.close()
  closed=stop_owned(process,OUT/'conf',HTTP);closed['bolt_port_closed']=not live(BOLT);assert closed['bolt_port_closed']
  write(OUT/'shutdown_receipt.json',closed);stdout.close();stderr.close();session.close()
 assert stores(SOURCE)==source_before;write(OUT/'source_store_manifest_after.json',source_before)
 write(OUT/'new_store_manifest_after_shutdown.json',stores(OUT))
 after_budget=budget();assert before_budget==after_budget and runtime()==before_runtime
 assert all(sha(CANON/p)==h for p,h in frozen.items())
 terminals=[read(p) for p in (OUT/'shared_access').glob('*/terminal.json')]
 summary={'kind':'ELASTICSEARCH_OFFLINE_RESOURCE_RECOVERY_NOT_MODEL_RESULTS','accepted_graphs':0 if failure else 1,
  'planned_reference_checks':4,'dispatched_reference_checks':dispatched,'accepted_reference_checks':len(accepted),
  'failed_reference_checks':dispatched-len(accepted),'undispatched_reference_checks':4-dispatched,
  'full_graph_fingerprint':admitted,'previous_failed_run_preserved':True,'cleanup_replayed':0,
  'HTTP_readiness':gets,'Bolt_Cypher_statements':len(terminals),'Bolt_client_seconds':sum(t['client_seconds'] for t in terminals),
  'failure':failure,'new_model_calls':0,'budget_before':before_budget,'budget_after':after_budget,
  'canonical32_and_adapter46_unchanged':True,'source_clean_copy_unchanged':True,'new_owned_JVM_stopped':True,
  'formal_N':None,'formal_admission':False,'entire_T1_complete':False}
 write(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True),flush=True)
 if failure:raise RuntimeError('Recovery stopped; no automatic third attempt')


if __name__=='__main__':main()
