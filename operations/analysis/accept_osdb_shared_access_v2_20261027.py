"""Bounded real Bolt acceptance on one NEW owned semantic-only tiny store.

No model calls, legacy store mutation, shared-node service change or query retry.
The exposed synthetic expectations are fixed before boot, not independent EX.
"""
from collections import Counter
from copy import deepcopy
import hashlib,json,os,shutil,socket,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,r'D:\ch7\tools\python_deps\neo4j_5_15')
import requests
from neo4j import GraphDatabase
from accept_osdb_actor_graph_references_20261026 import stop_owned,live,budget,runtime,now
from observed_profile_semantic_cases import PEOPLE,EDGES,SCHEMA,cases
import observed_neo4j_services_dev_v2 as compiler
from osdb_common_input_v2 import messages,public_profile
from osdb_shared_read_executor_dev import SharedReadExecutor,AdmissionError,digest

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OUT=ROOT/'reports/osdb_shared_access_dev_v2_20261027'
HOME=Path('D:/Program Files (x86)/neo4j-community-5.15.0')
HTTP=18497;BOLT=18498


def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,x):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(x,f,ensure_ascii=False,indent=2);f.write('\n')
def bag(rows):return Counter(json.dumps(x,sort_keys=True) for x in rows)


def main():
 assert not OUT.exists() and not live(HTTP) and not live(BOLT)
 assert shutil.disk_usage('D:/').free>1024**3
 before_budget=budget();before_runtime=runtime();assert not before_budget['pending']
 freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/n)==v for n,v in freeze.items())
 assert not (ROOT/'experiments/ch7_private/continuation_20261021/budget/.t1-reservation.lock').exists()
 schema=deepcopy(SCHEMA);schema['entities']=schema['entities'][:1];schema=public_profile(schema)
 tasks=[t for t in cases() if t['name']!='integer_key_intersection'];assert len(tasks)==13
 native=[
  {'name':'native_null','query':'MATCH(n:Person) WHERE n.eligible = false RETURN n.key AS id','columns':['id'],'rows':[['b'],['d']]},
  {'name':'native_union','query':"MATCH(n:Person {key:'a'}) RETURN n.key AS id UNION MATCH(n:Person {key:'b'}) RETURN n.key AS id",'columns':['id'],'rows':[['a'],['b']]},
  {'name':'native_read_subquery','query':'CALL { MATCH(n:Person) RETURN count(n) AS n } RETURN n','columns':['n'],'rows':[[6]]},
  {'name':'physical_labels','query':'MATCH(n) UNWIND labels(n) AS label RETURN DISTINCT label','columns':['label'],'rows':[['Person']]},
  {'name':'native_property_map','query':"MATCH(n:Person {key:'a'}) RETURN properties(n) AS props",'columns':['props'],'rows':[[PEOPLE[0]]]}]
 rejected=['MATCH(n) SET n.key="changed" RETURN n','CALL db.labels() YIELD label RETURN label','RETURN 1; RETURN 2']
 OUT.mkdir();sources=[Path(__file__),Path(compiler.__file__)]+[Path(__file__).with_name(n) for n in (
  'osdb_shared_read_executor_dev.py','osdb_common_input.py','osdb_common_input_v2.py',
  'observed_profile_semantic_cases.py','test_osdb_shared_access_dev.py','requirements_osdb_access_dev.txt')]
 (OUT/'executed_sources').mkdir()
 for p in sources:shutil.copyfile(p,OUT/'executed_sources'/p.name)
 write(OUT/'protocol.json',{'sealed_local':now(),'kind':'EXPOSED_SYNTHETIC_SHARED_ACCESS_DEVELOPMENT_NOT_FORMAL',
  'sources':{p.name:sha(p) for p in sources},'schema':schema,'fixture':{'people':PEOPLE,'edges':EDGES},
  'compiler_cases':tasks,'strategies':['contract','full_detail'],'native_cases':native,'guard_rejections':rejected,
  'planned_compiler_strategy_checks':26,'planned_native_checks':5,'planned_guard_checks':3,
  'planned_dirty_snapshot_rejections':2,'planned_dirty_arm_blocks':4,'planned_cap_checks':1,
  'budget_before':before_budget,'adapter_before':before_runtime,'canonical_before':freeze,
  'readiness_max3_at_least10_seconds':True,'no_model_calls_or_statement_retry':True,
  'admission_audit_timeout_seconds':240,'formal_query_timeouts_seconds':{'explain':30,'read':60},
  'scope':'Only this new private fixture; real four graph materialization and full task/scoring freeze remain pending.'})
 for folder in ('conf','data','logs','run','plugins'):(OUT/folder).mkdir()
 config=['server.default_listen_address=127.0.0.1',f'server.http.listen_address=127.0.0.1:{HTTP}',
  'server.http.enabled=true','server.https.enabled=false','server.bolt.enabled=true',f'server.bolt.listen_address=127.0.0.1:{BOLT}',
  f'server.bolt.advertised_address=127.0.0.1:{BOLT}','dbms.security.auth_enabled=false','dbms.default_database=neo4j',
  'server.memory.heap.initial_size=256m','server.memory.heap.max_size=512m','server.memory.pagecache.size=128m',
  'dbms.security.allow_csv_import_from_file_urls=false','server.jvm.additional=-XX:ActiveProcessorCount=2']
 for folder in ('data','logs','run','plugins'):config.append('server.directories.'+folder+'='+str(OUT/folder).replace('\\','/'))
 (OUT/'conf/neo4j.conf').write_text('\n'.join(config)+'\n',encoding='utf-8')
 launch=OUT/'launch_owned.ps1';launch.write_text("& '"+str(HOME/'bin/neo4j.bat')+"' console\n",encoding='utf-8')
 session=requests.Session();session.trust_env=False;posts=0;gets=0;executors=[];results=[];failure=None;driver=None
 def setup(q,params):
  nonlocal posts
  posts+=1;p=OUT/'trusted_setup'/f'{posts:04d}';p.mkdir(parents=True)
  payload={'statements':[{'statement':q,'parameters':params,'resultDataContents':['row']}]}
  write(p/'request.json',payload);start=time.perf_counter();t={}
  try:
   r=session.post(f'http://127.0.0.1:{HTTP}/db/neo4j/tx/commit',json=payload,timeout=(3,30))
   t.update(http_status=r.status_code,body=r.json());assert r.status_code==200 and not t['body']['errors']
   return [v['row'] for v in t['body']['results'][0]['data']]
  except Exception as exc:t.update(error_type=type(exc).__name__,error=str(exc));raise
  finally:t['client_seconds']=time.perf_counter()-start;write(p/'terminal.json',t)
 def new_executor(name,**kwargs):
  x=SharedReadExecutor(driver,schema,OUT/name,**kwargs);executors.append(x);return x
 def accept(name,result,columns,rows):
  # The compiler certifies each physical observation; its composed result has
  # no top-level complete field. Keep the original v1 harness error archived.
  complete=result.get('complete') if 'complete' in result else bool(result.get('observations')) and all(o.get('complete') is True for o in result['observations'])
  ok=complete is True and result['status']=='EXECUTED' and result['columns']==columns and bag(result['rows'])==bag(rows)
  row={'name':name,'matches':ok,'result':result,'expected_columns':columns,'expected_rows':rows}
  write(OUT/'checks'/(name+'.json'),row);results.append({'name':name,'passed':ok});assert ok,name
 stdout=(OUT/'console_stdout.log').open('wb');stderr=(OUT/'console_stderr.log').open('wb')
 process=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(launch)],
  env=dict(os.environ,NEO4J_CONF=str(OUT/'conf'),NEO4J_HOME=str(HOME)),stdout=stdout,stderr=stderr,creationflags=subprocess.CREATE_NO_WINDOW)
 write(OUT/'execution_owner.json',{'host':socket.gethostname(),'wrapper_pid':process.pid,'config':str(OUT/'conf'),'ports':[HTTP,BOLT]})
 try:
  ready=False
  for i in range(3):
   time.sleep(12 if i==0 else 10);gets+=1;t={'attempt':i+1}
   try:r=session.get(f'http://127.0.0.1:{HTTP}/',timeout=(2,3));t['status']=r.status_code;ready=r.status_code==200
   except requests.RequestException as exc:t['error']=type(exc).__name__
   write(OUT/f'readiness_{i+1}.json',t)
   if ready:break
   if process.poll() is not None:break
  assert ready,'Readiness failed; no blind restart'
  assert setup('MATCH(n) RETURN count(n)',{})==[[0]]
  setup('UNWIND $rows AS p CREATE (n:Person:PilotNode) SET n=p,n.__pilot_uid=p.key',{'rows':PEOPLE})
  setup('UNWIND $rows AS e MATCH(a:Person {key:e.a}),(b:Person {key:e.b}) CREATE(a)-[r:LINK]->(b) SET r.physical_key=e.key,r.clock=e.clock',{'rows':EDGES})
  driver=GraphDatabase.driver(f'bolt://127.0.0.1:{BOLT}',auth=None,connection_timeout=3,max_transaction_retry_time=0,max_connection_pool_size=1)
  for label in ('dirty_labels','dirty_property'):
   x=new_executor(label)
   try:x.admit()
   except AdmissionError as exc:write(OUT/(label+'_expected_rejection.json'),{'expected':True,'reason':str(exc)})
   else:raise AssertionError('Dirty snapshot admitted')
   for arm in ('direct','ours'):
    try:x.execute('MATCH(n:Person) RETURN n.key')
    except AdmissionError:results.append({'name':label+'_'+arm+'_blocked','passed':True})
    else:raise AssertionError('Dirty arm could read')
   if label=='dirty_labels':setup('MATCH(n:Person) REMOVE n:PilotNode',{})
  setup('MATCH(n:Person) REMOVE n.__pilot_uid',{})
  x=new_executor('clean_shared');receipt=x.admit()
  hashes=[digest(['node','Person',p]) for p in PEOPLE]
  hashes += [digest(['edge','LINK',('Person',e['a']),('Person',e['b']),{'physical_key':e['key'],'clock':e['clock']}]) for e in EDGES]
  assert receipt['semantic_snapshot_sha256']==hashlib.sha256(''.join(sorted(hashes)).encode()).hexdigest()
  for task in tasks:
   for strategy in ('contract','full_detail'):
    r=compiler.execute_plan(task['plan'],schema,x.execute,strategy=strategy)
    accept(task['name']+'_'+strategy,r,task['expected_columns'],task['expected_rows'])
  for task in native:accept(task['name'],x.execute(task['query']),task['columns'],task['rows'])
  n=x.number
  for i,q in enumerate(rejected):
   try:x.execute(q)
   except ValueError:results.append({'name':'preflight_guard_'+str(i),'passed':True})
   else:raise AssertionError('Read guard allowed write/procedure/multiple statement')
  assert x.number==n
  cap=new_executor('small_row_cap',max_rows=2);cap.admit();r=cap.execute('MATCH(n:Person) RETURN n.key AS id')
  assert not r['complete'] and r['status']=='TRUNCATED' and len(r['rows'])==2
  assert 'LIMIT' not in read(cap.folder/f'{cap.number:04d}_read/request.json')['query']
  results.append({'name':'incomplete_output_no_limit_rewrite','passed':True})
  assert setup('MATCH(n:Person) RETURN count(n),sum(CASE WHEN n.key="changed" THEN 1 ELSE 0 END)',{})==[[6,0]]
 except Exception as exc:failure={'type':type(exc).__name__,'message':str(exc)};write(OUT/'failure.json',failure)
 finally:
  if driver:driver.close()
  receipt=stop_owned(process,OUT/'conf',HTTP);receipt['bolt_port_closed']=not live(BOLT);assert receipt['bolt_port_closed']
  write(OUT/'shutdown_receipt.json',receipt);stdout.close();stderr.close();session.close()
 after_budget=budget();assert before_budget==after_budget and before_runtime==runtime()
 assert all(sha(CANON/n)==v for n,v in freeze.items())
 terminals=[read(p) for x in executors for p in x.folder.glob('*/terminal.json')]
 summary={'kind':'SYNTHETIC_SHARED_ACCESS_DEVELOPMENT_NOT_FORMAL','checks_passed':sum(r['passed'] for r in results),'checks':results,
  'planned_compiler_strategy_checks':26,'completed_compiler_strategy_checks':sum(r['name'].endswith(('_contract','_full_detail')) for r in results),
  'planned_native_checks':5,'completed_native_checks':sum(r['name'] in {t['name'] for t in native} for r in results),
  'expected_dirty_admission_rejections':2,'completed_expected_dirty_admission_rejections':sum((OUT/(label+'_expected_rejection.json')).exists() for label in ('dirty_labels','dirty_property')),
  'readiness_HTTP':gets,'trusted_setup_and_integrity_POST':posts,'Bolt_Cypher_statements_including_admission_explain_reads':len(terminals),
  'Bolt_expected_admission_errors':sum(not t['complete'] for t in terminals),'Bolt_total_client_seconds':sum(t['client_seconds'] for t in terminals),
  'no_automatic_statement_retry':True,'physical_statement_not_network_packet_count':True,'failure':failure,
  'budget_before':before_budget,'budget_after':after_budget,'model_calls':0,'training':0,
  'new_owned_JVM_stopped':True,'canonical32_and_adapter46_unchanged':True,'real_four_graphs_admitted':0,
  'formal_admission':False,'entire_T1_complete':False}
 write(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True),flush=True)
 if failure:raise RuntimeError('Acceptance stopped; preserve failed evidence')


if __name__=='__main__':main()
