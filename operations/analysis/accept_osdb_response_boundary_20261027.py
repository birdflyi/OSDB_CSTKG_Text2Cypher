"""Real tiny DB + prewritten response replay, not live model inference or EX."""
from copy import deepcopy
import hashlib,json,os,shutil,socket,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,r'D:\ch7\tools\python_deps\neo4j_5_15')
import requests
from neo4j import GraphDatabase
from accept_osdb_actor_graph_references_20261026 import HOME,budget,runtime,live,stop_owned,now
from observed_profile_semantic_cases import PEOPLE,EDGES,SCHEMA,call,single
from osdb_common_input_v3 import messages
from osdb_response_boundary_dev import replay_case,score_numeric
from osdb_shared_read_executor_dev import SharedReadExecutor
from osdb_model_read_executor_dev_v2 import ModelReadExecutor

ROOT=Path('D:/ch7');OUT=ROOT/'reports/osdb_response_boundary_live_v1_20261027'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1';HTTP=18505;BOLT=18506


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,x):
 p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(x,f,ensure_ascii=False,indent=2);f.write('\n')
def reply(obj):return {'finish_reason':'stop','content':obj if type(obj) is str else json.dumps(obj)}


def scenarios():
 direct={'status':'ANSWER','query':'MATCH(n:Person) WHERE n.eligible=false RETURN count(n) AS n'}
 c=call();c['where']=[{'field':'n.eligible','op':'eq','value':False}];c['returns']=[{'field':'n.key','agg':'count','alias':'n'}]
 ours=single(c);wrong=deepcopy(ours);wrong['calls'][0]['where']=[]
 bad=deepcopy(ours);bad['calls'][0]['returns'][0]['field']='n.phantom'
 rows=[]
 def add(name,arm,replies,status='EXECUTED',expected=None,score=True,consumed=1,**extra):
  rows.append(dict(name=name,arm=arm,replies=[reply(x) for x in replies],status=status,expected_rows=[[2]] if expected is None and score is not None else expected,
   expected_numeric_EX=score,consumed=consumed,**extra))
 add('direct_correct','direct',[direct]);add('ours_correct','ours',[ours])
 add('direct_parser_repair','direct',['{',direct],consumed=2);add('ours_parser_repair','ours',['{',ours],consumed=2)
 add('direct_syntax_repair_same_snapshot','direct',[{'status':'ANSWER','query':'MATCH(n:Person RETURN n.key AS id'},direct],consumed=2)
 add('ours_plan_repair','ours',[bad,ours],consumed=2)
 add('direct_valid_wrong_no_repair','direct',[{'status':'ANSWER','query':'RETURN 999 AS n'},direct],score=False)
 add('ours_valid_wrong_no_repair','ours',[wrong,ours],score=False)
 for arm in ('direct','ours'):
  add(arm+'_insufficient',arm,[{'status':'INSUFFICIENT_EVIDENCE','reason':'Fixture has no full prior history'}],status='INSUFFICIENT_EVIDENCE',score=None)
 add('direct_catalog_repair','direct',[{'status':'ANSWER','query':'SHOW INDEXES YIELD name RETURN name'},direct],consumed=2)
 add('direct_complete_empty','direct',[{'status':'ANSWER','query':'MATCH(n:Person {key:"absent"}) RETURN n.key AS id'}],expected=[],question='List fixture accounts with key absent')
 add('shared_plan_full_detail','ours',[ours],strategy='full_detail',shared_plan_with='ours_correct')
 add('direct_output_cap','direct',[{'status':'ANSWER','query':'MATCH(n:Person) RETURN n.key AS id'}],status='TRUNCATED_RESULT',expected=[[p['key']] for p in PEOPLE],score=False,small_cap=True)
 add('ours_output_cap','ours',[single(call())],status='TRUNCATED_RESULT',expected=[[p['key']] for p in PEOPLE],score=False,small_cap=True)
 return rows


def main():
 assert not OUT.exists() and not live(HTTP) and not live(BOLT)
 before_budget=budget();before_runtime=runtime();assert not before_budget['pending']
 frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/p)==h for p,h in frozen.items())
 schema=deepcopy(SCHEMA);schema['entities']=schema['entities'][:1];tasks=scenarios();assert len(tasks)==15
 OUT.mkdir();(OUT/'executed_sources').mkdir()
 sources=[Path(__file__)]+[Path(__file__).with_name(n) for n in ('osdb_common_input_v3.py','osdb_response_boundary_dev.py',
  'osdb_model_read_executor_dev_v2.py','osdb_shared_read_executor_dev.py','observed_neo4j_services_dev_v2.py','observed_profile_semantic_cases.py')]
 for p in sources:shutil.copyfile(p,OUT/'executed_sources'/p.name)
 write(OUT/'protocol.json',{'sealed_local':now(),'kind':'PREWRITTEN_RESPONSE_REPLAY_REAL_TINY_DB_NOT_MODEL_PREDICTIONS',
  'sources':{p.name:sha(p) for p in sources},'schema':schema,'fixture':{'people':PEOPLE,'edges':EDGES},
  'planned_scenarios':15,'scenarios':tasks,'baseline_probes':['old_catalog_read_classification','old_syntax_error_latch'],
  'budget_before':before_budget,'runtime_before':before_runtime,'canonical_before':frozen,
  'no_model_client_or_api_call':True,'no_historical_graph_reuse_or_scan':True,'formal_admission':False})
 for d in ('conf','data','logs','run','plugins'):(OUT/d).mkdir()
 config=['server.default_listen_address=127.0.0.1',f'server.http.listen_address=127.0.0.1:{HTTP}',
  'server.http.enabled=true','server.https.enabled=false','server.bolt.enabled=true',
  f'server.bolt.listen_address=127.0.0.1:{BOLT}',f'server.bolt.advertised_address=127.0.0.1:{BOLT}',
  'dbms.security.auth_enabled=false','initial.dbms.default_database=neo4j','server.memory.heap.initial_size=256m',
  'server.memory.heap.max_size=512m','server.memory.pagecache.size=128m','server.jvm.additional=-XX:ActiveProcessorCount=2']
 for d in ('data','logs','run','plugins'):config.append('server.directories.'+d+'='+str(OUT/d).replace('\\','/'))
 (OUT/'conf/neo4j.conf').write_text('\n'.join(config)+'\n',encoding='utf-8')
 launch=OUT/'launch_owned.ps1';launch.write_text("& '"+str(HOME/'bin/neo4j.bat')+"' console\n",encoding='utf-8')
 stdout=(OUT/'console_stdout.log').open('wb');stderr=(OUT/'console_stderr.log').open('wb')
 process=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(launch)],
  env=dict(os.environ,NEO4J_HOME=str(HOME),NEO4J_CONF=str(OUT/'conf')),stdout=stdout,stderr=stderr,creationflags=subprocess.CREATE_NO_WINDOW)
 write(OUT/'execution_owner.json',{'host':socket.gethostname(),'runner_pid':os.getpid(),'wrapper_pid':process.pid,'config':str(OUT/'conf'),'ports':[HTTP,BOLT]})
 session=requests.Session();session.trust_env=False;driver=None;gets=0;posts=0;executors=[];accepted=[];attempted=0;failure=None;probes={}
 def setup(q,p):
  nonlocal posts
  posts+=1;folder=OUT/'setup'/f'{posts:04d}';folder.mkdir(parents=True);payload={'statements':[{'statement':q,'parameters':p,'resultDataContents':['row']}]}
  write(folder/'request.json',payload);t={};start=time.perf_counter()
  try:
   r=session.post(f'http://127.0.0.1:{HTTP}/db/neo4j/tx/commit',json=payload,timeout=(3,30));t.update(http_status=r.status_code,body=r.json());assert r.status_code==200 and not t['body']['errors']
   return [v['row'] for v in t['body']['results'][0]['data']]
  finally:t['client_seconds']=time.perf_counter()-start;write(folder/'terminal.json',t)
 def executor(name,cls=ModelReadExecutor,**kwargs):
  x=cls(driver,schema,OUT/name,**kwargs);executors.append(x);x.admit();return x
 try:
  ready=False
  for i in range(3):
   time.sleep(12 if i==0 else 10);gets+=1;t={'attempt':i+1}
   try:r=session.get(f'http://127.0.0.1:{HTTP}/',timeout=(2,3));t['status']=r.status_code;ready=r.status_code==200
   except requests.RequestException as exc:t['error']=type(exc).__name__
   write(OUT/f'readiness_{i+1}.json',t)
   if ready:break
   if process.poll() is not None:break
  assert ready,'No automatic readiness recovery'
  assert setup('MATCH(n) RETURN count(n)',{})==[[0]]
  setup('UNWIND $rows AS p CREATE(n:Person) SET n=p',{'rows':PEOPLE})
  setup('UNWIND $rows AS e MATCH(a:Person {key:e.a}),(b:Person {key:e.b}) CREATE(a)-[r:LINK]->(b) SET r.physical_key=e.key,r.clock=e.clock',{'rows':EDGES})
  setup('CREATE INDEX pilot_uid_fixture FOR(n:PilotNode) ON(n.__pilot_uid)',{})
  driver=GraphDatabase.driver(f'bolt://127.0.0.1:{BOLT}',auth=None,connection_timeout=3,max_transaction_retry_time=0,max_connection_pool_size=1)
  old=executor('old_catalog',SharedReadExecutor)
  try:
   r=old.execute('SHOW INDEXES YIELD name RETURN name');probes['old_catalog']={'allowed':True,'rows':len(r['rows']),'query_type_r':True}
  except Exception as exc:probes['old_catalog']={'allowed':False,'error_type':type(exc).__name__}
  old=executor('old_syntax',SharedReadExecutor)
  try:old.execute('MATCH(n:Person RETURN n.key AS id')
  except Exception as exc:probes['old_syntax_error']=type(exc).__name__
  probes['old_syntax_failed_latch']=old.failed
  try:old.execute('RETURN 1 AS n')
  except Exception as exc:probes['old_repair_blocked']=type(exc).__name__
  assert probes['old_syntax_failed_latch'] and 'old_repair_blocked' in probes
  write(OUT/'baseline_probes.json',probes)
  current=executor('new_model_boundary');small=None
  for task in tasks:
   attempted+=1
   if task.get('small_cap'):
    if small is None:small=executor('new_small_cap',max_rows=2)
    active=small
   else:active=current
   initial=messages(task['arm'],task_id=task['name'],graph='synthetic',question=task.get('question','Count fixture Person accounts with eligible=false'),schema=schema)['messages']
   result=replay_case(task['arm'],initial,schema,task['replies'],active.execute,strategy=task.get('strategy','contract'))
   score=score_numeric(result['final'],task['expected_rows'])
   ok=result['final']['status']==task['status'] and result['response_records_consumed']==task['consumed'] and score['numeric_EX'] is task['expected_numeric_EX']
   write(OUT/'scenarios'/(task['name']+'.json'),{'replay':result,'score':score,'expected_status':task['status'],'accepted':ok})
   assert ok,task['name'];accepted.append(task['name'])
 except Exception as exc:failure={'type':type(exc).__name__,'message':str(exc)[:1500]};write(OUT/'failure.json',failure)
 finally:
  if driver:driver.close()
  closed=stop_owned(process,OUT/'conf',HTTP);closed['bolt_port_closed']=not live(BOLT);assert closed['bolt_port_closed']
  write(OUT/'shutdown_receipt.json',closed);session.close();stdout.close();stderr.close()
 after_budget=budget();assert before_budget==after_budget and runtime()==before_runtime and all(sha(CANON/p)==h for p,h in frozen.items())
 physical=[read(p) for x in executors for p in x.folder.glob('*/terminal.json')]
 summary={'kind':'SYNTHETIC_RESPONSE_REPLAY_ACTUAL_DB_NOT_MODEL_RESULTS','planned_scenarios':15,'dispatched_scenarios':attempted,
  'accepted_scenarios':len(accepted),'failed_scenarios':attempted-len(accepted),'undispatched_scenarios':15-attempted,
  'baseline_probes':probes,'HTTP_readiness':gets,'HTTP_fixture_POST':posts,'Bolt_Cypher_statements':len(physical),
  'Bolt_statement_errors':sum(not t['complete'] for t in physical),'Bolt_client_seconds':sum(t['client_seconds'] for t in physical),
  'failure':failure,'model_calls':0,'prewritten_response_records_are_not_model_calls':True,'budget_before':before_budget,'budget_after':after_budget,
  'new_owned_JVM_stopped':True,'canonical32_and_adapter46_unchanged':True,'formal_admission':False,'entire_T1_complete':False}
 write(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True),flush=True)
 if failure:raise RuntimeError('Stop and preserve failed tiny replay evidence')


if __name__=='__main__':main()
