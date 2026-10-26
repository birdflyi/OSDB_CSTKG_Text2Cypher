"""Read-only acceptance on NEW isolated copies of four stopped owned stores.

Never launch legacy C: scripts, migrate shared owners, clear locks, import data,
call models or modify old graph stores. One new local DB at a time, own children
only. Record every physical HTTP request, no query retry after unknown outcome.
"""
from datetime import datetime,timezone,timedelta
import hashlib,json,os,shutil,socket,subprocess,sys,time
from pathlib import Path

import psutil
import requests

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
BASE=ROOT/'experiments/ch7_private/continuation_20261021';PRIV=ROOT/'experiments/ch7_private'
OUT=ROOT/'reports/osdb_actor_graph_acceptance_v2_20261026'
HOME=Path('D:/Program Files (x86)/neo4j-community-5.15.0')
SOURCES={
 'redis':PRIV/'automated_dev_pilot_v1_20261021/neo4j_runtime',
 'duckdb':PRIV/'publication_duckdb_runtime_v1_20261021',
 'iotdb':BASE/'e8_iotdb_runtime_v1',
 'elasticsearch':BASE/'e8_elasticsearch_runtime_v1'}
REGISTRY=ROOT/'reports/osdb_actor_candidate_intake_v1_20261026/candidate_registry.json'
PORTS=dict(zip(SOURCES,(18475,18476,18477,18478)))


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8*1024**2),b''):h.update(b)
 return h.hexdigest()
def write(p,x):
 p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('x',encoding='utf-8') as f:json.dump(x,f,ensure_ascii=False,indent=2);f.write('\n')
def now():return datetime.now(timezone(timedelta(hours=8))).isoformat()
def live(port):
 with socket.socket() as s:s.settimeout(0.5);return s.connect_ex(('127.0.0.1',port))==0
def store_directories(root):
 assert (root/'data').is_dir()
 if (root/'transactions').is_dir():return ('data','transactions')
 assert (root/'data/transactions').is_dir(),'Default transaction directory must be present, never silently omit it'
 return ('data',)
def stores(root):
 result={}
 for directory in store_directories(root):
  assert (root/directory).is_dir()
  for p in (root/directory).rglob('*'):
   assert os.path.commonpath([str(root.resolve()),str(p.resolve())])==str(root.resolve())
   if p.is_file():result[str(p.relative_to(root))]={'sha256':sha(p),'bytes':p.stat().st_size}
 return result
def runtime():
 folder=REPO/'experiment-harness/public_benchmark_v1'
 return {str(p.relative_to(folder)):sha(p) for p in folder.rglob('*.py')}
def budget():
 sys.path.insert(0,str(CANON/'experiment-harness/public_benchmark_v1'))
 from attempt_budget import AttemptBudget
 x=AttemptBudget(BASE/'budget',ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
  path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',PRIV)]).inspect()
 return {k:x[k] for k in ('reserved','cap','remaining','pending')}


def stop_owned(process,conf,port):
 parent=psutil.Process(process.pid) if process.poll() is None else None
 children=parent.children(recursive=True) if parent else []
 verified=[]
 for child in children:
  try:
   command=' '.join(child.cmdline()).replace('\\','/').casefold()
   if child.name().lower() in ('java.exe','javaw.exe') and str(conf).replace('\\','/').casefold() in command:
    verified.append(child)
  except psutil.NoSuchProcess:pass
 if live(port) and not verified:raise RuntimeError('Cannot establish owned DB child identity; never kill unrelated process')
 pids=[p.pid for p in verified]
 for p in reversed(verified):
  try:p.terminate()
  except psutil.NoSuchProcess:pass
 _,alive=psutil.wait_procs(verified,timeout=15)
 for p in alive:p.kill()
 if parent:
  try:process.wait(timeout=10)
  except subprocess.TimeoutExpired:
   # This exact Popen wrapper belongs to this audit, never a general PS kill.
   process.terminate();process.wait(timeout=5)
 assert not live(port),'Owned DB port remains active; do not start next DB'
 return {'verified_db_child_pids_stopped':pids,'wrapper_pid':process.pid,'port_closed':True}


def main():
 assert not OUT.exists(),'Preserve previous acceptance run'
 assert all(not live(p) for p in list(PORTS.values())+[17475,17476,17477,17478])
 assert not any(p.name().lower() in ('java.exe','javaw.exe') for p in psutil.process_iter())
 assert shutil.disk_usage('D:/').free>6*1024**3
 registry=read(REGISTRY);assert len(registry)==24
 tasks=[x for x in registry if x['reference_query'] is not None];assert len(tasks)==16
 before=runtime();b=budget();assert not b['pending']
 freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/p)==h for p,h in freeze.items())
 OUT.mkdir()
 write(OUT/'protocol.json',{'sealed_local':now(),'script_sha256':sha(Path(__file__)),
  'registry_sha256':sha(REGISTRY),'expected_graph_task_queries':16,'graph_validation_queries':12,
  'sources':{k:str(v) for k,v in SOURCES.items()},'new_ports':PORTS,
  'rules':['Never mutate original D: or legacy C: stores/configs; byte-verify copy before boot.',
   'Source file identity plus receipt graph ID and node/edge counts precede task queries.',
   'One isolated JVM at a time;512MiBheap128MiBpagecache2activeprocessors;HTTPloopbackonly.',
   'GET readiness at most3times, >=10sec apart; no physical POST retries.',
   'Stop on first uncertain/failed query; preserve every request/terminal.',
   'Shutdown only new DB children tied to new config; never clear a lock.',
   'Fixed four tasks per project; no model or method tuning; no speedup claim from cached sequential execution.'],
  'budget_before':b,'runtime_before':before,'canonical_frozen':freeze,
  'host':socket.gethostname(),'model_calls':0})
 session=requests.Session();session.trust_env=False
 projects={};http_count=0
 try:
  for name,source in SOURCES.items():
   root=OUT/'runtime'/name;root.mkdir(parents=True)
   source_before=stores(source);write(root/'source_store_manifest_before.json',source_before)
   copy_start=time.monotonic()
   for directory in store_directories(source):shutil.copytree(source/directory,root/directory)
   assert stores(root)==source_before,'Copied store differs; do not boot'
   copy_seconds=time.monotonic()-copy_start
   for directory in ('conf','logs','run','plugins'):(root/directory).mkdir()
   conf=root/'conf';port=PORTS[name]
   text=(source/'conf/neo4j.conf').read_text(encoding='utf-8-sig')
   old_http=next(line for line in text.splitlines() if line.startswith('server.http.listen_address='))
   text=text.replace(old_http,'server.http.listen_address=127.0.0.1:'+str(port))
   lines=[]
   for line in text.splitlines():
    if line.startswith('server.directories.'):
     key=line.split('=',1)[0];suffix='transactions' if key.endswith('transaction.logs.root') else key.split('.')[-1]
     line=key+'='+str(root/suffix).replace('\\','/')
    lines.append(line)
   lines.append('server.jvm.additional=-XX:ActiveProcessorCount=2')
   (conf/'neo4j.conf').write_text('\n'.join(lines)+'\n',encoding='utf-8')
   assert str(source).replace('\\','/') not in '\n'.join(lines)
   launcher=root/'launch_owned.ps1'
   launcher.write_text("& '"+str(HOME/'bin/neo4j.bat')+"' console\n",encoding='utf-8')
   env=dict(os.environ,NEO4J_CONF=str(conf),NEO4J_HOME=str(HOME))
   outlog=(root/'console_stdout.log').open('wb');errlog=(root/'console_stderr.log').open('wb')
   process=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(launcher)],
    env=env,stdout=outlog,stderr=errlog,creationflags=subprocess.CREATE_NO_WINDOW)
   write(root/'execution_owner.json',{'host':socket.gethostname(),'wrapper_pid':process.pid,
    'new_config':str(conf),'new_port':port,'source_receipt_sha256':sha(source/'import_receipt.json'),
    'scope':'new private local reference-audit clone, not ownership migration of shared remote root'})
   requests_dir=root/'requests';requests_dir.mkdir()
   try:
    ready=False
    for attempt in range(1,4):
     time.sleep(12 if attempt==1 else 10)
     if process.poll() is not None:raise RuntimeError('Owned DB launcher exited; inspect logs, no blind restart')
     meta={'kind':'readiness','attempt':attempt,'time_local':now()};http_count+=1
     try:
      response=session.get('http://127.0.0.1:'+str(port)+'/',timeout=(2,3));meta['http_status']=response.status_code
      ready=response.status_code==200
     except requests.RequestException as error:meta['transport_error']=type(error).__name__
     write(requests_dir/('readiness_'+str(attempt)+'.json'),meta)
     if ready:break
    assert ready,'Three readiness failures; stop batch'
    receipt=read(source/'import_receipt.json')
    jobs=[('identity','CALL db.info() YIELD id,name RETURN id,name',{},[receipt['database_identity'][0]['row']]),
          ('nodes','MATCH (n) RETURN count(n) AS count',{},[[receipt['nodes']]]),
          ('edges','MATCH ()-[r]->() RETURN count(r) AS count',{},[[receipt['edges']]])]
    jobs +=[(t['task_id'],t['reference_query'],t['parameters'],t['csv_expected_rows']) for t in tasks if t['project']==name]
    accepted=[]
    for label,query,parameters,expected in jobs:
     job=requests_dir/label;job.mkdir()
     payload={'statements':[{'statement':query,'parameters':parameters,'resultDataContents':['row']} ]}
     write(job/'request.json',payload)
     terminal={'started_local':now()};started=time.monotonic();http_count+=1
     try:
      response=session.post('http://127.0.0.1:'+str(port)+'/db/neo4j/tx/commit',json=payload,timeout=(3,35))
      terminal['http_status']=response.status_code;terminal['body']=response.json()
     except Exception as error:terminal['transport_error']=type(error).__name__
     terminal['client_seconds']=time.monotonic()-started;write(job/'terminal.json',terminal)
     assert terminal.get('http_status')==200,'Unknown/failed HTTP query, do not retry'
     body=terminal['body'];assert not body.get('errors'), 'Graph returned query error; stop batch'
     actual=[row['row'] for row in body['results'][0]['data']]
     assert actual==expected,'Reference mismatch; preserve all evidence without method changes'
     accepted.append(label)
    projects[name]={'accepted_queries':accepted,'task_queries_accepted':4,
     'graph_identity_and_node_edge_counts_match':True,'clone_bytes':sum(x['bytes'] for x in source_before.values()),
     'clone_files':len(source_before),'copy_and_verify_seconds':copy_seconds,'port':port}
   finally:
    shutdown=stop_owned(process,conf,port);write(root/'shutdown_receipt.json',shutdown)
    outlog.close();errlog.close()
   assert stores(source)==source_before,'Original source store changed'
   write(root/'source_store_manifest_after.json',source_before)
   print(json.dumps({'project':name,'graph_task_queries_accepted':4,'own_clone_stopped':True}),flush=True)
  assert sha(REGISTRY)==read(OUT/'protocol.json')['registry_sha256']
  assert runtime()==before and all(sha(CANON/p)==h for p,h in freeze.items())
  b2=budget();assert b2==b
  summary={'kind':'OSDB_MANUAL_REFERENCE_REAL_GRAPH_ACCEPTANCE_NOT_MODEL_TEST',
   'completed_local':now(),'projects':projects,'planned_graph_task_queries':16,'dispatched_graph_task_queries':16,
   'accepted_graph_task_queries':16,'failed_graph_task_queries':0,'undispatched_graph_task_queries':0,
   'graph_identity_validation_queries':12,'all_HTTP_requests_including_readiness':http_count,
   'same_body_query_retries':0,'new_model_calls':0,'budget_before':b,'budget_after':b2,
   'canonical32_and_runtime46_unchanged':True,'old_graph_sources_byte_unchanged':True,
   'new_owned_clones_stopped':True,'original_candidate_registry_unchanged':True,
   'formal_predictions':0,'confirmation_admitted':0,'entire_T1_complete':False}
  write(OUT/'summary.json',summary);print(json.dumps(summary))
 except Exception as error:
  write(OUT/'failure.json',{'time_local':now(),'error_class':type(error).__name__,
   'projects_completed':list(projects),'http_requests':http_count,'no_automatic_retry':True})
  raise


if __name__=='__main__':main()
