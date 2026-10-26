"""Actual Neo4j semantics on a NEW private tiny fixture; no model calls.

No legacy stores/configs are reused or mutated. Own JVM only, no model or shared
node changes. Fixed cases and exact expected row multisets sealed before boot.
"""
import argparse
from collections import Counter
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

import requests
from accept_osdb_actor_graph_references_20261026 import stop_owned,live,budget,runtime,now
from observed_profile_semantic_cases import PEOPLE,EDGES,SCHEMA,cases

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
HOME=Path('D:/Program Files (x86)/neo4j-community-5.15.0')


def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def write(p,v):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')
def multiset(rows):return Counter(json.dumps(r,sort_keys=True,ensure_ascii=False) for r in rows)


def main():
 parser=argparse.ArgumentParser();parser.add_argument('--output',required=True)
 parser.add_argument('--module',default='observed_neo4j_services_dev');parser.add_argument('--port',type=int,default=18495)
 args=parser.parse_args();out=Path(args.output).resolve();port=args.port
 assert out.parent==ROOT/'reports' and not out.exists()
 assert 18495<=port<=18499 and not live(port)
 assert shutil.disk_usage('D:/').free>1024**3
 module=importlib.import_module(args.module);source=Path(module.__file__)
 assert source.parent==Path(__file__).parent
 tasks=cases();assert len(tasks)==14
 before_runtime=runtime();before_budget=budget();assert not before_budget['pending']
 assert not (ROOT/'experiments/ch7_private/continuation_20261021/budget/.t1-reservation.lock').exists()
 frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/n)==v for n,v in frozen.items())
 out.mkdir();(out/'executed_sources').mkdir()
 sources=[source,Path(__file__),Path(__file__).with_name('observed_profile_semantic_cases.py'),
  Path(__file__).with_name('accept_osdb_actor_graph_references_20261026.py')]
 for p in sources:shutil.copyfile(p,out/'executed_sources'/p.name)
 write(out/'protocol.json',{'sealed_local':now(),'kind':'NEW_PRIVATE_SYNTHETIC_NEO4J_SEMANTIC_ACCEPTANCE_NOT_FORMAL_T1',
  'module':args.module,'source_sha256':{p.name:sha(p) for p in sources},'planned_cases':14,
  'planned_plan_executions':28,'strategies':['contract','full_detail'],'fixture':{'people':PEOPLE,'edges':EDGES,'integer_keys':[1,2]},
  'schema':SCHEMA,'cases':tasks,'budget_before':before_budget,'runtime_before':before_runtime,'canonical_before':frozen,
  'rules':['New empty store and loopback-only port, never clone or modify historical stores.',
   'No POST retry; stop at first unknown HTTP, query error or semantic mismatch; preserve negative result.',
   'At most3readiness checks at least10seconds apart; stop only JVM proven child of new wrapper/config.',
   'No model calls, training, official predictions or benchmark resource switch.']})
 for folder in ('conf','data','logs','run','plugins'):(out/folder).mkdir()
 config=['server.default_listen_address=127.0.0.1',f'server.http.listen_address=127.0.0.1:{port}',
  'server.http.enabled=true','server.https.enabled=false','server.bolt.enabled=false',
  'dbms.security.auth_enabled=false','dbms.default_database=neo4j','server.memory.heap.initial_size=256m',
  'server.memory.heap.max_size=512m','server.memory.pagecache.size=128m',
  'dbms.security.allow_csv_import_from_file_urls=false','server.jvm.additional=-XX:ActiveProcessorCount=2']
 for folder in ('data','logs','run','plugins'):config.append('server.directories.'+folder+'='+str(out/folder).replace('\\','/'))
 (out/'conf/neo4j.conf').write_text('\n'.join(config)+'\n',encoding='utf-8')
 launcher=out/'launch_owned.ps1';launcher.write_text("& '"+str(HOME/'bin/neo4j.bat')+"' console\n",encoding='utf-8')
 session=requests.Session();session.trust_env=False;wire_count=0;posts=0;accepted=[];attempted=[];failed=None
 def query(q,params):
  nonlocal wire_count,posts
  posts+=1;wire_count+=1;folder=out/'requests'/f'{posts:04d}';folder.mkdir(parents=True)
  payload={'statements':[{'statement':q,'parameters':params,'resultDataContents':['row']}]}
  write(folder/'request.json',payload);start=time.monotonic();terminal={'started_local':now()}
  try:
   response=session.post(f'http://127.0.0.1:{port}/db/neo4j/tx/commit',json=payload,timeout=(3,30))
   terminal.update(http_status=response.status_code,body=response.json())
  except Exception as exc:terminal['transport_error']=type(exc).__name__
  terminal['client_seconds']=time.monotonic()-start;write(folder/'terminal.json',terminal)
  if terminal.get('http_status')!=200 or terminal['body'].get('errors'):raise RuntimeError('HTTP/query failure; no retry')
  result=terminal['body']['results'][0]
  return {'columns':result['columns'],'rows':[row['row'] for row in result['data']],'complete':True}
 stdout=(out/'console_stdout.log').open('wb');stderr=(out/'console_stderr.log').open('wb')
 process=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(launcher)],
  env=dict(os.environ,NEO4J_CONF=str(out/'conf'),NEO4J_HOME=str(HOME)),stdout=stdout,stderr=stderr,creationflags=subprocess.CREATE_NO_WINDOW)
 write(out/'execution_owner.json',{'host':socket.gethostname(),'wrapper_pid':process.pid,'config':str(out/'conf'),
  'port':port,'scope':'new synthetic local fixture only','no_existing_store_owner_change':True})
 try:
  ready=False
  for i in range(3):
   time.sleep(12 if i==0 else 10);wire_count+=1;result={'attempt':i+1}
   try:response=session.get(f'http://127.0.0.1:{port}/',timeout=(2,3));result['status']=response.status_code;ready=response.status_code==200
   except requests.RequestException as exc:result['error']=type(exc).__name__
   write(out/f'readiness_{i+1}.json',result)
   if ready:break
   if process.poll() is not None:break
  assert ready,'Readiness failed; preserve logs, no blind restart'
  assert query('MATCH(n) RETURN count(n) AS n',{})['rows']==[[0]]
  query('UNWIND $rows AS p CREATE (n:Person) SET n=p',{'rows':PEOPLE})
  query('UNWIND $rows AS e MATCH(a:Person {key:e.a}),(b:Person {key:e.b}) CREATE(a)-[r:LINK]->(b) SET r.physical_key=e.key,r.clock=e.clock',{'rows':EDGES})
  query('UNWIND [1,2] AS x CREATE (:NumberPerson {number:x})',{})
  for task in tasks:
   for strategy in ('contract','full_detail'):
    attempted.append({'case':task['name'],'strategy':strategy})
    result=module.execute_plan(task['plan'],SCHEMA,query,strategy=strategy)
    matches=result['columns']==task['expected_columns'] and multiset(result['rows'])==multiset(task['expected_rows'])
    record={'case':task['name'],'strategy':strategy,'result':result,'expected_rows':task['expected_rows'],
     'expected_columns':task['expected_columns'],'matches':matches}
    write(out/'results'/(task['name']+'_'+strategy+'.json'),record)
    if not matches:raise AssertionError('Semantic mismatch: '+task['name']+' / '+strategy)
    accepted.append({'case':task['name'],'strategy':strategy})
 except Exception as exc:
  failed={'type':type(exc).__name__,'message':str(exc)};write(out/'failure.json',failed)
 finally:
  write(out/'shutdown_receipt.json',stop_owned(process,out/'conf',port));stdout.close();stderr.close();session.close()
 after_budget=budget();assert after_budget==before_budget
 assert runtime()==before_runtime and all(sha(CANON/n)==v for n,v in frozen.items())
 summary={'kind':'SYNTHETIC_NEO4J_SEMANTIC_ACCEPTANCE_NOT_FORMAL_T1','module':args.module,
  'planned_plan_executions':28,'dispatched_plan_executions':len(attempted),'accepted_plan_executions':len(accepted),
  'failed_plan_executions':len(attempted)-len(accepted),'undispatched_plan_executions':28-len(attempted),'accepted':accepted,'failure':failed,
  'physical_HTTP_requests':wire_count,'physical_POST_queries_including_fixture_setup':posts,'model_calls':0,
  'budget_before':before_budget,'budget_after':after_budget,'canonical32_and_adapter46_unchanged':True,
  'new_owned_JVM_stopped':True,'formal_admission':False,'entire_T1_complete':False}
 write(out/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True),flush=True)


if __name__=='__main__':main()
