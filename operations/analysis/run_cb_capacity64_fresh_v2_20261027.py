"""New human-requested64 TRAIN capacity wave. Never resumes an old campaign.

No graph, training, scoring, OSDB allowance use or automatic request retry.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from collections import Counter
import argparse,base64,hashlib,importlib.metadata,json,os,shutil,socket,subprocess,sys,threading,time,traceback
from copy import deepcopy
from osdb_scoped_attempts_dev import read,sha,wire,immutable
from osdb_node_transport_live import node_worker_source,UnknownRemoteAttempt
from osdb_budgeted_model_client_live_v2 import normalized_reply
from osdb_model_limit_guard_live_v2 import CentralModelGate
from cb_capacity_transport64_20261027 import PersistentSSH64,CapacityNodeTransport
from prepare_osdb_exposed_regression_20261027 import dependencies,runtime
from osdb_live_monitor_bridge import atomic

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
RUN=ROOT/'experiments/cb_train_capacity64_fresh_v2_20261027'
REMOTE='/mnt/data/0/louzehua/ch7_cb_train_capacity64_fresh_v2_20261027'
HOST='interactive-zk06dlgpxt9z-5c484c5f4-vh46c'
KEY='C:/Users/10651/.codex/private/ch7_ssh_node3107_20261026/interactive_3107.key'
SSH=['ssh','-i',KEY,'-p','3107','-o','BatchMode=yes','-o','ConnectTimeout=10','root@117.50.195.94']
OLD=ROOT/'reports/t1_parallel_capacity_sshroute_v6_20261026/executed_sources'
SAMPLING={'model':'ling-3.0-flash-n72-75','temperature':.2,'top_p':.95,'reasoning_effort':'high','max_tokens':8192}

def remote(source,payload=None):
 command="python3 -c \"import base64;exec(base64.b64decode('"+base64.b64encode(source.encode()).decode()+"'))\""
 q=subprocess.run(SSH+[command],input=wire(payload) if payload is not None else None,capture_output=True,timeout=30)
 if q.returncode:raise RuntimeError(q.stderr.decode('utf-8',errors='replace')[:1000])
 return json.loads(q.stdout)

def legacy_budget():
 sys.path.insert(0,str(RUN/'experiment-harness/public_benchmark_v1'))
 from attempt_budget import AttemptBudget
 return AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
  path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])

def prepare():
 assert not RUN.exists();assert not (ROOT/'state/.t1-global-model-dispatch.lock').exists()
 CentralModelGate(ROOT/'state/T1_MODEL_CONCURRENCY_POLICY_20261027.json',64)
 source=ROOT/'experiments/public_benchmark_v1/dev_validation_deepseek_v4_20261024'
 manifest=read(source/'run_manifest.json');assert manifest['kind']=='TRAIN_PREPARATION_ONLY_NOT_MAIN_TABLE' and len(manifest['task_ids'])==12
 train=ROOT/'experiments/public_benchmark_v1/cypherbench/runtime/train.jsonl'
 trainids={json.loads(line)['qid'] for line in train.read_text(encoding='utf-8-sig').splitlines()}
 assert set(manifest['task_ids'])<=trainids
 pack=[]
 for i,qid in enumerate(manifest['task_ids']):
  for arm in ('direct','ours'):
   p=source/'progress'/('%d_%s_0.json'%(i,arm));v=read(p)
   assert v['qid']==qid and v['arm']==arm and v['views'][0]['step']==0
   request=Path(v['views'][0]['reply']['attempt_folders'][0])/'request.json';body=read(request)
   pack.append({'qid':qid,'arm':arm,'messages':body['messages'],'source_path':str(request),'source_sha256':sha(request)})
 rows=[]
 for i in range(64):
  p=deepcopy(pack[i%24]);key='fresh64_20261027v2_%04d__%s'%(i,p['arm'])
  p.update(path_key=key,trial_id=key+'__initial')
  p['messages'][1]['content']+='\nNew capacity trial '+key+'. Answer the original TRAIN graph question normally.'
  assert [m['role'] for m in p['messages']]==['system','user'];rows.append(p)
 RUN.mkdir();code=RUN/'operations/analysis';code.mkdir(parents=True)
 sources=dependencies([Path(__file__).name,'test_cb_capacity_transport64_20261027.py'])
 for n,h in sources.items():shutil.copyfile(Path(__file__).with_name(n),code/n);assert sha(code/n)==h
 external={}
 for n in ('parallel_budget.py','common.py','monitor.py'):
  target=code/('capacity_'+n if n=='common.py' else n);shutil.copyfile(OLD/n,target);external[target.name]=sha(target)
 # parallel_budget and the monitor use the accepted common helper by this name.
 shutil.copyfile(OLD/'common.py',code/'common.py');external['common.py']=sha(code/'common.py')
 guard=RUN/'experiment-harness/public_benchmark_v1';guard.mkdir(parents=True)
 src=REPO/'experiment-harness/public_benchmark_v1/attempt_budget.py'
 adapter=runtime()
 for n,h in adapter.items():
  target=guard/n;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(REPO/'experiment-harness/public_benchmark_v1'/n,target);assert sha(target)==h
 from importlib import util
 spec=util.spec_from_file_location('capacity_legacy_budget',guard/src.name);mod=util.module_from_spec(spec);spec.loader.exec_module(mod)
 budget=mod.AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
  path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])
 before=budget.inspect();assert before['pending']==[]
 base=sum(read(p).get('scope_id') is None for p in budget.ledger.glob('*.json'));assert base+64<=12000
 immutable(RUN/'manifest.json',rows)
 worker=node_worker_source().replace('T1 OSDB exposed development, single-shot only','T1 CypherBench TRAIN capacity64, single-shot only')
 owner={'hostname':HOST,'purpose':'T1 CypherBench TRAIN capacity64, single-shot only','worker_sha256':hashlib.sha256(worker.encode()).hexdigest(),
  'private_configuration':'/mnt/data/0/louzehua/ch7_t1_route_repair_v1_20261027/private/connection_louzehua_20261027.json',
  'sampling':SAMPLING,'allowed_request_ids':[v['trial_id'] for v in rows]}
 publisher='''import pathlib,json,sys,socket,os
r=pathlib.Path(__file__).parent;assert json.loads((r/'execution_owner.json').read_text())['hostname']==socket.gethostname()
v=json.load(sys.stdin);p=r/'state.writing';p.write_text(json.dumps(v,ensure_ascii=False),encoding='utf-8');os.replace(p,r/'state.json');print('{}')
'''
 files={'node_model_once.py':worker,'execution_owner.json':json.dumps(owner),'monitor.py':(OLD/'monitor.py').read_text(encoding='utf-8-sig'),
  'common.py':(OLD/'common.py').read_text(encoding='utf-8-sig'),'publish_state.py':publisher}
 deployment=remote('''import pathlib,json,sys,socket,hashlib
v=json.load(sys.stdin);assert socket.gethostname()==v['host'];old=pathlib.Path('/mnt/data/0/louzehua/ch7_t1_osdb_live_v3_20261027')
assert json.loads((old/'execution_owner.json').read_text())['hostname']==socket.gethostname()
workers=[]
for p in pathlib.Path('/proc').iterdir():
 if p.name.isdigit():
  try:
   if any(x.endswith(b'/node_model_once.py') for x in (p/'cmdline').read_bytes().split(b'\\0')):workers.append(p.name)
  except OSError:pass
assert not workers
mem={line.split(':')[0]:int(line.split()[1]) for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:')}
assert mem['MemAvailable']>2*1024*1024
r=pathlib.Path(v['root']);r.mkdir(mode=0o700)
for n,s in v['files'].items():(r/n).write_text(s,encoding='utf-8')
o=json.loads((r/'execution_owner.json').read_text());p=pathlib.Path(o['private_configuration']);c=json.loads(p.read_text());assert c['model']==v['model'] and c.get('api_key')
o['private_configuration_sha256']=hashlib.sha256(p.read_bytes()).hexdigest();(r/'execution_owner.json').write_text(json.dumps(o))
print(json.dumps({'hostname':socket.gethostname(),'root':str(r),'owner_verified':True,'workers_before':workers,'mem_available_kB':mem['MemAvailable'],'private_configuration_sha256':o['private_configuration_sha256']}))
''',{'host':HOST,'root':REMOTE,'files':files,'model':SAMPLING['model']})
 immutable(RUN/'freeze.json',{'kind':'NEW_HUMAN_REQUESTED_64_TRAIN_CAPACITY_NOT_FORMAL','human_instruction':'64并发',
  'source_sha256':sources,'copied_accepted_components':external,'attempt_budget_sha256':sha(guard/'attempt_budget.py'),'adapter46':adapter,
  'train_runtime_sha256':sha(train),'source_manifest_sha256':sha(source/'run_manifest.json'),'manifest_sha256':sha(RUN/'manifest.json'),
  'sampling':SAMPLING,'approved_total_cap':12096,'CB_pool_cap':12000,'CB_before':base,'OSDB_used_untouched':before['reserved']-base,
  'budget_before':{k:before[k] for k in ('reserved','cap','remaining','pending')},'request_count':64,'unique_exposed_TRAIN_tasks':12,
  'distinct_base_method_inputs':24,'new_namespace_not_old_queue':True,'automatic_retry':False,'new_OSDB_requests':0,
  'max_global_inflight':64,'SSH_pool':[8,8],'model_deadline_seconds':240,'node_deployment':deployment,
  'node_files_sha256':{n:hashlib.sha256(s.encode('utf-8')).hexdigest() for n,s in files.items() if n!='execution_owner.json'},
  'money':None,'steady_state_or_saturation_claim':False,'python':sys.version,
  'package_versions':{n:importlib.metadata.version(n) for n in ('paramiko','cryptography','bcrypt','PyNaCl','rich')}})
 print(json.dumps({'prepared':str(RUN),'sources':len(sources),'calls':0,'count':64,'CB_remaining_before':12000-base,'node':deployment}))

def run():
 frozen=read(RUN/'freeze.json');code=Path(__file__).parent
 assert all(sha(code/n)==h for n,h in frozen['source_sha256'].items())
 assert all(sha(code/n)==h for n,h in frozen['copied_accepted_components'].items())
 assert all(sha(RUN/'experiment-harness/public_benchmark_v1'/n)==h for n,h in frozen['adapter46'].items());assert sha(RUN/'manifest.json')==frozen['manifest_sha256'];assert sha(RUN/'experiment-harness/public_benchmark_v1/attempt_budget.py')==frozen['attempt_budget_sha256']
 assert not (RUN/'run_claim.json').exists()
 immutable(RUN/'run_claim.json',{'pid':os.getpid(),'hostname':socket.gethostname(),'started_epoch':time.time(),'command':sys.argv})
 from parallel_budget import ParallelLease
 from common import rates,distribution
 rows=read(RUN/'manifest.json');initial={v['path_key']:v['messages'] for v in rows};b=legacy_budget()
 stop=threading.Event();lock=threading.RLock();active={};events=[];outcomes={};peak=0;infra=0;reason=None;pool=None;started=time.time()
 jobs=[{'benchmark':name,'method':arm,'state':'已完成' if name=='CypherBench' else '准备中','completed':2348 if name=='CypherBench' else 0,
  'total':2348 if name=='CypherBench' else '待冻结','reason':'原冻结成绩保留' if name=='CypherBench' else '待独立任务/评分/资源准入'}
  for name in ('CypherBench','OSDB-STKG','Text2GQL','MindTheQuery','ZOGRASCOPE') for arm in ('直接查询+反馈','我们的方法')]
 try:
  with CentralModelGate(ROOT/'state/T1_MODEL_CONCURRENCY_POLICY_20261027.json',64) as gate:
   with ParallelLease(b,RUN/'campaign',64) as lease:
    base=sum(read(p).get('scope_id') is None for p in b.ledger.glob('*.json'));assert base+64<=12000
    pool=PersistentSSH64(KEY,Path.home()/'.ssh/known_hosts')
    transport=CapacityNodeTransport(SSH,REMOTE,initial,RUN/'node_transfers',pool)
    def work(row):
     nonlocal peak
     rid=row['trial_id'];body=dict(SAMPLING,messages=row['messages']);begin=time.time()
     with gate.slot(wait_seconds=30):
      if stop.is_set():return {'undispatched':True,'rid':rid}
      ack=lease.reserve(rid,body);folder=RUN/'campaign/attempts'/rid
      immutable(folder/'dispatch_claim.json',{'slot':ack['slot'],'request_id':rid,'no_retry':True})
      with lock:
       active[rid]={'trial_id':rid,'question':row['messages'][1]['content'].split('Question:\n')[-1].split('\n')[0][:100],
        'arm':row['arm'],'started_epoch':begin};peak=max(peak,len(active))
      try:t=transport(body)
      except UnknownRemoteAttempt:
       gate.preserve_unknown(rid);stop.set();return {'unknown':True,'rid':rid,'ended_epoch':time.time()}
      finally:
       with lock:active.pop(rid,None)
      raw=t.pop('raw_response_bytes',None)
      if raw is not None:(folder/'response.raw').write_bytes(raw)
      reply=normalized_reply(t,SAMPLING['model']);end=time.time()
      t.update(budget_slot=ack['slot'],provider_wall_seconds=end-begin,started_epoch=begin,ended_epoch=end,
       formal_prediction=False,scope='CypherBench_TRAIN_capacity_development',automatic_retry=False)
      lease.terminal(rid,t)
      return {'rid':rid,'seconds':end-begin,'ended_epoch':end,'http_status':t.get('http_status'),
       'complete':not reply['transport_error'] and reply['finish_reason']=='stop' and bool(reply['content']),
       'infrastructure_failure':bool(reply['transport_error']),'finish_reason':reply['finish_reason'],**reply['usage']}
    def snapshot(final=False):
     with lock:a=list(active.values())
     good=[v for v in events if 'seconds' in v]
     state={'kind':'T1_RUNTIME_SNAPSHOT_V1','capacity':True,'model':SAMPLING['model'],'owner_hostname':HOST,
      'campaign_started':started,'stage_started':started,'updated_epoch':time.time(),'job_id':'新64并发验收 · 12个暴露TRAIN题',
      'jobs':jobs,'stages':[],'state':'已终结' if final else '运行中','stage_name':'新64并发单波','total':64,
      'completed':len(events),'dispatched':len(lease.reserved),'undispatched':64-len(lease.reserved),
      'concurrency_target':64,'actual_peak_requests':peak,'active':a,'complete_responses':sum(v.get('complete',False) for v in events),
      'budget':lease.status(),'server_load':None,'server_queue':None,'output_budget':None,'stop_reason':reason,
      'rates':rates(good,time.time(),started),'recent':[{'trial_id':v['rid'],'state':'完整响应' if v.get('complete') else '失败/截断',
       'seconds':v.get('seconds',0)} for v in events[-6:][::-1]],
      'statistics':{'latency':distribution([v['seconds'] for v in good]),'tokens':{k:distribution([v[k] for v in good if type(v.get(k)) is int]) for k in ('prompt_tokens','completion_tokens','reasoning_tokens')}}}
     atomic(RUN/'state.json',state);return state
    last_remote=0
    with ThreadPoolExecutor(max_workers=64) as executor:
     futures={executor.submit(work,row):row for row in rows}
     while futures:
      done,_=wait(futures,timeout=.5,return_when=FIRST_COMPLETED)
      for f in sorted(done,key=lambda x:0 if x.exception() else x.result().get('ended_epoch',0)):
       row=futures.pop(f)
       try:v=f.result()
       except BaseException as exc:
        reason='Controller exception; stop new dispatch, drain, audit';stop.set();v={'unknown':True,'rid':row['trial_id'],'error':type(exc).__name__}
       if not v.get('undispatched'):
        events.append(v);outcomes[v['rid']]=v
       if v.get('unknown'):stop.set();reason='Unknown request retained; no resend'
       elif v.get('infrastructure_failure'):
        infra+=1
        if infra>=3:stop.set();reason='Three consecutive infrastructure failures; drain'
       else:infra=0
      state=snapshot()
      if time.monotonic()-last_remote>=15:
       last_remote=time.monotonic()
       try:subprocess.run(SSH+['python3 '+REMOTE+'/publish_state.py'],input=wire(state),capture_output=True,timeout=10)
       except Exception:pass
    final=snapshot(True);immutable(RUN/'outcomes.json',outcomes)
    immutable(RUN/'summary.json',{'planned':64,'dispatched':len(lease.reserved),'terminal':len(lease.terminals),
     'unknown':len(lease.reserved)-len(lease.terminals),'peak_client_transports':peak,'HTTP_counts':dict(Counter(str(v.get('http_status')) for v in events)),
     'stop_reason':reason,'statistics':final['statistics'],'wall_seconds':time.time()-started,
     'complete_responses':final['complete_responses'],'steady_state_sample_sufficient':False,
     'not_accuracy_or_saturation_proof':True,'old_results_unchanged':True,'money':None})
    try:subprocess.run(SSH+['python3 '+REMOTE+'/publish_state.py'],input=wire(final),capture_output=True,timeout=10)
    except Exception:pass
 finally:
  if pool:pool.close()
  immutable(RUN/'runner_exit.json',{'ended_epoch':time.time(),'PID':os.getpid(),'summary_exists':(RUN/'summary.json').exists(),'no_auto_restart':True})

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('action',choices=('prepare','run'));a=p.parse_args()
 prepare() if a.action=='prepare' else run()
