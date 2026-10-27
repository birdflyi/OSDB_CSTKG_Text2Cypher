"""Cost-separated DEVELOPMENT dispatcher; not deployed or live accepted.

Run loop is derived from the preserved dispatcher, with an isolated timed path,
read-lock spans and method-only latency. No launch CLI or graph factory.
A live entry must use the central model gate and a physically accepted factory.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from copy import deepcopy
from pathlib import Path
import threading, time
from osdb_campaign_dispatch_dev import AsyncObserver, distribution, PROJECTS, FAILURES
from osdb_scoped_attempts_dev import immutable, sha, read
from osdb_budgeted_model_client_live_v2 import normalized_reply
from osdb_live_path_native_cost_v2 import PathCosts, timed_scheduled_path
from osdb_model_limit_guard_live_v2 import LimitedModelClient

def run_campaign(client,manifest_path,directory,graph_factory,*,max_inflight,graph_snapshot_hashes,evidence_kind,observer=None,prior_paths=None):
 """One owned graph at a time; bounded in-flight paths, then drain before close.

 Returned counts concern method paths, NOT independent questions or accuracy.
 Unknown backend/server waits still require actual resource/deadline acceptance.
 """
 if evidence_kind not in ('synthetic','live_development'):raise ValueError('Explicit evidence kind required')
 if type(max_inflight) is not int or not 1<=max_inflight<=64:raise ValueError('Explicit bounded admitted concurrency required')
 if evidence_kind=='live_development':
  if not isinstance(client,LimitedModelClient):raise RuntimeError('Live dispatch requires the central permitted model client')
  client.model_gate.check()
  if max_inflight>client.model_gate.requested:raise RuntimeError('Path workers exceed admitted central model permits')
 if not client.lease.active or client.lease.stopped:raise RuntimeError('No active admitted scope lease')
 manifest_path=Path(manifest_path);directory=Path(directory)
 if sha(manifest_path)!=client.lease.plan_sha256:raise RuntimeError('Dispatch manifest changed')
 rows=read(manifest_path);keys=[r['case_id']+'__'+r['arm'] for r in rows]
 if (len(rows)!=48 or len(set(keys))!=48 or set(keys)!=set(client.initial) or
  [r['ordinal'] for r in rows]!=list(range(1,49)) or set(graph_snapshot_hashes)!=set(PROJECTS)):
  raise RuntimeError('Incomplete sealed 48-path schedule')
 if [r['graph'] for r in rows]!=[g for g in PROJECTS for _ in range(12)]:raise RuntimeError('Fixed graph order differs')
 if any(sum(r['graph']==g and r['arm']==a for r in rows)!=6 for g in PROJECTS for a in ('direct','ours')):
  raise RuntimeError('Unbalanced method schedule')
 directory.mkdir(parents=True,exist_ok=False)
 immutable(directory/'protocol.json',{'kind':'OSDB_DEVELOPMENT_DISPATCH_NOT_FORMAL',
  'evidence_kind':evidence_kind,'manifest_sha256':client.lease.plan_sha256,'planned_paths':48,
  'max_inflight_paths':max_inflight,'graph_order':list(PROJECTS),'graph_snapshot_hashes':graph_snapshot_hashes,
  'same_method_and_feedback_policy':True,'model_retry':False,'resume':False,'formal_prediction':False})
 lock=threading.RLock();local=threading.local();active={};model_active={};events=[];model_events=[]
 prior_paths={} if prior_paths is None else prior_paths
 if not set(prior_paths)<=set(keys):raise RuntimeError('Unsealed prior path identity')
 results={k:read(path) for k,path in prior_paths.items()};errors={};graph_receipts=[];stop_reason=None;started=time.time();last_graph=None
 for key,value in results.items():
  row=next(r for r in rows if r['case_id']+'__'+r['arm']==key)
  events.append({'case_id':row['case_id'],'arm':row['arm'],'graph':row['graph'],'path_key':key,
   'status':value['final']['status'],'seconds':value['costs']['method_path_wall_seconds'],'ended_epoch':value['method_ended_epoch'],'prior_execution':True})
 sink=AsyncObserver(observer) if observer is not None else None
 original_transport=client.transport
 def transport(body):
  key=local.key;phase='initial' if len(body['messages'])==2 else 'feedback';rid=key+'__'+phase;t=time.time();begin=time.monotonic();terminal={}
  with lock:model_active[rid]={'path_key':key,'phase':phase,'started_epoch':t};active[key]['stage']='等待模型'
  try:terminal=original_transport(body);return terminal
  finally:
   reply=normalized_reply(terminal,client.private['model']) if type(terminal) is dict else {'usage':{}}
   with lock:
    model_active.pop(rid,None);active[key]['stage']='校验回答'
    model_events.append({'ended_epoch':time.time(),'seconds':time.monotonic()-begin,'usage':reply.get('usage',{})})
 client.transport=transport
 def snapshot():
  now=time.time()
  with lock:
   with client.lease.mutex:
    claimed=set(client.lease.claimed);reserved=len(client.lease.reserved);terminal_count=len(client.lease.terminals)
   dispatched=len(prior_paths)+sum(k+'__initial' in claimed for k in keys)
   completed=len(results);pair_times=[]
   for case in {r['case_id'] for r in rows}:
    ends=[e['ended_epoch'] for e in events if e['case_id']==case]
    if len(ends)==2:pair_times.append(max(ends))
   rates={}
   for span in (60,300):
    exposure=max(.001,min(span,now-started));recent=[e for e in events if max(now-span,started)<e['ended_epoch']<=now]
    attempts=[e for e in model_events if max(now-span,started)<e['ended_epoch']<=now]
    output=[e['usage'].get('completion_tokens') for e in attempts]
    rates[str(span)]={'path_completed':len(recent),'paths_per_minute':len(recent)*60/exposure,
     'paired_cases_completed':sum(now-span<t<=now for t in pair_times),'exposure_seconds':exposure,
     'output_tokens_per_second':sum(output)/exposure if output and all(type(v) is int and v>=0 for v in output) else None}
   states=Counter(v['final']['status'] for v in results.values())
   by_graph={g:{a:sum(v['graph']==g and v['arm']==a for v in events) for a in ('direct','ours')} for g in PROJECTS}
   eta=(48-completed)*60/rates['300']['paths_per_minute'] if now-started>=30 and completed>=16 and rates['300']['paths_per_minute'] and not client.lease.stopped else None
   return {'kind':'OSDB_DEVELOPMENT_PIPELINE','evidence_kind':evidence_kind,'state':'已停止' if client.lease.stopped else '已完成' if completed==48 else '运行中',
    'model':client.private['model'],'updated_epoch':now,'started_epoch':started,'current_graph':last_graph,
    'prior_completed_paths':len(prior_paths),'current_block_initial_dispatched':dispatched-len(prior_paths),'planned_paths':48,'initial_paths_dispatched':dispatched,'paths_completed':completed,'undispatched_paths':48-dispatched,
    'unresolved_dispatched_paths':dispatched-completed,'runner_error_paths':len(errors),
    'terminal_failure_paths':sum(states[k] for k in FAILURES),'status_counts':dict(states),
    'paired_cases_completed':len(pair_times),'complete_answers':states['EXECUTED'],'by_graph':by_graph,
    'active_paths':deepcopy(list(active.values())),'active_model_requests':deepcopy(list(model_active.values())),
    'max_inflight_paths':max_inflight,'model_requests_inflight':len(model_active),'rates':rates,'eta_seconds':eta,
    'model_latency':distribution([e['seconds'] for e in model_events]),'path_latency':distribution([e['seconds'] for e in events if isinstance(e['seconds'],(int,float))]),'path_latency_unknown_count':sum(e['seconds'] is None for e in events),
    'path_latency_scope':'method_until_final_answer_including_observed_contention_excluding_control_replay',
    'budget':{'reserved':client.lease.base+reserved,'cap':12096,'pending':reserved-terminal_count},
    'server_load':None,'server_queue':None,'stop_reason':stop_reason,'observer_errors':sink.errors if sink else 0,
    'recent':deepcopy(events[-6:][::-1]),'formal_prediction':False,'entire_T1_complete':False}
 def offer():
  if sink:sink.offer(snapshot())
 def stop(reason):
  nonlocal stop_reason
  stop_reason=stop_reason or reason
  with client.lease.mutex:client.lease.stopped=True
 def work(row,execute,db_lock,schema,statement_count=None):
  key=row['case_id']+'__'+row['arm'];local.key=key;begin=time.monotonic();costs=PathCosts(key)
  with lock:active[key]={'path_key':key,'case_id':row['case_id'],'arm':row['arm'],'stage':'准备首轮','started_epoch':time.time()}
  def data_read(q,p):
   with lock:active[key]['stage']='等待图读取'
   def physical(query,params):
    with lock:active[key]['stage']='读取图数据'
    return execute(query,params)
   return costs.read(physical,q,p,db_lock,statement_count)
  try:return timed_scheduled_path(client,key,row['arm'],schema,data_read,costs)
  finally:
   with lock:active.pop(key,None)
 try:
  for graph in PROJECTS:
   if client.lease.stopped:break
   if all(r['case_id']+'__'+r['arm'] in prior_paths for r in rows if r['graph']==graph):continue
   last_graph=graph;offer()
   try:
    with graph_factory(graph) as handle:
     a=handle['admission']
     if (a.get('graph')!=graph or a.get('admitted') is not True or a.get('model_views_equal') is not True or
      a.get('current_snapshot_sha256')!=graph_snapshot_hashes[graph] or a.get('client_and_server_deadlines_accepted') is not True):
      raise RuntimeError('Current graph ownership/view/hash/deadline admission absent')
     graph_receipts.append(deepcopy(a));immutable(directory/'graph_receipts'/(graph+'.json'),a)
     remaining=iter([r for r in rows if r['graph']==graph and r['case_id']+'__'+r['arm'] not in prior_paths]);pending={};db_lock=threading.RLock()
     with ThreadPoolExecutor(max_workers=max_inflight) as pool:
      def fill():
       while len(pending)<max_inflight and not client.lease.stopped:
        row=next(remaining,None)
        if row is None:break
        future=pool.submit(work,row,handle['execute'],db_lock,handle['schema'],handle.get('physical_statement_count'));pending[future]=row
      fill()
      while pending:
       try:done,_=wait(pending,timeout=.5,return_when=FIRST_COMPLETED)
       except BaseException as exc:
        stop('调度控制中断，停止新派发并等待已派发请求终结')
        p=directory/'control_interrupt.json'
        if not p.exists():immutable(p,{'error_type':type(exc).__name__,'no_new_dispatch':True})
        continue
       for future in done:
        row=pending.pop(future);key=row['case_id']+'__'+row['arm']
        try:result=future.result()
        except BaseException as exc:
         if key+'__initial' not in client.lease.claimed and client.lease.stopped:continue
         errors[key]={'error_type':type(exc).__name__,'initial_attempt_claimed':key+'__initial' in client.lease.claimed}
         immutable(directory/'runner_errors'/(key+'.json'),errors[key]);stop('路径出现未知状态，停止新派发并排空在途')
        else:
         with lock:
          results[key]=result;events.append({'case_id':row['case_id'],'arm':row['arm'],'graph':graph,
           'path_key':key,'status':result['final']['status'],'seconds':result['costs']['method_path_wall_seconds'],'ended_epoch':time.time()})
       if client.lease.stopped:stop_reason=stop_reason or '批次停止条件已触发；在途已派发请求继续终结'
       fill();offer()
     # All model/path futures have drained before graph context exit.
   except BaseException as exc:
    immutable(directory/'graph_errors'/(graph+'.json'),{'error_type':type(exc).__name__,'graph':graph})
    stop('图环境或批次控制出现未知状态，保留证据后停止')
   offer()
 finally:
  client.transport=original_transport
  final=snapshot();offer()
  if sink:sink.close()
  final['observer_errors']=sink.errors if sink else 0
  final['observer_thread_still_running']=bool(sink and sink.thread.is_alive())
  final['graph_admission_receipts']=graph_receipts
  immutable(directory/'summary.json',final)
 return final
