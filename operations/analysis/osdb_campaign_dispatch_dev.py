# coding: utf-8
"""Finite, no-resume OSDB DEVELOPMENT dispatch; dependencies are injected.

No CLI, graph launcher, endpoint probe or approval writer. A graph factory must
already certify current bytes/owner/read permission/deadlines before dispatch.
Monitoring is asynchronous and cannot authorize, retry or stop model work.
"""
from collections import Counter,deque
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from copy import deepcopy
import hashlib,json,queue,threading,time
from pathlib import Path
from osdb_scoped_attempts_dev import immutable,sha,read
from osdb_budgeted_model_client_dev import outcome,normalized_reply
from osdb_response_boundary_dev import feedback_messages,execute_response

PROJECTS=('redis','duckdb','iotdb','elasticsearch')
FAILURES={'INFRASTRUCTURE_ERROR','MODEL_TRUNCATED','MODEL_PROTOCOL_ERROR','PLAN_VALIDATION_ERROR',
 'READ_POLICY_ERROR','EXECUTION_ERROR','EXECUTION_TIMEOUT','TRUNCATED_RESULT'}


class AsyncObserver:
 def __init__(self,callback):
  self.callback=callback;self.queue=queue.Queue(maxsize=1);self.closed=threading.Event();self.errors=0
  self.thread=threading.Thread(target=self.loop,daemon=True);self.thread.start()
 def offer(self,state):
  try:self.queue.put_nowait(deepcopy(state))
  except queue.Full:
   try:self.queue.get_nowait()
   except queue.Empty:pass
   try:self.queue.put_nowait(deepcopy(state))
   except queue.Full:pass
 def loop(self):
  while not self.closed.is_set() or not self.queue.empty():
   try:item=self.queue.get(timeout=.1)
   except queue.Empty:continue
   try:self.callback(item)
   except Exception:self.errors+=1
 def close(self):self.closed.set();self.thread.join(timeout=.2)


def distribution(values):
 values=sorted(values)
 def p(frac):
  if not values:return None
  position=(len(values)-1)*frac;i=int(position);return values[i]+(values[min(i+1,len(values)-1)]-values[i])*(position-i)
 return {'n':len(values),'mean':sum(values)/len(values) if values else None,'p50':p(.5),'p90':p(.9)}


def scheduled_path(client,key,arm,schema,execute):
 """Same method policy; keep first outcome if a global stop blocks feedback."""
 start=time.monotonic();first=client.generate(key,'initial');records=[first]
 first_result=outcome(arm,first,schema,execute);result=first_result;final=first;withheld=False
 if not first.get('transport_error') and result['status']=='INFRASTRUCTURE_ERROR':client.stop_for_unknown_read(key)
 if feedback_messages(client.initial[key],first.get('content') or '',result) is not None:
  if client.lease.stopped:withheld=True
  else:
   try:final=client.generate(key,'feedback',result)
   except RuntimeError:
    if client.lease.stopped and key+'__feedback' not in client.lease.reserved:withheld=True
    else:raise
   else:
    records.append(final);result=outcome(arm,final,schema,execute)
    if not final.get('transport_error') and result['status']=='INFRASTRUCTURE_ERROR':client.stop_for_unknown_read(key)
 controls={}
 if arm=='ours' and result['status']=='EXECUTED' and result.get('complete') is True:
  detailed=execute_response('ours',final['content'],schema,execute,strategy='full_detail')
  if detailed['status']=='INFRASTRUCTURE_ERROR':client.stop_for_unknown_read(key)
  controls={'full_detail':{'result':detailed,'shared_planner':True,'extra_model_calls':0},
   'simple_correct':{'same_result_as':'ours_final','shared_planner':True,'extra_model_calls':0,
    'reason':'Fixed static safe-count/full-identity rule equals current contract policy'}}
 saved={'path_key':key,'arm':arm,'first_result':first_result,'final':result,'records':records,
  'feedback_withheld_due_to_batch_stop':withheld,'shared_plan_controls':controls,
  'attached_ledger_attempts':len(records),'path_wall_seconds':time.monotonic()-start,'formal_prediction':False,
  'evaluator_reference_read_by_this_function':False}
 immutable(client.lease.directory/'paths'/(key+'.json'),saved);return saved


def run_campaign(client,manifest_path,directory,graph_factory,*,max_inflight,graph_snapshot_hashes,evidence_kind,observer=None):
 """One owned graph at a time; bounded in-flight paths, then drain before close.

 Returned counts concern method paths, NOT independent questions or accuracy.
 Unknown backend/server waits still require actual resource/deadline acceptance.
 """
 if evidence_kind not in ('synthetic','live_development'):raise ValueError('Explicit evidence kind required')
 if type(max_inflight) is not int or not 1<=max_inflight<=128:raise ValueError('Explicit bounded admitted concurrency required')
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
 results={};errors={};graph_receipts=[];stop_reason=None;started=time.time();last_graph=None
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
   dispatched=sum(k+'__initial' in claimed for k in keys)
   completed=len(results);pair_times=[]
   for case in {r['case_id'] for r in rows}:
    ends=[e['ended_epoch'] for e in events if e['case_id']==case]
    if len(ends)==2:pair_times.append(max(ends))
   rates={}
   for span in (60,300):
    exposure=max(.001,min(span,now-started));recent=[e for e in events if now-span<e['ended_epoch']<=now]
    attempts=[e for e in model_events if now-span<e['ended_epoch']<=now]
    output=[e['usage'].get('completion_tokens') for e in attempts]
    rates[str(span)]={'path_completed':len(recent),'paths_per_minute':len(recent)*60/exposure,
     'paired_cases_completed':sum(now-span<t<=now for t in pair_times),'exposure_seconds':exposure,
     'output_tokens_per_second':sum(output)/exposure if output and all(type(v) is int and v>=0 for v in output) else None}
   states=Counter(v['final']['status'] for v in results.values())
   by_graph={g:{a:sum(v['graph']==g and v['arm']==a for v in events) for a in ('direct','ours')} for g in PROJECTS}
   eta=(48-completed)*60/rates['300']['paths_per_minute'] if now-started>=30 and completed>=16 and rates['300']['paths_per_minute'] and not client.lease.stopped else None
   return {'kind':'OSDB_DEVELOPMENT_PIPELINE','evidence_kind':evidence_kind,'state':'已停止' if client.lease.stopped else '已完成' if completed==48 else '运行中',
    'model':client.private['model'],'updated_epoch':now,'started_epoch':started,'current_graph':last_graph,
    'planned_paths':48,'initial_paths_dispatched':dispatched,'paths_completed':completed,'undispatched_paths':48-dispatched,
    'unresolved_dispatched_paths':dispatched-completed,'runner_error_paths':len(errors),
    'terminal_failure_paths':sum(states[k] for k in FAILURES),'status_counts':dict(states),
    'paired_cases_completed':len(pair_times),'complete_answers':states['EXECUTED'],'by_graph':by_graph,
    'active_paths':deepcopy(list(active.values())),'active_model_requests':deepcopy(list(model_active.values())),
    'max_inflight_paths':max_inflight,'model_requests_inflight':len(model_active),'rates':rates,'eta_seconds':eta,
    'model_latency':distribution([e['seconds'] for e in model_events]),'path_latency':distribution([e['seconds'] for e in events]),
    'budget':{'reserved':client.lease.base+reserved,'cap':12096,'pending':reserved-terminal_count},
    'server_load':None,'server_queue':None,'stop_reason':stop_reason,'observer_errors':sink.errors if sink else 0,
    'recent':deepcopy(events[-6:][::-1]),'formal_prediction':False,'entire_T1_complete':False}
 def offer():
  if sink:sink.offer(snapshot())
 def stop(reason):
  nonlocal stop_reason
  stop_reason=stop_reason or reason
  with client.lease.mutex:client.lease.stopped=True
 def work(row,execute,db_lock,schema):
  key=row['case_id']+'__'+row['arm'];local.key=key;begin=time.monotonic()
  with lock:active[key]={'path_key':key,'case_id':row['case_id'],'arm':row['arm'],'stage':'准备首轮','started_epoch':time.time()}
  def data_read(q,p):
   with lock:active[key]['stage']='等待图读取'
   with db_lock:
    with lock:active[key]['stage']='读取图数据'
    return execute(q,p)
  try:return scheduled_path(client,key,row['arm'],schema,data_read)
  finally:
   with lock:active.pop(key,None)
 try:
  for graph in PROJECTS:
   if client.lease.stopped:break
   last_graph=graph;offer()
   try:
    with graph_factory(graph) as handle:
     a=handle['admission']
     if (a.get('graph')!=graph or a.get('admitted') is not True or a.get('model_views_equal') is not True or
      a.get('current_snapshot_sha256')!=graph_snapshot_hashes[graph] or a.get('client_and_server_deadlines_accepted') is not True):
      raise RuntimeError('Current graph ownership/view/hash/deadline admission absent')
     graph_receipts.append(deepcopy(a));immutable(directory/'graph_receipts'/(graph+'.json'),a)
     remaining=iter([r for r in rows if r['graph']==graph]);pending={};db_lock=threading.RLock()
     with ThreadPoolExecutor(max_workers=max_inflight) as pool:
      def fill():
       while len(pending)<max_inflight and not client.lease.stopped:
        row=next(remaining,None)
        if row is None:break
        future=pool.submit(work,row,handle['execute'],db_lock,handle['schema']);pending[future]=row
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
           'path_key':key,'status':result['final']['status'],'seconds':result['path_wall_seconds'],'ended_epoch':time.time()})
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
