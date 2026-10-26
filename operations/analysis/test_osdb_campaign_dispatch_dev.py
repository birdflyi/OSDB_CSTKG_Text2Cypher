# coding: utf-8
"""Synthetic 48-path schedules; no actual approvals, models, DBs or sockets."""
from contextlib import contextmanager
from copy import deepcopy
import hashlib,io,json,sys,tempfile,threading,time,unittest
from pathlib import Path
from rich.console import Console
sys.path.insert(0,'D:/ch7/code/OSDB_CSTKG_Text2Cypher_publication_t1/experiment-harness/public_benchmark_v1')
from attempt_budget import AttemptBudget
from osdb_scoped_attempts_dev import ScopedAttempts,SCOPE,wire,sha,read,immutable
from osdb_budgeted_model_client_dev import BudgetedModelClient
from osdb_campaign_dispatch_dev import run_campaign,PROJECTS
from osdb_campaign_monitor_dev import render
from observed_profile_semantic_cases import SCHEMA,call,single


class CampaignTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);(self.root/'ledger').mkdir()
  self.rows=[];self.initial={};self.lock=threading.RLock();self.active_model=0;self.peak_model=0;self.model_calls=0
  self.active_db=0;self.peak_db=0;self.db_calls=0;self.open_graph=None;self.graph_events=[]
  for graph in PROJECTS:
   for i in range(6):
    for arm in (('direct','ours') if i%2==0 else ('ours','direct')):
     case=graph+'_fixture'+str(i);key=case+'__'+arm
     self.rows.append({'case_id':case,'graph':graph,'arm':arm,'ordinal':len(self.rows)+1})
     self.initial[key]=[{'role':'system','content':'synthetic protocol'},{'role':'user','content':key}]
  self.manifest=self.root/'model_path_manifest.json';immutable(self.manifest,self.rows);self.plan=sha(self.manifest)
  approval=self.root/'synthetic_approval_NOT_HUMAN.json'
  immutable(approval,{'kind':'SYNTHETIC_FIXTURE_NOT_HUMAN_APPROVAL','approved_cumulative_cap':12096,'historical_attempts_at_approval':0,
   'osdb_exposed_development':{'scope_id':SCOPE,'approved':True,'approved_additional_attempts':96,'plan_manifest_sha256':self.plan}})
  self.legacy=AttemptBudget(self.root/'ledger',approval)
  self.private={'model':'ling-3.0-flash-n72-75','temperature':.2,'top_p':.95,'reasoning_effort':'high','max_tokens':8192,
   'endpoint':'http://synthetic-not-a-service.invalid:1/chat','api_key':'SYNTHETIC_NOT_A_KEY'}
  self.admission={'scope_id':SCOPE,'plan_manifest_sha256':self.plan,'route_repair_verified':True,'online_launch_accepted':True,
   'formal_prediction':False,'model_configuration_sha256':hashlib.sha256(wire(self.private)).hexdigest(),
   'initial_messages_sha256':{k:hashlib.sha256(wire(v)).hexdigest() for k,v in self.initial.items()}}
  self.hashes={g:'synthetic-'+g for g in PROJECTS};self.schema=deepcopy(SCHEMA);self.schema['entities']=self.schema['entities'][:1]
 def tearDown(self):self.temp.cleanup()
 def lease(self):return ScopedAttempts(self.legacy,self.root/'campaign',self.plan,self.initial)
 def client(self,lease,transport=None):return BudgetedModelClient(lease,self.private,self.initial,self.admission,transport or self.transport)
 def response(self,text):return {'http_status':200,'body':{'model':self.private['model'],
  'choices':[{'message':{'content':text},'finish_reason':'stop'}],'usage':{}}}
 def transport(self,body):
  with self.lock:self.active_model+=1;self.model_calls+=1;self.peak_model=max(self.peak_model,self.active_model)
  try:
   time.sleep(.004)
   if body['messages'][1]['content'].endswith('__direct'):obj={'status':'ANSWER','query':'RETURN 1 AS n'}
   else:
    c=call();c['returns']=[{'field':'n.key','agg':'count','alias':'n'}];obj=single(c)
   return self.response(json.dumps(obj))
  finally:
   with self.lock:self.active_model-=1
 @contextmanager
 def graph(self,name):
  with self.lock:
   if self.open_graph is not None:raise AssertionError('Graphs overlapped')
   self.open_graph=name;self.graph_events.append(('open',name))
  def execute(q,p):
   with self.lock:self.active_db+=1;self.db_calls+=1;self.peak_db=max(self.peak_db,self.active_db)
   try:time.sleep(.001);return {'status':'EXECUTED','complete':True,'columns':['n'],'rows':[[1]]}
   finally:
    with self.lock:self.active_db-=1
  try:yield {'schema':self.schema,'execute':execute,'admission':{'graph':name,'admitted':True,'model_views_equal':True,
   'current_snapshot_sha256':self.hashes[name],'client_and_server_deadlines_accepted':True}}
  finally:
   with self.lock:
    assert self.active_model==self.active_db==0,'Graph closed before draining local work'
    self.graph_events.append(('close',name));self.open_graph=None
 def run_all(self,lease,client=None,observer=None,max_inflight=4,factory=None):
  return run_campaign(client or self.client(lease),self.manifest,self.root/'scheduler',factory or self.graph,
   max_inflight=max_inflight,graph_snapshot_hashes=self.hashes,evidence_kind='synthetic',observer=observer)
 def test_all48_progress_bounded_model_parallelism_serialized_graph_and_drain(self):
  with self.lease() as lease:result=self.run_all(lease)
  self.assertEqual(result['paths_completed'],48);self.assertEqual(result['paired_cases_completed'],24)
  self.assertEqual(result['undispatched_paths'],0);self.assertEqual(result['unresolved_dispatched_paths'],0)
  self.assertEqual(self.model_calls,48);self.assertEqual(self.db_calls,72);self.assertEqual(self.peak_db,1)
  self.assertLessEqual(self.peak_model,4);self.assertGreaterEqual(self.peak_model,2)
  self.assertEqual(self.graph_events,[(event,g) for g in PROJECTS for event in ('open','close')])
  self.assertFalse(self.legacy.inspect()['pending'])
 def test_observer_errors_do_not_interrupt_work(self):
  def broken(s):raise OSError('synthetic display failure')
  with self.lease() as lease:result=self.run_all(lease,observer=broken)
  self.assertEqual(result['paths_completed'],48);self.assertGreater(result['observer_errors'],0)
 def test_blocking_observer_does_not_block_campaign_completion(self):
  release=threading.Event();entered=threading.Event()
  def blocked(s):entered.set();release.wait()
  try:
   with self.lease() as lease:result=self.run_all(lease,observer=blocked)
   self.assertEqual(result['paths_completed'],48);self.assertTrue(entered.is_set());self.assertTrue(result['observer_thread_still_running'])
  finally:release.set()
 def test_bad_graph_admission_prevents_first_model_call(self):
  @contextmanager
  def wrong(name):
   with self.graph(name) as handle:
    handle['admission']['current_snapshot_sha256']='wrong';yield handle
  with self.lease() as lease:result=self.run_all(lease,factory=wrong)
  self.assertEqual(self.model_calls,0);self.assertEqual(result['initial_paths_dispatched'],0)
  self.assertEqual(result['undispatched_paths'],48);self.assertEqual(result['state'],'已停止')
 def test_three_infra_failures_stop45_undispatched_and_no_next_graph(self):
  def bad(body):self.model_calls+=1;return {'http_status':503,'body':{}}
  with self.lease() as lease:result=self.run_all(lease,client=self.client(lease,bad),max_inflight=1)
  self.assertEqual(result['paths_completed'],3);self.assertEqual(result['terminal_failure_paths'],3)
  self.assertEqual(result['undispatched_paths'],45);self.assertEqual(self.model_calls,3)
  self.assertEqual(self.graph_events,[('open','redis'),('close','redis')])
 def test_unknown_crash_retains_pending_stops_next_graph_drains_local_futures(self):
  def crash(body):
   if body['messages'][1]['content']=='redis_fixture0__direct':raise KeyboardInterrupt('synthetic unknown physical outcome')
   return self.transport(body)
  with self.lease() as lease:result=self.run_all(lease,client=self.client(lease,crash))
  self.assertEqual(result['runner_error_paths'],1);self.assertEqual(result['unresolved_dispatched_paths'],1)
  self.assertEqual(len(self.legacy.inspect()['pending']),1)
  self.assertEqual(self.graph_events,[('open','redis'),('close','redis')])
  self.assertEqual(result['initial_paths_dispatched']+result['undispatched_paths'],48)
 def test_batch_stop_withholds_feedback_without_fabricating_second_attempt(self):
  with self.lease() as lease:
   def stop_after_initial(body):lease.stopped=True;return self.response('{')
   result=self.run_all(lease,client=self.client(lease,stop_after_initial),max_inflight=1)
   path=read(lease.directory/'paths/redis_fixture0__direct.json')
   self.assertTrue(path['feedback_withheld_due_to_batch_stop']);self.assertEqual(path['attached_ledger_attempts'],1)
  self.assertEqual(result['paths_completed'],1);self.assertEqual(result['undispatched_paths'],47)
 def test_chinese_ASCII_view_does_not_claim_capacity_or_accuracy(self):
  with self.lease() as lease:result=self.run_all(lease)
  stream=io.StringIO();console=Console(file=stream,width=140,color_system=None)
  console.print(render(result,'job'));console.print(render(result,'pipeline'));text=stream.getvalue()
  for phrase in ('方法路径终结','服务端总负载：未知','模型请求耗时','整张T1未完成'):self.assertIn(phrase,text)
  self.assertNotIn('当前为容量测试',text);self.assertNotIn('EXECUTED',text)


if __name__=='__main__':unittest.main()
