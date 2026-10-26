"""Temporary synthetic ledger + injected transport: NEVER real model attempts."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,'D:/ch7/code/OSDB_CSTKG_Text2Cypher_publication_t1/experiment-harness/public_benchmark_v1')
from attempt_budget import AttemptBudget
from osdb_scoped_attempts_dev import ScopedAttempts,SCOPE,wire,read
from osdb_budgeted_model_client_dev import BudgetedModelClient,OneShotHTTP,run_development_path,normalized_reply,load_model_inputs
from osdb_response_boundary_dev import score_numeric
from observed_profile_semantic_cases import SCHEMA,call,single

SCHEMA=deepcopy(SCHEMA);SCHEMA['entities']=SCHEMA['entities'][:1]


class ClientTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);(self.root/'ledger').mkdir()
  self.approval=self.root/'synthetic_approval.json';self.plan='b'*64
  self.approval.write_bytes(wire({'approved_cumulative_cap':12096,'historical_attempts_at_approval':0,
   'osdb_exposed_development':{'scope_id':SCOPE,'approved':True,'approved_additional_attempts':96,'plan_manifest_sha256':self.plan}}))
  self.legacy=AttemptBudget(self.root/'ledger',self.approval)
  self.keys=['case'+str(i)+'__direct' for i in range(100)]+['case__ours']
  self.messages={k:[{'role':'system','content':'fixed protocol'},{'role':'user','content':'public question/schema only'}] for k in self.keys}
  self.private={'model':'ling-3.0-flash-n72-75','temperature':.2,'top_p':.95,'reasoning_effort':'high','max_tokens':8192,
   'endpoint':'http://not-a-real-service.invalid:1/chat?fixture=1','api_key':'FAKE_TEST_CREDENTIAL_DO_NOT_EXPORT'}
  self.admission={'scope_id':SCOPE,'plan_manifest_sha256':self.plan,'route_repair_verified':True,
   'online_launch_accepted':True,'formal_prediction':False,'model_configuration_sha256':hashlib.sha256(wire(self.private)).hexdigest(),
   'initial_messages_sha256':{k:hashlib.sha256(wire(v)).hexdigest() for k,v in self.messages.items()}}
 def tearDown(self):self.temp.cleanup()
 def lease(self,name='campaign'):return ScopedAttempts(self.legacy,self.root/name,self.plan,self.messages)
 def response(self,text,**extra):return {'http_status':200,'body':{'model':self.private['model'],
  'choices':[{'message':{'content':text},'finish_reason':'stop'}],'usage':{}},**extra}
 def terminal(self,ack):return {'budget_slot':ack['slot'],'http_status':200,'body':{'synthetic':True}}
 def client(self,lease,transport,admission=None,messages=None):
  return BudgetedModelClient(lease,self.private,self.messages if messages is None else messages,
   self.admission if admission is None else admission,transport)
 def test_original_approval_cannot_enable_OSDB_or_create_lock(self):
  self.approval.write_bytes(wire({'approved_cumulative_cap':12000,'historical_attempts_at_approval':0}))
  with self.assertRaises(RuntimeError):self.lease().__enter__()
  self.assertFalse((self.root/'campaign').exists());self.assertFalse((self.root/'ledger/.t1-reservation.lock').exists())
 def test_route_config_or_message_change_blocks_before_reservation(self):
  with self.lease() as lease:
   for field,value in [('route_repair_verified',False),('model_configuration_sha256','changed')]:
    a=deepcopy(self.admission);a[field]=value
    with self.assertRaises(RuntimeError):self.client(lease,lambda body:self.fail('No dispatch'),a)
   changed=deepcopy(self.messages);changed[self.keys[0]][1]['content']='changed'
   with self.assertRaises(RuntimeError):self.client(lease,lambda body:self.fail('No dispatch'),messages=changed)
   self.assertFalse(lease.reserved)
 def test_parallel_reservations_unique_and_scope_labeled(self):
  with self.lease() as lease:
   def go(k):
    r=k+'__initial';ack=lease.reserve(r,{'messages':[k]});lease.claim_once(r);lease.terminal(r,self.terminal(ack),False);return ack['slot']
   with ThreadPoolExecutor(max_workers=16) as pool:slots=list(pool.map(go,self.keys[:32]))
   self.assertEqual(sorted(slots),list(range(1,33)))
  self.assertFalse(self.legacy.inspect()['pending'])
  self.assertTrue(all(read(p)['scope_id']==SCOPE and 'OSDB exposed' in read(p)['purpose'] for p in (self.root/'ledger').glob('*.json')))
 def test_duplicate_generate_never_sends_twice(self):
  calls=[]
  with self.lease() as lease:
   client=self.client(lease,lambda b:(calls.append(b) or self.response('{')))
   client.generate(self.keys[0],'initial')
   with self.assertRaises(RuntimeError):client.generate(self.keys[0],'initial')
   self.assertEqual(len(calls),1);self.assertEqual(len(lease.reserved),1)
 def test_unknown_crash_stays_pending_and_new_campaign_cannot_resend(self):
  def crash(body):raise KeyboardInterrupt('synthetic crash')
  with self.lease() as lease:
   with self.assertRaises(KeyboardInterrupt):self.client(lease,crash).generate(self.keys[0],'initial')
  self.assertEqual(self.legacy.inspect()['pending'],[1])
  with self.assertRaises(RuntimeError):self.lease('another').__enter__()
  self.assertFalse((self.root/'ledger/.t1-reservation.lock').exists())
 def test_terminated_logical_generation_cannot_be_resent_in_new_campaign(self):
  with self.lease() as lease:self.client(lease,lambda b:self.response('{')).generate(self.keys[0],'initial')
  with self.lease('another') as lease:
   with self.assertRaises(RuntimeError):lease.reserve(self.keys[0]+'__initial',{})
 def test_three_infrastructure_failures_stop_no_fourth_request(self):
  calls=[]
  def fail(body):calls.append(body);raise ConnectionResetError('synthetic error contains secret-like text')
  with self.lease() as lease:
   client=self.client(lease,fail)
   for key in self.keys[:3]:self.assertTrue(client.generate(key,'initial')['transport_error'])
   with self.assertRaises(RuntimeError):client.generate(self.keys[3],'initial')
   self.assertTrue(lease.stopped);self.assertEqual(len(calls),3)
  self.assertFalse(self.legacy.inspect()['pending'])
 def test_separate_pool_96_hard_cap(self):
  with self.lease() as lease:
   for key in self.keys[:96]:
    r=key+'__initial';ack=lease.reserve(r,{});lease.claim_once(r);lease.terminal(r,self.terminal(ack),False)
   with self.assertRaises(RuntimeError):lease.reserve(self.keys[96]+'__initial',{})
  self.assertEqual(self.legacy.inspect()['reserved'],96)
 def test_one_operational_feedback_and_final_score_separate(self):
  replies=iter(['{',json.dumps({'status':'ANSWER','query':'RETURN 1 AS id'})]);calls=[]
  with self.lease() as lease:
   client=self.client(lease,lambda b:(calls.append(deepcopy(b)) or self.response(next(replies))))
   result=run_development_path(client,self.keys[0],'direct',SCHEMA,lambda q,p:{'status':'EXECUTED','complete':True,'columns':['id'],'rows':[[1]]})
   self.assertEqual(result['attached_ledger_attempts'],2);self.assertTrue(score_numeric(result['final'],[[1]])['numeric_EX'])
   self.assertEqual([m['role'] for m in calls[1]['messages']],['system','user','assistant','user'])
   self.assertNotIn('numeric_reference_rows',json.dumps(calls));self.assertNotIn(self.private['api_key'],json.dumps(calls))
 def test_executable_wrong_never_repaired(self):
  calls=[]
  with self.lease() as lease:
   client=self.client(lease,lambda b:(calls.append(b) or self.response(json.dumps({'status':'ANSWER','query':'RETURN 9 AS id'}))))
   result=run_development_path(client,self.keys[0],'direct',SCHEMA,lambda q,p:{'status':'EXECUTED','complete':True,'columns':['id'],'rows':[[9]]})
   self.assertFalse(score_numeric(result['final'],[[1]])['numeric_EX']);self.assertEqual(len(calls),1)
 def test_shared_final_plan_controls_add_no_model_request(self):
  calls=[];queries=[];text=json.dumps(single(call()))
  with self.lease() as lease:
   client=self.client(lease,lambda b:(calls.append(b) or self.response(text)))
   def execute(q,p):queries.append(q);return {'status':'EXECUTED','complete':True,'columns':['id'],'rows':[['a']]}
   result=run_development_path(client,'case__ours','ours',SCHEMA,execute)
   self.assertEqual(len(calls),1);self.assertEqual(len(queries),2)
   self.assertEqual(result['shared_plan_controls']['full_detail']['result']['rows'],[['a']])
   self.assertEqual(result['shared_plan_controls']['simple_correct']['same_result_as'],'ours_final')
 def test_unknown_and_boolean_usage_remain_unknown_not_zero(self):
  t=self.response('{}');t['body']['usage']={'prompt_tokens':True,'completion_tokens':4,'total_tokens':None}
  r=normalized_reply(t,self.private['model']);self.assertIsNone(r['usage']['prompt_tokens']);self.assertEqual(r['usage']['completion_tokens'],4)
  self.assertIsNone(r['usage']['reasoning_tokens']);self.assertIsNone(r['usage']['cached_tokens'])
 def test_low_level_transport_one_post_no_retry_no_credential_log(self):
  calls=[]
  class FakeConnection:
   sock=None
   def __init__(self,*a,**k):pass
   def connect(self):pass
   def request(self,method,path,**kwargs):calls.append((method,path));raise ConnectionResetError('private detail')
   def close(self):pass
  with patch('osdb_budgeted_model_client_dev.http.client.HTTPConnection',FakeConnection):
   result=OneShotHTTP(self.private)({'model':self.private['model']})
  self.assertEqual(calls,[('POST','/chat?fixture=1')]);self.assertTrue(result['remote_outcome_unknown'])
  self.assertNotIn(self.private['api_key'],json.dumps(result));self.assertNotIn('private detail',json.dumps(result))
 def test_foreign_lock_is_never_removed(self):
  p=self.root/'ledger/.t1-reservation.lock';p.write_text('foreign owner')
  with self.assertRaises(FileExistsError):self.lease().__enter__()
  self.assertEqual(p.read_text(),'foreign owner')
 def test_total_deadline_interrupts_owned_read_retains_HTTP_status(self):
  calls=[];timers=[]
  class FakeSocket:
   def settimeout(self,value):pass
   def shutdown(self,value):calls.append('shutdown_owned_socket')
  class FakeTimer:
   def __init__(self,seconds,callback):self.callback=callback;timers.append(self);calls.append(seconds)
   def start(self):pass
   def cancel(self):calls.append('cancel_timer')
  class FakeResponse:
   status=200
   def read(self,limit):timers[0].callback();raise TimeoutError('synthetic partial read')
  class FakeConnection:
   def __init__(self,*a,**k):self.sock=FakeSocket()
   def connect(self):pass
   def request(self,*a,**k):calls.append('one_POST')
   def getresponse(self):return FakeResponse()
   def close(self):pass
  with patch('osdb_budgeted_model_client_dev.threading.Timer',FakeTimer),patch('osdb_budgeted_model_client_dev.http.client.HTTPConnection',FakeConnection):
   result=OneShotHTTP(self.private)({})
  self.assertEqual(result['http_status'],200);self.assertTrue(result['deadline_expired'])
  self.assertEqual(calls.count('one_POST'),1);self.assertIn('shutdown_owned_socket',calls);self.assertIn('cancel_timer',calls)
 def test_unknown_database_read_state_stops_before_next_model_request(self):
  calls=[]
  with self.lease() as lease:
   client=self.client(lease,lambda b:(calls.append(b) or self.response(json.dumps({'status':'ANSWER','query':'RETURN 1'}))))
   def unknown(q,p):raise ConnectionResetError('unknown query outcome')
   result=run_development_path(client,self.keys[0],'direct',SCHEMA,unknown)
   self.assertEqual(result['final']['status'],'INFRASTRUCTURE_ERROR');self.assertTrue(lease.stopped)
   with self.assertRaises(RuntimeError):client.generate(self.keys[1],'initial')
   self.assertEqual(len(calls),1)
 def test_decline_and_length_do_not_execute_or_repair(self):
  def forbidden(q,p):self.fail('No data query for decline/length')
  with self.lease() as lease:
   client=self.client(lease,lambda b:self.response(json.dumps({'status':'UNSUPPORTED','reason':'missing evidence'})))
   result=run_development_path(client,self.keys[0],'direct',SCHEMA,forbidden)
   self.assertEqual(result['final']['status'],'UNSUPPORTED');self.assertEqual(result['attached_ledger_attempts'],1)
   def length(b):
    reply=self.response('{');reply['body']['choices'][0]['finish_reason']='length';return reply
   client=self.client(lease,length)
   result=run_development_path(client,self.keys[1],'direct',SCHEMA,forbidden)
   self.assertEqual(result['final']['status'],'MODEL_TRUNCATED');self.assertEqual(result['attached_ledger_attempts'],1)
 def test_sealed_loader_rejects_changed_manifest_without_reading_any_messages(self):
  p=self.root/'input';p.mkdir();(p/'model_path_manifest.json').write_bytes(wire([]))
  with self.assertRaises(RuntimeError):load_model_inputs(p,'changed')


if __name__=='__main__':unittest.main()
