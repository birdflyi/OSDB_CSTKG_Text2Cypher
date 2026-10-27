"""Handwritten archives only: no model callback, DB or actual approvals."""
import json,tempfile,unittest
from pathlib import Path
from osdb_scoped_attempts_dev import wire,sha,read,SCOPE
from osdb_budgeted_model_client_dev import normalized_reply
from osdb_posthoc_accounting_dev import audit


class AccountingTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.c=self.root/'campaign';self.l=self.root/'ledger';self.l.mkdir()
  self.rows=[];self.refs=[]
  for g in ('redis','duckdb','iotdb','elasticsearch'):
   for i in range(6):
    case=g+'_fixture'+str(i);self.refs.append({'task_id':case,'numeric_reference_rows':[[1]] if i<4 else None})
    for arm in ('direct','ours'):self.rows.append({'case_id':case,'graph':g,'arm':arm,'ordinal':len(self.rows)+1})
  self.m=self.root/'manifest.json';self.e=self.root/'evaluator.json';self.save(self.m,self.rows);self.save(self.e,self.refs)
  self.save(self.c/'scope_lease.json',{'scope':SCOPE,'plan_manifest_sha256':sha(self.m),'budget_before':{'reserved':0}})
  self.count=0
  for row in self.rows:
   key=row['case_id']+'__'+row['arm'];first=self.attempt(key,'initial',[[0]] if key=='redis_fixture0__direct' else [[1]])
   records=[first];final=self.result([[0]] if len(records)==1 and key=='redis_fixture0__direct' else [[1]])
   initial_result=final
   if key=='redis_fixture0__direct':records.append(self.attempt(key,'feedback',[[1]]));final=self.result([[1]])
   controls={} if row['arm']=='direct' else {'full_detail':{'shared_planner':True,'extra_model_calls':0,'result':self.result([[1]])},
    'simple_correct':{'shared_planner':True,'extra_model_calls':0,'same_result_as':'ours_final'}}
   self.save(self.c/'paths'/(key+'.json'),{'path_key':key,'arm':row['arm'],'records':records,'final':final,
    'first_result':initial_result,'attached_ledger_attempts':len(records),'shared_plan_controls':controls})
  self.save(self.c/'scope_close.json',{'reserved':self.count,'pending':[]})
 def tearDown(self):self.temp.cleanup()
 def save(self,p,v):p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(wire(v))
 def result(self,rows):return {'status':'EXECUTED','complete':True,'columns':['n'],'rows':rows}
 def attempt(self,key,phase,rows):
  self.count+=1;rid=key+'__'+phase;p=self.c/'attempts'/rid/'request.json'
  messages=[{'role':'system','content':'synthetic'},{'role':'user','content':key}]
  if phase=='feedback':messages.extend([{'role':'assistant','content':'synthetic first'},{'role':'user','content':'synthetic error'}])
  body={'model':'synthetic-model','messages':messages};self.save(p,body)
  self.save(self.l/f'{self.count:04d}.json',{'request_id':rid,'request_path':str(p),'request_sha256':sha(p),
   'scope_id':SCOPE,'plan_manifest_sha256':sha(self.m)})
  self.save(p.with_name('dispatch_claim.json'),{'request_id':rid,'budget_slot':self.count,'automatic_retry':False})
  t={'http_status':200,'body':{'model':'synthetic-model','choices':[{'message':{'content':'synthetic'},'finish_reason':'stop'}],
   'usage':{'prompt_tokens':10,'completion_tokens':5,'total_tokens':15,'prompt_tokens_details':{'cached_tokens':2},
    'completion_tokens_details':{'reasoning_tokens':3}}}}
  reply=normalized_reply(t,'synthetic-model')
  t.update(normalized_reply=reply,budget_slot=self.count,scope_id=SCOPE,physical_attempt_in_request=1,automatic_retry=False,
   provider_wall_seconds=2,prompt_characters=sum(len(m['content']) for m in messages))
  self.save(p.with_name('terminal.json'),t)
  return dict(reply,budget_slot=self.count,terminal_path=str(p.with_name('terminal.json')))
 def run_audit(self):return audit(self.c,self.l,self.m,self.e,manifest_sha256=sha(self.m),evaluator_sha256=sha(self.e),evidence_kind='synthetic')
 def test_final_scores_and_shared_billing_are_separate(self):
  a=self.run_audit();self.assertEqual(a['reserved_attempts'],49);self.assertEqual(a['completed_paths'],48)
  self.assertFalse(a['paths']['redis_fixture0__direct']['first_score']['numeric_EX'])
  self.assertTrue(a['paths']['redis_fixture0__direct']['final_score']['numeric_EX'])
  self.assertEqual(a['by_arm']['direct']['usage']['total_tokens']['sum_all'],25*15)
  self.assertEqual(a['by_arm']['ours']['usage']['total_tokens']['sum_all'],24*15)
  self.assertEqual(a['by_arm']['direct']['planned_numeric_reference_paths'],16)
  self.assertEqual(a['full_detail_model_calls_extra'],0);self.assertIsNone(a['independent_control_DB_cost_seconds'])
 def test_missing_usage_stays_unknown_even_when_other_slots_known(self):
  p=self.c/'attempts/redis_fixture1__direct__initial/terminal.json';t=read(p);t['body']['usage']={}
  t['normalized_reply']=normalized_reply(t,'synthetic-model');self.save(p,t)
  p=self.c/'paths/redis_fixture1__direct.json';v=read(p);v['records'][0].update(t['normalized_reply']);self.save(p,v)
  x=self.run_audit()['by_arm']['direct']['usage']['total_tokens']
  self.assertEqual(x['unknown'],1);self.assertEqual(x['sum_known'],24*15);self.assertIsNone(x['sum_all'])
 def test_unknown_pending_not_failed_and_no_complete_rate(self):
  key='redis_fixture1__direct';p=self.c/'paths'/(key+'.json');v=read(p);slot=v['records'][0]['budget_slot'];p.unlink()
  (self.c/'attempts'/(key+'__initial')/'terminal.json').unlink();self.save(self.c/'scope_close.json',{'reserved':49,'pending':[slot]})
  a=self.run_audit();self.assertEqual(a['unresolved_paths'],1);self.assertEqual(a['undispatched_paths'],0)
  self.assertIsNone(a['by_arm']['direct']['numeric_rate_all_planned']);self.assertIsNone(a['paths'][key]['final_score']['numeric_EX'])
 def test_decline_numeric_false_boundary_null(self):
  for key in ('redis_fixture1__direct','redis_fixture4__direct'):
   p=self.c/'paths'/(key+'.json');v=read(p);v['final']={'status':'INSUFFICIENT_EVIDENCE','complete':False};self.save(p,v)
  a=self.run_audit();self.assertIs(a['paths']['redis_fixture1__direct']['final_score']['numeric_EX'],False)
  self.assertIsNone(a['paths']['redis_fixture4__direct']['final_score']['numeric_EX'])
 def test_bool_is_not_integer_correctness(self):
  p=self.c/'paths/redis_fixture1__direct.json';v=read(p);v['final']=self.result([[True]]);self.save(p,v)
  self.assertIs(self.run_audit()['paths']['redis_fixture1__direct']['final_score']['numeric_EX'],False)
 def test_changed_request_bytes_rejected(self):
  p=self.c/'attempts/redis_fixture1__direct__initial/request.json';v=read(p);v['model']='changed';self.save(p,v)
  with self.assertRaisesRegex(ValueError,'hash'):self.run_audit()
 def test_orphan_terminal_and_reply_mismatch_rejected(self):
  p=self.c/'attempts/redis_fixture1__direct__initial/dispatch_claim.json';p.unlink()
  with self.assertRaisesRegex(ValueError,'Orphan'):self.run_audit()
 def test_reference_hash_and_shared_control_claim_rejected(self):
  with self.assertRaisesRegex(ValueError,'hash'):audit(self.c,self.l,self.m,self.e,manifest_sha256=sha(self.m),evaluator_sha256='changed',evidence_kind='synthetic')
  p=self.c/'paths/redis_fixture1__ours.json';v=read(p);v['shared_plan_controls']['full_detail']['extra_model_calls']=1;self.save(p,v)
  with self.assertRaisesRegex(ValueError,'Control'):self.run_audit()


if __name__=='__main__':unittest.main()
