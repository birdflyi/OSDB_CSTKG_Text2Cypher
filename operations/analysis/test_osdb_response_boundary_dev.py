"""Meaningful offline protocol, repair and final-answer checks, not model EX."""
from copy import deepcopy
import json,unittest
from observed_profile_semantic_cases import SCHEMA,call,single
from osdb_common_input_v3 import messages
from osdb_response_boundary_dev import decode,replay_case,score_numeric,ProtocolError,execute_response
from osdb_model_read_executor_dev_v2 import data_read_operators,operators,known_rolled_back_read_error

SCHEMA=deepcopy(SCHEMA);SCHEMA['entities']=SCHEMA['entities'][:1]
INITIAL=[{'role':'system','content':'protocol'},{'role':'user','content':'public schema and question only'}]


def record(value,**kwargs):return dict({'content':value if isinstance(value,str) else json.dumps(value),'finish_reason':'stop'},**kwargs)
def success(rows=None):return {'status':'EXECUTED','complete':True,'columns':['id'],'rows':[['a']] if rows is None else rows}


class BoundaryTests(unittest.TestCase):
 def test_equal_outer_declines_and_common_bytes(self):
  args=dict(task_id='dev',graph='fixture',question='Count all accounts',schema=SCHEMA)
  a=messages('direct',**args);b=messages('ours',**args);self.assertEqual(a['messages'][1],b['messages'][1])
  for arm in ('direct','ours'):
   for status in ('UNSUPPORTED','INSUFFICIENT_EVIDENCE'):
    self.assertEqual(decode(arm,json.dumps({'status':status,'reason':'missing evidence'}))['status'],status)
 def test_strict_json_rejects_duplicate_nonfinite_fences_and_extras(self):
  for value in ('{"status":"ANSWER","status":"UNSUPPORTED"}','{"status":NaN}','```json\n{}\n```',
   '{"status":"UNSUPPORTED","reason":"x","query":"RETURN 1"}',
   '{"status":"ANSWER","query":"RETURN 1","nested":[1e999]}'):
   with self.assertRaises(ProtocolError):decode('direct',value)
 def test_no_database_dispatch_for_either_decline(self):
  def forbidden(*args):raise AssertionError('Decline cannot dispatch')
  for arm in ('direct','ours'):
   final=execute_response(arm,'{"status":"INSUFFICIENT_EVIDENCE","reason":"history unavailable"}',SCHEMA,forbidden)
   self.assertFalse(score_numeric(final,[[3]])['numeric_EX']);self.assertIsNone(score_numeric(final,None)['numeric_EX'])
 def test_one_repair_then_score_actual_final_answer(self):
  rows=[]
  def execute(q,p):rows.append(q);return success()
  r=replay_case('direct',INITIAL,SCHEMA,[record('{'),record({'status':'ANSWER','query':'RETURN "a" AS id'})],execute)
  self.assertEqual(len(r['history']),2);self.assertEqual(len(rows),1);self.assertTrue(score_numeric(r['final'],[['a']])['numeric_EX'])
  self.assertIsNone(r['next_messages_if_feedback_pending']);self.assertEqual(r['physical_model_calls'],0)
 def test_valid_wrong_answer_never_uses_gold_for_feedback(self):
  r=replay_case('direct',INITIAL,SCHEMA,[record({'status':'ANSWER','query':'RETURN "wrong" AS id'}),record({'status':'ANSWER','query':'RETURN "gold" AS id'})],lambda q,p:success([['wrong']]))
  self.assertEqual(r['response_records_consumed'],1);self.assertIsNone(r['next_messages_if_feedback_pending'])
  self.assertFalse(score_numeric(r['final'],[['gold']])['numeric_EX'])
 def test_pending_feedback_contains_operational_error_only(self):
  r=replay_case('direct',INITIAL,SCHEMA,[record('{')],lambda *a:success())
  text=json.dumps(r['next_messages_if_feedback_pending']);self.assertIn('operational error',text)
  self.assertNotIn('987654321',text);self.assertEqual(r['response_records_consumed'],1)
 def test_second_failure_stops_without_third_repair(self):
  r=replay_case('ours',INITIAL,SCHEMA,[record('{'),record('{')],lambda *a:success())
  self.assertEqual(r['response_records_consumed'],2);self.assertEqual(r['final']['status'],'MODEL_PROTOCOL_ERROR')
  self.assertIsNone(r['next_messages_if_feedback_pending'])
  with self.assertRaises(ValueError):replay_case('ours',INITIAL,SCHEMA,[record('{')]*3,lambda *a:success())
 def test_transport_and_generation_length_not_model_repair(self):
  for first,status in [(record('{}',http_status=503),'INFRASTRUCTURE_ERROR'),(record('{}',finish_reason='length'),'MODEL_TRUNCATED')]:
   r=replay_case('direct',INITIAL,SCHEMA,[first,record({'status':'ANSWER','query':'RETURN 1'})],lambda *a:success())
   self.assertEqual(r['response_records_consumed'],1);self.assertEqual(r['final']['status'],status)
 def test_both_truncated_paths_not_complete_answers(self):
  trunc=lambda *a:{'status':'TRUNCATED','complete':False,'columns':['id'],'rows':[['a']]}
  for arm,obj in [('direct',{'status':'ANSWER','query':'RETURN "a" AS id'}),('ours',single(call()))]:
   r=replay_case(arm,INITIAL,SCHEMA,[record(obj)],trunc)
   self.assertEqual(r['final']['status'],'TRUNCATED_RESULT');self.assertFalse(score_numeric(r['final'],[['a']])['numeric_EX'])
 def test_strategy_control_shares_plan_not_extra_generation(self):
  for strategy in ('contract','full_detail'):
   r=replay_case('ours',INITIAL,SCHEMA,[record(single(call()))],lambda *a:success(),strategy=strategy)
   self.assertEqual(r['response_records_consumed'],1);self.assertTrue(score_numeric(r['final'],[['a']])['numeric_EX'])
 def test_typed_bag_preserves_bool_zero_multiplicity_and_empty(self):
  self.assertFalse(score_numeric(success([[False]]),[[0]])['numeric_EX'])
  self.assertFalse(score_numeric(success([[1.0]]),[[1]])['numeric_EX'])
  self.assertFalse(score_numeric(success([['a']]),[['a'],['a']])['numeric_EX'])
  self.assertFalse(score_numeric(success([]),[[0]])['numeric_EX'])
  self.assertTrue(score_numeric(success([['b'],['a']]),[['a'],['b']])['numeric_EX'])
 def test_catalog_plan_rejected_but_normal_data_operations_allowed(self):
  self.assertFalse(data_read_operators(['ProduceResults','ShowIndexes']))
  self.assertFalse(data_read_operators(['ShowConstraints@neo4j']))
  self.assertTrue(data_read_operators(['ProduceResults','Union','Apply','NodeByLabelScan','Projection']))
  self.assertEqual(operators({'operatorType':'ProduceResults','children':[{'operatorType':'Projection'}]}),['ProduceResults','Projection'])
 def test_only_known_server_read_errors_are_recoverable(self):
  x=Exception('syntax');x.code='Neo.ClientError.Statement.SyntaxError';self.assertTrue(known_rolled_back_read_error(x))
  self.assertFalse(known_rolled_back_read_error(ConnectionResetError('unknown')))


if __name__=='__main__':unittest.main()
