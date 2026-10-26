"""Durable injected-transport demonstration; no real model, DB or sockets.

Artificial approvals/slots stay in an explicitly synthetic evidence subtree.
Never pass these fixture approvals or launch booleans to a production runner.
"""
from copy import deepcopy
import hashlib,json,shutil,sys,time
from pathlib import Path
from unittest.mock import patch
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now
from prepare_osdb_exposed_regression_20261027 import dependencies
from osdb_budgeted_model_client_dev import BudgetedModelClient,load_model_inputs,run_development_path
from osdb_scoped_attempts_dev import ScopedAttempts,SCOPE,wire,sha,read,immutable
from osdb_response_boundary_dev import score_numeric
from observed_profile_semantic_cases import SCHEMA,call,single

ROOT=Path('D:/ch7');CODE=Path(__file__).parent
OUT=ROOT/'reports/osdb_model_bridge_offline_v2_20261027'
PLAN=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027'


def main():
 assert not OUT.exists()
 before=budget();assert not before['pending'];runtime_before=runtime()
 canon=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
 freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(canon/n)==h for n,h in freeze.items())
 manifest_hash=sha(PLAN/'model_path_manifest.json');inputs=load_model_inputs(PLAN,manifest_hash);assert len(inputs)==48
 sources=dependencies(['osdb_budgeted_model_client_dev.py','osdb_scoped_attempts_dev.py'])
 sources.update({Path(__file__).name:sha(Path(__file__)),'test_osdb_budgeted_model_client_dev.py':sha(CODE/'test_osdb_budgeted_model_client_dev.py')})
 OUT.mkdir();(OUT/'executed_sources').mkdir()
 for name in sources:shutil.copyfile(CODE/name,OUT/'executed_sources'/name)
 immutable(OUT/'protocol.json',{'sealed_local':now(),'kind':'SYNTHETIC_TRANSPORT_AND_LEDGER_NOT_PHYSICAL_MODEL_ATTEMPTS',
  'planned_behavior_demonstrations':['one_repair','valid_wrong_no_repair','shared_plan_controls','crash_pending_blocks_restart','three_infrastructure_failures_stop'],
  'real_prepared_input_paths_hash_verified':48,'real_plan_manifest_sha256':manifest_hash,
  'real_inputs_are_not_sent_to_transport':True,'sources':sources,'original_adapter46':runtime_before,'canonical32':freeze,
  'budget_before':before,'sockets_forbidden':True,'physical_model_calls':0,'DB_queries':0,'formal_admission':False})
 # Production allowance absence is checked by READ ONLY of the actual approval;
 # no real campaign/lock/slot is created by this probe.
 sys.path.insert(0,str(canon/'experiment-harness/public_benchmark_v1'))
 from attempt_budget import AttemptBudget
 actual=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json')
 actual_gate_rejected=False
 try:ScopedAttempts(actual,OUT/'must_not_exist',manifest_hash,inputs).approval()
 except RuntimeError:actual_gate_rejected=True
 assert actual_gate_rejected and not (OUT/'must_not_exist').exists()
 private={'model':'ling-3.0-flash-n72-75','temperature':.2,'top_p':.95,'reasoning_effort':'high','max_tokens':8192,
  'endpoint':'http://synthetic-offline-fixture.invalid:1/chat','api_key':'SYNTHETIC_NOT_A_CREDENTIAL'}
 keys=['repair__direct','wrong__direct','plan__ours','unknown__direct']+['infra'+str(i)+'__direct' for i in range(4)]
 initial={k:[{'role':'system','content':'synthetic protocol'},{'role':'user','content':'synthetic fixture question '+k}] for k in keys}
 fake_plan=hashlib.sha256(wire(initial)).hexdigest()
 fake_admission={'scope_id':SCOPE,'plan_manifest_sha256':fake_plan,'route_repair_verified':True,'online_launch_accepted':True,
  'formal_prediction':False,'model_configuration_sha256':hashlib.sha256(wire(private)).hexdigest(),
  'initial_messages_sha256':{k:hashlib.sha256(wire(v)).hexdigest() for k,v in initial.items()},
  'is_synthetic_fixture_not_real_route_acceptance':True}
 def fixture(name):
  p=OUT/'synthetic'/name;p.mkdir(parents=True);(p/'ledger').mkdir()
  a=p/'synthetic_approval_NOT_HUMAN.json'
  immutable(a,{'kind':'ARTIFICIAL_APPROVAL_FOR_OFFLINE_TEST_ONLY','approved_cumulative_cap':12096,'historical_attempts_at_approval':0,
   'osdb_exposed_development':{'scope_id':SCOPE,'approved':True,'approved_additional_attempts':96,'plan_manifest_sha256':fake_plan}})
  return p,AttemptBudget(p/'ledger',a)
 schema=deepcopy(SCHEMA);schema['entities']=schema['entities'][:1]
 counters={'injected_transport_invocations':0,'fake_data_execute_callbacks':0};replies=iter(['{',json.dumps({'status':'ANSWER','query':'RETURN 42 AS id'}),
  json.dumps({'status':'ANSWER','query':'RETURN 999 AS id'}),json.dumps(single(call()))])
 def transport(body):
  counters['injected_transport_invocations']+=1
  if body['messages']==initial['unknown__direct']:raise KeyboardInterrupt('Intentional synthetic unknown outcome')
  return {'http_status':200,'body':{'model':private['model'],'choices':[{'finish_reason':'stop','message':{'content':next(replies)}}],'usage':{}}}
 def execute(q,p):
  counters['fake_data_execute_callbacks']+=1
  return {'status':'EXECUTED','complete':True,'columns':['id'],'rows':[[999 if '999' in q else 42]] if q.startswith('RETURN') else [['a']]}
 outcomes={};p,legacy=fixture('reply_flow_and_pending');p2,legacy2=fixture('infra_stop')
 with patch('socket.socket',side_effect=AssertionError('Socket forbidden in offline fixture')) as socket_guard:
  with ScopedAttempts(legacy,p/'campaign',fake_plan,initial) as lease:
   client=BudgetedModelClient(lease,private,initial,fake_admission,transport)
   for key in ('repair__direct','wrong__direct','plan__ours'):
    result=run_development_path(client,key,key.rsplit('__',1)[1],schema,execute)
    outcomes[key]={'final_status':result['final']['status'],'attached_synthetic_attempts':result['attached_ledger_attempts']}
    if key!='plan__ours':outcomes[key]['post_final_developer_score']=score_numeric(result['final'],[[42]])['numeric_EX']
   try:client.generate('unknown__direct','initial')
   except KeyboardInterrupt:pass
   else:raise AssertionError('Synthetic crash did not propagate')
  assert legacy.inspect()['pending']==[5]
  restart_rejected=False
  try:
   with ScopedAttempts(legacy,p/'not_a_resume',fake_plan,initial):raise AssertionError('Pending was ignored')
  except RuntimeError:restart_rejected=True
  assert restart_rejected
  def infra(body):counters['injected_transport_invocations']+=1;return {'http_status':503,'body':{}}
  with ScopedAttempts(legacy2,p2/'campaign',fake_plan,initial) as lease:
   client=BudgetedModelClient(lease,private,initial,fake_admission,infra)
   for i in range(3):assert client.generate('infra'+str(i)+'__direct','initial')['transport_error']
   fourth_rejected=False
   try:client.generate('infra3__direct','initial')
   except RuntimeError:fourth_rejected=True
   assert lease.stopped and fourth_rejected
  assert socket_guard.call_count==0
 assert outcomes['repair__direct']['post_final_developer_score'] is True
 assert outcomes['wrong__direct']['post_final_developer_score'] is False
 assert outcomes['wrong__direct']['attached_synthetic_attempts']==1
 assert outcomes['plan__ours']['attached_synthetic_attempts']==1
 assert counters=={'injected_transport_invocations':8,'fake_data_execute_callbacks':4}
 assert legacy2.inspect()['reserved']==3 and not legacy2.inspect()['pending']
 assert all(sha(canon/n)==h for n,h in freeze.items()) and runtime()==runtime_before
 assert all(sha(CODE/n)==h for n,h in sources.items()) and sha(PLAN/'model_path_manifest.json')==manifest_hash
 after=budget();assert after==before
 summary={'kind':'OFFLINE_MODEL_BUDGET_BRIDGE_COMPONENT_NOT_REAL_MODEL_OR_DB_ACCEPTANCE',
  'demonstrations_planned':5,'demonstrations_completed':5,'demonstrations_failed':0,'demonstrations_undispatched':0,
  'real_prepared_model_paths_loaded_and_hash_verified':48,'real_allowance_gate_rejected_read_only':True,
  'outcomes':outcomes,'synthetic_transport_invocations':8,'synthetic_ledger_slots':8,
  'synthetic_unknown_slot_retained':5,'synthetic_unknown_blocks_restart':True,'synthetic_three_infra_errors_stop':True,
  'synthetic_fourth_request_not_dispatched':True,'fake_data_callbacks':4,'socket_calls':0,'HTTP_requests':0,'DB_queries':0,
  'actual_model_calls':0,'training':0,'no_actual_budget_approval_or_lock_created':True,
  'latest_unit_checks_observed_in_tool':{'tests_passed':18,'failures':0,'reported_seconds':1.701,
   'initial_unit_run':{'tests_passed':14,'reported_seconds':1.644},
   'note':'Observed unittest invocations before this acceptance script; not rerun by this script. Tests use deleted temporary synthetic ledgers and injected transports.'},
  'budget_before':before,'budget_after':after,'original32_and46_unchanged':True,'sources':sources,
  'no_live_gateway_DB_node_deployment_or_full_campaign_admission':True,'formal_N':None,
  'OSDB_real_model_dispatched':0,'OSDB_real_model_completed':0,'OSDB_real_model_failed':0,'OSDB_initial_paths_undispatched':48,
  'formal_admission':False,'entire_T1_complete':False,
  'remaining':['Concrete route repair information and separate96 approval still pending human reply.',
   'Live one-shot transport/model identity/context/usage plus actual admitted graph and full campaign deployment remain untested.',
   'Full controls/cost/cache/independent-deployment attribution and final scoring/statistics/resource freeze.',
   'Sufficient independent OSDB task/structure scope; these24 historical positions are developer materials only.']}
 immutable(OUT/'summary.json',summary)
 for dest in (ROOT/'reports/t1_osdb_model_bridge_offline_20261027.json',CODE/'t1_osdb_model_bridge_offline_20261027.json'):immutable(dest,summary)
 print(json.dumps({k:summary[k] for k in ('demonstrations_completed','synthetic_ledger_slots','socket_calls','actual_model_calls','budget_after')},ensure_ascii=True))


if __name__=='__main__':main()
