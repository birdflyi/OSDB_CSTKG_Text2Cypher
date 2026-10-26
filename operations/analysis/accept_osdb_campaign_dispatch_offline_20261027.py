# coding: utf-8
"""Durable synthetic campaign schedules, never a live launch or approval.

Reuse the unit fixture builder with persistent, explicitly artificial paths.
All callback counts below are injected Python calls, NOT model/DB requests.
"""
import io,json,shutil,threading
from pathlib import Path
from unittest.mock import patch
from rich.console import Console
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now
from prepare_osdb_exposed_regression_20261027 import dependencies
from osdb_scoped_attempts_dev import sha,read,immutable
from osdb_budgeted_model_client_dev import load_model_inputs
from osdb_campaign_monitor_dev import render
from test_osdb_campaign_dispatch_dev import CampaignTests

ROOT=Path('D:/ch7');CODE=Path(__file__).parent
OUT=ROOT/'reports/osdb_campaign_dispatch_offline_v1_20261027'
PLAN=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027'


def main():
 assert not OUT.exists(),'Preserve all previous runs'
 before=budget();assert before=={'reserved':6664,'cap':12000,'remaining':5336,'pending':[]}
 approval=ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json';approval_sha=sha(approval)
 adapter=runtime();canon=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
 frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(canon/n)==h for n,h in frozen.items())
 manifest_sha=sha(PLAN/'model_path_manifest.json');assert len(load_model_inputs(PLAN,manifest_sha))==48
 sources=dependencies(['osdb_campaign_dispatch_dev.py','osdb_campaign_monitor_dev.py','test_osdb_campaign_dispatch_dev.py'])
 sources[Path(__file__).name]=sha(Path(__file__))
 OUT.mkdir();(OUT/'executed_sources').mkdir()
 for name in sources:shutil.copyfile(CODE/name,OUT/'executed_sources'/name)
 names=['full48','three_infra_stop','unknown_pending_drain']
 immutable(OUT/'protocol.json',{'sealed_local':now(),'kind':'SYNTHETIC_SCHEDULE_NOT_REAL_MODEL_OR_GRAPH_ACCEPTANCE',
  'planned_scenarios':names,'sources':sources,'actual48_input_manifest_sha256':manifest_sha,
  'actual_inputs_hash_verified_not_dispatched':48,'sockets_forbidden':True,'budget_before':before,
  'actual_approval_sha256':approval_sha,'canonical32':frozen,'adapter46':adapter,
  'real_model_calls':0,'real_graph_queries':0,'formal_admission':False,
  'fixture_builder':'CampaignTests.setUp; persistent synthetic directories replace TemporaryDirectory only during setup'})
 receipts={};totals={'synthetic_slots':0,'synthetic_terminals':0,'synthetic_pending':0,'injected_transport_callbacks':0,'fake_data_callbacks':0}
 for name in names:
  folder=OUT/'synthetic'/name;folder.mkdir(parents=True)
  class PersistentFixture:
   def __init__(self):self.name=str(folder)
   def cleanup(self):pass
  fixture=CampaignTests()
  with patch('test_osdb_campaign_dispatch_dev.tempfile.TemporaryDirectory',PersistentFixture):fixture.setUp()
  immutable(folder/'NOT_A_HUMAN_APPROVAL_OR_LIVE_ROUTE.json',{'kind':'SYNTHETIC_FIXTURE_ONLY','production_use_forbidden':True})
  transport_count=[0]
  def transport(body):
   with fixture.lock:transport_count[0]+=1
   if name=='three_infra_stop':return {'http_status':503,'body':{}}
   if name=='unknown_pending_drain' and body['messages'][1]['content']=='redis_fixture0__direct':
    raise KeyboardInterrupt('Intentional synthetic unknown; preserve pending')
   return fixture.transport(body)
  with patch('socket.socket',side_effect=AssertionError('Offline evidence forbids sockets')) as guard:
   with fixture.lease() as lease:
    result=fixture.run_all(lease,client=fixture.client(lease,transport),max_inflight=1 if name=='three_infra_stop' else 4)
   assert guard.call_count==0
  ledger=fixture.legacy.inspect()
  assert result['paths_completed']+result['undispatched_paths']+result['unresolved_dispatched_paths']==48
  assert fixture.active_model==fixture.active_db==0 and fixture.open_graph is None
  if name=='full48':
   assert result['paths_completed']==48 and result['paired_cases_completed']==24
   assert transport_count[0]==48 and fixture.db_calls==72 and fixture.peak_db==1 and 2<=fixture.peak_model<=4
   assert not ledger['pending']
   for view in ('pipeline','job'):
    stream=io.StringIO();console=Console(file=stream,record=True,width=140,color_system=None)
    console.print(render(result,view));text=stream.getvalue()
    assert '离线合成回放' in text and '方法路径终结' in text
    (OUT/('chinese_'+view+'_preview.txt')).write_bytes(text.encode('utf-8'))
    console.save_html(str(OUT/('chinese_'+view+'_preview.html')),inline_styles=True)
  elif name=='three_infra_stop':
   assert result['paths_completed']==3 and result['terminal_failure_paths']==3 and result['undispatched_paths']==45
   assert transport_count[0]==3 and not ledger['pending']
   assert fixture.graph_events==[('open','redis'),('close','redis')]
  else:
   assert result['runner_error_paths']==1 and result['unresolved_dispatched_paths']==1 and len(ledger['pending'])==1
   assert fixture.graph_events==[('open','redis'),('close','redis')]
  receipt={'evidence_kind':'synthetic','planned_paths':48,'initial_dispatched':result['initial_paths_dispatched'],
   'completed_paths':result['paths_completed'],'terminal_failures':result['terminal_failure_paths'],
   'undispatched_paths':result['undispatched_paths'],'unresolved_dispatched_paths':result['unresolved_dispatched_paths'],
   'synthetic_slots':ledger['reserved'],'synthetic_pending':ledger['pending'],
   'injected_transport_callbacks':transport_count[0],'fake_data_callbacks':fixture.db_calls,
   'peak_simultaneous_fake_model_callbacks':fixture.peak_model,'peak_simultaneous_fake_DB_callbacks':fixture.peak_db,
   'graph_events':fixture.graph_events,'local_futures_drained_before_graph_close':True,'sockets':0,
   'real_model_calls':0,'real_DB_queries':0,'formal_admission':False}
  immutable(folder/'acceptance.json',receipt);receipts[name]=receipt
  totals['synthetic_slots']+=ledger['reserved'];totals['synthetic_terminals']+=ledger['reserved']-len(ledger['pending'])
  totals['synthetic_pending']+=len(ledger['pending']);totals['injected_transport_callbacks']+=transport_count[0];totals['fake_data_callbacks']+=fixture.db_calls
 assert runtime()==adapter and all(sha(canon/n)==h for n,h in frozen.items())
 assert all(sha(CODE/n)==h for n,h in sources.items()) and sha(approval)==approval_sha
 assert sha(PLAN/'model_path_manifest.json')==manifest_sha
 after=budget();assert after==before
 summary={'kind':'OFFLINE_CAMPAIGN_DISPATCH_NOT_LIVE_MODEL_OR_GRAPH_ACCEPTANCE','scenarios_planned':3,'scenarios_completed':3,
  'scenarios_failed':0,'scenarios_undispatched':0,'scenarios':receipts,'totals':totals,
  'unit_checks_observed_not_rerun':{'passed':8,'seconds':11.072,'failures':0},
  'actual_model_calls':0,'actual_DB_queries':0,'socket_calls':0,'training':0,'sources':sources,
  'actual_prepared_input_paths_loaded_hash_verified':48,'actual_prepared_input_paths_dispatched':0,
  'actual_prepared_input_paths_completed':0,'actual_prepared_input_paths_failed':0,'actual_prepared_input_paths_undispatched':48,
  'budget_before':before,'budget_after':after,'actual_approval_sha256_unchanged':approval_sha,
  'original32_and46_unchanged':True,'formal_N':None,'formal_admission':False,'entire_T1_complete':False,
  'not_deployed':True,'actual_gateway_graph_factory_and_server_deadline_acceptance_missing':True,
  'unknown_synthetic_pending_preserved_not_real_model_usage':True,
  'remaining':['Concrete gateway repair and separately scoped96 approval pending existing human questions.',
   'Real one-shot gateway/model/context/usage and real graph factory ownership/byte/permission/deadline acceptance.',
   'New development launch source/resource/control/cost/cache/scoring freeze and remote deployment acceptance.',
   'Independent OSDB tasks and remaining T1 benchmark/author-resource cells.']}
 immutable(OUT/'summary.json',summary)
 for dest in (ROOT/'reports/t1_osdb_campaign_dispatch_offline_20261027.json',CODE/'t1_osdb_campaign_dispatch_offline_20261027.json'):immutable(dest,summary)
 print(json.dumps({'scenarios_completed':3,'totals':totals,'actual_model_calls':0,'budget_after':after},ensure_ascii=True))


if __name__=='__main__':main()
