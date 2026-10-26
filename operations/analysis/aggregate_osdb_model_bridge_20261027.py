"""Include the failed v1 synthetic driver, not just the successful v2 run."""
from pathlib import Path
import json
from osdb_scoped_attempts_dev import read,sha,immutable

ROOT=Path('D:/ch7');CODE=Path(__file__).parent


def main():
 v1=ROOT/'reports/osdb_model_bridge_offline_v1_20261027'
 v2=ROOT/'reports/osdb_model_bridge_offline_v2_20261027'
 failure=read(v1/'failure_audit.json');raw=read(v2/'summary.json')
 assert raw['demonstrations_completed']==5 and raw['actual_model_calls']==raw['socket_calls']==0
 assert failure['preserved_synthetic_pending']==[1] and failure['actual_model_calls']==0
 protocols=[read(p/'protocol.json') for p in (v1,v2)]
 assert all(protocols[i]['budget_before']==raw['budget_after'] for i in range(2))
 counts=[]
 for root in (v1,v2):
  slots=list((root/'synthetic').glob('*/ledger/*.json'))
  pending=[];terminals=0
  for slot in slots:
   record=read(slot);request=Path(record['request_path'])
   assert sha(request)==record['request_sha256']
   if request.with_name('terminal.json').exists():terminals+=1
   else:pending.append(str(request.relative_to(root)))
  counts.append({'artificial_reserved':len(slots),'artificial_terminal':terminals,'artificial_pending':pending})
 assert [c['artificial_reserved'] for c in counts]==[1,8]
 assert [c['artificial_terminal'] for c in counts]==[0,7]
 assert [len(c['artificial_pending']) for c in counts]==[1,1]
 for root,protocol in zip((v1,v2),protocols):
  assert all(sha(root/'executed_sources'/n)==h for n,h in protocol['sources'].items())
 result={'kind':'ALL_ATTEMPTS_OSDB_OFFLINE_MODEL_BRIDGE_NOT_REAL_MODEL_OR_DB_RESULTS',
  'driver_runs_planned':2,'driver_runs_started':2,'driver_runs_completed':1,'driver_runs_failed':1,'driver_runs_undispatched':0,
  'initial_driver_failure':failure,'persistent_synthetic_ledger_counts':counts,
  'artificial_persisted_reservations_all_runs':9,'artificial_persisted_terminals_all_runs':7,'artificial_unknown_slots_all_runs':2,
  'successful_v2_summary':raw,'initial_protocol_sha256':sha(v1/'protocol.json'),'v2_protocol_sha256':sha(v2/'protocol.json'),
  'scope_note':'Synthetic fixture allowances/slots are never human approvals or physical model attempts. Unit-test temporary ledgers are additional test artifacts, not real usage.',
  'new_real_model_calls':0,'new_real_HTTP_requests':0,'new_real_DB_queries':0,'new_training':0,
  'real_budget_before':protocols[0]['budget_before'],'real_budget_after':raw['budget_after'],
  'actual_pending_requests':raw['budget_after']['pending'],'actual_approved_cap':12000,
  'real_OSDB_initial_paths_planned':48,'real_OSDB_initial_paths_dispatched':0,'real_OSDB_initial_paths_completed':0,
  'real_OSDB_initial_paths_failed':0,'real_OSDB_initial_paths_undispatched':48,
  'formal_N':None,'formal_admission':False,'entire_T1_complete':False,
  'limits':['No real gateway, actual model context/usage or live client-to-real-graph integration has been accepted here.',
   'Deadline and wire behavior are checked with fake HTTP connections; actual provider operation remains to be verified.',
   'This component has no deployment/scheduling CLI and cannot resume stopped queues or migrate a shared owner.',
   '96-call pool approval and concrete route repair information remain pending; no new question or permission is inferred.']}
 for p in (ROOT/'reports/t1_osdb_model_bridge_all_attempts_20261027.json',CODE/'t1_osdb_model_bridge_all_attempts_20261027.json'):
  immutable(p,result)
 print(json.dumps({k:result[k] for k in ('driver_runs_completed','driver_runs_failed','artificial_persisted_reservations_all_runs','new_real_model_calls','real_budget_after')},ensure_ascii=True))


if __name__=='__main__':main()
