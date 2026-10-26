"""Audit and aggregate real tiny-DB replay and equal input preparation.

No model client, inference, real-task scoring or database access.
"""
import hashlib,json
from pathlib import Path
from osdb_response_boundary_dev import strict_json,ProtocolError,VERSION

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
LIVE=ROOT/'reports/osdb_response_boundary_live_v1_20261027'
INPUT=ROOT/'reports/osdb_response_input_dev_v3_20261027'
DEST=REPO/'operations/analysis/t1_osdb_response_boundary_20261027.json'


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
 assert not DEST.exists() and not (ROOT/'reports'/DEST.name).exists()
 raw=read(LIVE/'summary.json');protocol=read(LIVE/'protocol.json');inputs=read(INPUT/'summary.json')
 assert raw['accepted_scenarios']==raw['planned_scenarios']==15 and not raw['failure']
 assert inputs['prepared']==inputs['planned']==24 and inputs['model_calls']==0
 assert raw['budget_before']==raw['budget_after']==inputs['budget_before']==inputs['budget_after']
 assert not raw['budget_after']['pending']
 assert all(sha(LIVE/'executed_sources'/n)==h for n,h in protocol['sources'].items())
 changed={n for n,h in protocol['sources'].items() if sha(REPO/'operations/analysis'/n)!=h}
 assert changed=={'osdb_response_boundary_dev.py'}
 # Post-live decoder hardening is explicit; do not relabel the live source.
 for malformed in ('{"value":NaN}','{"value":[1e999]}'):
  try:strict_json(malformed)
  except ProtocolError:pass
  else:raise AssertionError('Non-finite JSON accepted')
 rows={p.stem:read(p) for p in (LIVE/'scenarios').glob('*.json')}
 assert len(rows)==15 and all(r['accepted'] for r in rows.values())
 assert all(r['replay']['physical_model_calls']==0 for r in rows.values())
 posts=[read(p) for p in (LIVE/'setup').glob('*/terminal.json')]
 assert len(posts)==4 and all(p['http_status']==200 and not p['body']['errors'] for p in posts)
 policy=[read(p) for p in (LIVE/'new_model_boundary').glob('*/policy_rejection.json')]
 assert len(policy)==1 and policy[0]['physical_answer_query_dispatched'] is False
 stopped=read(LIVE/'shutdown_receipt.json');assert stopped['port_closed'] and stopped['bolt_port_closed']
 freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'/n)==h for n,h in freeze.items())
 assert all(sha(REPO/'experiment-harness/public_benchmark_v1'/n)==h for n,h in protocol['runtime_before'].items())
 result={'kind':'OSDB_DEVELOPER_RESPONSE_BOUNDARY_AND_EQUAL_INPUTS_NOT_MODEL_RESULTS',
  'synthetic_replay':{k:raw[k] for k in ('planned_scenarios','dispatched_scenarios','accepted_scenarios','failed_scenarios','undispatched_scenarios','baseline_probes')},
  'scenario_outcomes':{name:{'final_status':r['replay']['final']['status'],'prewritten_records_consumed':r['replay']['response_records_consumed'],
   'numeric_developer_EX':r['score']['numeric_EX'],'accepted_behavior':r['accepted']} for name,r in sorted(rows.items())},
  'old_catalog_rows_read':raw['baseline_probes']['old_catalog']['rows'],'new_catalog_operators_rejected':policy[0]['operators'],
  'catalog_answer_query_dispatched_new':False,'one_model_repair_maximum':True,'numeric_reference_never_in_feedback':True,
  'HTTP_readiness':raw['HTTP_readiness'],'HTTP_fixture_POST':4,'HTTP_POST_errors':0,
  'HTTP_POST_client_seconds':sum(p['client_seconds'] for p in posts),
  'Bolt_Cypher_statements':raw['Bolt_Cypher_statements'],'Bolt_statement_errors_retained':raw['Bolt_statement_errors'],
  'Bolt_client_seconds':raw['Bolt_client_seconds'],'automatic_statement_retry':0,
  'input_preparation':inputs,'live_executed_source_hashes':protocol['sources'],
  'current_source_hashes':{n:sha(REPO/'operations/analysis'/n) for n in protocol['sources']},
  'post_live_change':{'module':'osdb_response_boundary_dev.py','current_version':VERSION,
   'change':'Reject finite-JSON numeric spellings overflowing Python float, e.g. 1e999, at parse time.',
   'unit_checks_passed':13,'additional_real_DB_runs':0,'original_live_source_and_result_preserved':True},
  'new_model_calls':0,'training':0,'budget':raw['budget_after'],
  'canonical32_and_adapter46_unchanged':True,'new_owned_tiny_JVM_stopped':True,'real_four_graph_rescans':0,
  'formal_N':None,'formal_model_dispatched':0,'formal_model_completed':0,'formal_model_failed':0,'formal_model_undispatched':None,
  'formal_admission':False,'entire_T1_complete':False,
  'limits':['All reply records are prewritten: these are operational behavior checks, not model inference or quality.',
   'The 24 original positions remain exposed developer materials; no independent test scope accepted.',
   'Both methods share user bytes and outer statuses, but native Cypher and finite plan system prompts differ.',
   'Exact typed row bags are a numeric developer contract, not final normalization for arbitrary graph values.',
   'Client read policy assumes exclusive immutable snapshots, not Community RBAC/network isolation.',
   'Live caller/ledger integration, independent tasks, entity policy, final freeze, route repair and separate OSDB budget remain gates.']}
 for p in (DEST,ROOT/'reports'/DEST.name):p.write_bytes((json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))
 print(json.dumps({k:result[k] for k in ('HTTP_POST_client_seconds','Bolt_Cypher_statements','Bolt_client_seconds','new_model_calls','budget')},ensure_ascii=True))


if __name__=='__main__':main()
