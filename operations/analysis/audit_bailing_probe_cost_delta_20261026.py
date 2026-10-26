"""Read-only5195-attempt reconciliation after one user-restored resource probe.

Reuses prior formal checks only after exact raw evidence identity. Does not run
the private one-shot requester, models, old queues, graph databases or networks.
"""
from datetime import datetime,timezone,timedelta
import hashlib,json,sys
from pathlib import Path

from reconcile_t1_cost_evidence_20261026 import new_bucket,add,seal

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
BASELINE=ROOT/'reports/all_attempt_cost_reconciliation_v2_20261026'
PROBE=ROOT/'reports/bailing_single_request_acceptance_20261026'
OUT=ROOT/'reports/bailing_probe_ledger_delta_20261026'


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,x):
 with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(x,f,ensure_ascii=False,indent=2);f.write('\n')
def stamp():return datetime.now(timezone(timedelta(hours=8))).isoformat()


def main():
 assert not OUT.exists(),'Preserve previous reconciliation'
 old=read(BASELINE/'summary.json');manifest=read(BASELINE/'physical_hash_manifest.json')
 assert len(manifest)==5194 and seal(manifest)==old['all_physical_evidence_seal']
 helper=Path(__file__).with_name('reconcile_t1_cost_evidence_20261026.py')
 assert sha(helper)==old['source_sha256'][helper.name]
 probe=read(PROBE/'summary.json');assert probe['slot']==5195 and probe['resource_response_accepted']
 assert sha(PROBE/'request.json')==probe['request_sha256'] and sha(PROBE/'terminal.json')==probe['terminal_sha256']
 assert sha(PROBE/'run_single_probe.py')==read(PROBE/'protocol.json')['script_sha256']
 sys.path.insert(0,str(CANON/'experiment-harness/public_benchmark_v1'))
 from attempt_budget import AttemptBudget
 budget=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
  ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
  path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])
 b=budget.inspect();before={k:b[k] for k in ('reserved','cap','remaining','pending')}
 assert before==probe['budget_after'] and not before['pending']
 frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(CANON/p)==h for p,h in frozen.items())
 base=REPO/'experiment-harness/public_benchmark_v1'
 runtime={str(p.relative_to(base)):sha(p) for p in base.rglob('*.py')}
 OUT.mkdir()
 write(OUT/'protocol.json',{'prepared_local':stamp(),'script_sha256':sha(Path(__file__)),
  'helper_sha256':sha(helper),'baseline_summary_sha256':sha(BASELINE/'summary.json'),
  'baseline_manifest_sha256':sha(BASELINE/'physical_hash_manifest.json'),
  'probe_request_sha256':probe['request_sha256'],'probe_terminal_sha256':probe['terminal_sha256'],
  'budget_before':before,'rules':['All old5194slot/request/terminal hashes must match; no resets.',
   'All4696 original prediction records retain their exact hashes.',
   'New5195is one resource/client probe, not an extra formal method or benchmark sample.',
   'Unknown fields stay unknown; reasoning not added again to completion/total.',
   'Model identity echo is not a weights/backend-revision certification.']})
 old_rows={x[0]:x for x in manifest};new_manifest=[];total=new_bucket()
 for path in sorted(budget.ledger.glob('*.json'),key=lambda p:int(p.stem)):
  n=int(path.stem);slot=read(path);request=budget._request(slot['request_path']).resolve();terminal=budget.terminal_path(request)
  scope=old_rows[n][4] if n<=5194 else 'fresh_bailing_resource_client_probe'
  row=[n,sha(path),sha(request),sha(terminal),scope]
  if n<=5194:assert row==old_rows[n],'Old raw evidence changed'
  else:assert n==5195 and request==(PROBE/'request.json').resolve()
  new_manifest.append(row);add(total,read(terminal))
 assert len(new_manifest)==5195 and new_manifest[:-1]==manifest
 predictions=read(BASELINE/'prediction_hash_manifest.json');assert len(predictions)==4696
 run=ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024'
 assert all(sha(run/name)==h for name,h in predictions)
 terminal=read(PROBE/'terminal.json');body=terminal['body']
 assert body['model']==probe['model'] and body['choices'][0]['finish_reason']=='stop'
 usage=body['usage'];assert usage['prompt_tokens']==49 and usage['completion_tokens']==281 and usage['total_tokens']==330
 assert total['all_three_standard_fields_known']==old['all_three_standard_fields_known']+1
 for field in ('prompt_tokens','completion_tokens','total_tokens'):
  assert total['tokens'][field]['known_sum']==old['tokens'][field]['known_sum']+usage[field]
  assert total['tokens'][field]['unknown_attempts']==old['tokens'][field]['unknown_attempts']==101
 assert runtime=={str(p.relative_to(base)):sha(p) for p in base.rglob('*.py')}
 assert all(sha(CANON/p)==h for p,h in frozen.items())
 b=budget.inspect();after={k:b[k] for k in ('reserved','cap','remaining','pending')};assert after==before
 write(OUT/'physical_hash_manifest.json',new_manifest);write(OUT/'aggregate_cost_fields.json',total)
 summary={'kind':'RESTORED_BAILING_ONE_REQUEST_AND5195_LEDGER_DELTA_NOT_FORMAL_RESULT',
  'completed_local':stamp(),'model_requested':probe['model'],'model_response_echo':body['model'],
  'weights_backend_revision_verified':False,'concurrency_used':1,'global_load_unknown':True,
  'probe_planned':1,'probe_dispatched':1,'probe_complete':1,'probe_failed':0,'probe_undispatched':0,
  'http_status':200,'finish_reason':'stop','trivial_requested_query_returned':probe['content_matches_trivial_requested_query'],
  'slot':5195,'new_usage':probe['known_standard_usage'],
  'new_reasoning_tokens_literal':usage['completion_tokens_details']['reasoning_tokens'],
  'new_client_seconds':probe['client_seconds'],'new_retry_wait_seconds':0.0,'new_physical_retries':0,
  'actual_cumulative_attempts':5195,'approved_cap':12000,'remaining_original_CB_scope':6805,'pending':[],
  'old5194slot_request_terminal_hashes_unchanged':True,'old4696_prediction_record_hashes_unchanged':True,
  'global_all_three_usage_known':total['all_three_standard_fields_known'],'global_usage_unknown':101,
  'global_known_token_subtotal':{k:total['tokens'][k]['known_sum'] for k in ('prompt_tokens','completion_tokens','total_tokens')},
  'global_cache_fields_unobserved':True,'monetary_cost':'unknown','complete_T4':False,
  'formal_primary_paths':4696,'formal_physical_calls':4756,'new_formal_predictions':0,
  'old_stopped_Ling_batch_resumed':False,'second_backbone_result':False,
  'other_benchmark_budget_activated':False,'entire_T1_complete':False,
  'frozen32_runtime46_unchanged':True,'physical_evidence_seal':seal(new_manifest),
  'probe_request_sha256':probe['request_sha256'],'probe_terminal_sha256':probe['terminal_sha256'],
  'baseline_summary_sha256':sha(BASELINE/'summary.json'),'protocol_sha256':sha(OUT/'protocol.json'),
  'source_sha256':sha(Path(__file__)),
  'limits':['One tiny synthetic client probe does not establish throughput,5concurrency,complex-query accuracy or long-output stability.',
   'Ling and DeepSeek remain distinct frozen model contexts; no pooled metric.',
   'Global mixed-history token subtotal is not a formal-arm token count or zero-unknown bill.']}
 write(OUT/'summary.json',summary);write(ROOT/'reports/t1_bailing_probe_and_cost_delta_20261026.json',summary)
 print(json.dumps(summary,ensure_ascii=True))


if __name__=='__main__':main()
