"""Audit existing synthetic archives once, without replaying any callback."""
import json,shutil
from pathlib import Path
from unittest.mock import patch
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now
from prepare_osdb_exposed_regression_20261027 import dependencies
from osdb_scoped_attempts_dev import read,sha,immutable
from osdb_posthoc_accounting_dev import audit

ROOT=Path('D:/ch7');CODE=Path(__file__).parent
OUT=ROOT/'reports/osdb_posthoc_accounting_v1_20261027'
ARCHIVE=ROOT/'reports/osdb_campaign_dispatch_offline_v1_20261027/synthetic'


def tree(folder):return {str(p.resolve()):sha(p) for p in sorted(folder.rglob('*')) if p.is_file()}


def main():
 assert not OUT.exists()
 before=budget();assert before=={'reserved':6664,'cap':12000,'remaining':5336,'pending':[]}
 actual_approval=ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json';approval_sha=sha(actual_approval)
 adapter=runtime();canon=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
 frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 assert all(sha(canon/p)==h for p,h in frozen.items())
 actual_evaluator=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027/evaluator_only.json'
 actual_refs=read(actual_evaluator);actual_ref_sha=sha(actual_evaluator)
 assert len(actual_refs)==24 and sum(r['numeric_reference_rows'] is not None for r in actual_refs)==16
 originals=tree(ARCHIVE)
 sources=dependencies(['osdb_posthoc_accounting_dev.py','test_osdb_posthoc_accounting_dev.py'])
 sources[Path(__file__).name]=sha(Path(__file__))
 OUT.mkdir();(OUT/'executed_sources').mkdir()
 for name in sources:shutil.copyfile(CODE/name,OUT/'executed_sources'/name)
 names=('full48','three_infra_stop','unknown_pending_drain')
 immutable(OUT/'protocol.json',{'sealed_local':now(),'kind':'READ_ONLY_POSTHOC_SYNTHETIC_ARCHIVES_NOT_MODEL_EX',
  'planned_archive_audits':list(names),'sources':sources,'actual_budget_before':before,
  'old_archive_before':originals,'actual_approval_sha256':approval_sha,
  'actual_evaluator_sha256':actual_ref_sha,'actual_references_loaded_only_for_identity_and_denominator_review':True,
  'synthetic_references_are_new_not_actual_answers':True,'callbacks_replayed':0,'sockets_forbidden':True})
 receipts={}
 with patch('socket.socket',side_effect=AssertionError('No socket allowed for posthoc audit')) as guard:
  for name in names:
   folder=ARCHIVE/name;manifest=folder/'model_path_manifest.json';rows=read(manifest)
   cases=sorted({r['case_id'] for r in rows})
   refs=[{'task_id':c,'numeric_reference_rows':[[1]] if int(c.rsplit('fixture',1)[1])<4 else None,
    'source':'SYNTHETIC_POSTHOC_REFERENCE_NOT_ACTUAL_OSDB_GOLD'} for c in cases]
   evaluator=OUT/(name+'_synthetic_evaluator.json');immutable(evaluator,refs)
   result=audit(folder/'campaign',folder/'ledger',manifest,evaluator,
    manifest_sha256=sha(manifest),evaluator_sha256=sha(evaluator),evidence_kind='synthetic')
   assert result['completed_paths']+result['unresolved_paths']+result['undispatched_paths']==48
   if name=='full48':
    assert result['completed_paths']==48 and result['reserved_attempts']==48
    assert all(a['planned_numeric_reference_paths']==16 and a['correct_completed_numeric_paths']==16 for a in result['by_arm'].values())
    assert all(a['usage']['total_tokens']['sum_all'] is None for a in result['by_arm'].values())
   elif name=='three_infra_stop':
    assert result['completed_paths']==3 and result['undispatched_paths']==45 and result['reserved_attempts']==3
   else:
    assert result['completed_paths']==3 and result['unresolved_paths']==1 and result['undispatched_paths']==44
    assert result['pending_slots']==[1]
   immutable(OUT/(name+'_audit.json'),result)
   receipts[name]={k:result[k] for k in ('planned_paths','completed_paths','unresolved_paths','undispatched_paths',
    'reserved_attempts','claimed_attempts','terminal_attempts','pending_slots')}
  assert guard.call_count==0
 assert tree(ARCHIVE)==originals and runtime()==adapter and sha(actual_approval)==approval_sha
 assert sha(actual_evaluator)==actual_ref_sha and all(sha(canon/p)==h for p,h in frozen.items())
 assert all(sha(CODE/p)==h for p,h in sources.items())
 after=budget();assert after==before
 summary={'kind':'OFFLINE_POSTHOC_RECONCILIATION_NOT_FORMAL','archive_audits_planned':3,'archive_audits_completed':3,
  'archive_audits_failed':0,'archive_audits_undispatched':0,'archives':receipts,
  'synthetic_reservations_reconciled':55,'synthetic_terminals_reconciled':54,'synthetic_pending_preserved':1,
  'new_synthetic_model_callbacks':0,'new_synthetic_DB_callbacks':0,'actual_model_calls':0,'actual_DB_queries':0,'socket_calls':0,
  'actual_budget_before':before,'actual_budget_after':after,'actual_approval_unchanged':True,
  'actual_evaluator_denominators':{'exposed_positions':24,'numeric_reference_positions':16,'boundary_no_numeric_reference':8},
  'actual_evaluator_not_used_for_synthetic_scores':True,'actual_model_paths_planned':48,'actual_model_paths_dispatched':0,
  'actual_model_paths_completed':0,'actual_model_paths_failed':0,'actual_model_paths_undispatched':48,
  'original32_and46_and_all_prior_archive_bytes_unchanged':True,'sources':sources,
  'unit_checks_observed_not_rerun':{'passed':8,'seconds':7.426,'failures':0},
  'cost_finding':'Orchestration path seconds include shared full-detail replay and read-lock waits; independent strategy E2E/DB/cache costs remain unknown.',
  'token_cost_policy':'Known subtotals plus unknown counts; whole total null when any usage unknown; cached/reasoning subfields not additive.',
  'physical_vs_logical_cost_policy':'Unique ledger slots billed once; shared-plan controls independently require the Ours generations without extra actual model charges.',
  'formal_N':None,'formal_admission':False,'entire_T1_complete':False,'not_deployed':True,
  'remaining':['Pending existing gateway repair and separate96 development approval questions.',
   'Actual gateway-to-real-graph factory integration and deadline/permission/ownership acceptance.',
   'Independent DB/E2E/cache costs and new source/resource/scoring/statistical freeze before developer launch.',
   'Independent OSDB task/structure scope and remaining T1 benchmark/author-resource cells.']}
 immutable(OUT/'summary.json',summary)
 for p in (ROOT/'reports/t1_osdb_posthoc_accounting_20261027.json',CODE/'t1_osdb_posthoc_accounting_20261027.json'):immutable(p,summary)
 print(json.dumps({'audits_completed':3,'old55_slots_reconciled':True,'model_calls':0,'budget_after':after},ensure_ascii=True))


if __name__=='__main__':main()
