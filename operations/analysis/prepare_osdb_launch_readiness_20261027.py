"""Read-only preparation inventory, deliberately NOT a launch admission.

No socket, model, graph, approval writer, service manager or recovery queue.
This pins current artifacts only; it cannot certify future runtime ownership.
"""
import json,platform,subprocess,sys
from pathlib import Path
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now
from prepare_osdb_exposed_regression_20261027 import dependencies
from osdb_scoped_attempts_dev import read,sha,immutable
from osdb_budgeted_model_client_dev import load_model_inputs

ROOT=Path('D:/ch7');CODE=Path(__file__).parent
OUT=ROOT/'reports/osdb_launch_readiness_v2_20261027'
PLAN=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027'


def main():
 assert not OUT.exists()
 b=budget();assert b=={'reserved':6664,'cap':12000,'remaining':5336,'pending':[]}
 approval=ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json';approval_sha=sha(approval);a=read(approval)
 assert a['approved_cumulative_cap']==12000 and not a.get('osdb_exposed_development',{}).get('approved')
 state=read(ROOT/'state/t1_admission_check_20261024.json')
 assert state['formal_run_ready'] is False and state['entire_T1_complete'] is False
 manifest_sha=sha(PLAN/'model_path_manifest.json');inputs=load_model_inputs(PLAN,manifest_sha)
 rows=read(PLAN/'model_path_manifest.json');old=read(PLAN/'pre_dispatch_plan.json');adapter=runtime()
 frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 canon=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
 assert all(sha(canon/p)==h for p,h in frozen.items()) and adapter==old['original_adapter46']
 old_sources=old['development_source_closure'];assert len(old_sources)==7
 assert all(sha(CODE/p)==h for p,h in old_sources.items())
 roots=list(old_sources)+['osdb_scoped_attempts_dev.py','osdb_budgeted_model_client_dev.py',
  'osdb_campaign_dispatch_dev.py','osdb_campaign_monitor_dev.py','osdb_posthoc_accounting_dev.py']
 sources=dependencies(roots);assert set(old_sources)<=set(sources)
 snapshots=read(ROOT/'reports/t1_osdb_semantic_snapshots_20261027.json');cold={}
 for g,v in old['cold_snapshot_receipts'].items():
  path=Path(v['root'])/'new_store_manifest_after_shutdown.json'
  assert sha(path)==v['cold_manifest_sha256']
  cold[g]=dict(v,semantic_snapshot_sha256=snapshots['graphs'][g]['semantic_snapshot_sha256'],
   nodes=snapshots['graphs'][g]['nodes'],edges=snapshots['graphs'][g]['edges'],
   current_manifest_sha_verified=True,current_store_bytes_or_live_owner_verified_by_this_script=False)
 pairs={c:{r['arm']:inputs[c+'__'+r['arm']] for r in rows if r['case_id']==c} for c in {r['case_id'] for r in rows}}
 assert len(pairs)==24 and all(v['direct'][1]==v['ours'][1] for v in pairs.values())
 refs=read(PLAN/'evaluator_only.json');assert len(refs)==24 and sum(v['numeric_reference_rows'] is not None for v in refs)==16
 completed=[('semantic_snapshots','t1_osdb_semantic_snapshots_20261027.json'),
  ('response_boundary','t1_osdb_response_boundary_20261027.json'),
  ('model_budget_offline','t1_osdb_model_bridge_offline_20261027.json'),
  ('campaign_offline','t1_osdb_campaign_dispatch_offline_20261027.json'),
  ('posthoc_offline','t1_osdb_posthoc_accounting_20261027.json')]
 evidence={name:{'path':str(ROOT/'reports'/filename),'sha256':sha(ROOT/'reports'/filename),
  'scope':'Previously completed evidence only; no checks, callbacks or DB scans rerun.'} for name,filename in completed}
 blockers=[
  {'id':'gateway_repair','owner':'human_resource','status':'PENDING_EXISTING_QUESTION',
   'needed':'Concrete repair/new-service evidence and usable model configuration; no further stopped-route probes.'},
  {'id':'separate96','owner':'human_resource','status':'PENDING_EXISTING_QUESTION',
   'needed':'Explicit separate OSDB exposed-development allowance96 plus matching plan SHA; actual12000 approval unchanged.'},
  {'id':'live_gateway','owner':'agent_runtime','status':'NOT_ACCEPTED',
   'needed':'Accept model identity/context/usage/one-shot deadline with new concrete service evidence; do not infer from synthetic booleans.'},
  {'id':'real_graph_factory','owner':'agent_implementation_and_runtime','status':'NOT_IMPLEMENTED_AND_ACCEPTED',
   'needed':'Bind cold byte proof, owned process/store/driver, semantic view, permissions, query deadlines and drain/close into actual factory; never merely set admitted=True.'},
  {'id':'independent_cost_trace','owner':'agent_implementation','status':'NOT_INTEGRATED',
   'needed':'Separate model, compile/combination, read-lock wait, DB/return and control replay spans; retain unknown cache and avoid using total path time as independent method latency.'},
  {'id':'development_deployment','owner':'agent_runtime','status':'NOT_ACCEPTED',
   'needed':'Own-directory client/factory/dispatcher/posthoc integration and monitor adapter; no edits to shared services or original owner.'},
  {'id':'final_development_freeze','owner':'agent_protocol','status':'DRAFT_ONLY',
   'needed':'Pin completed launch entry/factory/traces/adapter, external runtime, full source/messages/scoring/control/budget/resources; this inventory is not that freeze.'},
  {'id':'formal_OSDB_scope','owner':'research_evidence','status':'INCOMPLETE',
   'needed':'Sufficient independent tasks/structure holdout, references, formalN and statistical protocol;24 exposed positions remain developer-only.'}]
 head=subprocess.check_output(['git','-C',str(CODE.parents[1]),'rev-parse','HEAD'],text=True).strip()
 summary={'kind':'OSDB_LAUNCH_READINESS_DRAFT_NOT_APPROVAL_OR_FREEZE','created_local':now(),'base_commit':head,
  'preparer_sha256':sha(Path(__file__)),'old_source_manifest':{'files':7,'sha256':sha(PLAN/'pre_dispatch_plan.json')},
  'current_existing_local_component_sources':sources,'new_components_after_old_draft':sorted(set(sources)-set(old_sources)),
  'source_inventory_is_not_complete_future_launch_closure':True,'original_adapter46':adapter,'canonical32':frozen,
  'actual_inputs':{'manifest_sha256':manifest_sha,'messages_sha_verified':48,'paired_user_messages_identical':24,
   'evaluator_sha256':sha(PLAN/'evaluator_only.json'),'numeric_reference_positions':16,'no_numeric_reference_positions':8},
  'cold_receipts':cold,'completed_evidence':evidence,'remaining_gates':blockers,
  'proposed_attempt_scope':{'initial_paths':48,'one_feedback_each_max':48,'additional96_approved':False,
   'transport_retries':0,'extra_probes':0,'actual_model_identity_or_runtime_concurrency_admitted':False,
   'concurrency_for_actual_launch':None,'no_borrowing_CypherBench_balance':True,'Text2GQL_condition_not_activated':True},
  'existing_node':'3107; latest human-authorized /mnt/data/0/louzehua/; other services untouched',
  'local_preparation_python':{'executable':sys.executable,'version':platform.python_version(),'not_an_accepted_remote_runtime':True},
  'budget':b,'actual_approval_sha256_unchanged':approval_sha,
  'new_model_calls':0,'new_DB_queries':0,'old_callbacks_replayed':0,'old_checks_rerun':0,'cold_store_bytes_rescanned':False,
  'plan_paths':48,'dispatched_paths':0,'completed_paths':0,'failed_paths':0,'undispatched_paths':48,
  'formal_N':None,'formal_run_ready':False,'entire_T1_complete':False,
  'no_new_formal_result_or_resource_approval_created':True}
 assert sha(approval)==approval_sha and all(sha(CODE/p)==h for p,h in sources.items())
 OUT.mkdir();immutable(OUT/'readiness.json',summary)
 for p in (ROOT/'reports/t1_osdb_launch_readiness_v2_20261027.json',CODE/'t1_osdb_launch_readiness_v2_20261027.json'):immutable(p,summary)
 print(json.dumps({'existing_local_sources':len(sources),'added_after7':len(set(sources)-set(old_sources)),
  'remaining_gates':len(blockers),'model_calls':0,'budget':b,'ready':False},ensure_ascii=True))


if __name__=='__main__':main()
