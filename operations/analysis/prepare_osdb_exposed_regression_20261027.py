"""Seal a REVIEWABLE exposed-development request plan, never run inference.

Model messages and evaluator references are separate local artifacts. Route,
scoped budget, online caller integration and launch admission remain required.
This is not a formal confirmation freeze or a benchmark execution entry point.
"""
import ast,hashlib,json
from copy import deepcopy
from pathlib import Path
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now
from osdb_common_input_v3 import messages

ROOT=Path('D:/ch7');CODE=Path(__file__).parent
OUT=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027'
REGISTRY=ROOT/'reports/osdb_actor_candidate_intake_v1_20261026/candidate_registry.json'
INPUT=ROOT/'reports/osdb_response_input_dev_v3_20261027'
PROFILES=ROOT/'reports/osdb_observed_profile_dev_v2_20261027'
PROJECTS=('redis','duckdb','iotdb','elasticsearch')
FAMILIES=('ACT01','ACT02','ACT03','ACT04','ACT05','ACT06')
POLICY={
 'ACT01':'Full-snapshot account population; no named account or artifact anchor is requested.',
 'ACT02':'Full-snapshot account intersection; no named account or artifact anchor is requested.',
 'ACT03':'Full-snapshot two-window account cohort; no named anchor is requested.',
 'ACT04':'Full-snapshot first-observed cohort; no named anchor is requested.',
 'ACT05':'Asks whole prior history and people identity; entity-name suggestions cannot supply those missing facts.',
 'ACT06':'Asks employees/organizations and all-contribution denominator; no specified employer name is an entity anchor.'}


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,value):
 p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('x',encoding='utf-8',newline='\n') as f:json.dump(value,f,ensure_ascii=False,indent=2);f.write('\n')
def dependencies(names):
 result={};queue=list(names)
 while queue:
  name=queue.pop();p=CODE/name
  if name in result:continue
  result[name]=sha(p)
  for node in ast.walk(ast.parse(p.read_text(encoding='utf-8-sig'))):
   imported=([node.module] if isinstance(node,ast.ImportFrom) and node.module else
    [a.name for a in node.names] if isinstance(node,ast.Import) else [])
   for module in imported:
    local=module.split('.')[0]+'.py'
    if (CODE/local).is_file():queue.append(local)
 return result


def main():
 assert not OUT.exists(),'Never overwrite an earlier plan'
 before=budget();assert not before['pending']
 registry_hash=sha(REGISTRY);assert registry_hash=='26f24882852e446ae0d28957d3f60b0edaf5d2ec68e66d275d0159a83ac183b3'
 rows=read(REGISTRY);index={r['task_id']:r for r in rows};assert len(index)==24
 old={str(p.relative_to(INPUT)):sha(p) for p in INPUT.rglob('*.json')}
 adapter=runtime();frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 canonical=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
 assert len(adapter)==46 and len(frozen)==32 and all(sha(canonical/n)==h for n,h in frozen.items())
 profiles={g:read(PROFILES/(g+'_profile.json')) for g in PROJECTS}
 roots={g:ROOT/'reports/osdb_semantic_snapshots_v1_20261027/runtime'/g for g in PROJECTS}
 roots['elasticsearch']=ROOT/'reports/osdb_elasticsearch_offline_scan_v2_20261027'
 snapshots={g:{'root':str(p),'cold_manifest_sha256':sha(p/'new_store_manifest_after_shutdown.json'),
  'scope':'accepted semantic-only snapshot; verify live bytes/owner/immutability before boot, no blind reimport'} for g,p in roots.items()}
 sources=dependencies(['osdb_common_input_v3.py','osdb_model_read_executor_dev_v2.py','osdb_response_boundary_dev.py'])
 OUT.mkdir();manifest=[];evaluations=[];reviews=[]
 for graph in PROJECTS:
  for number,family in enumerate(FAMILIES):
   task_id=graph+'_'+family;row=index[task_id];assert row['id']==family
   args=dict(task_id=task_id,graph=graph,question=row['question'],schema=deepcopy(profiles[graph]))
   arms={a:messages(a,**args) for a in ('direct','ours')}
   assert arms==read(INPUT/'method_messages'/(task_id+'.json'))
   assert arms['direct']['messages'][1]==arms['ours']['messages'][1]
   order=('direct','ours') if number%2==0 else ('ours','direct')
   reviews.append({'task_id':task_id,'family':family,'question_sha256':hashlib.sha256(row['question'].encode()).hexdigest(),
    'entity_candidate_policy':'empty_same_both_arms_for_this_reviewed_population_or_boundary_request',
    'reason':POLICY[family],'reviewer':'same AI assistant, not independent human review',
    'not_applicable_to_anchored_future_tasks':True})
   for arm in order:
    payload={'messages':arms[arm]['messages']}
    p=OUT/'model_inputs'/(task_id+'_'+arm+'.json');write(p,payload)
    manifest.append({'case_id':task_id,'graph':graph,'arm':arm,'ordinal':len(manifest)+1,
     'model_input_path':str(p),'model_input_sha256':sha(p),
     'common_input_sha256':arms[arm]['common_input_sha256'],
     'prompt_characters':sum(len(m['content']) for m in payload['messages']),
     'first_reply_max_physical_attempts':1,'feedback_generations_max':1,'feedback_reply_max_physical_attempts':1})
   evaluations.append({'task_id':task_id,'family':family,'project':graph,
    'numeric_reference_rows':row['csv_expected_rows'],'boundary_expected_status':row['expected_status'],
    'source':'existing exposed developer registry, not independent gold',
    'must_not_be_read_by_model_message_builder':True})
 assert len(manifest)==48 and sum(r['numeric_reference_rows'] is not None for r in evaluations)==16
 assert sum(r['numeric_reference_rows'] is None for r in evaluations)==8
 write(OUT/'model_path_manifest.json',manifest);write(OUT/'evaluator_only.json',evaluations);write(OUT/'entity_policy_review.json',reviews)
 proposal={'kind':'OSDB_EXPOSED_DEVELOPMENT_RESOURCE_PROPOSAL_NOT_APPROVAL',
  'scope':'24 already exposed development positions x Direct/Ours; no formal test or other benchmark/backbone',
  'initial_generations':48,'feedback_generations_upper_bound':48,'physical_attempts_upper_bound':96,
  'automatic_transport_retries':0,'additional_probes':0,'stop_after_consecutive_infrastructure_failures':3,
  'original_CypherBench_approved_cap_untouched':12000,'requested_additional_scoped_pool':96,
  'proposed_combined_approved_cap_only_after_human_approval':12096,'approved_now':False,
  'model_candidate':'ling-3.0-flash-n72-75','temperature':0.2,'top_p':0.95,'reasoning_effort':'high',
  'max_completion_tokens':8192,'proposed_model_read_timeout_seconds':240,
  'single_graph_query_seconds':60,'single_query_max_rows':1000000,'single_query_max_bytes':64*1024**2,
  'inference_route':'must supply concrete repair/new-service evidence before any request; no endpoint probes here',
  'concurrency':'not admitted by this plan; resource/route acceptance must precede any new campaign',
  'known_price':None,'token_total_estimate':None,'reason_token_estimate_unknown':'Character counts are not token usage.',
  'maximum_completion_tokens_all_attempts':96*8192,
  'gates_before_dispatch':['Explicit separate OSDB development allowance, recorded locally and scope-enforced in the ledger.',
   'Concrete route/resource repair evidence and owner/hostname check on authorized node3107.',
   'Online caller/ledger integration and complete final-answer/control/cost persistence accepted.',
   'Verify current cold snapshot bytes/owner/resources and identical model-side permissions.',
   'Seal actual model/config/prompt/code/input/control versions; this draft does not itself admit inference.']}
 write(OUT/'resource_proposal_not_approval.json',proposal)
 protocol={'created_local':now(),'script_sha256':sha(Path(__file__)),'registry_sha256':registry_hash,
  'existing_v3_input_hashes':old,'development_source_closure':sources,'original_adapter46':adapter,
  'canonical32':frozen,'cold_snapshot_receipts':snapshots,'actual_cold_store_bytes_rescanned':False,
  'project_order':list(PROJECTS),'task_family_order':list(FAMILIES),'within_task_arm_order':'alternate by family position',
  'no_model_DB_or_network_calls':True,'budget_before':before,'not_a_formal_freeze':True}
 write(OUT/'pre_dispatch_plan.json',protocol)
 assert registry_hash==sha(REGISTRY) and all(sha(INPUT/n)==h for n,h in old.items())
 assert runtime()==adapter and all(sha(canonical/n)==h for n,h in frozen.items())
 after=budget();assert after==before
 summary={'kind':'OSDB_EXPOSED_DEVELOPMENT_PLAN_NOT_PREDICTIONS_OR_FORMAL_FREEZE',
  'planned_positions':24,'prepared_positions':24,'failed_preparation':0,'unprepared_positions':0,
  'prepared_initial_method_paths':48,'planned_model_dispatched':0,'planned_model_completed':0,'planned_model_failed':0,
  'planned_initial_model_undispatched':48,'numeric_developer_positions':16,'boundary_developer_positions':8,
  'scoped_entity_policy_reviews':24,'same_user_bytes':True,'new_entity_candidates':0,
  'prompt_character_range':[min(r['prompt_characters'] for r in manifest),max(r['prompt_characters'] for r in manifest)],
  'all_initial_prompt_characters':sum(r['prompt_characters'] for r in manifest),'character_count_not_tokens':True,
  'resource_proposal':proposal,'budget_before':before,'budget_after':after,
  'prior_v3_registry_and_frozen_sources_unchanged':True,'new_model_calls':0,'DB_queries':0,'network_requests':0,
  'formal_N':None,'formal_confirmation_admitted':0,'independent_structural_holdout_admitted':0,
  'same_assistant_review_not_human_or_blind':True,'model_messages_exclude_reference_and_review_metadata':True,
  'plan_manifest_sha256':sha(OUT/'model_path_manifest.json'),'source_closure_count':len(sources),
  'entire_T1_complete':False}
 write(OUT/'summary.json',summary)
 public=CODE/'t1_osdb_exposed_regression_plan_20261027.json';assert not public.exists()
 write(public,summary)
 print(json.dumps({k:summary[k] for k in ('prepared_positions','prepared_initial_method_paths','prompt_character_range','all_initial_prompt_characters','source_closure_count','new_model_calls')},ensure_ascii=True))


if __name__=='__main__':main()
