"""Retrospective task lineage only; no model, database or network calls.

Annotations are review sidecars. Missing historical metadata stays missing.
The source inventory, questions and reference operations determine the review;
neither model success nor reference answer cardinality selects any position.
"""
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path

ROOT = Path('D:/ch7')
BASE = ROOT / 'experiments/ch7_private/continuation_20261021'
OUT = ROOT / 'reports/osdb_structure_lineage_v1_20261027'

# Explicit semantic review, not an AST equivalence or independence classifier.
DESCRIPTIONS = {
 'issue_openers': 'Fixed Issue -> OPENED_BY -> Actor projection; preserve requested row multiplicity.',
 'pr_links': 'Fixed PullRequest -> LINKS_TO -> ExternalResource projection, preserving link records.',
 'pr_domain_counts': 'One-window LINKS_TO record count grouped by domain for a fixed PR.',
 'issue_count': 'Issue node count under a literal identifier prefix.',
 'issue_commit_path_count': 'Count IssueComment -> Issue and same-comment -> Commit joined path records.',
 'pr_link_count': 'One-window LINKS_TO record count for a fixed PR, including zero.',
 'issues_new': 'One-window distinct Issue count under observed initial OPENED_BY events.',
 'issues_closed': 'One-window distinct Issue count under observed CLOSED_BY events; no current-state inference.',
 'pr_new': 'One-window distinct PR projection under observed initial OPENED_BY events.',
 'pr_accepted': 'One-window distinct PR count under MERGED_BY events.',
 'pr_declined': 'One-window distinct PR projection under explicit CLOSED_BY pull_merged=False; not absence of merge.',
 'pr_acceptance_ratio': 'Same-window conditional distinct accepted/declined counts and guarded ratio.',
 'missing': 'Missing PR identity/context boundary; no numeric reference.',
 'causality': 'Actual resolving causal relationship requested; references do not establish it.',
 'missing_text_and_current_state': 'Open-state/unanswered Issue and mentions/assignment boundary.',
 'missing_text': 'PR creation-to-first-review-comment and reviewer attribution evidence boundary.',
 'missing_authorship': 'Issue-referenced Commit plus actual Commit authorship boundary.',
 'missing_prior_history': 'First-ever project contribution boundary; bounded snapshot cannot certify prior absence.',
 'missing_file_changes': 'File-modifying Commit, validated authorship and full provenance/timeline boundary.',
 'missing_fix_causality': 'Actual bug-fixing Commit/PR/branch provenance boundary.',
 'GROUNDED2_AGG_REF_BY_TYPE': 'Fixed Issue outgoing REFERENCE records grouped by service_rel_type.',
 'GROUNDED2_AGG_EVENT_BY_ACTOR_TIME': 'Literal outgoing Actor EVENT_ACTION records in one window grouped by type; direction diagnostic.',
 'GROUNDED4_001': 'IssueComment-to-Issue join comment-to-Actor; earliest qualifying time per comment/Actor pair.',
 'GROUNDED4_002': 'Push-to-PR join Push-to-Branch; earliest qualifying reference time per Push/Branch pair.',
 'branch_events': 'One-window distinct Branch conditional creation/deletion counts.',
 'pushed_commits': 'One-window distinct Commit count reached by scoped Push COMMITTED_AS events.',
 'pr_observed_duration': 'Per-PR earliest observed opening then earliest subsequent close/merge; omit missing terminal.',
 'reviewed_prs': 'One-window distinct PR count reached by incoming formal review CREATED_IN.',
 'opening_actor_attribution': 'One-window Issue/PR opening records grouped by Actor with two distinct artifact counts.',
 'issue_observed_duration': 'Per-Issue earliest observed opening then earliest subsequent close; omit missing terminal.',
 'issue_observed_response': 'Earliest subsequent comment by non-opening Actor; OPTIONAL zero-response -> null duration.',
 'issues_observed_active': 'One-window union of Issue lifecycle activity and incoming comments; distinct Issue count.'
}

def read(p):
 return json.loads(Path(p).read_text(encoding='utf-8-sig'))

def sha(p):
 return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def digest(v):
 return hashlib.sha256(json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')).hexdigest()

def write(p,v):
 with Path(p).open('x',encoding='utf-8',newline='\n') as f:
  json.dump(v,f,ensure_ascii=False,indent=2); f.write('\n')

def main():
 OUT.mkdir(exist_ok=False)
 inventory_path=ROOT/'reports/osdb_prior_task_exposure_20261025.json'
 registry_path=ROOT/'reports/osdb_task_lineage_v1_20261026/registry.json'
 candidate_path=ROOT/'reports/osdb_actor_candidate_intake_v1_20261026/candidate_registry.json'
 manifest_path=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027/model_path_manifest.json'
 evidence={str(p):sha(p) for p in (inventory_path,registry_path,candidate_path,manifest_path)}
 write(OUT/'protocol.json',{'kind':'RETROSPECTIVE_STRUCTURAL_REVIEW_NOT_NEW_CONFIRMATION',
  'created_local':datetime.now().astimezone().isoformat(),'source_sha256':sha(__file__),
  'rules':['Review all18 packages/167 positions, specifically every missing semantic_goal; no success-based selection.',
   'Derived English descriptions do not fill or change original semantic_goal=null.',
   'Family/query hashes alone cannot establish semantic independence or reference correctness.',
   'ACT03 is compared by account key, event direction, population, time windows and anti-membership, not keyword absence.',
   'Every ACT01-06 position already dispatched in the exposed developer batch remains development-only.',
   'No model response/score, numeric expected_rows or actual graph data is used for inclusion.',
   'No query execution, new model attempt, source fetch, frozen source or old result modification.'],
  'source_inputs':evidence,'descriptions':DESCRIPTIONS,'new_model_calls':0,'new_DB_calls':0})
 prior=read(inventory_path); old=read(registry_path)
 oldmap={(r['specification_sha256'],r['id_sha256']):r for r in old}
 rows=[]; tasks=[]; grouped=defaultdict(list)
 for package in prior['inventory']:
  m=BASE/package['manifest_path']; s=m.with_name('specifications.json')
  assert sha(m)==package['manifest_sha256'] and sha(s)==package['specification_sha256']
  assert read(m)['task_sha256']==sha(s)
  evidence.update({str(m):sha(m),str(s):sha(s)})
  values=read(s); assert len(values)==package['task_count']
  for t in values:
   identity=digest(t['id']); r=oldmap[(sha(s),identity)]
   assert r['question_sha256']==digest(t.get('question')) and r['declared_semantic_goal']==t.get('semantic_goal')
   family=t['family']; assert family in DESCRIPTIONS
   q=t.get('reference_query'); call=t.get('reference_call')
   assert r['query_sha256']==(digest(q) if q else None)
   row={'project':t['project'],'stratum':package['stratum'],'specification_sha256':sha(s),
    'id_sha256':identity,'question_sha256':digest(t['question']),
    'family':family,'declared_semantic_goal':t.get('semantic_goal'),
    'derived_structure_description':DESCRIPTIONS[family],'original_goal_still_missing':t.get('semantic_goal') is None,
    'reference_query_sha256':digest(q) if q else None,'reference_call_sha256':digest(call) if call else None,
    'disposition':'HISTORICAL_DEVELOPMENT_ONLY','confirmation_eligible':False,
    'ACT03_exact_contract_match_in_scoped_review':False,
    'comparison_caveat':'Scoped retrospective semantic review; not global absence proof or blinded structural split.'}
   # Full trace stays local, including actual question and reference operation.
   row.update(question=t['question'],reference_query=q,reference_call=call)
   rows.append(row);tasks.append(t);grouped[family].append(row)
 assert len(rows)==167 and len(grouped)==32
 missing=[r for r in rows if r['original_goal_still_missing']]
 assert len(missing)==56 and Counter(r['family'] for r in missing)=={f:4 for f in list(DESCRIPTIONS)[:14]}
 # All 48 query-bearing missing-goal positions have exact reference syntax
 # counterparts in the already-declared known-goal transfer material.
 known=defaultdict(set)
 for r in rows:
  if (r['declared_semantic_goal'] or '').startswith('known:'):
   known[r['family']].add(r['reference_query_sha256'])
 comparisons=[]
 for r in missing:
  assert r['reference_query_sha256'] in known[r['family']]
  comparisons.append({'specification_sha256':r['specification_sha256'],'id_sha256':r['id_sha256'],
   'family':r['family'],'query_present':r['reference_query_sha256'] is not None,
   'known_same_family_reference_syntax_match':True,
   'null_query_match_is_not_semantic_proof':r['reference_query_sha256'] is None})
 # Exposure is established by initial request artifacts, never by correctness.
 candidates=read(candidate_path); manifest=read(manifest_path); assert len(candidates)==24 and len(manifest)==48
 requests={}
 for n in (1,2,3):
  run=ROOT/('experiments/osdb_exposed_live_v%d_20261027'%n)
  assert (run/'runner_exit.json').exists()
  for p in (run/'campaign/attempts').glob('*__initial/request.json'):
   key=p.parent.name.removesuffix('__initial')
   assert key not in requests; requests[key]=str(p)
 assert len(requests)==48
 exposure=[]
 for c in candidates:
  keys=[c['task_id']+'__'+a for a in ('direct','ours')]
  assert all(k in requests for k in keys)
  originals=[r for r in manifest if r['case_id']==c['task_id']]
  assert {r['arm'] for r in originals}=={'direct','ours'}
  exposure.append({'task_id':c['task_id'],'candidate':c['id'],'project':c['project'],
   'old_candidate_disposition_preserved':c['disposition'],'initial_requests':[requests[k] for k in keys],
   'current_disposition':'EXPOSED_DEVELOPMENT_ONLY','formal_confirmation_eligible':False,
   'ACT03_relation':'Cross-window same-account anti-membership differs from old167; already exposed in development.' if c['id']=='ACT03' else None})
  evidence.update({requests[k]:sha(requests[k]) for k in keys})
 contracts=[
  {'candidate':'ACT01','relation':'Projection of old per-Actor positive Issue/PR counts; union of opening-account keys.', 'formal_eligible':False},
  {'candidate':'ACT02','relation':'Positive intersection filter on old per-Actor Issue/PR counts; no independent-goal certification.', 'formal_eligible':False},
  {'candidate':'ACT03','relation':'Earlier-window opening-account set minus later-window opening-account set; same account across windows. No exact contract found in scoped167. Old response non-opening-actor predicate and explicit PR decline are different absence semantics. All four ACT03 bindings now exposed.', 'formal_eligible':False},
  {'candidate':'ACT04','relation':'Bounded first-observed rather than first-ever; changed evidence contract, not an unseen independent task.', 'formal_eligible':False},
  {'candidate':'ACT05','relation':'Old first-ever contribution/prior-history boundary; no numeric gold or correct-refusal claim.', 'formal_eligible':False},
  {'candidate':'ACT06','relation':'Employer attribution plus contribution denominator boundary remains unadjudicated; current candidate already exposed.', 'formal_eligible':False}]
 write(OUT/'historical_review.json',rows);write(OUT/'missing_goal_comparisons.json',comparisons)
 write(OUT/'current_candidate_exposure.json',exposure);write(OUT/'candidate_contract_comparison.json',contracts)
 assert all(sha(p)==h for p,h in evidence.items())
 summary={'kind':'OSDB_HISTORICAL_STRUCTURE_AND_CURRENT_EXPOSURE_AUDIT','created_local':datetime.now().astimezone().isoformat(),
  'historical_packages_reviewed':18,'historical_positions_reviewed':167,'missing_goals_reviewed':56,
  'missing_goals_original_null_preserved':56,'missing_goal_family_groups_not_independent_goals':14,
  'missing_goal_query_positions_with_known_reference_syntax':48,'missing_goal_no_query_boundaries':8,
  'historical_family_labels_not_independent_goals':32,'historical_confirmation_eligible':0,
  'ACT03_scoped_contract_difference_observed':True,'ACT03_already_development_exposed_positions':4,
  'current_candidate_positions':24,'current_initial_path_requests':48,'current_candidates_confirmation_eligible':0,
  'current_candidate_exposure_counts':dict(Counter(r['candidate'] for r in exposure)),
  'independent_formal_N':None,'new_formal_model_dispatched':0,'new_formal_model_completed':0,
  'new_formal_model_failed':0,'new_formal_model_undispatched':None,
  'review_planned':167,'review_completed':167,'review_failed':0,'review_undispatched':0,
  'new_model_calls':0,'new_database_calls':0,'new_source_HTTP_calls':0,
  'input_files_unchanged':len(evidence),'artifacts':{p.name:sha(p) for p in OUT.glob('*.json')},
  'limits':['No exact match in scoped167 is not a complete archive/unseen/independence proof.',
   'Post-development review cannot retrospectively certify ACT03 as an independent confirmation task.',
   'Historical family labels and derived descriptions are not a statistical group count.',
   'Ours independent algorithmic increment remains unestablished; no performance or generalization claim.',
   'No task/reference answer or method score changed; T1/T2-T5 necessary gaps remain.'],
  'T1_complete':False}
 write(OUT/'input_hashes.json',evidence);write(OUT/'summary.json',summary)
 print(json.dumps(summary,ensure_ascii=True))

if __name__=='__main__':
 main()
