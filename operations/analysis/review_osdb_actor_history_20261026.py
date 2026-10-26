"""Record same-assistant scoped semantic review of all167 old specifications.

Question masking groups text for inspection only; it never proves independence.
Original goal-null fields, task data, outputs, scores and methods are untouched.
"""
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
import hashlib,json,re
from pathlib import Path

ROOT=Path('D:/ch7');BASE=ROOT/'experiments/ch7_private/continuation_20261021'
OUT=ROOT/'reports/osdb_actor_history_review_20261026'
NOTES={
 'issue_openers':'Actor IDs for one anchored Issue; no account-wide temporal absence.',
 'pr_links':'Anchored PR outgoing link records, with duplicates; not actor activity.',
 'pr_domain_counts':'Anchored PR timed link records grouped by domain; not actor activity.',
 'issue_count':'Issue population count by literal prefix, no actor timeline.',
 'issue_commit_path_count':'IssueComment-to-Issue and same-comment-to-Commit path count.',
 'pr_link_count':'Anchored PR timed link record count including zero.',
 'issues_new':'Distinct Issue openings in one window, counted Issue identity.',
 'issues_closed':'Observed Issue closes in one window, not current state.',
 'pr_new':'Distinct observed PR openings in one window; artifact identity.',
 'pr_accepted':'Distinct PR merge records in one window.',
 'pr_declined':'Explicit false-merge flag on closing records, not absence inference.',
 'pr_acceptance_ratio':'Two event cohorts accepted/explicitly declined, ratio/zero policy.',
 'missing':'Unresolved PR anchor or context, not temporal anti-membership.',
 'causality':'Actual resolving Commit link required, not observational reference.',
 'source:branch_events':'Distinct Branch create/delete counts per action in one window.',
 'source:pushed_commits':'Distinct Commit targets reached from timed Push COMMITTED_AS.',
 'source:pr_observed_duration':'Per-PR earliest opening followed by earliest terminal event.',
 'source:reviewed_prs':'PR identity with incoming timed formal-review event.',
 'source:opening_actor_attribution':'Per-Actor distinct Issue/PR opening counts in one window; ACT01 projection, ACT02 positive-count filter; two parameterized instances suffice for ACT03.',
 'source:issue_observed_duration':'Per-Issue earliest observed open-to-close duration.',
 'source:issue_observed_response':'Per-Issue first later response by non-opening Actor; anti-other-actor constraint and NULL differ from account-wide monthly gap.',
 'source:issues_observed_active':'Union of direct action/comment membership counted at Issue identity, not actor monthly absence.',
 'grounded:GROUNDED2_AGG_REF_BY_TYPE':'Anchored Issue outgoing REFERENCE edge counts by type, paraphrases retained.',
 'grounded:GROUNDED2_AGG_EVENT_BY_ACTOR_TIME':'Literal outgoing-Actor edges in one window, explicitly not inferred performed activity; opposite direction from ACT03 incoming opening facts.',
 'grounded:GROUNDED4_001':'Anchored Issue incoming comments joined to authors, earliest time per comment/Actor pair, paraphrases retained.',
 'grounded:GROUNDED4_002':'Anchored PR incoming Push references joined to Branch, earliest time per Push/Branch pair, paraphrases retained.',
 'role_boundary:BLIND1_001':'Open Issue current state, unanswered elapsed time and mentioned/assigned Actors; boundary, not account-wide gap.',
 'role_boundary:BLIND1_002':'Per-PR creation-to-first review-comment elapsed time with reviewers; incomplete history boundary.',
 'role_boundary:BLIND1_003':'Referenced Commit and authorship attribution, not actor cohort absence.',
 'role_boundary:BLIND3_NEW_CONTRIB':'True first contribution and attribution with incomplete prior history; ACT05 duplicate boundary, ACT04 scoped proxy.',
 'role_boundary:BLIND4_COMMIT_PROVENANCE':'File-modifying Commit anchor and complete provenance/timeline; boundary, not a fixed-window account count.',
 'role_boundary:BLIND4_BUG_FIX_CHAIN':'Actual bug-fix chain through PR/Branch/reviews with attribution; causal/complete-history boundary.'}


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def fp(x):return hashlib.sha256(json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def write(p,x):
 with p.open('x',encoding='utf-8') as f:json.dump(x,f,ensure_ascii=False,indent=2);f.write('\n')
def mask(s):
 for project in ('redis','duckdb','iotdb','elasticsearch'):s=s.replace(project,'<project>')
 return re.sub(r'\d+','<n>',s)


def main():
 assert not OUT.exists(),'Preserve prior review'
 OUT.mkdir();prior_path=ROOT/'reports/osdb_prior_task_exposure_20261025.json';prior=read(prior_path)
 candidate_path=ROOT/'reports/osdb_actor_candidate_intake_v1_20261026/candidate_registry.json'
 write(OUT/'protocol.json',{'created_local':datetime.now(timezone(timedelta(hours=8))).isoformat(),
  'script_sha256':sha(Path(__file__)),'prior_inventory_sha256':sha(prior_path),
  'candidate_registry_sha256':sha(candidate_path),'review_notes':NOTES,
  'rules':['All18packages and167positions; no model-output/score reads.',
   'Question text masking is inspection bookkeeping, not semantic proof.',
   'Review notes are tied to original question/query/call hashes; source goal fields stay unchanged.',
   'Same assistant review, not independent human adjudication or blind review.',
   'No claim about history outside the18registered packages.',
   'ACT03 is two-window composition of an old keyed attribution goal, not new primitive information.']})
 positions=[];groups=defaultdict(list);input_hashes={}
 for package in prior['inventory']:
  manifest=BASE/package['manifest_path'];spec=manifest.parent/'specifications.json'
  assert sha(manifest)==package['manifest_sha256'] and sha(spec)==package['specification_sha256']
  input_hashes[str(manifest)]=sha(manifest);input_hashes[str(spec)]=sha(spec)
  tasks=read(spec);assert len(tasks)==package['task_count']
  receipts={x['id_sha256']:x for x in package['task_receipts']}
  for task in tasks:
   old=receipts[fp(task['id'])]
   assert fp(task['question'])==old['question_sha256'] and fp(task.get('semantic_goal'))==old['semantic_goal_label_sha256']
   declared=task.get('semantic_goal');key=declared or task['family']
   if key.startswith('known:'):key=key[len('known:'):]
   assert key in NOTES
   entry={'project':task['project'],'id_sha256':fp(task['id']),
    'question_sha256':fp(task['question']),'reference_query_sha256':fp(task.get('reference_query')),
    'reference_call_sha256':fp(task.get('reference_call')),'original_semantic_goal':declared,
    'review_note_key':key,'question_contract_review_note':NOTES[key],
    'question_cluster_fingerprint':fp(mask(task['question'])),
    'reference_verified_by_this_review':False,'new_confirmation_admitted':False}
   positions.append(entry);groups[(declared is None,mask(task['question']))].append(entry)
 assert len(positions)==167 and sum(x['original_semantic_goal'] is None for x in positions)==56
 assert len(groups)==49 and sum(k[0] for k in groups)==14
 # Specific task evidence: identical July attribution, different ACT03 answer;
 # two old keyed attribution instances recover the answer exactly.
 diagnostics=[]
 for name,events in [('late_only',[('a','Issue',12)]),('early_extra',[('a','Issue',12),('b','PullRequest',5)])]:
  early={a for a,k,t in events if 0<=t<10};late={a for a,k,t in events if 10<=t<20}
  per_actor=Counter((a,k) for a,k,t in events if 10<=t<20)
  answer=sum(max(t for aa,k,t in events if aa==a and 0<=t<20)<10 for a in early|late)
  assert answer==len(early-late)
  diagnostics.append({'world':name,'late_attribution':sorted((a,k,n) for (a,k),n in per_actor.items()),
   'ACT03_answer':answer,'two_old_query_key_sets_answer':len(early-late)})
 assert diagnostics[0]['late_attribution']==diagnostics[1]['late_attribution']
 assert [x['ACT03_answer'] for x in diagnostics]==[0,1]
 for p,h in input_hashes.items():assert sha(Path(p))==h
 write(OUT/'review_positions.json',positions)
 write(OUT/'inspection_clusters.json',[{'original_goal_missing':k[0],
  'masked_question_fingerprint':fp(k[1]),'positions':len(v),
  'review_keys':sorted(set(x['review_note_key'] for x in v))} for k,v in groups.items()])
 write(OUT/'ACT03_specific_composition_diagnostic.json',diagnostics)
 summary={'kind':'SCOPED_SAME_ASSISTANT_HISTORICAL_CONTRACT_REVIEW_NOT_NOVELTY_CERTIFICATION',
  'historical_packages':18,'positions_reviewed':167,'original_missing_goal_positions_retained':56,
  'inspection_text_clusters':49,'missing_goal_text_clusters':14,'labeled_goal_text_clusters':35,
  'review_note_contracts':len(NOTES),'source_manifest_and_specification_files_verified':len(input_hashes),
  'ACT03_exact_same_request_contract_found_in_reviewed_scope':False,
  'ACT03_composition_of_old_attribution_goal':True,'ACT03_can_derive_from_two_keyed_old_query_results':True,
  'ACT03_new_primitive_capability_or_information_claim':False,'ACT03_confirmation_admitted':0,
  'ACT03_pending':['real graph reference acceptance','method and task freeze','same-model budget admission',
   'claim limited to scoped compositional request; no independent user/new-schema claim'],
  'original_goal_nulls_overwritten':0,'history_outside_registered_packages':'unknown',
  'same_assistant_not_independent_human':True,'new_model_DB_SSH_HTTP_requests':0,
  'reference_diagnostic_worlds':2,'diagnostic_new_model_predictions':0,
  'review_positions_sha256':sha(OUT/'review_positions.json'),
  'original_inputs_sha256':input_hashes,'protocol_sha256':sha(OUT/'protocol.json'),
  'candidate_registry_sha256_unchanged':sha(candidate_path),'entire_T1_complete':False}
 write(OUT/'summary.json',summary)
 print(json.dumps({k:v for k,v in summary.items() if k not in ('original_inputs_sha256',)}))


if __name__=='__main__':main()
