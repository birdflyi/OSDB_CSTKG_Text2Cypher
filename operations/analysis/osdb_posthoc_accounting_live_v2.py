"""Read-only closed-campaign joins; developer row bags, never live scoring.

No runner, transport, query or approval mutation. Missing paths are unobserved,
not model failures. Shared generation is billed once; unknown usage stays null.
"""
from collections import Counter
import math
from pathlib import Path
from osdb_scoped_attempts_dev import read,sha,SCOPE
from osdb_budgeted_model_client_live_v2 import normalized_reply
from osdb_response_boundary_dev import score_numeric

ARMS=('direct','ours')
FIELDS=('prompt_tokens','completion_tokens','total_tokens','cached_tokens','reasoning_tokens')


def numeric(value):return type(value) in (int,float) and math.isfinite(value) and value>=0


def measure(values):
 known=[v for v in values if numeric(v)];ordered=sorted(known)
 def p(frac):
  if not ordered:return None
  index=(len(ordered)-1)*frac;i=int(index)
  return ordered[i]+(ordered[min(i+1,len(ordered)-1)]-ordered[i])*(index-i)
 return {'known':len(known),'unknown':len(values)-len(known),'sum_known':sum(known),
  'sum_all':sum(known) if len(known)==len(values) else None,
  'mean_known':sum(known)/len(known) if known else None,'p50_known':p(.5),'p90_known':p(.9)}


def audit(campaign,ledger,manifest,evaluator,*,manifest_sha256,evaluator_sha256,evidence_kind):
 if evidence_kind not in ('synthetic','live_development'):raise ValueError('Explicit evidence kind required')
 campaign=Path(campaign).resolve();ledger=Path(ledger).resolve();manifest=Path(manifest);evaluator=Path(evaluator)
 fingerprints={}
 def load(path):
  path=Path(path);fingerprints[str(path.resolve())]=sha(path);return read(path)
 rows=load(manifest);references=load(evaluator)
 if sha(manifest)!=manifest_sha256 or sha(evaluator)!=evaluator_sha256:raise ValueError('Manifest/evaluator hash differs')
 keys=[r['case_id']+'__'+r['arm'] for r in rows];cases={r['case_id'] for r in rows}
 if len(keys)!=48 or len(set(keys))!=48 or len(cases)!=24:raise ValueError('Expected 48 unique paired paths')
 if any({r['arm'] for r in rows if r['case_id']==c}!=set(ARMS) for c in cases):raise ValueError('Incomplete method pairs')
 refs={r['task_id']:r for r in references}
 if len(refs)!=len(references) or set(refs)!=cases:raise ValueError('Evaluator identities differ')
 lease=load(campaign/'scope_lease.json');close=load(campaign/'scope_close.json')
 if lease['scope']!=SCOPE or lease['plan_manifest_sha256']!=manifest_sha256:raise ValueError('Scope/manifest mismatch')
 base=lease['budget_before']['reserved'];count=close['reserved']-base
 if type(count) is not int or not 0<=count<=96:raise ValueError('Invalid campaign reservation extent')
 attempts={};claimed=set();terminal_slots=set();slots=set()
 for slot in range(base+1,close['reserved']+1):
  entry=load(ledger/f'{slot:04d}.json');rid=entry['request_id'];key,phase=rid.rsplit('__',1)
  if key not in keys or phase not in ('initial','feedback') or rid in attempts:raise ValueError('Unsealed/duplicate attempt')
  if entry['scope_id']!=SCOPE or entry['plan_manifest_sha256']!=manifest_sha256:raise ValueError('Ledger scope differs')
  request=campaign/'attempts'/rid/'request.json'
  if Path(entry['request_path']).resolve()!=request.resolve() or sha(request)!=entry['request_sha256']:raise ValueError('Request path/hash differs')
  body=load(request);messages=body.get('messages')
  if type(messages) is not list or any(type(m.get('content')) is not str for m in messages):raise ValueError('Invalid request messages')
  if [m.get('role') for m in messages]!=(['system','user'] if phase=='initial' else ['system','user','assistant','user']):
   raise ValueError('Unexpected generation phase')
  folder=request.parent;claim_path=folder/'dispatch_claim.json';terminal_path=folder/'terminal.json'
  is_claimed=claim_path.exists();terminal=None
  if is_claimed:
   claim=load(claim_path)
   if claim['request_id']!=rid or claim['budget_slot']!=slot or claim.get('automatic_retry') is not False:raise ValueError('Dispatch identity differs')
   claimed.add(rid)
  if terminal_path.exists():
   terminal=load(terminal_path)
   if not is_claimed or terminal['budget_slot']!=slot or terminal['scope_id']!=SCOPE:raise ValueError('Orphan terminal')
   reply=normalized_reply(terminal,body['model'])
   if terminal['normalized_reply']!=reply or terminal['prompt_characters']!=sum(len(m['content']) for m in messages):
    raise ValueError('Terminal normalization/character count differs')
   if terminal.get('physical_attempt_in_request')!=1 or terminal.get('automatic_retry') is not False:raise ValueError('Unexpected retry')
   terminal_slots.add(slot)
  attempts[rid]={'slot':slot,'phase':phase,'key':key,'body':body,'terminal':terminal,'claimed':is_claimed,
   'prompt_characters':sum(len(m['content']) for m in messages)};slots.add(slot)
 if sorted(slots-terminal_slots)!=close['pending']:raise ValueError('Pending reconciliation differs')
 if any(rid.endswith('__feedback') and rid[:-8]+'initial' not in claimed for rid in attempts):raise ValueError('Feedback without initial dispatch')
 folders=campaign/'attempts'
 actual_request_files={p.resolve() for p in folders.glob('*/request.json')}
 if actual_request_files!={(folders/rid/'request.json').resolve() for rid in attempts}:raise ValueError('Unaccounted request files')
 outputs={};path_dir=campaign/'paths'
 for p in path_dir.glob('*.json'):
  value=load(p);key=value['path_key']
  if key not in keys or p.stem!=key or key in outputs:raise ValueError('Unknown/duplicate final path')
  records=value['records'];ids=[key+'__initial']+([key+'__feedback'] if len(records)==2 else [])
  if len(records) not in (1,2) or value['attached_ledger_attempts']!=len(records):raise ValueError('Invalid path record count')
  for rid,record in zip(ids,records):
   if rid not in attempts or attempts[rid]['terminal'] is None:raise ValueError('Path cites unterminated/unbilled generation')
   item=attempts[rid];reply=dict(item['terminal']['normalized_reply'],budget_slot=item['slot'],
    terminal_path=str(campaign/'attempts'/rid/'terminal.json'))
   if record!=reply:raise ValueError('Path/terminal reply differs')
  if value['arm']!=key.rsplit('__',1)[1]:raise ValueError('Final method differs')
  outputs[key]=value
 per_path={}
 for row in rows:
  key=row['case_id']+'__'+row['arm'];value=outputs.get(key);ref=refs[row['case_id']]['numeric_reference_rows']
  state='completed' if value else 'unresolved' if key+'__initial' in claimed else 'undispatched'
  scored=score_numeric(value['final'],ref) if value else {'numeric_EX':None,'reason':'NO_FINAL_PATH_NOT_A_MODEL_FAILURE'}
  controls={}
  if value:
   for name,control in value['shared_plan_controls'].items():
    if control.get('shared_planner') is not True or control.get('extra_model_calls')!=0:raise ValueError('Control generation accounting differs')
    if name=='simple_correct':
     if control.get('same_result_as')!='ours_final':raise ValueError('Unknown control alias')
     controls[name]=dict(scored,alias_of='ours_final')
    elif name=='full_detail':controls[name]=score_numeric(control['result'],ref)
    else:raise ValueError('Unadmitted control name')
  per_path[key]={'case_id':row['case_id'],'graph':row['graph'],'arm':row['arm'],'state':state,
   'final_status':value['final']['status'] if value else None,'final_score':scored,
   'first_score':score_numeric(value['first_result'],ref) if value else None,'shared_plan_control_scores':controls}
 by_arm={}
 for arm in ARMS:
  items=[v for v in per_path.values() if v['arm']==arm];billed=[a for a in attempts.values() if a['key'].endswith('__'+arm)]
  observed=[v for v in items if v['state']=='completed'];numeric_items=[v for v in items if refs[v['case_id']]['numeric_reference_rows'] is not None]
  numeric_observed=[v for v in numeric_items if v['state']=='completed'];known_scores=[v['final_score']['numeric_EX'] for v in numeric_observed]
  def metrics(field):return measure([a['terminal']['normalized_reply']['usage'].get(field) if a['terminal'] else None for a in billed])
  by_arm[arm]={'planned_paths':len(items),'completed_paths':len(observed),'unresolved_paths':sum(v['state']=='unresolved' for v in items),
   'undispatched_paths':sum(v['state']=='undispatched' for v in items),'planned_numeric_reference_paths':len(numeric_items),
   'completed_numeric_reference_paths':len(numeric_observed),'correct_completed_numeric_paths':sum(s is True for s in known_scores),
   'numeric_rate_all_planned':sum(s is True for s in known_scores)/len(numeric_items) if numeric_items and len(numeric_observed)==len(numeric_items) else None,
   'status_counts':dict(Counter(v['final_status'] for v in observed)),
   'reserved_attempts':len(billed),'claimed_attempts':sum(a['claimed'] for a in billed),
   'terminal_attempts':sum(a['terminal'] is not None for a in billed),
   'usage':{field:metrics(field) for field in FIELDS},
   'prompt_characters':measure([a['prompt_characters'] for a in billed]),
   'response_characters':measure([a['terminal']['normalized_reply']['response_characters'] if a['terminal'] else None for a in billed]),
   'request_wall_seconds':measure([a['terminal'].get('provider_wall_seconds') if a['terminal'] else None for a in billed]),
   'independent_method_end_to_end_seconds':None,'monetary_cost':None}
 for path,h in fingerprints.items():
  if sha(Path(path))!=h:raise ValueError('Source changed during posthoc audit')
 return {'kind':'POSTHOC_EXPOSED_DEVELOPMENT_NOT_FORMAL','evidence_kind':evidence_kind,
  'planned_paths':48,'completed_paths':len(outputs),'unresolved_paths':sum(v['state']=='unresolved' for v in per_path.values()),
  'undispatched_paths':sum(v['state']=='undispatched' for v in per_path.values()),
  'reserved_attempts':len(attempts),'claimed_attempts':len(claimed),'terminal_attempts':len(terminal_slots),
  'pending_slots':sorted(slots-terminal_slots),'by_arm':by_arm,'paths':per_path,'read_source_sha256':fingerprints,
  'physical_billing':'Unique ledger slots once; Direct initial and feedback reuse not charged twice.',
  'shared_plan_controls_logical_generation_cost':'Ours generation required independently by each shared-plan strategy; actual bill counted once.',
  'full_detail_model_calls_extra':0,'simple_correct_model_calls_extra':0,'cached_and_reasoning_are_subfields_not_additive':True,
  'independent_control_DB_cost_seconds':None,'independent_control_DB_cost_note':'Not derivable from orchestration wall time; do not add fixed-plan measurement as mandatory double execution.',
  'path_wall_note':'Stored path time includes shared full-detail control, read-lock waiting and orchestration; not independent method latency.',
  'coverage_note':'Numeric reference denominator separate from no-reference boundary positions; declines never correct numeric answers.',
  'formal_N':None,'formal_admission':False,'entire_T1_complete':False}
