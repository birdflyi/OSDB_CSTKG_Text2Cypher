"""Post-development read-only causal-boundary audit, not a repair or new score.

All model/DB I/O is forbidden. Archived outputs only enter the unchanged strict
decoder/compiler. No stripping prose, JSON repair, answer replacement or resend.
CSV witnesses are bounded prefixes, never a claimed census of property values.
"""
from pathlib import Path
from collections import Counter,defaultdict
import csv,hashlib,json,sys,time

ROOT=Path('D:/ch7');RUNS=[ROOT/('experiments/osdb_exposed_live_v%d_20261027'%i) for i in (1,2,3)]
OUT=ROOT/'reports/osdb_completed_campaign_diagnostic_v1_20261027'
PLAN=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027'
sys.path.insert(0,str(RUNS[-1]/'operations/analysis'))
from osdb_response_boundary_dev import decode,feedback_messages,ProtocolError
from osdb_budgeted_model_client_live_v2 import load_model_inputs,normalized_reply
import observed_neo4j_services_dev_v2 as compiler
from osdb_shared_read_executor_dev import read_query

def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def digest(v):return hashlib.sha256(json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')).hexdigest()
def write(p,v):
 with Path(p).open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')

class DatabaseBoundaryReached(BaseException):pass


def main():
 assert all((r/'runner_exit.json').exists() for r in RUNS)
 OUT.mkdir(parents=True,exist_ok=False)
 write(OUT/'protocol.json',{'kind':'READ_ONLY_POST_DEVELOPMENT_NOT_BLIND_OR_CONFIRMATION',
  'source_sha256':sha(Path(__file__)),'rules':['All48 initial/all16 feedback/all64 terminals, no outcome-based sampling',
   'Unchanged strict decoder/compiler with database callback always raising before any I/O',
   'Exact actual read query/parameter hash matching, never rewritten candidate queries',
   'At most2000 source CSV rows and3 OPENED_BY witnesses per graph; not exhaustive value absence evidence',
   'No new model/DB calls, replayed answers, relaxed score or changes to old files'],
  'created_epoch':time.time()})
 manifest_hash=sha(PLAN/'model_path_manifest.json');initial=load_model_inputs(PLAN,manifest_hash)
 manifest=read(PLAN/'model_path_manifest.json');bykey={m['case_id']+'__'+m['arm']:m for m in manifest}
 common_pairs={};evidence={};db=defaultdict(lambda:defaultdict(list));attempts={};paths={}
 for r in RUNS:
  for p in (r/'campaign/paths').glob('*.json'):
   assert p.stem not in paths;paths[p.stem]=read(p);evidence[str(p)]=sha(p)
  for p in (r/'graphs').glob('*/reads/*_read/request.json'):
   b=read(p);key=p.parents[2].name;db[key][digest([b['query'],b['parameters']])].append(str(p))
  for p in (r/'campaign/attempts').glob('*/terminal.json'):
   t=read(p);slot=t['budget_slot'];assert slot not in attempts
   rid=p.parent.name;key,phase=rid.rsplit('__',1);b=read(p.with_name('request.json'))
   m=bykey[key];sampling={'model':'ling-3.0-flash-n72-75','temperature':.2,'top_p':.95,'reasoning_effort':'high','max_tokens':8192}
   assert all(b.get(k)==v for k,v in sampling.items())
   if phase=='initial':
    assert b['messages']==initial[key];user=b['messages'][1]['content']
    u=json.loads(user);assert set(u)=={'access_contract','database_notes','entity_candidates','graph','question','response_contract','schema','task_id'}
    common_pairs.setdefault(m['case_id'],{})[m['arm']]=digest(user)
   else:
    path=paths[key];expected=feedback_messages(initial[key],path['records'][0]['content'],path['first_result'])
    assert expected is not None and b['messages']==expected
   raw=p.with_name('response.raw')
   if t.get('http_status')==200:
    assert raw.exists() and json.loads(raw.read_bytes())==t['body']
   attempts[slot]={'rid':rid,'key':key,'phase':phase,'terminal_path':str(p),'body':t,'request':b}
   evidence[str(p)]=sha(p)
 assert len(attempts)==64 and len(common_pairs)==24
 assert all(set(v)=={'direct','ours'} and len(set(v.values()))==1 for v in common_pairs.values())
 diagnostics=[];static_counts=Counter();query_presence=Counter();withheld=Counter()
 for slot,item in sorted(attempts.items()):
  key=item['key'];m=bykey[key];arm=m['arm'];reply=normalized_reply(item['body'],'ling-3.0-flash-n72-75')
  row={'slot':slot,'path_key':key,'phase':item['phase'],'arm':arm,'terminal_sha256':sha(item['terminal_path'])}
  if reply['transport_error']:row['static_status']='TRANSPORT_FAILURE_NO_GENERATED_ANSWER'
  elif reply['finish_reason']=='length':row['static_status']='MODEL_TRUNCATED_NO_DECODING'
  else:
   text=reply['content'];stripped=text.lstrip();row['prefix_kind']='JSON_OBJECT' if stripped.startswith('{') else 'MARKDOWN_FENCE' if stripped.startswith('```') else 'PROSE_OR_OTHER'
   try:value=decode(arm,text)
   except ProtocolError as exc:
    row.update(static_status='STRICT_PROTOCOL_REJECTED',error=str(exc))
   else:
    if value['status']!='ANSWER':row['static_status']='DECLINED_NOT_NUMERIC_CORRECT'
    elif arm=='direct':
     query=read_query(value['query']);matches=db[m['graph']].get(digest([query,{}]),[])
     row.update(static_status='DIRECT_NATIVE_QUERY',actual_read_request_matches=matches)
     row['property_name_observations']={p:p in query for p in ('service_rel_type','eventaction_verb_label','source_event_time','event_time')}
     query_presence['direct_with_actual_read' if matches else 'direct_without_read_terminal']+=1
    else:
     calls=value.get('calls') or [];declared=[];unbound=[]
     for index,call in enumerate(calls):
      if type(call) is not dict:continue
      variables={v.get('id') for v in (call.get('nodes') or [])+(call.get('edges') or []) if type(v) is dict}
      fields=[x.get('field') for kind in ('where','returns') for x in call.get(kind,[]) if type(x) is dict]
      for f in fields:
       if type(f) is str and '.' in f and f.split('.',1)[0] not in variables:unbound.append({'call':index,'field':f})
      declared.append({'call':index,'variables':sorted(v for v in variables if type(v) is str)})
     row['unbound_field_references']=unbound
     def forbidden(query,params):
      row['first_compiled_query_hash']=digest([query,params]);row['actual_read_request_matches']=db[m['graph']].get(row['first_compiled_query_hash'],[])
      raise DatabaseBoundaryReached()
     try:compiler.execute_plan(value,read(ROOT/('reports/osdb_observed_profile_dev_v2_20261027/'+m['graph']+'_profile.json')),forbidden)
     except DatabaseBoundaryReached:row['static_status']='COMPILER_REACHED_DB_BOUNDARY_NOT_EXECUTED'
     except compiler.UnsupportedPlan as exc:row.update(static_status='PLAN_STATICALLY_REJECTED',error=str(exc))
     else:raise AssertionError('Unexpected compiler path without DB-boundary signal')
  static_counts[(arm,row['static_status'])]+=1;diagnostics.append(row)
 for key,path in paths.items():
  if path.get('feedback_withheld_due_to_batch_stop'):withheld[key.rsplit('__',1)[1]]+=1
 # These files are the same source snapshots used by the independent reference.
 # Read only bounded prefixes to ground field semantics, not to recompute answers.
 base=ROOT/'experiments/ch7_private/continuation_20261021';private=ROOT/'experiments/ch7_private'
 sources={'redis':ROOT/'code/OSDB_CSTKG_Text2Cypher/data_real/d1_typed_projection_staging_v1_2/execution_csv/edges.csv',
  'duckdb':private/'duckdb_projection_publication_v1_20261021/execution_csv/edges.csv',
  'iotdb':base/'e8_iotdb_projection/execution_csv/edges.csv','elasticsearch':base/'e8_elasticsearch_projection/execution_csv/edges.csv'}
 csv.field_size_limit(64*1024**2);witnesses={}
 for graph,p in sources.items():
  samples=[];scanned=0
  with p.open(encoding='utf-8-sig',newline='') as f:
   for n,b in enumerate(csv.DictReader(f),1):
    if n>2000:break
    scanned=n
    if b.get('native_rel_type')!='EVENT_ACTION' or b.get('service_rel_type')!='OPENED_BY':continue
    props=json.loads(b['properties_json']);samples.append({'CSV_line':n+1,'properties_sha256':digest(props),
      'service_rel_type':props.get('service_rel_type'),'eventaction_verb_label':props.get('eventaction_verb_label'),
      'event_time':props.get('event_time'),'source_event_time':props.get('source_event_time')})
    if len(samples)==3:break
  witnesses[graph]={'source_path':str(p),'prefix_rows_scanned':scanned,'samples':samples,'not_a_census':True}
 write(OUT/'per_attempt_diagnostic.json',diagnostics);write(OUT/'bounded_property_witnesses.json',witnesses)
 result={'kind':'POST_DEVELOPMENT_DIAGNOSTIC_NO_NEW_SCORE','original_initial_messages_matched':48,
  'paired_common_inputs_identical':24,'actual_feedback_messages_matched':16,'all_sampling_matched':64,
  'raw_HTTP200_bodies_match_recorded_terminals':63,'scoped_attempts':64,'old_paths':len(paths),
  'static_counts':{arm:{status:n for (a,status),n in static_counts.items() if a==arm} for arm in ('direct','ours')},
  'direct_query_trace_counts':dict(query_presence),'feedback_withheld_due_to_stops':dict(withheld),
  'old_result_files_sha256':evidence,'source_sha256':sha(Path(__file__)),
  'compiled_queries_executed':0,'model_requests':0,'database_requests':0,'network_requests':0,
  'score_changes':0,'JSON_repairs':0,'reference_rows_used_by_dry_compiler':False,
  'remaining_limits':['Static output diagnosis is not a new model result or complete causal attribution.',
    'CSV prefixes prove witnesses only, not global absence or query counterfactual.',
    'Valid native Cypher can be semantically wrong; no gold-driven feedback or retries.',
    'Compiler accepts only stated IR; no new algorithmic increment established.'],
  'T1_complete':False}
 assert all(sha(p)==h for p,h in evidence.items()),'Original evidence changed during read-only audit'
 write(OUT/'summary.json',result);print(json.dumps({k:result[k] for k in ['original_initial_messages_matched','paired_common_inputs_identical','static_counts','direct_query_trace_counts','feedback_withheld_due_to_stops','model_requests','database_requests']}))


if __name__=='__main__':main()
