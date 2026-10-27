"""One-shot offline all-position input preparation, no model/DB/network I/O.

Use unchanged common input/candidate code and sealed graph-only catalogs.
Only source ID and initial_question enter the task view. Never read evaluator
reference stores, responses, concern/order/difficulty/source labels or scores.
"""
from collections import Counter
from datetime import datetime
from pathlib import Path
import hashlib,json,os,sys,time,traceback

R=Path('D:/ch7');REPO=R/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
RUNTIME=REPO/'experiment-harness/public_benchmark_v1';sys.path.insert(0,str(RUNTIME))
from tugraph_common_input import NativeEntityCandidates,common_input,messages,canonical

OUT=R/'reports/text2gql_all_common_inputs_v1_20261027'
CACHE=R/'reports/text2gql_common_contract_20261026'
ASSETS=R/'literature/public_candidate_metadata_20261024/Text2GQL-Bench/selected_zip_assets_20261024'
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def bsha(v):return hashlib.sha256(v).hexdigest()
def write(p,v):
 with Path(p).open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')
def progress(v):
 p=OUT/'progress.writing';p.write_text(json.dumps(v,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');os.replace(p,OUT/'progress.json')
def input_view(row):
 # Source packages also contain gold and metadata. The projection accesses
 # these two public task fields only; no entire record reaches the builder.
 return {'id':str(row['id']),'question':row['initial_question']}
def stat(values):
 v=sorted(values)
 def quantile(p):
  x=(len(v)-1)*p;a=int(x);return v[a]+(v[min(a+1,len(v)-1)]-v[a])*(x-a)
 return {'n':len(v),'mean':sum(v)/len(v),'p50':quantile(.5),'p90':quantile(.9),'min':v[0],'max':v[-1]}

def main():
 OUT.mkdir(exist_ok=False);start=time.perf_counter()
 planpath=R/'reports/text2gql_native_engine_20261026/six_graph_input_plan.json'
 summarypath=R/'reports/t1_text2gql_common_contract_summary_20261026.json'
 plan=read(planpath);old=read(summarypath);sourcehashes={str(p):sha(p) for p in [planpath,summarypath,Path(__file__)]}
 runtime_hashes={n:sha(RUNTIME/n) for n in ['tugraph_common_input.py','entity_candidates.py','schema_services.py']}
 assert sha(RUNTIME/'tugraph_common_input.py')==old['source_sha256']['tugraph_common_input.py']
 write(OUT/'protocol.json',{'kind':'ALL_PUBLIC_TASK_INPUTS_PREPARED_NOT_FORMAL_OR_MODEL_RUN',
  'created_local':datetime.now().astimezone().isoformat(),'pid':os.getpid(),'source_sha256':sourcehashes,
  'runtime_sha256':runtime_hashes,'positions':3826,'method_input_paths':7652,
  'rules':['All six source packs and all3826 source initial_question positions; no exclusions or question rewrite.',
   'Source record projection accesses id/initial_question only. Gold fields/answers/order/concern/source/difficulty labels never enter messages.',
   'Reuse sealed graph-only21,315 entity catalogs and unchanged lexical candidate policy, maximum12 each.',
   'Direct/Ours common user content byte-identical; only output-format system instructions differ.',
   'No model configuration/credential, scorer/reference-store import, transport, model/DB/network call.',
   'Output is an offline input artifact, not budget activation, formal freeze/admission or background model runner.']})
 write(OUT/'active_process.json',{'pid':os.getpid(),'started_local':datetime.now().astimezone().isoformat(),
  'source_sha256':sha(__file__),'role':'OFFLINE_INPUT_PREPARATION_NO_MODEL_OR_DB','model_calls':0})
 manifest=[];graphs=[];chars={'direct':[],'ours':[]};candidate_counts=Counter();identities=set()
 public_keys={'task_id','graph','question','schema','entity_candidates','candidate_notice','database_dialect'}
 forbidden={'initial_gql','answer','answers','expected_rows','difficulty','source','semantic_goal','reference_query','reference_known','reference_contract_concern','order_rule'}
 try:
  for i,g in enumerate(plan['graphs']):
   started=time.perf_counter();prior=next(x for x in old['graphs'] if x['graph']==g['graph'])
   cp=CACHE/('%02d_catalog.json'%i);sp=CACHE/('%02d_shared_probe.json'%i)
   assert sha(cp)==prior['catalog_sha256'] and sha(sp)==prior['shared_probe_sha256']
   sourcehashes.update({str(cp):sha(cp),str(sp):sha(sp)})
   schema=read(sp)['shared']['payload']['schema'];entities=read(cp);assert len(entities)==prior['catalog_entities']
   catalog=NativeEntityCandidates(schema,entities)
   files=[p for p in (ASSETS/'dataset/test'/g['graph']/'Cypher').glob('*.json') if sha(p)==g['query_file_sha256']]
   assert len(files)==1;taskfile=files[0];sourcehashes[str(taskfile)]=sha(taskfile)
   tasks=[input_view(row) for row in read(taskfile)]
   assert [r['id'] for r in tasks]==g['reference_id_order'] and len(tasks)==g['reference_records']
   streams={arm:(OUT/('%02d_%s.jsonl'%(i,arm))).open('xb') for arm in ('direct','ours')}
   lookup_seconds=0.;generation_seconds=0.
   try:
    for ordinal,t in enumerate(tasks):
     identity=(g['graph'],t['id']);assert identity not in identities;identities.add(identity)
     before=time.perf_counter();candidates=catalog.find(t['question'],limit=12);lookup_seconds+=time.perf_counter()-before
     before=time.perf_counter();shared=common_input(task_id=t['id'],graph=g['graph'],question=t['question'],schema=schema,candidates=candidates)
     assert set(shared['payload'])==public_keys and not(forbidden&set(shared['payload']))
     direct,ours=messages(shared,'direct'),messages(shared,'ours')
     assert direct[1]['content']==ours[1]['content']==shared['serialized']
     assert shared['payload']['question']==t['question'] and len(candidates)<=12
     candidate_counts[len(candidates)]+=1
     for arm,ms in [('direct',direct),('ours',ours)]:
      assert [m['role'] for m in ms]==['system','user']
      payload={'graph':g['graph'],'id':t['id'],'messages':ms,'formal_admission':False}
      raw=(canonical(payload)+'\n').encode('utf-8');position=streams[arm].tell();streams[arm].write(raw)
      textchars=sum(len(m['content']) for m in ms);chars[arm].append(textchars)
      manifest.append({'graph':g['graph'],'id':t['id'],'arm':arm,'source_ordinal':ordinal,
       'input_file':Path(streams[arm].name).name,'offset':position,'bytes':len(raw),'record_sha256':bsha(raw),
       'messages_sha256':bsha(canonical(ms).encode('utf-8')),'common_user_sha256':shared['sha256'],
       'candidate_count':len(candidates),'message_characters':textchars,'model_tokens':None})
     generation_seconds+=time.perf_counter()-before
     if ordinal%25==0:
      for f in streams.values():f.flush()
      progress({'state':'OFFLINE_INPUT_PREPARATION','pid':os.getpid(),'graph':g['graph'],'current_id':t['id'],
       'completed_positions':len(identities),'planned_positions':3826,'prepared_paths':len(manifest),
       'new_model_calls':0,'new_database_calls':0,'updated_local':datetime.now().astimezone().isoformat()})
   finally:
    for f in streams.values():f.close()
   graphs.append({'graph':g['graph'],'positions':len(tasks),'graph_catalog_entities':len(entities),
    'candidate_lookup_seconds':lookup_seconds,'serialization_write_seconds':generation_seconds,
    'offline_wall_seconds':time.perf_counter()-started})
  assert len(identities)==3826 and len(manifest)==7652 and sum(candidate_counts.values())==3826
  write(OUT/'model_input_manifest.json',manifest);write(OUT/'source_input_hashes.json',sourcehashes)
  assert all(sha(p)==h for p,h in sourcehashes.items()) and all(sha(RUNTIME/n)==h for n,h in runtime_hashes.items())
  seal={p.name:sha(p) for p in OUT.glob('*.jsonl')}
  write(OUT/'artifact_seal.json',{'input_files_sha256':seal,'manifest_sha256':sha(OUT/'model_input_manifest.json'),
   'protocol_sha256':sha(OUT/'protocol.json'),'formal_freeze':False,'model_budget_activated':False})
  result={'kind':'TEXT2GQL_ALL_COMMON_INPUT_PREPARATION_NOT_PREDICTIONS','planned_positions':3826,
   'prepared_positions':3826,'planned_method_paths':7652,'prepared_method_paths':7652,
   'failed_positions':0,'undispatched_preparation_positions':0,'identical_common_input_pairs':3826,
   'candidate_limit':12,'candidate_count_distribution':dict(candidate_counts),'graph_entity_catalog_total':sum(g['graph_catalog_entities'] for g in graphs),
   'message_characters':{arm:stat(v) for arm,v in chars.items()},'model_tokens_unknown':True,
   'graphs':graphs,'input_sources_unchanged':len(sourcehashes),'runtime_files_unchanged':len(runtime_hashes),
   'model_dispatched':0,'model_completed':0,'model_failed':0,'model_undispatched_paths':7652,
   'new_model_calls':0,'new_database_calls':0,'new_network_calls':0,'reference_values_to_model':False,
   'protocol_sha256':sha(OUT/'protocol.json'),'artifact_seal_sha256':sha(OUT/'artifact_seal.json'),
   'formal_method_model_resource_freeze':False,'formal_prediction_admitted':False,'conditional_budget_activated':False,
   'offline_wall_seconds':time.perf_counter()-start,'T1_complete':False}
  write(OUT/'summary.json',result);progress({'state':'OFFLINE_INPUT_PREPARATION_COMPLETED','pid':os.getpid(),
   'completed_positions':3826,'prepared_paths':7652,'new_model_calls':0,'updated_local':datetime.now().astimezone().isoformat()})
  write(OUT/'runner_exit.json',{'pid':os.getpid(),'finished_local':datetime.now().astimezone().isoformat(),'state':'COMPLETED_NO_MODEL_CALLS'})
  print(json.dumps(result,ensure_ascii=True))
 except BaseException:
  write(OUT/'runner_failure.json',{'pid':os.getpid(),'finished_local':datetime.now().astimezone().isoformat(),
   'error':traceback.format_exc(),'prepared_positions':len(identities),'prepared_paths':len(manifest),'model_calls':0})
  raise

if __name__=='__main__':main()
