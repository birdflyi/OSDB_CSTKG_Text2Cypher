"""One-shot identity bridge over accepted archived reference responses.

No old audit script is imported/executed and no query is replayed. The final
scorer integration uses fabricated PROTOCOL_ERROR predictions only; no real
prediction, accuracy, semantic gold adjudication or metric freeze is produced.
"""
from collections import Counter
from datetime import datetime
from pathlib import Path
import hashlib,json,sys,time

R=Path('D:/ch7');REPO=R/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
sys.path.insert(0,str(REPO/'experiment-harness/public_benchmark_v1'))
from text2gql_reference_store_eval_20261027 import SealedReferenceStore,ReferenceIdentityError
from text2gql_score_contract_dev_20261027 import FinalAnswerContract,summarize

OUT=R/'reports/text2gql_reference_identity_bridge_v1_20261027'
ASSETS=R/'literature/public_candidate_metadata_20261024/Text2GQL-Bench/selected_zip_assets_20261024'
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def wire(v):return (json.dumps(v,ensure_ascii=False,indent=2)+'\n').encode('utf-8')
def write(p,v):
 with Path(p).open('xb') as f:f.write(wire(v))
def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def fixture(name,*,known=True,extra=None):
 root=OUT/'fixtures'/name;root.mkdir(parents=True)
 body={'header':[{'name':'x'}],'result':[[1]],'size':1} if known else {'error_message':'fixed reference type error'}
 response=root/'response.json';write(response,body)
 record={'id':'one','index':0,'query_sha256':'a'*64,'response_sha256':sha(response),
  'status':'EXECUTED_NONEMPTY' if known else 'GOLD_EXECUTION_ERROR','http_status':200 if known else 400,
  'size':1 if known else None,'request_started':'2026-10-26T00:00:00+00:00','request_finished':'2026-10-26T00:00:01+00:00'}
 rp=root/'record.json';write(rp,record)
 entry={'graph':'g','id':'one','ordinal':0,'query_sha256':'a'*64,'reference_known':known,
  'reference_status':record['status'],'record_file':'record.json','record_sha256':sha(rp),
  'response_file':'response.json','response_sha256':sha(response)}
 if extra:entry.update(extra)
 index=root/'index.json';write(index,[entry]);seal=root/'seal.json'
 write(seal,{'kind':'EVALUATOR_ONLY_ARCHIVED_REFERENCE_INDEX_NOT_FORMAL_FREEZE','cache_root':str(root),
  'index_file':'index.json','index_sha256':sha(index),'positions':1,'known_reference_positions':int(known)})
 return root,seal,sha(seal)

def main():
 OUT.mkdir(exist_ok=False);start=time.perf_counter()
 cohortpath=R/'reports/text2gql_reference_cohorts_provisional_20261026.json'
 planpath=R/'reports/text2gql_native_engine_20261026/six_graph_input_plan.json'
 priorpath=R/'reports/t1_text2gql_native_reference_summary_20261026.json'
 concerns_path=R/'reports/text2gql_nonfinal_order_semantic_review_v1_20261027/reviewed_positions.json'
 code=[Path(__file__),Path(__file__).with_name('text2gql_reference_store_eval_20261027.py'),
  Path(__file__).with_name('text2gql_score_contract_dev_20261027.py')]
 inputs={str(p):sha(p) for p in [cohortpath,planpath,priorpath,concerns_path]+code}
 write(OUT/'protocol.json',{'kind':'EVALUATOR_ONLY_REFERENCE_IDENTITY_BRIDGE_NOT_METHOD_EX',
  'created_local':datetime.now().astimezone().isoformat(),'input_sha256':inputs,
  'rules':['All3826 positions and original3815 known/11 unknown retained;13 concern tags never exclude.',
   'Bind source task-file, task ID, query bytes, ordinal terminal, raw response, graph acceptance and archive manifest.',
   'Hash and completed terminal evidence only; no source query repair or replay, semantic certification or author-Bolt claim.',
   'References remain evaluator-only; no model-input/candidate/planner module reads this store.',
   'New bridge tamper/namespace checks; scorer all-ID integration uses artificial terminal failures, not method answers.']})
 checks=[]
 def check(name,condition):assert condition,name;checks.append(name)
 def rejects(fn):
  try:fn()
  except ReferenceIdentityError:return True
  return False
 root,seal,h=fixture('valid');store=SealedReferenceStore(seal,expected_seal_sha256=h)
 check('sealed_success_loads_exact_rows',store.load('g','one',expected_query_sha256='a'*64)['answer']['rows']==[[1]])
 check('foreign_graph_or_id_rejected',rejects(lambda:store.load('other','one',expected_query_sha256='a'*64)) and rejects(lambda:store.load('g','other',expected_query_sha256='a'*64)))
 check('wrong_task_query_hash_rejected',rejects(lambda:store.load('g','one',expected_query_sha256='b'*64)))
 meta=store.metadata('g','one');meta['reference_known']=False
 check('metadata_copy_does_not_mutate_store',store.metadata('g','one')['reference_known'] is True)
 root,seal,h=fixture('record_tamper');store=SealedReferenceStore(seal,expected_seal_sha256=h);(root/'record.json').write_bytes(b'{}')
 check('terminal_bytes_tamper_rejected',rejects(lambda:store.load('g','one',expected_query_sha256='a'*64)))
 root,seal,h=fixture('response_tamper');store=SealedReferenceStore(seal,expected_seal_sha256=h);(root/'response.json').write_bytes(b'{}')
 check('response_bytes_tamper_rejected',rejects(lambda:store.load('g','one',expected_query_sha256='a'*64)))
 root,seal,h=fixture('index_tamper');(root/'index.json').write_bytes(b'[]')
 check('index_bytes_tamper_rejected',rejects(lambda:SealedReferenceStore(seal,expected_seal_sha256=h)))
 root,seal,h=fixture('seal_tamper');seal.write_bytes(b'{}')
 check('seal_bytes_tamper_rejected',rejects(lambda:SealedReferenceStore(seal,expected_seal_sha256=h)))
 root,seal,h=fixture('escape',extra={'record_file':'../outside.json'})
 check('cache_traversal_rejected_before_read',rejects(lambda:SealedReferenceStore(seal,expected_seal_sha256=h)))
 root,seal,h=fixture('unknown',known=False);store=SealedReferenceStore(seal,expected_seal_sha256=h)
 check('fixed_reference_error_not_empty_answer',store.load('g','one',expected_query_sha256='a'*64)['answer'] is None and store.cohort()[0][1]['reference_known'] is False)
 # Construct the index using archived acceptance chains, never a remote host.
 co=read(cohortpath);plan=read(planpath);prior=read(priorpath)
 assert sha(cohortpath)==prior['reference_cohort_sha256'] and sha(planpath)==co['source_plan_sha256']
 cohorts={(r['graph'],str(r['id'])):(True,r) for r in co['native_reference_executable']}
 for r in co['native_reference_unscoreable']:
  k=(r['graph'],str(r['id']));assert k not in cohorts;cohorts[k]=(False,r)
 assert len(cohorts)==3826
 concerns={(r['graph'],r['id']) for r in read(concerns_path) if r['concern']}
 entries=[];chains=[];cache_root=R/'reports'
 for i,g in enumerate(plan['graphs']):
  folder='text2gql_native_reference_graph_'+str(i).zfill(2)+('_v3' if i==0 else '')+'_20261026'
  base=cache_root/folder;e=base/'evidence';ap=base/'acceptance.json';mp=base/'evidence_manifest.json';rp=e/'receipt.json';sp=e/'spec.json'
  accepted=read(ap);spec=read(sp);receipt=read(rp);manifest=read(mp)
  old=next(a for a in prior['graphs'] if a['graph']==g['graph']);assert sha(ap)==old['acceptance_sha256']
  assert sha(mp)==accepted['manifest_sha256'] and sha(rp)==accepted['receipt_sha256'] and sha(sp)==receipt['source_spec_sha256']
  assert receipt['phase']=='GRAPH_REFERENCE_AUDIT_FINISHED' and receipt['identity_pass'] and all(receipt['child_pids_absent'].values())
  assert spec['graph']==g['graph']==receipt['graph'] and spec['source_plan_sha256']==sha(planpath)
  assert spec['source_task_file_sha256']==g['query_file_sha256']==accepted['source_task_file_sha256']
  queryfiles=[p for p in (ASSETS/'dataset/test'/g['graph']/'Cypher').glob('*.json') if sha(p)==g['query_file_sha256']]
  assert len(queryfiles)==1;qp=queryfiles[0];tasks=read(qp)
  assert [r['id'] for r in tasks]==[r['id'] for r in spec['references']]==g['reference_id_order']
  mf={r['path']:r for r in manifest['files']};assert len(mf)==len(manifest['files'])
  inputs.update({str(p):sha(p) for p in [ap,mp,rp,sp,qp]})
  records=[]
  for ordinal,(task,reference) in enumerate(zip(tasks,spec['references'])):
   assert task['initial_gql']==reference['query']
   qhash=hashlib.sha256(reference['query'].encode('utf-8')).hexdigest();k=(g['graph'],str(task['id']))
   known,cr=cohorts[k];record=e/'reference_records'/('%05d.json'%ordinal);response=e/'responses'/('reference_%05d.json'%ordinal)
   rr=read(record);records.append(rr)
   assert rr['id']==task['id'] and rr['index']==ordinal and rr['query_sha256']==qhash==cr['source_query_sha256']
   assert rr['response_sha256']==cr['reference_response_sha256'] and rr['status']==cr['status']
   for p in [record,response]:
    rel=p.relative_to(e).as_posix();assert rel in mf and p.stat().st_size==mf[rel]['bytes']
    if p==record:assert sha(p)==mf[rel]['sha256']
   assert mf[response.relative_to(e).as_posix()]['sha256']==rr['response_sha256']
   inputs[str(record)]=sha(record);inputs[str(response)]=rr['response_sha256']
   entries.append({'graph':g['graph'],'id':str(task['id']),'ordinal':ordinal,'query_sha256':qhash,
    'reference_known':known,'reference_status':rr['status'],'reference_contract_concern':k in concerns,
    'record_file':record.relative_to(cache_root).as_posix(),'record_sha256':sha(record),
    'response_file':response.relative_to(cache_root).as_posix(),'response_sha256':rr['response_sha256'],
    'graph_acceptance_sha256':sha(ap),'source_task_file_sha256':sha(qp)})
  assert digest(records)==accepted['query_response_hash_seal']
  assert len(records)==accepted['planned']==accepted['dispatched']==accepted['completed_receipts']==g['reference_records']
  chains.append({'graph':g['graph'],'positions':len(records),'acceptance_sha256':sha(ap),'spec_sha256':sha(sp),'receipt_sha256':sha(rp)})
 assert len(entries)==len({(r['graph'],r['id']) for r in entries})==3826
 index=OUT/'reference_index.json';write(index,entries);seal=OUT/'reference_index_seal.json'
 write(seal,{'kind':'EVALUATOR_ONLY_ARCHIVED_REFERENCE_INDEX_NOT_FORMAL_FREEZE','cache_root':str(cache_root),
  'index_file':index.name,'index_sha256':sha(index),'positions':3826,'known_reference_positions':3815,
  'unknown_reference_positions':11,'graph_chains':chains,'cohort_sha256':sha(cohortpath),
  'raw_values_flow':'Evaluator only; never model inputs, schema selection, prompt or planner development.',
  'formal_metric_frozen':False})
 store=SealedReferenceStore(seal,expected_seal_sha256=sha(seal));references=[];graph_counts=Counter();empty=0;byte_count=0
 for g,qid in store.identities():
  m=store.metadata(g,qid);loaded=store.load(g,qid,expected_query_sha256=m['query_sha256']);ref=loaded['answer']
  references.append(((g,qid),ref));graph_counts[(g,loaded['reference_known'])]+=1
  if ref is not None:empty+=not ref['rows'];byte_count+=ref['response_bytes']
 assert sum(ref is not None for k,ref in references)==3815 and empty==1
 check('all3826_real_archived_ID_query_terminal_response_chains_load',len(references)==3826 and len(store.cohort())==3826)
 # Scorer integration: synthetic errors only, so author comparator must not run.
 def forbidden_compare(gold,pred):raise AssertionError('No real answer comparison authorized in this integration fixture')
 metric=FinalAnswerContract(store.cohort(),forbidden_compare,comparator_sha256='0'*64)
 predictions=[(k,{'status':'PROTOCOL_ERROR'}) for k,ref in references]
 results=metric.score_batch(references,predictions);s=summarize(results,expected_keys=store.identities())
 check('scorer_integration_retains_known3815_unknown11_concern13_without_comparing_answers',s['reference_known']==3815 and s['reference_unknown']==11 and s['primary_unresolved_known']==0 and sum(t['all_positions'] for t in s['strata'] if t['reference_contract_concern'])==13)
 write(OUT/'fabricated_failure_integration_not_method_EX.json',{'kind':'ALL_PREDICTIONS_FABRICATED_PROTOCOL_ERROR_NOT_METHOD_RESULTS','summary':s})
 assert all(sha(p)==h for p,h in inputs.items())
 cf=read(R/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 af=read(R/'experiments/cb_train_capacity64_fresh_v2_20261027/freeze.json')['adapter46']
 assert all(sha(R/'code/OSDB_CSTKG_Text2Cypher_publication_t1'/p)==h for p,h in cf.items())
 assert all(sha(REPO/'experiment-harness/public_benchmark_v1'/p)==h for p,h in af.items())
 write(OUT/'input_hashes.json',inputs)
 summary={'kind':'TEXT2GQL_EVALUATOR_REFERENCE_IDENTITY_BRIDGE_NOT_METHOD_EX','planned_positions':3826,
  'loaded_positions':len(references),'failed_identity_checks':0,'undispatched_positions':0,
  'reference_known':3815,'reference_unknown':11,'legitimate_empty':empty,'reference_contract_concerns_retained':13,
  'new_bridge_checks_planned':12,'new_bridge_checks_passed':len(checks),'new_bridge_checks_failed':0,'checks':checks,
  'real_reference_values_read_only_in_evaluator':True,'real_model_predictions_read':0,'reference_values_to_model_input':False,
  'reference_index_seal_sha256':sha(seal),'reference_index_sha256':sha(index),
  'known_reference_response_bytes':byte_count,'input_files_unchanged':len(inputs),
  'canonical_frozen_files_unchanged':len(cf),'adapter_frozen_files_unchanged':len(af),'wall_seconds':time.perf_counter()-start,
  'new_model_calls':0,'new_database_queries':0,'new_network_calls':0,'reference_queries_replayed':0,
  'gold_queries_repaired':0,'positions_excluded':0,'formal_metric_frozen':False,'formal_prediction_admitted':False,
  'conditional_budget_activated':False,'semantic_gold_certified':False,'T1_complete':False}
 assert len(checks)==12;write(OUT/'summary.json',summary)
 print(json.dumps({k:v for k,v in summary.items() if k!='checks'},ensure_ascii=True))

if __name__=='__main__':main()
