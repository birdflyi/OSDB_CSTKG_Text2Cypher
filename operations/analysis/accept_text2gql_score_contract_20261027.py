"""One-shot offline acceptance. All answer rows are explicitly fabricated.

Real task IDs/cohort/concern metadata only; no real reference result, model
prediction, database, API configuration or network is read by this harness.
"""
from pathlib import Path
from copy import deepcopy
import hashlib,json,sys,time

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
sys.path.insert(0,str(REPO/'experiment-harness/public_benchmark_v1'))
from text2gql_score_contract_dev_20261027 import FinalAnswerContract,summarize,NONTERMINAL
from audit_text2gql_score_boundaries_20261027 import subset

OUT=ROOT/'reports/text2gql_score_contract_dev_v2_20261027'
AUTHOR=ROOT/'reports/text2gql_driver_contract_20261026'
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,v):
 with Path(p).open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')
def answer(rows,columns=None):return {'status':'EXECUTED','complete':True,'columns':columns or ['x'],'rows':rows}

def main():
 OUT.mkdir(exist_ok=False);start=time.perf_counter()
 author=AUTHOR/'upstream/contrib/standalone_text2graph_scripts/reference/ea_cypher.py'
 receipt=next(r for r in read(AUTHOR/'source_receipts.json') if r['path']==author.relative_to(AUTHOR/'upstream').as_posix())
 assert sha(author)==receipt['sha256']
 raw=author.read_bytes();assert hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()==receipt['git_blob_sha']
 canonical=ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
 adapter=ROOT/'experiments/cb_train_capacity64_fresh_v2_20261027/freeze.json'
 cohort_path=ROOT/'reports/text2gql_reference_cohorts_provisional_20261026.json'
 concerns_path=ROOT/'reports/text2gql_nonfinal_order_semantic_review_v1_20261027/reviewed_positions.json'
 sourcefiles=[Path(__file__),Path(__file__).with_name('text2gql_score_contract_dev_20261027.py'),
  Path(__file__).with_name('audit_text2gql_score_boundaries_20261027.py'),
  REPO/'experiment-harness/public_benchmark_v1/tugraph_results.py',author,cohort_path,concerns_path]
 sources={str(p):sha(p) for p in sourcefiles}
 write(OUT/'protocol.json',{'kind':'DEVELOPMENT_SCORE_ADAPTER_ACCEPTANCE_NOT_BENCHMARK_EX',
  'sources_sha256':sources,'created_epoch':time.time(),'author_commit':read(AUTHOR/'manifest.json')['commit'],
  'rules':['New adapter lifecycle/cohort/column/order fixtures; do not rerun previous16 author boundary checks.',
   'All answer rows fabricated. Real3826 IDs/3815 known/11 unknown/13 concern metadata only.',
   'No model/DB/HTTP/actual predictions/actual reference answers; no automatic formal admission.',
   'Known reference loss leaves denominator fixed and estimate unresolved; no silent exclusions.',
   'Unknown-reference tasks still contribute complete-answer coverage across all3826.',
   'No task-order inference from model/query outputs; unadjudicated remains null.']})
 obj,methods=subset(author,'ExecutionEvaluator',['compare_results'])
 calls=[]
 def compare(g,p):calls.append(1);return obj.compare_results(g,p)
 def contract(metadata=None):
  return FinalAnswerContract([(('g','one'),metadata or {'reference_known':True})],compare,comparator_sha256=sha(author))
 checks=[]
 def check(name,condition):
  assert condition,name;checks.append(name)
 c=contract();g=answer([[1]])
 check('complete_numeric_answer',c.score_one(('g','one'),g,deepcopy(g))['primary'] is True)
 r=c.score_one(('g','one'),answer([[1],[1]]),g)
 check('primary_set_and_strict_bag_both_preserved',r['primary'] is True and r['strict_bag'] is False)
 r=c.score_one(('g','one'),answer([[True]]),g)
 check('primary_bool_int_vs_type_diagnostic',r['primary'] is True and r['strict_bag'] is False)
 r=c.score_one(('g','one'),answer([[1.00000000001]]),answer([[1.0]]))
 check('primary_9dp_vs_exact_float_diagnostic',r['primary'] is True and r['strict_bag'] is False)
 r=c.score_one(('g','one'),answer([[1,2]],['x','x']),answer([[1,2]],['left','right']))
 s=c.score_one(('g','one'),answer([[1,2]],['x','x']),answer([[2,1]],['right','left']))
 check('duplicate_aliases_do_not_collapse_columns',r['primary'] is True and s['primary'] is False)
 empty=answer([])
 check('complete_empty_valid_and_null_row_distinct',c.score_one(('g','one'),empty,deepcopy(empty))['primary'] is True and c.score_one(('g','one'),empty,answer([[None]]))['primary'] is False)
 calls.clear()
 results=[c.score_one(('g','one'),empty,{'status':s}) for s in ('PROTOCOL_ERROR','TRUNCATED','MODEL_TRUNCATED','REFUSED','INFRASTRUCTURE_ERROR')]
 check('terminal_failures_are_not_empty_success',all(r['primary'] is False and not r['complete_answer'] for r in results) and not calls)
 r=contract({'reference_known':False}).score_one(('g','one'),None,g)
 check('unknown_gold_retains_complete_answer_coverage',r['primary'] is None and r['complete_answer'] is True)
 r=c.score_one(('g','one'),None,g)
 check('known_gold_temporarily_missing_not_dropped',r['primary'] is None and r['reference_known'] is True and r['complete_answer'] is True)
 results=[c.score_one(('g','one'),g,{'status':s}) for s in NONTERMINAL]
 check('nonterminal_not_completed_failure',all(r['primary'] is None and not r['prediction_terminal'] for r in results))
 r=c.score_one(('g','one'),g,answer([[1,2]]))
 check('malformed_row_width_not_scored',r['primary'] is False and r['prediction_status']=='MALFORMED_ANSWER')
 r=c.score_one(('g','one'),g,{'status':'EXECUTED','complete':False})
 check('unverified_execution_completeness_rejected',r['primary'] is False and r['prediction_status']=='INCOMPLETE_ANSWER')
 g2=answer([[1],[2]]);p2=answer([[2],[1]])
 r=contract({'reference_known':True,'order_rule':'ORDERED'}).score_one(('g','one'),g2,p2)
 s=contract().score_one(('g','one'),g2,p2)
 check('ordering_fixed_externally_or_left_unadjudicated',r['primary'] is True and r['task_order_sensitive'] is False and s['task_order_sensitive'] is None)
 rejected=0
 for pairs in ([],[(('g','other'),g)],[(('g','one'),g),(('g','one'),g)]):
  try:c.score_batch([(('g','one'),g)],pairs)
  except ValueError:rejected+=1
 check('missing_foreign_duplicate_IDs_rejected_before_comparison',rejected==3)
 meta={'reference_known':True,'order_rule':'ORDERED'};fixed=contract(meta);meta['order_rule']='BAG'
 check('constructor_seals_metadata_against_caller_mutation',fixed.score_one(('g','one'),g2,p2)['task_order_sensitive'] is False)
 # Complete real-ID inventory with FABRICATED rows, never actual answers.
 co=read(cohort_path);known={(r['graph'],str(r['id'])) for r in co['native_reference_executable']}
 unknown=set()
 for name,items in co.items():
  if name!='native_reference_executable' and isinstance(items,list):
   unknown.update((r['graph'],str(r['id'])) for r in items if isinstance(r,dict) and 'graph' in r and 'id' in r)
 concern={(r['graph'],r['id']) for r in read(concerns_path) if r['concern'] is not None}
 assert len(known)==3815 and len(unknown)==11 and len(concern)==13 and not known&unknown
 population=sorted(known|unknown)
 cohort=[(k,{'reference_known':k in known,'reference_contract_concern':k in concern}) for k in population]
 big=FinalAnswerContract(cohort,compare,comparator_sha256=sha(author))
 refs=[(k,answer([[0]])) if k in known else (k,None) for k in population]
 preds=[(k,answer([[0]])) for k in population]
 rows=big.score_batch(refs,list(reversed(preds)));summary=summarize(rows,expected_keys=population)
 check('all3826_coverage_includes_unknown11_and_keeps_concern13',summary['all_positions']==3826 and summary['reference_known']==3815 and summary['reference_unknown']==11 and summary['complete_answers_all_positions']==3826 and sum(s['all_positions'] for s in summary['strata'] if s['reference_contract_concern'])==13)
 pending_key=next(k for k in population if k in known);pending=[(k,{'status':'UNKNOWN'} if k==pending_key else p) for k,p in preds]
 incomplete=summarize(big.score_batch(refs,pending),expected_keys=population)
 check('unknown_request_keeps_fixed_denominator_and_blocks_final_point',incomplete['reference_known']==3815 and incomplete['primary_final_point'] is None and incomplete['primary_unresolved_known']==1 and incomplete['terminal']==3825 and incomplete['primary_upper_bound_fixed_known_N']==1.0)
 missing=[(k,None if k==pending_key else ref) for k,ref in refs]
 missing_summary=summarize(big.score_batch(missing,preds),expected_keys=population)
 check('known_reference_loss_keeps_denominator_and_blocks_point',missing_summary['reference_known']==3815 and missing_summary['primary_final_point'] is None and missing_summary['terminal']==3826 and missing_summary['primary_unresolved_known']==1)
 rejected=0
 for partial in (rows[:-1],rows+rows[:1]):
  try:summarize(partial,expected_keys=population)
  except ValueError:rejected+=1
 check('standalone_summary_rejects_partial_or_duplicate_population',rejected==2)
 write(OUT/'synthetic_replay_only.json',{'kind':'FABRICATED_ROWS_NOT_ACTUAL_MODEL_OR_REFERENCE_ANSWERS',
  'completed_fixture':summary,'unknown_request_fixture':incomplete,'missing_reference_fixture':missing_summary})
 assert all(sha(p)==h for p,h in sources.items())
 cf=read(canonical)['code_files'];af=read(adapter)['adapter46']
 assert all(sha(ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'/p)==h for p,h in cf.items())
 assert all(sha(REPO/'experiment-harness/public_benchmark_v1'/p)==h for p,h in af.items())
 result={'kind':'TEXT2GQL_SCORE_CONTRACT_DEVELOPMENT_ACCEPTED_NOT_FORMAL',
  'planned_checks':19,'passed_checks':len(checks),'failed_checks':0,'undispatched_checks':0,'checks':checks,
  'synthetic_ID_inventory':3826,'synthetic_known_reference_slots':3815,'synthetic_unknown_reference_slots':11,
  'reference_concern_metadata_slots_retained':13,'actual_gold_or_model_answer_rows_read':0,
  'author_source_sha256':sha(author),'author_methods_executed':methods,
  'new_model_calls':0,'new_database_queries':0,'new_network_calls':0,
  'canonical_frozen_files_unchanged':len(cf),'adapter_frozen_files_unchanged':len(af),
  'input_files_unchanged':len(sources),'wall_seconds':time.perf_counter()-start,
  'formal_metric_frozen':False,'formal_prediction_admitted':False,'conditional_budget_activated':False,
  'remaining':['Task ordering/ties/column contract adjudication and raw-reference identity wiring before formal use.',
   'Independent task/source and statistical grouping; exact paper subset/REST-Bolt claims remain unestablished.',
   'End-to-end native model/feedback/cost entry, separately admitted development scope and whole formal freeze.'],
  'T1_complete':False}
 assert len(checks)==19
 write(OUT/'summary.json',result)
 print(json.dumps({k:v for k,v in result.items() if k not in ('checks','author_methods_executed','remaining')},ensure_ascii=True))

if __name__=='__main__':main()
