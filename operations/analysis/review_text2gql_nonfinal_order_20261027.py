"""Bounded static reference review, selected before any model predictions.

All22 non-final ordering positions, no score-based selection, gold repair,
database execution or assertion that observed answer rows differ.
"""
from collections import Counter
from datetime import datetime
import hashlib,json
from pathlib import Path

ROOT=Path('D:/ch7');SOURCE=ROOT/'reports/text2gql_order_queue_v1_20261027/review_queue.json'
OUT=ROOT/'reports/text2gql_nonfinal_order_semantic_review_v1_20261027'
# Review annotations are evaluator-only and never method-specific tools.
NOTES={
 'at2gsynth_financialfinancialpayment_540':('PER_ENTITY_LATEST_NOT_SELECTED','Orders all customer/transaction/merchant rows; no per-customer extremum or selection.'),
 'at2gsynth_FInancial_Financial_Payment_batch3_563':('GLOBAL_LIMIT_INSTEAD_OF_PER_ENTITY','One global LIMIT1 does not select the latest audit event for each customer.'),
 'at2gsynth_FInancial_Financial_Payment_batch3_604':('PER_ENTITY_LATEST_NOT_SELECTED','MAX is grouped by customer and payment method; query does not select one most recent method per customer.'),
 'at2gsynth_Information_Technology_IT_Asset_Management_1309':('RANKING_CRITERION_CONFLICT','Question selects top requested-asset counts then orders contract value; query selects top total contract value.'),
 'at2gsynth_Information_Technology_IT_Asset_Management_1331':('CAPACITY_MEANING_UNRESOLVED','Question total asset capacity versus LOCATION.capacity needs an explicit data-definition contract; later aggregation may not preserve row order.'),
 'at2gsynth_Knowledge_Graph_Geography_1316':('MISSING_THRESHOLD_AND_UNREQUESTED_SLICE','No at-least-three area count filter, and final SKIP2 LIMIT1 is not requested by the question.'),
 'at2gsynth_Manufacturing_BOM（Bill_Of_Materials）_1027':('PER_ENTITY_LATEST_NOT_SELECTED','Sorts all versions; no per-product/assembly latest-version selection.'),
 'at2gsynth_Manufacturing_BOM（Bill_Of_Materials）_1077':('PER_ENTITY_LATEST_NOT_SELECTED','Filters active versions and sorts; does not choose latest active version per product.'),
 'at2gsynth_Manufacturing_BOM（Bill_Of_Materials）_1135':('LATEST_AND_OPTIONAL_POPULATION_UNRESOLVED','Whole OPTIONAL pattern does not explicitly anchor all Product nodes; no per-product latest-version selection.'),
 'at2gsynth_Manufacturing_BOM（Bill_Of_Materials）_1161':('PER_ENTITY_LATEST_NOT_SELECTED','Sorts all changes; no per-product/assembly most-recent engineering-change selection.'),
 'at2gsynth_Manufacturing_BOM（Bill_Of_Materials）_1228':('PER_ENTITY_LATEST_NOT_SELECTED','Approved-change filter plus sort does not select latest approved change per product.'),
 'at2gsynth_Manufacturing_BOM（Bill_Of_Materials）_1255':('LATEST_CRITERION_AND_SELECTION_UNRESOLVED','Expiration ascending sort does not by itself define or select latest version per entity.'),
 'at2gsynth_Manufacturing_BOM（Bill_Of_Materials）_1295':('GLOBAL_LIMIT_INSTEAD_OF_PER_ENTITY','One global LIMIT1 after joining changes and versions does not return a row for each product.'),
}
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,v):
 with Path(p).open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')
def main():
 OUT.mkdir(exist_ok=False);before=sha(SOURCE)
 write(OUT/'protocol.json',{'kind':'RETROSPECTIVE_STATIC_REFERENCE_REVIEW_NOT_GOLD_CORRECTION',
  'created_local':datetime.now().astimezone().isoformat(),'source_sha256':sha(__file__),'input_sha256':before,
  'scope':'All22 non-final/nested-order positions from the complete3826 query-only queue; no predictions read.',
  'historical_exposure':'Questions/reference queries already inspected; not blinded or full-history preregistration.',
  'rules':['No official query/answer changes, exclusions, method tuning, new predictions or DB calls.',
   'Text-contract concerns do not prove an answer mismatch on the actual graph.',
   'Nine without an additional identified concern are not nine certified semantic golds.',
   'Keep official-reference score, semantic concern strata, unknown gold and all-position coverage distinct.']})
 rows=[r for r in read(SOURCE) if r['review_category']=='NONFINAL_OR_NESTED_ORDER_REQUIRES_REVIEW']
 assert len(rows)==22 and set(NOTES)<=set(r['id'] for r in rows)
 reviewed=[]
 for r in rows:
  note=NOTES.get(r['id']);reviewed.append({**r,
   'static_disposition':'REFERENCE_CONTRACT_CONCERN' if note else 'NO_ADDITIONAL_CONCERN_IDENTIFIED_IN_BOUNDED_REVIEW',
   'concern':note[0] if note else None,'explanation':note[1] if note else 'Top-k selection is present; ties/order/actual answer correctness still unadjudicated.',
   'actual_answer_divergence_established':False,'official_gold_changed':False,'excluded_from_population':False,
   'formal_semantic_adjudication_complete':False})
 assert sha(SOURCE)==before
 write(OUT/'reviewed_positions.json',reviewed)
 summary={'kind':'TEXT2GQL_NONFINAL_ORDER_STATIC_REVIEW_NOT_MODEL_SCORE',
  'planned':22,'reviewed':22,'review_failed':0,'undispatched':0,
  'reference_contract_concerns':sum(r['concern'] is not None for r in reviewed),
  'no_additional_concern_not_certified_gold':sum(r['concern'] is None for r in reviewed),
  'concern_counts':dict(Counter(r['concern'] for r in reviewed if r['concern'])),
  'all_snapshot_positions_retained':3826,'official_reference_edits':0,'exclusions':0,
  'reference_executable_not_semantically_certified':3815,'unknown_reference':11,
  'actual_graph_answer_mismatches_measured':0,'model_outputs_read':0,'new_model_calls':0,'new_DB_calls':0,
  'new_network_calls':0,'semantic_adjudication_complete':False,'formal_metric_frozen':False,
  'conditional_budget_activated':False,'source_query_queue_sha256':before,
  'reviewed_positions_sha256':sha(OUT/'reviewed_positions.json'),
  'next_required':'Freeze official-reference/semantic-audit strata and ordering/tie handling; do not repair gold from method outcomes.',
  'T1_complete':False}
 assert summary['reference_contract_concerns']==13
 write(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True))
if __name__=='__main__':main()
