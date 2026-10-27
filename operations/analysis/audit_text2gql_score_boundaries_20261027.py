"""New offline boundary witnesses for two pinned author comparators.

Only whitelisted AST methods execute on fabricated rows. No author module
imports, database driver, recovery, benchmark prediction or network I/O runs.
"""
import ast
from collections import Counter
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path

BASE=Path('D:/ch7/reports/text2gql_driver_contract_20261026')
OUT=Path('D:/ch7/reports/text2gql_score_boundaries_v1_20261027')

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def write(p,v):
 with Path(p).open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')

def subset(path,name,methods):
 tree=ast.parse(path.read_text(encoding='utf-8-sig'))
 cls=next(x for x in tree.body if isinstance(x,ast.ClassDef) and x.name==name)
 selected=[x for x in cls.body if isinstance(x,ast.FunctionDef) and x.name in methods]
 assert {x.name for x in selected}==set(methods)
 for m in selected:
  for n in ast.walk(m):
   assert not isinstance(n,(ast.Import,ast.ImportFrom,ast.Global,ast.Nonlocal))
   if isinstance(n,ast.Call) and isinstance(n.func,ast.Name):
    assert n.func.id not in {'open','eval','exec','compile','__import__','globals','locals'}
 c=ast.ClassDef(name=name,bases=[],keywords=[],body=selected,decorator_list=[])
 isolated=ast.fix_missing_locations(ast.Module(body=[c],type_ignores=[]))
 env={'Counter':Counter,'Decimal':Decimal,'math':math}
 exec(compile(isolated,str(path),'exec'),env)
 return env[name](),[{'method':m.name,'line':m.lineno,'end_line':m.end_lineno} for m in selected]

def reference_row_score(evaluator,gold,pred,gold_complete,pred_complete):
 """Proposed common final-answer boundary; not the author's full runner.

 Rows must already be ordered columns from complete decoded responses. No
 value conversion, dictionary-name sorting, dedup or output repair occurs.
 """
 if not gold_complete or gold is None:return None
 if not pred_complete or pred is None:return False
 return evaluator.compare_results(gold,pred)

def assert_identity_universe(expected,received):
 if len(set(expected))!=len(expected) or len(set(received))!=len(received) or set(expected)!=set(received):
  raise ValueError('Missing, duplicate or foreign task identity; never zip-truncate')

def main():
 OUT.mkdir(exist_ok=False)
 oop_path=BASE/'upstream/impl/evaluation/metrics.py'
 ref_path=BASE/'upstream/contrib/standalone_text2graph_scripts/reference/ea_cypher.py'
 receipts=read(BASE/'source_receipts.json'); evidence={str(p):sha(p) for p in (oop_path,ref_path,BASE/'source_receipts.json')}
 for p in (oop_path,ref_path):
  r=next(x for x in receipts if x['path']==p.relative_to(BASE/'upstream').as_posix())
  assert r['sha256']==sha(p)
  raw=p.read_bytes();assert hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()==r['git_blob_sha']
 write(OUT/'protocol.json',{'kind':'OFFLINE_FABRICATED_BOUNDARIES_NOT_BENCHMARK_EX',
  'created_local':datetime.now().astimezone().isoformat(),'source_sha256':sha(__file__),
  'driver_commit':read(BASE/'manifest.json')['commit'],'input_sha256':evidence,
  'scope':['New column-order/type/nesting/complete-response/identity witnesses; do not rerun old11 checks.',
   'Pre-prediction metric design only; no Text2GQL model budget activation or final freeze.',
   'Standalone row-set comparator is the proposed common author-derived main answer metric; OOP text-trigger cannot rank a multi-call IR method without an invented query.',
   'Keep bag/type-sensitive diagnostic mandatory; preserve all3826 and unknown-reference11 separately.',
   'No benchmark task text, predictions, expected cardinality or answer selection.']})
 oop,om=subset(oop_path,'ExecutionAccuracy',['_normalize','_compare_results'])
 ref,rm=subset(ref_path,'ExecutionEvaluator',['compare_results'])
 witnesses=[]
 cases=[
  ('column_alias_only',[{'a':1,'b':2}],[{'x':1,'y':2}],'RETURN 1,2',[True,True]),
  ('column_position_reversed',[{'a':1,'b':2}],[{'b':2,'a':1}],'RETURN 1,2',[False,False]),
  ('nested_list_order',[{'a':[1,2]}],[{'a':[2,1]}],'RETURN [1,2]',[False,False]),
  ('nested_map_key_order',[{'a':{'x':1,'y':2}}],[{'a':{'y':2,'x':1}}],'RETURN {}',[True,True]),
  ('null_cell_vs_absent_row',[{'a':None}],[],'RETURN null',[False,False]),
  ('null_cell_vs_empty_string',[{'a':None}],[{'a':''}],'RETURN null',[False,False]),
  ('decimal_float_representation',[{'a':Decimal('1.25')}],[{'a':1.25}],'RETURN 1.25',[True,False]),
  ('binary_hex_representation',[{'a':b'ab'}],[{'a':'6162'}],'RETURN x',[True,False]),
  ('datetime_string_representation',[{'a':datetime(2023,1,1)}],[{'a':'2023-01-01T00:00:00'}],'RETURN x',[False,True]),
  ('IR_final_rows_without_synthetic_order_query',[{'a':1},{'a':2}],[{'a':2},{'a':1}],None,[True,True]),
 ]
 for name,g,p,q,want in cases:
  got=[oop._compare_results(g,p,q),ref.compare_results(g,p)];assert got==want,name
  witnesses.append({'case':name,'oop':got[0],'standalone':got[1],'expected':want})
 for name,g,p,gc,pc,want in [
  ('complete_empty_is_valid',[],[],True,True,True),
  ('error_not_empty',[],None,True,False,False),
  ('truncated_empty_not_valid',[],[],True,False,False),
  ('unknown_gold_not_wrong_or_correct',None,[],False,True,None),
  ('two_errors_not_equal',None,None,False,False,None)]:
  got=reference_row_score(ref,g,p,gc,pc);assert got is want,name
  witnesses.append({'case':name,'common_boundary':got,'expected':want})
 ids=['g1:0','g1:1','g2:0'];assert_identity_universe(ids,list(reversed(ids)))
 rejected=[]
 for bad in (ids[:-1],ids+[ids[0]],ids[:-1]+['foreign']):
  try:assert_identity_universe(ids,bad)
  except ValueError:rejected.append(bad)
 assert len(rejected)==3
 witnesses.append({'case':'same_ID_universe_required','permutation_accepted':True,'missing_duplicate_foreign_rejected':3})
 assert all(sha(p)==h for p,h in evidence.items())
 result={'kind':'TEXT2GQL_SCORE_BOUNDARY_DESIGN_NOT_FORMAL_FREEZE',
  'driver_commit':read(BASE/'manifest.json')['commit'],'new_checks_planned':16,'new_checks_completed':len(witnesses),
  'new_checks_passed':len(witnesses),'new_checks_failed':0,'new_checks_undispatched':0,
  'witnesses':witnesses,'author_methods':{'oop':om,'standalone':rm},'source_sha256':evidence,
  'proposed_primary':'EA_REFERENCE_ROW_SET_9DP_COMMON_FINAL_ANSWER',
  'mandatory_auxiliary':['type_sensitive_multiset_final_answer','gold_fixed_order_where_adjudicated','coverage_failure_unknown_reference_all3826'],
  'OOP_predicted_query_order_heuristic_common_primary':False,
  'execution_mode':'AST methods only; fake rows; author full pipeline never imported',
  'snapshot_positions':3826,'preaudited_gold_executable':3815,'preaudited_unknown_gold':11,
  'paper2783_ID_mapping_established':False,'author_Bolt_REST_equivalence_established':False,
  'formal_scoring_frozen':False,'formal_prediction_admitted':False,'conditional_20000_activated':False,
  'new_model_calls':0,'new_database_queries':0,'new_network_calls':0,
  'limits':['Proposed primary preserves author standalone set behavior, including loss of row multiplicity and bool/int equality; auxiliary diagnostics are required.',
   'Column order must come from raw header/declared output contract; never dictionary-key sorting.',
   'Decimal/binary/datetime are fabricated boundary witnesses, not measured REST/Bolt dataset differences.',
   'No manufactured single predicted Cypher for a multi-call IR/composed final answer.',
   'No full author reproduction, paper score comparability, new model EX or method advantage established.']}
 assert len(witnesses)==16
 write(OUT/'summary.json',result)
 print(json.dumps({k:v for k,v in result.items() if k not in ('witnesses','source_sha256','author_methods','limits')},ensure_ascii=True))

if __name__=='__main__':main()
