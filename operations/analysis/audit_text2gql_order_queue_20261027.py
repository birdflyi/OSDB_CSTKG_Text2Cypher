"""Prediction-independent ordering review queue, not an order-semantics oracle.

Reuse the already reviewed lexical tokenizer. Read all six sealed reference
query packages, never model outputs or numeric answers. Preserve all positions.
"""
from collections import Counter,defaultdict
from datetime import datetime
import hashlib,importlib.util,json
from pathlib import Path

ROOT=Path('D:/ch7');OUT=ROOT/'reports/text2gql_order_queue_v1_20261027'
BASE=ROOT/'literature/public_candidate_metadata_20261024/Text2GQL-Bench'
HELPER=ROOT/'reports/audit_text2gql_query_features_20261025.py'
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,v):
 with Path(p).open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')

def classify(query,tokens):
 ts=tokens(query);stack=[];events=[]
 for i,(kind,value) in enumerate(ts):
  if kind=='punctuation':
   if value in ('(','[','{'):stack.append(value)
   elif value in (')',']','}'):
    if not stack or stack.pop()!={')':'(',']':'[','}':'{'}[value]:raise ValueError('Unbalanced delimiters')
  if kind!='word' or i and ts[i-1][1] in ('.',':','$'):continue
  word=value.upper()
  if word in ('RETURN','WITH','UNION','LIMIT','SKIP'):
   events.append((word,i,len(stack)))
  if word=='ORDER' and i+1<len(ts) and ts[i+1][0]=='word' and ts[i+1][1].upper()=='BY':
   events.append(('ORDER_BY',i,len(stack)))
 if stack:raise ValueError('Unbalanced delimiters')
 orders=[e for e in events if e[0]=='ORDER_BY'];returns=[e[1] for e in events if e[0]=='RETURN' and e[2]==0]
 unions=[e for e in events if e[0]=='UNION' and e[2]==0]
 final=[e for e in orders if e[2]==0 and returns and e[1]>max(returns)]
 category=('UNION_SCOPE_REQUIRES_REVIEW' if unions and orders else
  'FINAL_RETURN_ORDER_CANDIDATE' if final else
  'NONFINAL_OR_NESTED_ORDER_REQUIRES_REVIEW' if orders else 'NO_ORDER_TOKEN_NOT_UNORDERED_CERTIFICATE')
 return {'raw_author_order_trigger':'order by' in query.lower(),'lexical_order_token':bool(orders),
  'final_return_order_candidate':bool(final),'top_level_union':bool(unions),
  'limit_or_skip':any(e[0] in ('LIMIT','SKIP') for e in events),
  'review_category':category,'order_requirement_adjudicated':False}

def main():
 OUT.mkdir(exist_ok=False)
 inventory=BASE/'cypher_test_snapshot_inventory_20261024.json'
 cohort=ROOT/'reports/text2gql_reference_cohorts_provisional_20261026.json'
 sourcehashes={str(p):sha(p) for p in (HELPER,inventory,cohort)}
 write(OUT/'protocol.json',{'kind':'GOLD_QUERY_ORDERING_QUEUE_NOT_NEW_SCORE_OR_PARSER_CERTIFICATION',
  'created_local':datetime.now().astimezone().isoformat(),'source_sha256':sha(__file__),'input_sha256':sourcehashes,
  'rules':['All3826 positions, no prediction/answer-based selection.',
   'Token/depth categories only; no clause semantic, tie-equivalence or NL-order requirement proof.',
   'Every final order/LIMIT/tie/column contract needs adjudication before fixed-order auxiliary freeze.',
   'Current3815 executable and11 unknown cohort identity remains fixed; no gold repair or query execution.']})
 spec=importlib.util.spec_from_file_location('sealed_query_profile',HELPER);h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
 # New depth-sensitive witnesses, not repetitions of the old tokenizer checks.
 checks=[('RETURN n ORDER\nBY n.x','FINAL_RETURN_ORDER_CANDIDATE',False),
  ('WITH n ORDER BY n.x RETURN n','NONFINAL_OR_NESTED_ORDER_REQUIRES_REVIEW',True),
  ('CALL { RETURN n ORDER BY n.x } RETURN n','NONFINAL_OR_NESTED_ORDER_REQUIRES_REVIEW',True),
  ("RETURN 'order by'",'NO_ORDER_TOKEN_NOT_UNORDERED_CERTIFICATE',True),
  ('RETURN n UNION RETURN m ORDER BY m.x','UNION_SCOPE_REQUIRES_REVIEW',True)]
 for q,want,trigger in checks:
  v=classify(q,h.tokens);assert v['review_category']==want and v['raw_author_order_trigger']==trigger
 c=read(cohort);valid={(r['graph'],str(r['id'])):r for r in c['native_reference_executable']}
 unknown=[]
 for key,value in c.items():
  if isinstance(value,list) and key!='native_reference_executable':
   unknown.extend(r for r in value if isinstance(r,dict) and 'graph' in r and 'id' in r)
 unknownmap={(r['graph'],str(r['id'])):r for r in unknown}
 assert len(valid)==3815 and len(unknownmap)==11 and not(set(valid)&set(unknownmap))
 inv=read(inventory);assets={x['member']:x for x in inv['assets']}
 files=sorted(m for m in assets if m.endswith('.json') and m.count('/')==4);assert len(files)==6
 rows=[];totals=Counter();bygraph=defaultdict(Counter)
 for member in files:
  path=BASE/'selected_zip_assets_20261024'/member;assert sha(path)==assets[member]['sha256'];sourcehashes[str(path)]=sha(path)
  graph=member.split('/')[2]
  for row in read(path):
   key=(graph,str(row['id']));q=row['initial_gql'];ref=valid.get(key) or unknownmap.get(key);assert ref is not None
   assert hashlib.sha256(q.encode('utf-8')).hexdigest()==ref['source_query_sha256']
   try:v=classify(q,h.tokens)
   except ValueError as e:v={'review_category':'LEXICAL_ERROR_REQUIRES_REVIEW','error':str(e),'order_requirement_adjudicated':False}
   r={'graph':graph,'id':str(row['id']),'query_sha256':ref['source_query_sha256'],
    'cohort':'REFERENCE_EXECUTABLE' if key in valid else 'REFERENCE_UNKNOWN',**v}
   rows.append(r);totals[r['review_category']]+=1;bygraph[graph][r['review_category']]+=1
   # Full question and reference query remain in local evaluator-only queue.
   r.update(reference_query=q,question=row['initial_question'])
 assert len(rows)==3826 and len({(r['graph'],r['id']) for r in rows})==3826
 assert all(sha(p)==hsh for p,hsh in sourcehashes.items())
 write(OUT/'review_queue.json',rows);write(OUT/'input_hashes.json',sourcehashes)
 summary={'kind':'TEXT2GQL_ORDER_REVIEW_QUEUE_NOT_ADJUDICATED','positions_planned':3826,'positions_completed':3826,
  'lexical_failed':sum(r['review_category']=='LEXICAL_ERROR_REQUIRES_REVIEW' for r in rows),'undispatched':0,
  'depth_sensitive_fabricated_checks':len(checks),'counts':dict(totals),'by_graph':dict(bygraph),
  'raw_order_trigger_count':sum(r.get('raw_author_order_trigger',False) for r in rows),
  'lexical_order_token_count':sum(r.get('lexical_order_token',False) for r in rows),
  'raw_trigger_without_order_token':sum(r.get('raw_author_order_trigger',False) and not r.get('lexical_order_token',False) for r in rows),
  'order_token_without_raw_trigger':sum(r.get('lexical_order_token',False) and not r.get('raw_author_order_trigger',False) for r in rows),
  'limit_skip_positions':sum(r.get('limit_or_skip',False) for r in rows),
  'gold_known':3815,'gold_unknown':11,'order_adjudicated':0,'formal_scoring_frozen':False,
  'new_model_calls':0,'new_database_queries':0,'new_network_calls':0,'input_files_unchanged':len(sourcehashes),
  'review_queue_sha256':sha(OUT/'review_queue.json'),'protocol_sha256':sha(OUT/'protocol.json'),
  'limits':['Lexical order absence does not certify unordered NL semantics.',
   'Final ORDER BY candidate does not resolve ties, LIMIT memberships, hidden sort keys or nested list semantics.',
   'No per-method predicted query is used to select an ordering rule or denominator.',
   'No claimed difference in actual benchmark EX, all original task and reference bytes preserved.']}
 write(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True))

if __name__=='__main__':main()
