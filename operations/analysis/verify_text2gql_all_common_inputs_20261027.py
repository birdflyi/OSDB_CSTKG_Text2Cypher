"""Read-back acceptance of prepared input spans, never model inference.

Checks actual serialized artifacts, pairing, public-field isolation and source
hashes. Does not regenerate candidates or read any reference/score artifacts.
"""
from collections import Counter
from pathlib import Path
import hashlib,json,sys,time

R=Path('D:/ch7');P=R/'reports/text2gql_all_common_inputs_v1_20261027'
REPO=R/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def bsha(v):return hashlib.sha256(v).hexdigest()
def canonical(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def main():
 start=time.perf_counter();assert (P/'runner_exit.json').exists() and not(P/'runner_failure.json').exists()
 assert not(P/'verification.json').exists()
 manifest=read(P/'model_input_manifest.json');seal=read(P/'artifact_seal.json');sources=read(P/'source_input_hashes.json')
 assert len(manifest)==7652 and sha(P/'model_input_manifest.json')==seal['manifest_sha256']
 assert sha(P/'protocol.json')==seal['protocol_sha256']
 assert all(sha(P/n)==h for n,h in seal['input_files_sha256'].items())
 assert all(sha(p)==h for p,h in sources.items())
 public={'task_id','graph','question','schema','entity_candidates','candidate_notice','database_dialect'}
 pairs={};statuses=Counter();streams={};systems={};positions={};graph_counts=Counter()
 for row in manifest:
  fp=P/row['input_file'];assert fp.resolve().parent==P.resolve() and fp.name in seal['input_files_sha256']
  if fp.name not in streams:streams[fp.name]=fp.open('rb');positions[fp.name]=0
  f=streams[fp.name];assert row['offset']==positions[fp.name]
  f.seek(row['offset']);raw=f.read(row['bytes']);positions[fp.name]+=len(raw)
  assert len(raw)==row['bytes'] and raw.endswith(b'\n') and bsha(raw)==row['record_sha256']
  item=json.loads(raw);assert set(item)=={'graph','id','messages','formal_admission'} and item['formal_admission'] is False
  assert item['graph']==row['graph'] and item['id']==row['id']
  ms=item['messages'];assert [m['role'] for m in ms]==['system','user'] and all(set(m)=={'role','content'} for m in ms)
  assert bsha(canonical(ms).encode('utf-8'))==row['messages_sha256']
  user=ms[1]['content'];payload=json.loads(user)
  assert user==canonical(payload) and set(payload)==public
  assert payload['task_id']==row['id'] and payload['graph']==row['graph']
  assert bsha(user.encode('utf-8'))==row['common_user_sha256']
  assert len(payload['entity_candidates'])==row['candidate_count']<=12
  assert sum(len(m['content']) for m in ms)==row['message_characters']
  identity=(row['graph'],row['id']);arms=pairs.setdefault(identity,{})
  assert row['arm'] in ('direct','ours') and row['arm'] not in arms
  arms[row['arm']]=row['common_user_sha256']
  if row['arm'] in systems:assert systems[row['arm']]==ms[0]['content']
  else:systems[row['arm']]=ms[0]['content']
  statuses[row['arm']]+=1;graph_counts[row['graph']]+=1
 for n,f in streams.items():assert f.tell()==(P/n).stat().st_size;f.close()
 assert len(pairs)==3826 and all(set(arms)=={'direct','ours'} and len(set(arms.values()))==1 for arms in pairs.values())
 assert systems['direct']!=systems['ours'] and dict(statuses)=={'direct':3826,'ours':3826}
 assert all(sha(P/n)==h for n,h in seal['input_files_sha256'].items()) and all(sha(p)==h for p,h in sources.items())
 cf=read(R/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
 af=read(R/'experiments/cb_train_capacity64_fresh_v2_20261027/freeze.json')['adapter46']
 assert all(sha(R/'code/OSDB_CSTKG_Text2Cypher_publication_t1'/p)==h for p,h in cf.items())
 assert all(sha(REPO/'experiment-harness/public_benchmark_v1'/p)==h for p,h in af.items())
 v={'kind':'ALL_SERIALIZED_COMMON_INPUTS_READ_BACK_ACCEPTED_NOT_MODEL_RESULTS',
  'planned_records':7652,'verified_records':7652,'failed_records':0,'unverified_records':0,
  'identical_input_pairs':3826,'arm_counts':dict(statuses),'serialized_input_files':len(streams),
  'serialized_input_bytes':sum((P/n).stat().st_size for n in seal['input_files_sha256']),
  'source_files_unchanged':len(sources),'canonical_frozen_unchanged':len(cf),'adapter_frozen_unchanged':len(af),
  'manifest_sha256':sha(P/'model_input_manifest.json'),'artifact_seal_sha256':sha(P/'artifact_seal.json'),
  'verifier_sha256':sha(__file__),'verification_seconds':time.perf_counter()-start,
  'public_payload_fields_only':True,'actual_reference_or_model_answers_read':0,
  'new_model_calls':0,'new_database_queries':0,'new_network_calls':0,
  'model_dispatched':0,'model_completed':0,'model_failed':0,'model_undispatched_paths':7652,
  'formal_prediction_admitted':False,'conditional_budget_activated':False,'T1_complete':False}
 with (P/'verification.json').open('x',encoding='utf-8',newline='\n') as f:json.dump(v,f,ensure_ascii=False,indent=2);f.write('\n')
 print(json.dumps(v,ensure_ascii=True))
if __name__=='__main__':main()
