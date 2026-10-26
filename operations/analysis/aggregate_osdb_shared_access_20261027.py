"""Publish aggregate development evidence only; preserve failed harness attempt."""
import hashlib,json
from pathlib import Path
ROOT=Path('D:/ch7');BASE=ROOT/'reports'
DEST=Path(__file__).with_name('t1_osdb_shared_access_20261027.json')


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
 folders=[BASE/('osdb_shared_access_dev_v'+str(i)+'_20261027') for i in (1,2)]
 protocols=[read(p/'protocol.json') for p in folders]
 for key in ('schema','fixture','compiler_cases','strategies','native_cases','guard_rejections'):
  assert protocols[0][key]==protocols[1][key],key
 summaries=[read(p/'summary.json') for p in folders];assert summaries[0]['failure']=={'type':'KeyError','message':"'complete'"}
 assert summaries[1]['failure'] is None and summaries[1]['completed_compiler_strategy_checks']==26
 assert summaries[1]['completed_native_checks']==5 and summaries[1]['checks_passed']==39
 attempts=[];total_post=0;total_get=0;post_seconds=0;bolt_seconds=0;bolt_count=0
 for i,p in enumerate(folders):
  protocol=read(p/'protocol.json');assert all(sha(p/'executed_sources'/n)==h for n,h in protocol['sources'].items())
  posts=[read(f) for f in (p/'trusted_setup').glob('*/terminal.json')]
  assert all(t.get('http_status')==200 and not t['body']['errors'] for t in posts)
  bolt=[read(f) for name in ('dirty_labels','dirty_property','clean_shared','small_row_cap') for f in (p/name).glob('*/terminal.json')]
  assert len(bolt)==summaries[i]['Bolt_Cypher_statements_including_admission_explain_reads']
  total_post+=len(posts);total_get+=summaries[i]['readiness_HTTP'];post_seconds+=sum(t['client_seconds'] for t in posts)
  bolt_count+=len(bolt);bolt_seconds+=sum(t['client_seconds'] for t in bolt)
  attempts.append({'version':i+1,'summary':summaries[i],'protocol_sha256':sha(p/'protocol.json'),
   'retained_source_hashes':protocol['sources'],'compiler_strategy_counts':{
    'planned':26,'dispatched':1 if i==0 else 26,'accepted':0 if i==0 else 26,
    'harness_failures':1 if i==0 else 0,'undispatched':25 if i==0 else 0},
   'first_v1_physical_read_completed_before_harness_KeyError':i==0})
 inputs=read(BASE/'osdb_shared_input_dev_v2_20261027/summary.json');assert inputs['prepared']==24
 summary={'kind':'OSDB_SHARED_ACCESS_AND_COMMON_INPUT_DEVELOPMENT_EVIDENCE_NOT_FORMAL_T1',
  'attempts':attempts,'common_input':inputs,'local_behavioral_tests_passed':13,
  'total_HTTP_readiness_requests':total_get,'total_HTTP_setup_and_integrity_queries':total_post,
  'total_Bolt_Cypher_statements':bolt_count,'total_Bolt_expected_dirty_admission_errors':4,
  'total_HTTP_POST_client_seconds':post_seconds,'total_Bolt_client_seconds_including_expected_rejections':bolt_seconds,
  'same_body_statement_retries':0,'new_model_calls':0,'budget':inputs['budget_after'],
  'formal_N':None,'formal_predictions':0,'real_four_graphs_admitted':0,
  'formal_admission':False,'entire_T1_complete':False,
  'remaining':['New owned semantic-only materialization and full fingerprint/reference acceptance for four real graphs.',
   'Entity suggestions/retrieval decision; these 24 exposed positions currently give both arms empty candidates.',
   'Fixed exposed TRAIN model regression, sufficient independent goals and held-out task scope.',
   'Final value normalization/scoring/feedback/resource/model freeze and separate OSDB budget.',
   'Concrete inference-route repair evidence before restarting any stopped model batch.'],
  'limits':['Client policy over exclusively owned immutable snapshot, not Community RBAC/network security.',
   'Physical fingerprints are verified only on the tiny fixture; no real-graph access admission implied.',
   'Shared user bytes do not mean identical system output-format prompts; both sizes disclosed.',
   'Native Direct retains UNION and read subqueries; compiled strategies share a plan.',
   'Synthetic developer checks are not independent EX, model coverage or a speedup benchmark.',
   'Driver auto-commit statement counts exclude network handshake/packet counts; time is local client wall time.']}
 for p in (DEST,BASE/DEST.name):
  assert not p.exists(),'Preserve published evidence'
  p.write_bytes((json.dumps(summary,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))
 print(json.dumps({k:summary[k] for k in ('total_HTTP_readiness_requests','total_HTTP_setup_and_integrity_queries','total_Bolt_Cypher_statements','total_HTTP_POST_client_seconds','total_Bolt_client_seconds_including_expected_rejections','new_model_calls')},ensure_ascii=True))


if __name__=='__main__':main()
