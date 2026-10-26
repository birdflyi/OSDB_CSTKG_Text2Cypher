"""Offline integrity/aggregate publication of both semantic preparation runs."""
import hashlib
import json
from pathlib import Path

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'


def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,x):Path(p).write_bytes((json.dumps(x,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))


def main():
 roots=[ROOT/f'reports/osdb_profile_real_semantics_v{v}_20261027' for v in (1,2)]
 protocols=[read(p/'protocol.json') for p in roots];summaries=[read(p/'summary.json') for p in roots]
 assert all(protocols[0][k]==protocols[1][k] for k in ('cases','schema','fixture','strategies'))
 assert len(protocols[0]['cases'])==14
 versions=[];seconds=0;readiness_failed=0
 for folder,protocol,summary in zip(roots,protocols,summaries):
  assert read(folder/'shutdown_receipt.json')['port_closed']
  assert '======== Neo4j 5.15.0' in (folder/'console_stdout.log').read_text(encoding='utf-8')
  for name,digest in protocol['source_sha256'].items():assert sha(folder/'executed_sources'/name)==digest
  terminals=list(folder.glob('requests/*/terminal.json'))
  assert len(terminals)==summary['physical_POST_queries_including_fixture_setup']
  for p in terminals:
   t=read(p);assert t['http_status']==200 and not t['body']['errors'];seconds+=t['client_seconds']
  readiness=list(folder.glob('readiness_*.json'))
  readiness_failed+=sum(read(p).get('status')!=200 for p in readiness)
  assert len(terminals)+len(readiness)==summary['physical_HTTP_requests']
  assert summary['budget_before']==summary['budget_after'] and not summary['budget_after']['pending']
  versions.append({**summary,'protocol_sha256':sha(folder/'protocol.json'),
    'summary_sha256':sha(folder/'summary.json'),'console_sha256':sha(folder/'console_stdout.log')})
 assert summaries[0]['accepted_plan_executions']==14 and summaries[0]['failed_plan_executions']==1
 assert summaries[1]['accepted_plan_executions']==28 and summaries[1]['failure'] is None
 preparation=read(ROOT/'reports/t1_osdb_observed_profile_dev_v2_20261027.json')
 result={'kind':'OSDB_OBSERVED_PROFILE_REAL_SEMANTIC_PREPARATION_NOT_FORMAL_T1',
  'versions':versions,'base14_fixture_and_expected_rows_unchanged':True,
  'new_profile_version':'observed-neo4j-profile-dev-2','unit_tests_passed':13,
  'four_graph_schema_compilation':preparation['graphs'],
  'physical_HTTP_requests':sum(s['physical_HTTP_requests'] for s in summaries),
  'physical_POST_queries':sum(s['physical_POST_queries_including_fixture_setup'] for s in summaries),
  'fixture_setup_POST_queries':8,'semantic_POST_queries':sum(s['physical_POST_queries_including_fixture_setup'] for s in summaries)-8,
  'readiness_attempts':sum(s['physical_HTTP_requests']-s['physical_POST_queries_including_fixture_setup'] for s in summaries),
  'unsuccessful_readiness_attempts':readiness_failed,'POST_http_or_engine_errors':0,
  'all_physical_POST_client_seconds':seconds,'same_body_query_retries':0,
  'new_model_calls':0,'budget':summaries[-1]['budget_after'],
  'canonical32_and_adapter46_unchanged':True,'new_owned_JVMs_stopped':True,
  'formal_predictions':0,'formal_N':None,'formal_admission':False,'entire_T1_complete':False,
  'primary_documentation':['https://neo4j.com/docs/cypher-manual/current/values-and-types/ordering-equality-comparison/',
   'https://neo4j.com/docs/cypher-manual/current/values-and-types/temporal/'],
  'remaining':['Shared prompt/schema/permission integration and entity candidates.',
   'Fixed exposed TRAIN regression and sufficient independent held-out task scope.',
   'Feedback/scoring/controls/resource/model/budget freeze and separate OSDB model-call allocation.'],
  'limits':['Synthetic developer semantic preparation; not EX, validation accuracy, method speedup or generalization.',
   'V1 failure retained; V2 corrects absolute-instant lowering, without changing the fixed case expectations.',
   'The old real OSDB timestamp census contained UTC whole seconds; this synthetic offset failure is not claimed as an observed old OSDB wrong answer.',
   'No model endpoint probes, retired node/service restart or shared remote graph/model service changes.']}
 write(ROOT/'reports/t1_osdb_real_semantics_20261027.json',result)
 write(REPO/'operations/analysis/t1_osdb_real_semantics_20261027.json',result)
 print(json.dumps({'accepted_v2':28,'HTTP_requests':result['physical_HTTP_requests'],
  'POST_queries':result['physical_POST_queries'],'model_calls':0,'budget':result['budget']}))


if __name__=='__main__':main()
