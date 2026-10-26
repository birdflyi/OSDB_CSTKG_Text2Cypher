"""Audit all physical preparation attempts without publishing task/reference text."""
import hashlib,json
from pathlib import Path
ROOT=Path('D:/ch7');BASE=ROOT/'reports/osdb_semantic_snapshots_v1_20261027'
DEST=Path(__file__).with_name('t1_osdb_semantic_snapshots_20261027.json')


def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
 raw=read(BASE/'summary.json');protocol=read(BASE/'protocol.json')
 assert all(sha(BASE/'executed_sources'/p)==h for p,h in protocol['sources'].items())
 recovery_root=ROOT/'reports/osdb_elasticsearch_offline_scan_v2_20261027'
 recovery=read(recovery_root/'summary.json');recovery_protocol=read(recovery_root/'protocol.json')
 assert raw['completed_graphs']==3 and raw['accepted_reference_checks']==12
 assert 'TransactionTimedOutClientConfiguration' in raw['failure']['message']
 assert read(BASE/'failed_elasticsearch_audit.json')['source_original_files_unchanged']
 assert recovery['failure'] is None and recovery['accepted_graphs']==1 and recovery['accepted_reference_checks']==4
 assert recovery['cleanup_replayed']==0 and recovery['budget_after']==raw['budget_after']
 assert all(sha(recovery_root/'executed_sources'/p)==h for p,h in recovery_protocol['sources'].items())
 graphs={};posts=[];bolts=[];gets=[];source_files=0;copy_bytes=0
 for graph in protocol['fixed_graph_order']:
  root=BASE/'runtime'/graph;before=read(root/'source_store_manifest_before.json')
  if graph!='elasticsearch':
   item=read(root/'summary.json');assert before==read(root/'source_store_manifest_after.json')
  else:
   eq=read(recovery_root/'full_graph_equivalence.json');assert eq['matches']
   item={'nodes':eq['expected']['nodes'],'edges':eq['expected']['edges'],
    'semantic_snapshot_sha256':eq['admitted']['semantic_snapshot_sha256'],
    'full_semantic_CSV_fingerprint_matches':True,'all_storage_metadata_absent':True,
    'physical_graph_access_admitted_for_development':True,'reference_checks_passed':4,
    'cleanup_transactions':66,'cleaned_nodes':130413,'source_store_files':len(before),
    'source_store_bytes':sum(v['bytes'] for v in before.values()),'all_original_store_bytes_unchanged':True,
    'owned_copy_stopped':True,'initial_240s_scan_failed':True,'new_copy_600s_scan_recovery_accepted':True,
    'copy_and_verify_seconds':None,'total_project_client_seconds':None,'formal_admission':False}
  stopped=read(root/'shutdown_receipt.json');assert stopped['port_closed'] and stopped['bolt_port_closed']
  reference_root=recovery_root if graph=='elasticsearch' else root
  assert read(reference_root/'full_graph_equivalence.json')['matches'] and item['full_semantic_CSV_fingerprint_matches']
  refs=[read(p) for p in (reference_root/'reference_checks').glob('*.json')];assert len(refs)==4 and all(x['matches'] for x in refs)
  physical=[read(p) for p in (root/'trusted_requests').glob('*/terminal.json')]
  assert all(t['http_status']==200 and not t['body']['errors'] for t in physical)
  bolt=[read(p) for p in sorted((root/'shared_access').glob('*/terminal.json'))]
  if graph!='elasticsearch':assert len(bolt)==10 and all(t['complete'] and t['query_type']=='r' for t in bolt)
  else:assert len(bolt)==2 and bolt[0]['complete'] and not bolt[1]['complete']
  ready=[read(p) for p in root.glob('readiness_*.json')]
  assert any(r.get('http_status')==200 for r in ready)
  posts+=physical;bolts+=bolt;gets+=ready;source_files+=len(before);copy_bytes+=item['source_store_bytes']
  graphs[graph]=item
 recovery_bolt=[read(p) for p in (recovery_root/'shared_access').glob('*/terminal.json')]
 assert len(recovery_bolt)==10 and all(t['complete'] and t['query_type']=='r' for t in recovery_bolt)
 assert read(recovery_root/'source_store_manifest_before.json')==read(recovery_root/'source_store_manifest_after.json')
 stopped=read(recovery_root/'shutdown_receipt.json');assert stopped['port_closed'] and stopped['bolt_port_closed']
 recovery_get=[read(p) for p in recovery_root.glob('readiness_*.json')]
 bolts+=recovery_bolt;gets+=recovery_get
 graphs['elasticsearch']['Bolt_client_seconds']=sum(t['client_seconds'] for t in recovery_bolt)+sum(read(p)['client_seconds'] for p in (BASE/'runtime/elasticsearch/shared_access').glob('*/terminal.json'))
 result={'kind':'FOUR_REAL_OSDB_SEMANTIC_ONLY_COPY_AND_SHARED_ACCESS_DEVELOPMENT_ACCEPTANCE',
  'planned_graphs':4,'dispatched_graphs':4,'accepted_graphs':4,'failed_graphs':0,'undispatched_graphs':0,
  'planned_reference_checks':16,'dispatched_reference_checks':16,'accepted_reference_checks':16,
  'failed_reference_checks':0,'undispatched_reference_checks':0,'graphs':graphs,
  'nodes_total':sum(g['nodes'] for g in graphs.values()),'edges_total':sum(g['edges'] for g in graphs.values()),
  'cleanup_transactions':sum(g['cleanup_transactions'] for g in graphs.values()),
  'source_store_files_verified_unchanged':source_files,'copied_store_bytes':copy_bytes,
  'HTTP_readiness_attempts':len(gets),'HTTP_readiness_not_ready':sum(r.get('http_status')!=200 for r in gets),
  'HTTP_POST_statements':len(posts),'HTTP_POST_errors':0,'Bolt_Cypher_statements':len(bolts),
  'HTTP_POST_client_seconds':sum(t['client_seconds'] for t in posts),
  'Bolt_client_seconds':sum(t['client_seconds'] for t in bolts),
  'automatic_statement_retries':0,'explicit_audited_new_version_recovery':1,
  'initial_failed_edge_scan_client_seconds':read(BASE/'runtime/elasticsearch/shared_access/0002_admit_edges/terminal.json')['client_seconds'],
  'offline_scan_timeouts_initial_recovery_seconds':[240,600],'formal_explain_read_timeouts_unchanged_seconds':[30,60],
  'failed_physical_Bolt_statements_retained':1,'new_model_calls':0,'training':0,
  'budget':raw['budget_after'],'canonical32_and_adapter46_unchanged':raw['canonical32_and_adapter46_unchanged'],
  'all_five_new_owned_copies_stopped':True,'source_protocol_sha256':sha(BASE/'protocol.json'),
  'recovery_protocol_sha256':sha(recovery_root/'protocol.json'),
  'initial_failed_run':{'planned_graphs':4,'completed_graphs':3,'failed_graphs':1,'undispatched_graphs':0,
   'planned_reference_checks':16,'dispatched_reference_checks':12,'accepted_reference_checks':12,'undispatched_reference_checks':4},
  'recovery_run':{'planned_graphs':1,'completed_graphs':1,'failed_graphs':0,'planned_reference_checks':4,'dispatched_reference_checks':4,'accepted_reference_checks':4},
  'recovery_copy_additional_bytes':sum(v['bytes'] for v in recovery_protocol['source_snapshot_manifest'].values()),
  'executed_source_hashes':protocol['sources'],'executed_recovery_source_hashes':recovery_protocol['sources'],
  'local_fingerprint_tests_passed':4,'offline_budget_isolation_tests_passed':3,
  'real_graph_access_admitted_for_development':4,'formal_N':None,'formal_model_dispatched':0,
  'formal_model_completed':0,'formal_model_failed':0,'formal_model_undispatched':None,
  'formal_admission':False,'entire_T1_complete':False,
  'remaining':['Common entity-candidate policy/retrieval and fixed exposed TRAIN model regression.',
   'Sufficient independent task/goal scope, exposure and held-out structure plan.',
   'Final result normalization/scoring/feedback/controls/model/resource freeze and separate OSDB model budget.',
   'Concrete inference route/resource repair evidence before any stopped model batch restart.'],
  'limits':['Full equality concerns the declared semantic snapshot, not upstream completeness of repository history.',
   'The 16 manual references are exposed developer references, not Ours/Direct model results or independent EX.',
   'Removal affects only importer label/property in new verified copies; original data and semantic properties retained.',
   'Client read policy assumes exclusively owned immutable snapshot; not Community RBAC/network isolation.',
   'Preparation timing includes sequential cache/order effects; not a method speedup claim or completed T4.']}
 assert result['HTTP_POST_statements']+result['HTTP_readiness_attempts']==raw['total_HTTP_requests']+recovery['HTTP_readiness']
 for p in (DEST,ROOT/'reports'/DEST.name):
  assert not p.exists(),'Preserve an earlier evidence artifact'
  p.write_bytes((json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))
 print(json.dumps({k:result[k] for k in ('accepted_graphs','accepted_reference_checks','nodes_total','edges_total','cleanup_transactions','HTTP_readiness_attempts','HTTP_POST_statements','Bolt_Cypher_statements','HTTP_POST_client_seconds','Bolt_client_seconds','new_model_calls')},ensure_ascii=True))


if __name__=='__main__':main()
