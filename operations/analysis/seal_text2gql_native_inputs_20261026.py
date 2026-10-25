"""Seal cached author graph inputs; no schema edits, queries or model calls."""
from pathlib import Path
from datetime import datetime
import json,hashlib

ROOT=Path('D:/ch7/literature/public_candidate_metadata_20261024/Text2GQL-Bench')
OUT=Path('D:/ch7/reports/text2gql_native_engine_20261026')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
inventory=json.loads((ROOT/'cypher_test_snapshot_inventory_20261024.json').read_text(encoding='utf-8'))
audit=json.loads((ROOT/'offline_csv_graph_audit_20261024.json').read_text(encoding='utf-8'))
assets=inventory['assets']
assert len(assets)==133
for a in assets:
    p=ROOT/'selected_zip_assets_20261024'/a['member']
    assert p.stat().st_size==a['bytes'] and sha(p)==a['sha256']
graphs=[]
for g in audit['graphs']:
    folder=ROOT/'selected_zip_assets_20261024/dataset/test'/g['graph']/'Cypher/TuGraph-DB_Instance'
    config=folder/'import_config.json'
    assert sha(config)==g['config_sha256']
    cfg=json.loads(config.read_text(encoding='utf-8'))
    for item in cfg['files']:
        assert Path(item['path']).name==item['path'], 'No unexpected paths in import config'
    all_csv_assets=[a for a in assets if '/'+g['graph']+'/' in a['member'] and a['member'].endswith('.csv')]
    referenced={i['path'] for i in cfg['files']}
    assert referenced <= {a['member'].rsplit('/',1)[-1] for a in all_csv_assets}
    csv_assets=[a for a in all_csv_assets if a['member'].rsplit('/',1)[-1] in referenced]
    unused=[a for a in all_csv_assets if a['member'].rsplit('/',1)[-1] not in referenced]
    queryfile=next((folder.parent).glob('*_cypher.json'))
    records=json.loads(queryfile.read_text(encoding='utf-8'))
    graphs.append({
        'graph':g['graph'],'csv_bytes':sum(a['bytes'] for a in csv_assets),
        'csv_files':len(csv_assets),'expected_vertices':g['vertex_rows'],'expected_edges':g['edge_rows'],
        'expected_vertices_by_label':g['vertices_by_label'],'expected_edges_by_label':g['edges_by_label'],
        'property_types':g['property_declarations_by_type'],
        'expected_extra_parallel_edges':g['extra_edge_rows_on_repeated_typed_pairs'],
        'config_sha256':sha(config),'query_file_sha256':sha(queryfile),
        'reference_records':len(records),'reference_id_order':[str(r['id']) for r in records],
        'import_config_kept_original':True,
        'graph_data_assets':csv_assets,
        'preserved_csv_assets_not_referenced_by_author_import_config':unused,
    })
graphs.sort(key=lambda x:(x['csv_bytes'],x['graph']))
seal=hashlib.sha256(json.dumps(graphs,sort_keys=True,separators=(',',':')).encode()).hexdigest()
result={'kind':'SIX_GRAPH_NATIVE_REFERENCE_INPUT_PLAN_NOT_PREDICTION_ADMISSION',
        'time_local':datetime.now().astimezone().isoformat(),
        'graph_order_rule':'ascending total CSV bytes, graph name breaks ties; fixed before native execution',
        'graph_order':[g['graph'] for g in graphs],'graphs':graphs,
        'source_inventory_sha256':sha(ROOT/'cypher_test_snapshot_inventory_20261024.json'),
        'source_offline_graph_audit_sha256':sha(ROOT/'offline_csv_graph_audit_20261024.json'),
        'verified_assets':len(assets),'total_reference_records':sum(g['reference_records'] for g in graphs),
        'graph_input_seal_sha256':seal,
        'native_import_or_reference_execution_done':False,'model_calls':0,
        'engine_version_is_candidate_until_runtime_acceptance':True,
        'method_training_material_not_selected':True,
        'method_adaptation_must_not_use_these_TEST_reference_queries':True,
        'all_reference_success_failure_and_empty_results_must_be_retained':True,
        'model_prediction_budget_authorized':False}
(OUT/'six_graph_input_plan.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'verified_assets':len(assets),'reference_records':result['total_reference_records'],
  'seal':seal,'graphs':[{k:g[k] for k in ['graph','csv_bytes','expected_vertices','expected_edges','reference_records']} for g in graphs]}))
