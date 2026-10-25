"""Aggregate accepted six-graph reference audits, with fixed unresolved cases."""
from pathlib import Path
from collections import Counter
from datetime import datetime
import csv,hashlib,json

ROOT=Path('D:/ch7');BASE=ROOT/'reports/text2gql_native_engine_20261026'
read=lambda p:json.loads(p.read_text(encoding='utf-8'))
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
plan=read(BASE/'six_graph_input_plan.json')
graphs=[];totals=Counter();valid=[];unscoreable=[];query_count=0;metadata_count=0
for index in range(6):
    suffix='_v3' if index==0 else ''
    path=ROOT/('reports/text2gql_native_reference_graph_'+str(index).zfill(2)+suffix+'_20261026')
    acceptance=read(path/'acceptance.json');spec=read(path/'evidence/spec.json');receipt=read(path/'evidence/receipt.json')
    assert sha(path/'evidence/receipt.json')==acceptance['receipt_sha256']
    assert acceptance['planned']==plan['graphs'][index]['reference_records']
    assert acceptance['native_parser_identity_pass'] and all(receipt['child_pids_absent'].values())
    vertex_rows=sum(r['actual_rows'] for r in receipt['data_checks'] if r['source_label'] is None)
    edge_rows=sum(r['actual_rows'] for r in receipt['data_checks'] if r['source_label'] is not None)
    assert vertex_rows==plan['graphs'][index]['expected_vertices']
    assert edge_rows==plan['graphs'][index]['expected_edges']
    totals.update(acceptance['status_counts']);query_count+=receipt['native_data_queries'];metadata_count+=len(receipt['metadata_checks'])
    for i,task in enumerate(spec['references']):
        row=read(path/'evidence/reference_records'/(str(i).zfill(5)+'.json'))
        entry={'graph':spec['graph'],'id':row['id'],'source_query_sha256':row['query_sha256'],
               'reference_response_sha256':row['response_sha256'],'status':row['status']}
        if row['status'].startswith('EXECUTED_'):valid.append(entry)
        else:unscoreable.append(dict(entry,error=row.get('error')))
    graphs.append(dict(acceptance,vertices=vertex_rows,edges=edge_rows,
                      acceptance_sha256=sha(path/'acceptance.json'),local_evidence=str(path/'evidence')))
assert sum(totals.values())==3826 and len(valid)+len(unscoreable)==3826
assert len(valid)==3815 and len(unscoreable)==11
assert {x['error'] for x in unscoreable}=={'Unable to compare two FieldData with different types. STRING vs INT64'}
assert {x['graph'] for x in unscoreable}=={'Knowledge_Graph_Geography'}
geo=ROOT/'literature/public_candidate_metadata_20261024/Text2GQL-Bench/selected_zip_assets_20261024/dataset/test/Knowledge_Graph_Geography/Cypher/TuGraph-DB_Instance'
config=read(geo/'import_config.json');field=next(p for s in config['schema'] if s['label']=='PointOfInterest' for p in s['properties'] if p['name']=='capacity')
assert field['type']=='STRING'
file=next(x for x in config['files'] if x['label']=='PointOfInterest')
with (geo/file['path']).open(encoding='utf-8-sig',newline='') as f:
    values=[dict(zip(file['columns'],r))['capacity'] for i,r in enumerate(csv.reader(f),1) if i>file['header']]
assert all(not x or x.isdecimal() for x in values)
cohorts={'kind':'PREDICTION_INDEPENDENT_REFERENCE_COHORT_INVENTORY_NOT_FINAL_METRIC_FREEZE',
 'time_local':datetime.now().astimezone().isoformat(),'source_plan_sha256':sha(BASE/'six_graph_input_plan.json'),
 'full_N':3826,'native_reference_executable':valid,'native_reference_unscoreable':unscoreable,
 'future_prediction_scope':'Keep all 3826 source IDs; no outcome-dependent removal.',
 'proposed_metric_policy':'EX on the fixed executable-reference cohort with explicit N; full-N coverage and unknown-reference counts separate. Unknown gold is not model wrong. Must freeze before predictions.',
 'final_scoring_contract_admitted':False,'new_model_calls':0}
cohortpath=ROOT/'reports/text2gql_reference_cohorts_provisional_20261026.json'
cohortpath.write_text(json.dumps(cohorts,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
summary={'kind':'SIX_GRAPH_NATIVE_DATA_AND_REFERENCE_AUDIT_NOT_METHOD_EX',
 'time_local':datetime.now().astimezone().isoformat(),'graphs':graphs,
 'planned':3826,'dispatched':sum(g['dispatched'] for g in graphs),'completed_receipts':sum(g['completed_receipts'] for g in graphs),
 'status_counts':dict(totals),'reference_execution_coverage':len(valid)/3826,
 'verified_native_vertices':sum(g['vertices'] for g in graphs),'verified_native_edges':sum(g['edges'] for g in graphs),
 'native_data_queries_in_accepted_graph_runs':query_count,'schema_metadata_HTTP_gets_in_accepted_graph_runs':metadata_count,
 'correctly_rounded_decimal_difference_rows':sum(g['correctly_rounded_decimal_difference_rows'] for g in graphs),
 'native_parser_identity_all_graphs_pass':True,'correctly_rounded_decimal_identity_all_graphs_pass':False,
 'numeric_differences_do_not_change_answers_or_scoring':True,
 'source_graph_input_seal_sha256':plan['graph_input_seal_sha256'],'reference_cohort_sha256':sha(cohortpath),
 'reference_error_evidence':{'graph':'Knowledge_Graph_Geography','unscoreable_ids':[r['id'] for r in unscoreable],
   'capacity_schema':field,'csv_rows':len(values),'nonempty_integer_strings':sum(bool(x) for x in values),
   'null_fields':sum(not x for x in values),'min_numeric_lexeme':min(int(x) for x in values if x),'max_numeric_lexeme':max(int(x) for x in values if x),
   'schema_sha256':sha(geo/'import_config.json'),'csv_sha256':sha(geo/file['path']),
   'original_queries_repaired':False,'gold_answers_invented':False},
 'default_fixture_checks':{'original_fixed_cases':8,'matched':3,'native_calls':8},
 'profile_followups':{'legacy_fixed_cases_matched':2,'default_fixed_cases_matched':3,'additional_diagnostic_calls_per_profile':4,
   'profiles_tested':2,'total_additional_calls':24,'test_queries_or_model_predictions_used':False},
 'prior_failed_preparation_preserved':['fixtures_v1 address-space limit, zero queries',
   'graph_00_native_v1 escaped-identifier lookup incompatibility, 17 data queries, zero references',
   'graph_00_native_v2 101 numeric rows differ from correctly-rounded decimal parsing, 17 data queries, zero references'],
 'total_native_query_attempts_this_stage':32+34+query_count+3826,
 'native_query_retries':0,'unknown_native_query_receipts':0,
 'REST_vs_Bolt_answer_equivalence_established':False,'paper_2783_subset_IDs_established':False,
 'generic_method_backend_adaptation_completed':False,'formal_metric_frozen':False,
 'formal_model_prediction_admitted':False,'new_model_calls':0,'entire_T1_complete':False,
 'limits':['Reference executability is not independent NL/answer correctness or method EX.',
   'All six source packs identify AT2G-Synth; they are not human-authored evaluation tasks.',
   'Native parser fingerprint fixes ingestion semantics, not a relaxation of answer scores.',
   'Native NOT/NULL, multiple MATCH, quoted identifiers, and datetime syntax differ from the original Neo4j service contract.',
   'No reported 2783-case scores directly ranked against these 3826 source records.',
   'Full original scope retained; 11 unknown-reference cases cannot be counted as known model errors or successes.']}
out=ROOT/'reports/t1_text2gql_native_reference_summary_20261026.json'
out.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:summary[k] for k in ['planned','dispatched','completed_receipts','status_counts','verified_native_vertices','verified_native_edges','native_data_queries_in_accepted_graph_runs','schema_metadata_HTTP_gets_in_accepted_graph_runs','correctly_rounded_decimal_difference_rows','total_native_query_attempts_this_stage','new_model_calls']}))
