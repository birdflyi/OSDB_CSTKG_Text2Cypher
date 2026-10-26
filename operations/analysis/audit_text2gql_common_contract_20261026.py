"""Offline input parity/catalog and native response-shape audit, no new requests.

Public config/vertex CSV feed only the candidate/input branch. Previously cached
TEST reference responses feed only representation validation/counts, never model
inputs or method adaptation. No question JSON, model private config or query is
read by the input branch. No primary metric is chosen by this script.
"""
from pathlib import Path
from datetime import datetime
from collections import Counter
import csv,hashlib,json,subprocess,sys,time

ROOT=Path('D:/ch7');SOURCE=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph/experiment-harness/public_benchmark_v1'
sys.path.insert(0,str(SOURCE))
from tugraph_schema import schema_from_import_config
from tugraph_common_input import NativeEntityCandidates, common_input, messages, canonical
from tugraph_results import parse_rest_result, MalformedNativeResult

def read(p):return json.loads(p.read_text(encoding='utf-8'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
OUT=ROOT/'reports/text2gql_common_contract_20261026';OUT.mkdir(exist_ok=True)
assets=ROOT/'literature/public_candidate_metadata_20261024/Text2GQL-Bench/selected_zip_assets_20261024'
plan=read(ROOT/'reports/text2gql_native_engine_20261026/six_graph_input_plan.json')
graphs=[]
for index,entry in enumerate(plan['graphs']):
    graph_started=time.monotonic()
    base=assets/'dataset/test'/entry['graph']/'Cypher/TuGraph-DB_Instance'
    configpath=base/'import_config.json';assert sha(configpath)==entry['config_sha256']
    config=read(configpath);schema=schema_from_import_config(config)
    declarations={e['label']:e for e in schema['entities']}
    for file in entry['graph_data_assets']:
        path=(assets/file['member']).resolve()
        assert assets.resolve() in path.parents and sha(path)==file['sha256']
    entities=[];counts=Counter();source_files=[]
    for file in config['files']:
        label=file['label']
        if label not in declarations or 'SRC_ID' in file or 'DST_ID' in file:continue
        path=(base/file['path']).resolve();assert base.resolve() in path.parents
        assert file['format']=='CSV' and type(file['header']) is int
        columns=file['columns'];entity=declarations[label]
        string_fields={p for p,k in entity['properties'].items() if k=='str'}
        primary=entity['identity_property'];keep=string_fields|{primary}
        with path.open(encoding='utf-8',newline='') as stream:
            reader=csv.reader(stream)
            for _ in range(file['header']):next(reader)
            for raw in reader:
                assert len(raw)==len(columns)
                values=dict(zip(columns,raw));properties={}
                for key in keep:
                    value=values[key]
                    if value=='' and entity['nullable'][key]:value=None
                    elif key==primary and entity['properties'][key]=='int':value=int(value)
                    properties[key]=value
                entities.append({'label':label,'properties':properties});counts[label]+=1
        source_files.append({'name':path.name,'sha256':sha(path)})
    assert dict(counts)==entry['expected_vertices_by_label']
    indexing_started=time.monotonic();catalog=NativeEntityCandidates(schema,entities)
    indexing_seconds=time.monotonic()-indexing_started
    catalogpath=OUT/(str(index).zfill(2)+'_catalog.json')
    catalogpath.write_text(canonical(entities)+'\n',encoding='utf-8')
    # Constructed contract probe, not a benchmark question or a prediction.
    question='Development input contract: list '+schema['entities'][0]['label']+' identities.'
    lookup_started=time.monotonic();candidates=catalog.find(question,limit=12)
    lookup_seconds=time.monotonic()-lookup_started
    shared=common_input(task_id='development_contract_probe_'+str(index),graph=entry['graph'],
                         question=question,schema=schema,candidates=candidates)
    direct,ours=messages(shared,'direct'),messages(shared,'ours')
    assert direct[1]==ours[1]
    sharedpath=OUT/(str(index).zfill(2)+'_shared_probe.json')
    sharedpath.write_text(json.dumps({'shared':shared,'direct_messages':direct,'ours_messages':ours},
                                    ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    graphs.append({'graph':entry['graph'],'catalog_entities':len(entities),
      'expected_entities':entry['expected_vertices'],'vertex_csv_files':source_files,
      'catalog_sha256':sha(catalogpath),'shared_probe_sha256':sha(sharedpath),
      'common_user_content_sha256':shared['sha256'],'Direct_Ours_identical_user_bytes':True,
      'offline_preparation_wall_seconds':time.monotonic()-graph_started,
      'catalog_index_construction_seconds':indexing_seconds,'constructed_probe_lookup_seconds':lookup_seconds,
      'probe_is_constructed_not_TEST_question':True,'new_model_calls':0})

# Separate representation-only branch over existing reference audit artifacts.
# It does not check semantic answers, select tasks, rewrite scores or change gold.
records=[]
for index in range(6):
    folder=('text2gql_native_reference_graph_00_v3_20261026' if index==0 else
            'text2gql_native_reference_graph_'+str(index).zfill(2)+'_20261026')
    base=ROOT/'reports'/folder/'evidence';ok=bad=empty=0;columns=Counter()
    for file in sorted((base/'reference_records').glob('*.json')):
        record=read(file);response=base/'responses'/('reference_'+file.stem+'.json')
        assert sha(response)==record['response_sha256']
        if record['status'].startswith('EXECUTED'):
            parsed=parse_rest_result(response.read_bytes(),transport_complete=True,execution_complete=True)
            ok+=1;empty+=not parsed['rows'];columns[len(parsed['columns'])]+=1
        else:
            assert record['status']=='GOLD_EXECUTION_ERROR'
            try:parse_rest_result(response.read_bytes(),transport_complete=True,execution_complete=True)
            except MalformedNativeResult:bad+=1
            else:raise AssertionError('Gold execution error cannot be accepted as an empty successful result')
    records.append({'graph':plan['graphs'][index]['graph'],'existing_success_responses_parsed':ok,
                    'legitimate_empty':empty,'existing_gold_errors_rejected_as_success':bad,
                    'column_count_distribution':dict(columns),'new_database_queries':0})
assert sum(x['existing_success_responses_parsed'] for x in records)==3815
assert sum(x['existing_gold_errors_rejected_as_success'] for x in records)==11
assert sum(x['catalog_entities'] for x in graphs)==21315
ids=sorted(int(p.stem) for p in (ROOT/'experiments/ch7_private/continuation_20261021/budget').glob('*.json') if p.stem.isdigit())
assert ids==list(range(1,5193))
freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
canon=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
assert all(sha(canon/p)==digest for p,digest in freeze['code_files'].items())
code=r'''
from pathlib import Path
import json,platform
r=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
assert platform.node()==json.loads((r/'execution_owner.json').read_text())['hostname']=='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
active=[]
for p in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  args=p.read_bytes().split(b'\0')
  if str(r).encode() in b' '.join(args) and any(Path(a.decode(errors='ignore')).name in ('worker.py','lgraph_server','lgraph_import') for a in args):active.append(int(p.parent.name))
 except (OSError,ValueError):pass
print(json.dumps({'hostname':platform.node(),'main_lock':(r/'.public-runtime.lock').exists(),
 'native_processes':active,'read_only':True}))
'''
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
 '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
 input=code,text=True,encoding='utf-8',capture_output=True,timeout=30)
assert run.returncode==0,run.stderr[:200]
remote=json.loads(run.stdout);assert not remote['main_lock'] and not remote['native_processes']
remote['time_local']=datetime.now().astimezone().isoformat()
(OUT/'remote_readonly.json').write_text(json.dumps(remote,indent=2)+'\n',encoding='utf-8')
summary={'kind':'COMMON_INPUT_AND_NATIVE_REPRESENTATION_PREPARATION_NOT_FORMAL_EA',
 'time_local':datetime.now().astimezone().isoformat(),'graphs':graphs,'entity_catalog_total':21315,
 'candidate_limit':12,'reference_shape_audit':records,'existing_success_responses_parsed':3815,
 'existing_gold_errors_not_accepted_as_success':11,'legitimate_empty_responses':1,
 'TEST_question_JSON_read_by_input_branch':False,
 'cached_TEST_reference_responses_read_only_by_representation_branch':True,
 'reference_to_model_input_flow':False,'source_sha256':{name:sha(SOURCE/name) for name in
 ['tugraph_common_input.py','tugraph_results.py','test_tugraph_common_input.py','test_tugraph_results.py']},
 'new_model_calls':0,'new_database_queries':0,'cumulative_model_attempts':5192,'cumulative_cap':12000,
 'remaining_original_CypherBench_scope':6808,'new_budget_approved':False,'new_budget_question_pending':True,
 'original_core_source_hashes_unchanged':32,'local_new_component_checks_passed':15,
 'strict_comparator_role':'type/multiset/order sensitivity only; not author EA or chosen primary metric',
 'author_Bolt_equivalence_established':False,'formal_metric_frozen':False,
 'formal_prediction_admitted':False,'entire_T1_complete':False,'remote_readonly':remote,
 'pending':['bounded native execution wrapper and feedback/coverage/cost integration',
 'native author-driver/REST-Bolt representation and exact evaluation/cohort/order/statistics protocol',
 'new model budget approval then TRAIN-only/independent development model validation',
 'new source/task/method/model/metric/budget formal freeze before prediction']}
p=ROOT/'reports/t1_text2gql_common_contract_summary_20261026.json'
p.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'catalogs':6,'entities':21315,'identical_input_pairs':6,
 'cached_reference_success_shapes':3815,'known_errors_not_success':11,'new_model_calls':0,
 'new_database_queries':0,'remote_native_processes':0}))
