"""Offline observed-schema compiler preparation. No model/DB/node requests."""
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import hashlib
import json
from pathlib import Path
import shutil
import time

from observed_neo4j_services_dev_v2 import compile_selection,execute_plan,UnsupportedPlan,BASE_SOURCE_SHA256,VERSION

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OLD=ROOT/'reports/osdb_common_input_dev_v1_20261026'
OUT=ROOT/'reports/osdb_observed_profile_dev_v2_20261027'


def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,v):Path(p).write_bytes((json.dumps(v,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))


def selection(label,key):
    return {'nodes':[{'id':'n','label':label}],'edges':[],'where':[],
        'returns':[{'field':'n.'+key,'alias':'id'}]}


def main():
    assert not OUT.exists(),'Do not overwrite prior evidence'
    frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
    baseline=REPO/'experiment-harness/public_benchmark_v1/schema_services.py'
    assert sha(baseline)==BASE_SOURCE_SHA256
    runtime={str(p.relative_to(REPO)):sha(p) for p in (REPO/'experiment-harness/public_benchmark_v1').glob('*.py')}
    assert len(runtime)==46
    assert all(sha(CANON/name)==digest for name,digest in frozen.items())
    prior=read(OLD/'protocol.json')['runtime_before']
    assert all(sha(REPO/'experiment-harness/public_benchmark_v1'/name)==digest for name,digest in prior.items())
    OUT.mkdir();(OUT/'executed_sources').mkdir()
    sources={}
    for name in ('observed_neo4j_services_dev_v2.py','test_observed_neo4j_services_dev_v2.py',Path(__file__).name):
        src=Path(__file__).parent/name;shutil.copyfile(src,OUT/'executed_sources'/name);sources[name]=sha(src)
    write(OUT/'protocol.json',{'kind':'OFFLINE_OSDB_OBSERVED_SCHEMA_COMPILER_DEVELOPMENT_NOT_ADMISSION',
        'created_local':datetime.now(timezone(timedelta(hours=8))).isoformat(),'sources':sources,
        'base_source_sha256':BASE_SOURCE_SHA256,'canonical_before':frozen,'runtime_before':runtime,
        'planned_graphs':['redis','duckdb','iotdb','elasticsearch'],
        'temporal_policy':'Explicit source_event_time STRING annotations from prior importer/schema census; compile comparisons as instants, preserve storage and projection.',
        'database_dispatches':0,'model_calls':0,'official_metrics_changed':False})
    graphs=[]
    for graph in ('redis','duckdb','iotdb','elasticsearch'):
        start=time.perf_counter();source=OLD/(graph+'_schema.json');original=read(source);source_sha=sha(source)
        assert source_sha==sha(OLD/(graph+'_schema.json'))
        schema=deepcopy(original)
        # Trusted preparation policy, never a planner-supplied query override.
        for relation in schema['relations']:
            assert relation['properties']['edge_uid']=='str'
            assert relation['properties']['source_event_time']=='str'
            relation.update(identity_property='edge_uid',temporal_properties=['source_event_time'])
        records=[];rejections=0
        for entity in schema['entities']:
            call=selection(entity['label'],entity['identity_property'])
            compiled=compile_selection(call,schema);assert 'identity_fields' in compiled['contract']
            records.append({'kind':'identity_projection','label':entity['label'],'compiled':compiled})
            for prop,kind in entity['properties'].items():
                if kind=='bool':
                    boolean=deepcopy(call);boolean['where']=[{'field':'n.'+prop,'op':'eq','value':False}]
                    records.append({'kind':'boolean','label':entity['label'],'compiled':compile_selection(boolean,schema)})
            for prop in ('eid','rid','name','aliases','description','__pilot_uid'):
                if prop in entity['properties']:continue
                bad=deepcopy(call);bad['returns'][0]['field']='n.'+prop
                try:compile_selection(bad,schema)
                except UnsupportedPlan:rejections+=1
                else:raise AssertionError('Undeclared property accepted')
        for relation in schema['relations']:
            call={'nodes':[{'id':'a','label':relation['subj_label']},{'id':'b','label':relation['obj_label']}],
                'edges':[{'id':'r','source':'a','target':'b','label':relation['label']}],
                'where':[{'field':'r.source_event_time','op':'ge','value':'2023-07-01T00:00:00Z'},
                    {'field':'r.source_event_time','op':'lt','value':'2024-01-01T00:00:00.000000001Z'}],
                'returns':[{'field':'r.edge_uid','alias':'id'}]}
            c=compile_selection(call,schema)
            assert 'datetime(r.`source_event_time`).epochSeconds' in c['query'] and 'datetime($p1).nanosecond' in c['query']
            assert c['parameters']['p1'].endswith('.000000001Z')
            records.append({'kind':'typed_edge_time','signature':[relation[k] for k in ('label','subj_label','obj_label')],'compiled':c})
        first=schema['entities'][0];a=selection(first['label'],first['identity_property'])
        plan={'status':'ANSWER','calls':[a,deepcopy(a)],'combine':{'op':'difference','key':'id','output':'count','alias':'n'}}
        bags=iter([[['x'],['x'],['y']],[['y']]])
        result=execute_plan(plan,schema,lambda q,p:{'columns':['id'],'rows':next(bags),'complete':True})
        assert result['rows']==[[1]]
        assert read(source)==original and sha(source)==source_sha
        write(OUT/(graph+'_profile.json'),schema)
        write(OUT/(graph+'_compilation.json'),records)
        graphs.append({'graph':graph,'entities':len(schema['entities']),'relations':len(schema['relations']),
            'compiled_checks':len(records),'undeclared_properties_rejected':rejections,
            'mock_composition_checks':1,'mock_composition_result':result['rows'],
            'source_schema_sha256':source_sha,'profile_sha256':sha(OUT/(graph+'_profile.json')),
            'offline_preparation_seconds':time.perf_counter()-start,'database_dispatches':0,'model_calls':0})
    assert all(sha(CANON/name)==digest for name,digest in frozen.items())
    assert all(sha(REPO/name)==digest for name,digest in runtime.items())
    summary={'kind':'OSDB_OBSERVED_PROFILE_DEVELOPMENT_NOT_FORMAL_T1','version':VERSION,'graphs':graphs,
        'planned_graphs':4,'completed_graphs':4,'local_unit_tests_passed':13,
        'canonical32_and_adapter46_unchanged':True,'common24_inputs_unchanged':True,
        'source_schemas_unchanged':True,'model_calls':0,'database_dispatches':0,
        'formal_model_predictions':0,'formal_admission':False,'formal_N':None,'entire_T1_complete':False,
        'cost_currency':None,'limits':['Offline compile/type/shape checks and mocked composition, not database semantic execution.',
            'Runtime Direct/Ours common access enforcement and prompts not integrated or frozen.',
            'Two-call bound, single entity-type set contract and all task positions remain; no query-specific tools.',
            'Independent live Neo4j small-graph regression, fixed exposed development run and sufficient isolated task scope still required.',
            'OSDB model budget remains separate; CypherBench remaining allowance not used.']}
    write(OUT/'summary.json',summary)
    write(ROOT/'reports/t1_osdb_observed_profile_dev_v2_20261027.json',summary)
    write(REPO/'operations/analysis/t1_osdb_observed_profile_dev_v2_20261027.json',summary)
    print(json.dumps({'graphs':4,'compiled_checks':sum(g['compiled_checks'] for g in graphs),
        'rejected_undeclared':sum(g['undeclared_properties_rejected'] for g in graphs),'model_calls':0,'db_calls':0}))


if __name__=='__main__':main()
