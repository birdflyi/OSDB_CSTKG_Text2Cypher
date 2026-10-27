"""Finite four-real-graph factory acceptance; no model generation or scoring.

All original stores and model messages stay unchanged. One new read per graph
checks the new factory path. Last graph additionally receives a deliberate
client deadline fault on one read-only computation; never replay it.
"""
from pathlib import Path
import json, sys, shutil, time, subprocess, os
sys.path.insert(0,'D:/ch7/tools/python_deps/neo4j_5_15')
from osdb_owned_graph_factory_dev import OwnedGraphFactory, bounded_database_call, ClientDeadlineExceeded
from osdb_scoped_attempts_dev import read,sha,immutable
from prepare_osdb_exposed_regression_20261027 import dependencies
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now,live,HOME

ROOT=Path('D:/ch7');OUT=ROOT/'reports/osdb_owned_graph_factory_v2_20261027'
CODE=Path(__file__).parent; INVENTORY=ROOT/'reports/osdb_launch_readiness_v2_20261027/readiness.json'
PROJECTS=('redis','duckdb','iotdb','elasticsearch')


def main():
    assert OUT.is_dir() and (OUT/'before.json').is_file() and not (OUT/'protocol.json').exists()
    before=read(OUT/'before.json');assert budget()==before['budget'] and not before['budget']['pending']
    assert runtime()==before['runtime'];assert not before['experiment_processes']
    assert before['SSH']['node']['own_workers']==[]
    frozen=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
    canon=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
    assert all(sha(canon/p)==h for p,h in frozen.items())
    profiles={g:{'path':str(ROOT/'reports/osdb_observed_profile_dev_v2_20261027'/(g+'_profile.json'))} for g in PROJECTS}
    for p in profiles.values():p['sha256']=sha(p['path'])
    sources=dependencies([Path(__file__).name,'osdb_owned_graph_factory_dev.py','test_osdb_owned_graph_factory_dev.py'])
    (OUT/'executed_sources').mkdir()
    for p in sources:shutil.copyfile(CODE/p,OUT/'executed_sources'/p)
    tests=subprocess.run([sys.executable,'-m','unittest','-v','test_osdb_owned_graph_factory_dev'],
        cwd=CODE,capture_output=True,encoding='utf-8',
        env=dict(os.environ,PYTHONPATH='D:/ch7/tools/python_deps/neo4j_5_15'))
    (OUT/'boundary_tests.txt').write_text(tests.stdout+tests.stderr,encoding='utf-8')
    assert tests.returncode==0,'Audit failed boundary checks before any boot'
    query='MATCH(n) RETURN count(n) AS total'
    fault='UNWIND range(1, 1000000000) AS x RETURN sum(sin(x)) AS total'
    immutable(OUT/'protocol.json',{'sealed_local':now(),'kind':'REAL_GRAPH_FACTORY_ACCEPTANCE_NOT_MODEL_RESULT',
        'source_sha256':sources,'frozen32':frozen,'inventory_sha256':sha(INVENTORY),'profiles':profiles,
        'graphs':PROJECTS,'planned_graph_admissions':4,'planned_count_checks':4,'planned_client_deadline_faults':1,
        'query':query,'fault_query':fault,'fault_client_deadline_seconds':.5,
        'fault_expected':'ClientDeadlineExceeded; owned JVM stopped; subsequent read rejected before I/O',
        'same_future_executor':True,'no_model_or_old_reference_replay':True,'boundary_checks':4})
    checks=[];failure=None;start=time.monotonic()
    try:
        with OwnedGraphFactory(OUT/'runtime',INVENTORY,sha(INVENTORY),profiles,HOME,
                ROOT/'state/.t1-owned-graph-factory.lock') as factory:
            for graph in PROJECTS:
                with factory(graph) as handle:
                    t=time.monotonic();count_before=handle['physical_statement_count']()
                    result=handle['execute'](query,{})
                    pin=read(INVENTORY)['cold_receipts'][graph]
                    assert result['status']=='EXECUTED' and result['complete'] and result['rows']==[[pin['nodes']]]
                    receipt={'graph':graph,'passed':True,'result':result,'client_seconds':time.monotonic()-t,
                        'physical_statements':handle['physical_statement_count']()-count_before}
                    immutable(OUT/'checks'/(graph+'.json'),receipt);checks.append(receipt)
                    if graph=='elasticsearch':
                        t=time.monotonic();expired=False
                        try:bounded_database_call(lambda:handle['execute'](fault,{}),handle['abort_owned'],.5)
                        except ClientDeadlineExceeded:expired=True
                        assert expired,'Expected client deadline must actually interrupt real driver I/O'
                        counter=handle['physical_statement_count']()
                        try:handle['execute'](query,{})
                        except RuntimeError:pass
                        else:raise AssertionError('Stopped graph must reject further I/O')
                        assert handle['physical_statement_count']()==counter
                        immutable(OUT/'deadline_fault.json',{'passed':True,'client_seconds_including_owned_stop':time.monotonic()-t,
                            'error':'ClientDeadlineExceeded','subsequent_read_rejected_before_I_O':True,
                            'deliberate_infrastructure_fault_not_method_failure':True,'automatic_replay':False})
                print(json.dumps({'graph':graph,'completed_graphs':len(checks),'state':'CLOSED','model_calls':0}),flush=True)
    except BaseException as exc:
        failure={'type':type(exc).__name__,'message':str(exc)[:1500]};immutable(OUT/'failure.json',failure)
    assert budget()==before['budget'] and runtime()==before['runtime']
    assert all(sha(canon/p)==h for p,h in frozen.items())
    summary={'kind':'REAL_GRAPH_FACTORY_ACCEPTANCE_NOT_MODEL_RESULT','planned_graphs':4,
        'dispatched_graphs':len(list((OUT/'runtime').glob('*/execution_owner.json'))),
        'completed_graphs':len(checks),'failed_stage':failure,'undispatched_graphs':4-len(list((OUT/'runtime').glob('*/execution_owner.json'))),
        'planned_count_checks':4,'passed_count_checks':len(checks),'planned_deadline_faults':1,
        'passed_deadline_faults':int((OUT/'deadline_fault.json').exists()),'offline_boundary_checks':4,
        'wall_seconds':time.monotonic()-start,'model_calls':0,'training':0,'budget':budget(),
        'canonical32_adapter46_unchanged':True,'ports_closed':not live(18531) and not live(18532),
        'factory_lock_released':not (ROOT/'state/.t1-owned-graph-factory.lock').exists(),
        'source_bytes_verified_before_and_after':True if not failure else None,
        'live_development_paths':{'planned':48,'dispatched':0,'completed':0,'failed':0,'undispatched':48},
        'formal_admission':False,'T1_complete':False,'actual_development_entry_ready':False}
    immutable(OUT/'summary.json',summary);print(json.dumps(summary,ensure_ascii=True),flush=True)
    if failure:raise RuntimeError('Factory acceptance failed; preserve evidence before a new version')


if __name__=='__main__':main()
