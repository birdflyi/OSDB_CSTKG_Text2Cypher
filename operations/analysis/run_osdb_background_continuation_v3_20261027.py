"""Prepare once, then run the entire approved exposed-development campaign.

The model runner survives the chat turn. No resuming old runs or unknown IDs.
Local graph copies and node-only model transport share pinned source bytes.
"""
from pathlib import Path
import argparse,base64,hashlib,json,os,platform,shutil,subprocess,sys,time,traceback,importlib.metadata
from contextlib import contextmanager
sys.path.insert(0,'D:/ch7/tools/python_deps/neo4j_5_15')
from osdb_scoped_attempts_dev import read,sha,wire,immutable,ScopedAttempts,SCOPE
from osdb_budgeted_model_client_dev import load_model_inputs
from osdb_model_limit_guard_live_v2 import CentralModelGate,LimitedModelClient
from osdb_owned_graph_factory_dev import OwnedGraphFactory
from osdb_live_dispatch_v3 import run_campaign
from osdb_node_transport_live import node_worker_source
from osdb_node_transport_pool_v3 import PooledNodeTransport as NodeTransport
from osdb_live_monitor_bridge import Publisher,atomic
from osdb_posthoc_accounting_live_v2 import audit,measure
from osdb_response_boundary_dev import score_numeric
from prepare_osdb_exposed_regression_20261027 import dependencies
from accept_osdb_actor_graph_references_20261026 import budget,runtime,now,HOME

ROOT=Path('D:/ch7');REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
PLAN=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027'
INVENTORY=ROOT/'reports/osdb_launch_readiness_v2_20261027/readiness.json'
PRIOR=ROOT/'experiments/osdb_exposed_live_v1_20261027'
SECOND=ROOT/'experiments/osdb_exposed_live_v2_20261027'
AUDIT=ROOT/'reports/osdb_persistent_ssh_v3_20261027'
RUN=ROOT/'experiments/osdb_exposed_live_v3_20261027'
REMOTE='/mnt/data/0/louzehua/ch7_t1_osdb_live_v3_20261027'
HOST='interactive-zk06dlgpxt9z-5c484c5f4-vh46c'
SSH=['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_node3107_20261026/interactive_3107.key',
     '-p','3107','-o','BatchMode=yes','-o','ConnectTimeout=10','root@117.50.195.94']
SAMPLING={'model':'ling-3.0-flash-n72-75','temperature':.2,'top_p':.95,'reasoning_effort':'high','max_tokens':8192}
PROJECTS=('redis','duckdb','iotdb','elasticsearch')


def remote_code(source,payload=None,timeout=30):
    command="python3 -c \"import base64;exec(base64.b64decode('"+base64.b64encode(source.encode()).decode()+"'))\""
    q=subprocess.run(SSH+[command],input=wire(payload) if payload is not None else None,capture_output=True,timeout=timeout)
    if q.returncode:raise RuntimeError('Owned node operation failed: '+q.stderr.decode('utf-8',errors='replace')[:1500])
    return json.loads(q.stdout)


def prepare():
    assert not RUN.exists(),'Never overwrite/restart an old run'
    actual=budget();assert actual['pending']==[]
    assert not (ROOT/'state/.t1-global-model-dispatch.lock').exists()
    assert not (ROOT/'state/.t1-owned-graph-factory.lock').exists()
    accepted=read(ROOT/'reports/t1_osdb_owned_graph_factory_acceptance_20261027.json')
    assert accepted['real_graph_factory_accepted'] and accepted['completed_graphs']==4
    native=read(ROOT/'reports/t1_native_set_count_engine_acceptance_20261027.json')
    assert native['passed_strategy_checks']==108
    manifest_sha=sha(PLAN/'model_path_manifest.json');initial=load_model_inputs(PLAN,manifest_sha)
    approval=read(ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json')
    assert approval['osdb_exposed_development']['approved'] and approval['osdb_exposed_development']['plan_manifest_sha256']==manifest_sha
    # Policy validation sends nothing and does not acquire a runtime lease.
    CentralModelGate(ROOT/'state/T1_MODEL_CONCURRENCY_POLICY_20261027.json',12)
    prior_closed=read(PRIOR/'campaign/scope_close.json');assert prior_closed['pending']==[]
    prior_paths={p.stem:{'path':str(p),'sha256':sha(p)} for p in (PRIOR/'campaign/paths').glob('*.json')}
    assert len(prior_paths)==12 and all(key.startswith('redis_') for key in prior_paths)
    assert (PRIOR/'runner_exit.json').exists() and (PRIOR/'length_null_classification_audit.json').exists()
    assert read(SECOND/'runner_exit.json')['paths_completed']==23
    prior_paths.update({p.stem:{'path':str(p),'sha256':sha(p)} for p in (SECOND/'campaign/paths').glob('*.json')})
    audited=AUDIT/'audited_failed_path.json';prior_paths['duckdb_ACT05__direct']={'path':str(audited),'sha256':sha(audited)}
    assert len(prior_paths)==24 and set(k.split('_')[0] for k in prior_paths)=={'redis','duckdb'}
    RUN.mkdir();code=RUN/'operations/analysis';code.mkdir(parents=True)
    sources=dependencies([Path(__file__).name,'osdb_node_transport_live.py','osdb_live_monitor_bridge.py','test_osdb_node_transport_pool_v3.py'])
    for name in sources:shutil.copyfile(Path(__file__).with_name(name),code/name)
    adapter=runtime();guard=RUN/'experiment-harness/public_benchmark_v1';guard.mkdir(parents=True)
    for name,h in adapter.items():
        target=guard/name;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(REPO/'experiment-harness/public_benchmark_v1'/name,target);assert sha(target)==h
    profiles={g:{'path':str(ROOT/'reports/osdb_observed_profile_dev_v2_20261027'/(g+'_profile.json'))} for g in PROJECTS}
    for item in profiles.values():item['sha256']=sha(item['path'])
    worker=node_worker_source();worker_sha=hashlib.sha256(worker.encode()).hexdigest()
    monitor_view=Path(__file__).with_name('osdb_campaign_monitor_dev.py').read_text(encoding='utf-8-sig')
    monitor_view=monitor_view.replace("['题位','方法','当前阶段','已耗时']","['题位','问题摘要','方法','当前阶段','已耗时']")
    monitor_view=monitor_view.replace("t.add_row(row['case_id'],'直接查询'", "t.add_row(row['case_id'],row.get('question','')[:65],'直接查询'")
    monitor='''import pathlib,json,time,argparse,socket
from rich.console import Console
from rich.live import Live
from osdb_campaign_monitor_dev import render
p=argparse.ArgumentParser();p.add_argument('--view',required=True);a=p.parse_args()
root=pathlib.Path(__file__).resolve().parent
assert json.loads((root/'execution_owner.json').read_text())['hostname']==socket.gethostname()
console=Console()
with Live(console=console,refresh_per_second=.34,screen=True) as live:
 while True:
  try:live.update(render(json.loads((root/'state.json').read_text()),a.view,now=time.time()))
  except Exception as exc:live.update('等待运行快照：'+type(exc).__name__)
  time.sleep(3)
'''
    publish='''import pathlib,json,sys,socket,os
r=pathlib.Path(__file__).resolve().parent
assert json.loads((r/'execution_owner.json').read_text())['hostname']==socket.gethostname()
v=json.load(sys.stdin);t=r/'state.writing';t.write_text(json.dumps(v,ensure_ascii=False),encoding='utf-8');os.replace(t,r/'state.json')
print('{}')
'''
    owner={'hostname':HOST,'purpose':'T1 OSDB exposed development, single-shot only','worker_sha256':worker_sha,
        'private_configuration':'/mnt/data/0/louzehua/ch7_t1_route_repair_v1_20261027/private/connection_louzehua_20261027.json',
        'sampling':SAMPLING,'allowed_request_ids':[key+'__'+phase for key in initial for phase in ('initial','feedback')]}
    files={'node_model_once.py':worker,'monitor.py':monitor,'publish_state.py':publish,'osdb_campaign_monitor_dev.py':monitor_view,
        'execution_owner.json':json.dumps(owner,ensure_ascii=True)}
    for name,source in files.items():(RUN/('remote_'+name)).write_text(source,encoding='utf-8')
    deployment=remote_code('''import pathlib,json,sys,socket,subprocess,hashlib
v=json.load(sys.stdin);assert socket.gethostname()==v['host']
old=pathlib.Path('/mnt/data/0/louzehua/ch7_t1_monitor_release_20261027')
assert json.loads((old/'execution_owner.json').read_text())['hostname']==socket.gethostname()
tmux=['tmux','-L','ch7-t1-monitor']
starts=subprocess.check_output(tmux+['list-panes','-s','-t','ch7-t1','-F','#{pane_start_command}'],text=True)
assert len(starts.splitlines())==2 and all('/mnt/data/0/louzehua/ch7_t1_osdb_live_v2_20261027/monitor.py' in line for line in starts.splitlines()),starts
r=pathlib.Path(v['root']);r.mkdir(mode=0o700)
for name,text in v['files'].items():(r/name).write_text(text,encoding='utf-8')
owner=json.loads((r/'execution_owner.json').read_text());private=pathlib.Path(owner['private_configuration'])
c=json.loads(private.read_text());assert c['model']==v['model'] and c['endpoint']==v['endpoint'] and c.get('api_key')
owner['private_configuration_sha256']=hashlib.sha256(private.read_bytes()).hexdigest()
(r/'execution_owner.json').write_text(json.dumps(owner),encoding='utf-8')
print(json.dumps({'hostname':socket.gethostname(),'root':str(r),'worker_sha256':owner['worker_sha256'],'private_configuration_sha256':owner['private_configuration_sha256'],'existing_owned_panes_verified':True}))
''',{'host':HOST,'root':REMOTE,'files':files,'model':SAMPLING['model'],
        'endpoint':'http://127.0.0.1:18894/gpt-oss-120b/litellm/v1/chat/completions'})
    immutable(RUN/'node_deployment.json',deployment)
    immutable(RUN/'effective_model.json',dict(SAMPLING,endpoint='http://127.0.0.1:18894/gpt-oss-120b/litellm/v1/chat/completions',credential_source='node private configuration; no local credential copied'))
    # This freezes the controlled development attempt, not long-context success.
    frozen={'kind':'EXPOSED_DEVELOPMENT_LIVE_LAUNCH_FREEZE_NOT_FORMAL','created_local':now(),'source_sha256':sources,
        'adapter46':adapter,'inventory_sha256':sha(INVENTORY),'profiles':profiles,'sampling':SAMPLING,
        'manifest_sha256':manifest_sha,'evaluator_sha256':sha(PLAN/'evaluator_only.json'),
        'initial_messages_sha256':{k:hashlib.sha256(wire(v)).hexdigest() for k,v in initial.items()},
        'model_configuration_sha256':hashlib.sha256(wire(read(RUN/'effective_model.json'))).hexdigest(),
        'node_deployment':deployment,'max_inflight_paths':12,'model_global_cap':64,
        'budget_before':actual,'OSDB_scope_cap':96,'initial_paths':48,'feedback_max_each':1,'automatic_retry':False,
        'prior_completed_paths':prior_paths,'prior_posthoc':{'path':str(PRIOR/'posthoc.json'),'sha256':sha(PRIOR/'posthoc.json')},
        'continuation_only_unissued':True,'current_block_initial_paths':24,
        'prior_second_posthoc':{'path':str(SECOND/'posthoc.json'),'sha256':sha(SECOND/'posthoc.json')},
        'transport_audit':{'path':str(AUDIT/'reconciliation.json'),'sha256':sha(AUDIT/'reconciliation.json')},
        'credential_owner':'louzehua; new requests only; original credential records preserved',
        'transport_pool':{'authenticated_connections':2,'channels_per_connection':6,'automatic_reconnect':False},
        'classification_change':'length with null final content is MODEL_TRUNCATED, not infrastructure; no token/sampling/method change',
        'model_client_deadline_seconds':240,'explain_read_seconds':[30,60],'offline_scan_seconds':600,
        'source_graphs':'Accepted local isolated copies; node used only for one-shot model calls and own monitor',
        'cold_warm_state':'Admission scans warm the graph; no independent cold-performance claim',
        'controls':'Final-plan full detail, simple alias and applicable native counts; costs separated',
        'independent_method_increment_established':False,'long_context_success_already_claimed':False,
        'python':sys.version,'python_executable_sha256':sha(sys.executable),
        'package_versions':{name:importlib.metadata.version(name) for name in ('neo4j','requests','rich','psutil','paramiko','cryptography','bcrypt','PyNaCl')},
        'engine_jar_sha256':sha(HOME/'lib/neo4j-5.15.0.jar'),'formal_prediction':False,'money':None}
    immutable(RUN/'freeze.json',frozen)
    print(json.dumps({'prepared':str(RUN),'sources':len(sources),'remote':deployment,'model_calls':0},ensure_ascii=True))


def issue(stage,reason,details=None):
    with (ROOT/'state/T1_BACKGROUND_ISSUES_20261027.jsonl').open('ab') as f:
        f.write(wire({'issue_id':'LIVE-'+str(time.time_ns()),'created_local':now(),'stage':stage,
            'requires_user':False,'reason':reason,'details':details,'run':str(RUN),'no_automatic_retry':True}))


def run():
    frozen=read(RUN/'freeze.json');code=Path(__file__).parent
    assert all(sha(code/name)==h for name,h in frozen['source_sha256'].items())
    assert all(sha(RUN/'experiment-harness/public_benchmark_v1'/name)==h for name,h in frozen['adapter46'].items())
    assert sha(INVENTORY)==frozen['inventory_sha256']
    assert sha(PLAN/'evaluator_only.json')==frozen['evaluator_sha256']
    assert sha(sys.executable)==frozen['python_executable_sha256']
    assert sha(HOME/'lib/neo4j-5.15.0.jar')==frozen['engine_jar_sha256']
    assert {name:importlib.metadata.version(name) for name in frozen['package_versions']}==frozen['package_versions']
    assert sha(frozen['prior_posthoc']['path'])==frozen['prior_posthoc']['sha256']
    assert all(sha(v['path'])==v['sha256'] for v in frozen['prior_completed_paths'].values())
    assert sha(frozen['prior_second_posthoc']['path'])==frozen['prior_second_posthoc']['sha256']
    assert sha(frozen['transport_audit']['path'])==frozen['transport_audit']['sha256']
    initial=load_model_inputs(PLAN,frozen['manifest_sha256'])
    assert not (RUN/'run_claim.json').exists(),'No resume/relaunch of a claimed run'
    immutable(RUN/'run_claim.json',{'pid':os.getpid(),'hostname':platform.node(),'started_local':now(),'command':sys.argv})
    background=read(ROOT/'state/T1_BACKGROUND_PLAN_20261027.json')
    background.update(overall_state='P2_BACKGROUND_RUNNING',model_runner={'started':True,'pid':os.getpid(),
        'started_local':now(),'log_directory':str(RUN),'phase':'GRAPH_ADMISSION_THEN_MODEL_DEVELOPMENT',
        'stdout':str(RUN/'runner_stdout.log'),'stderr':str(RUN/'runner_stderr.log')})
    background['stages'][1]['state']='DONE_LAUNCH_FROZEN';background['stages'][2]['state']='RUNNING'
    atomic(ROOT/'state/T1_BACKGROUND_PLAN_20261027.json',background)
    questions={k:json.loads(v[1]['content']).get('question','') for k,v in initial.items()}
    pub=Publisher(RUN,SSH,REMOTE,ROOT/'state/T1_BACKGROUND_PLAN_20261027.json',ROOT/'state/T1_BACKGROUND_ISSUES_20261027.jsonl',questions)
    pub.start();final=None;failure=None;transport=None
    try:
        sys.path.insert(0,str(RUN/'experiment-harness/public_benchmark_v1'))
        from attempt_budget import AttemptBudget
        legacy=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
            path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])
        with CentralModelGate(ROOT/'state/T1_MODEL_CONCURRENCY_POLICY_20261027.json',12) as gate:
            with ScopedAttempts(legacy,RUN/'campaign',frozen['manifest_sha256'],initial) as lease:
                private=read(RUN/'effective_model.json')
                admission=dict(frozen,scope_id=SCOPE,plan_manifest_sha256=frozen['manifest_sha256'],
                    route_repair_verified=True,online_launch_accepted=True,formal_prediction=False)
                transport=NodeTransport(SSH,REMOTE,initial,RUN/'node_transfers')
                client=LimitedModelClient(lease,private,initial,admission,transport=transport,model_gate=gate)
                with OwnedGraphFactory(RUN/'graphs',INVENTORY,frozen['inventory_sha256'],frozen['profiles'],HOME,
                        ROOT/'state/.t1-owned-graph-factory.lock') as factory:
                    @contextmanager
                    def tracked(graph):
                        pub.set_phase('验收'+graph+'图环境')
                        with factory(graph) as handle:
                            pub.set_phase('开发运行：'+graph)
                            yield handle
                        pub.set_phase('已关图，自动接续')
                    hashes={g:read(INVENTORY)['cold_receipts'][g]['semantic_snapshot_sha256'] for g in PROJECTS}
                    final=run_campaign(client,PLAN/'model_path_manifest.json',RUN/'dispatch',tracked,
                        max_inflight=12,graph_snapshot_hashes=hashes,evidence_kind='live_development',observer=pub.offer,prior_paths={k:v['path'] for k,v in frozen['prior_completed_paths'].items()})
        pub.set_phase('自动评分与成本对账')
        result=audit(RUN/'campaign',ROOT/'experiments/ch7_private/continuation_20261021/budget',PLAN/'model_path_manifest.json',PLAN/'evaluator_only.json',
            manifest_sha256=frozen['manifest_sha256'],evaluator_sha256=frozen['evaluator_sha256'],evidence_kind='live_development')
        refs={r['task_id']:r['numeric_reference_rows'] for r in read(PLAN/'evaluator_only.json')}
        paths={p.stem:read(p) for p in (RUN/'campaign/paths').glob('*.json')}
        result['independent_client_costs']={arm:measure([p['costs']['method_path_wall_seconds'] for key,p in paths.items() if key.endswith('__'+arm)]) for arm in ('direct','ours')}
        result['native_controls']={key:{'status':p['native_control']['status'],'matches_ours':p['native_control'].get('matches_ours'),
            'score':score_numeric(p['native_control']['result'],refs[key.rsplit('__',1)[0]]) if p['native_control'].get('result') else None}
            for key,p in paths.items() if key.endswith('__ours')}
        result['prior_execution_reference']=frozen['prior_posthoc']
        result['current_block_scope']={'planned_new_initial_paths':24,'prior_terminated_paths':24,
            'completed_new_paths':len(paths),'prior_source_unchanged':True}
        immutable(RUN/'posthoc.json',result)
        prior=read(frozen['prior_posthoc']['path'])
        combined={k:v for k,v in prior['paths'].items() if v['state']=='completed'}
        second=read(frozen['prior_second_posthoc']['path'])
        combined.update({k:v for k,v in second['paths'].items() if v['state']=='completed'})
        combined['duckdb_ACT05__direct']={'state':'terminated_infrastructure_failure_by_audit','final_status':'INFRASTRUCTURE_ERROR','generated_answer':False,'audit':frozen['transport_audit'],'score':None}
        for key,value in result['paths'].items():
            if value['state']=='completed':
                assert key not in combined,'Never execute a previously completed path'
                combined[key]=value
        immutable(RUN/'cumulative_development_summary.json',{'kind':'CROSS_VERSION_EXPOSED_DEVELOPMENT_NOT_FORMAL',
            'planned_paths':48,'completed_paths':len(combined),'paths':combined,
            'physical_attempts':prior['reserved_attempts']+second['reserved_attempts']+result['reserved_attempts'],
            'classification_audit':str(PRIOR/'length_null_classification_audit.json'),
            'same_messages_sampling_and_method':True,'frozen_versions':[str(PRIOR/'freeze.json'),str(SECOND/'freeze.json'),str(RUN/'freeze.json')],
            'original_v1_results_not_rewritten':True,'formal_prediction':False,'T1_complete':False})
        if final['runner_error_paths'] or final['initial_paths_dispatched']<48 or result['pending_slots']:
            issue('P2','Bounded batch stopped or pending; audit evidence before any continuation',{'dispatch':final,'posthoc':str(RUN/'posthoc.json')})
    except BaseException as exc:
        failure={'type':type(exc).__name__,'message':str(exc)[:1600],'traceback':traceback.format_exc()}
        immutable(RUN/'runner_failure.json',failure);issue('P1/P2/P3','Runner stopped; no automatic request replay',failure)
    finally:
        if transport is not None:transport.close()
        pub.set_phase('已停止，见问题日志' if failure or final is None or final.get('paths_completed')!=48 else '开发已终结，评分已保存')
        pub.close()
        state=read(ROOT/'state/T1_BACKGROUND_PLAN_20261027.json')
        state['model_runner'].update(process_exited=True,ended_local=now(),phase=pub.phase)
        state['overall_state']='P2_P3_NEEDS_AUDIT' if failure or final is None or final.get('paths_completed')!=48 else 'P3_COMPLETE_P4_READY'
        state['stages'][2]['state']='TERMINATED_COMPLETE' if final and final.get('paths_completed')==48 else 'STOPPED_NEEDS_AUDIT'
        state['stages'][3]['state']='DONE' if (RUN/'posthoc.json').exists() else 'NEEDS_AUDIT'
        atomic(ROOT/'state/T1_BACKGROUND_PLAN_20261027.json',state)
        immutable(RUN/'runner_exit.json',{'ended_local':now(),'failure':failure,'paths_completed':final.get('paths_completed') if final else None,
            'posthoc_exists':(RUN/'posthoc.json').exists(),'monitor_publish_errors':pub.errors,'formal_prediction':False,'T1_complete':False})
    if failure:raise RuntimeError('Live runner stopped; see preserved failure and issue log')


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=('prepare','run'));args=parser.parse_args()
    prepare() if args.action=='prepare' else run()
