"""Bounded4096-request paired TRAIN schema-only development prefetch, NOT formal EX.

Four TRAIN domains,512 fixed new positions per domain, two original method prompts.
Only TRAIN questions and public schemas are read. Initial outputs only; no retry, DB or scores.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
import argparse,base64,hashlib,importlib.util,json,os,queue,shutil,subprocess,sys,threading,time
import paramiko
from osdb_scoped_attempts_dev import read,sha,wire,immutable
from osdb_node_transport_live import NodeTransport,node_worker_source,UnknownRemoteAttempt
from osdb_node_transport_pool_v3 import PersistentSSH
from osdb_budgeted_model_client_live_v2 import normalized_reply
from osdb_model_limit_guard_live_v2 import CentralModelGate
from osdb_live_monitor_bridge import atomic

R=Path('D:/ch7');REPO=R/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
RUN=R/'experiments/cb_train_schema_prefetch_v1_20261027'
REMOTE='/mnt/data/0/louzehua/ch7_cb_train_schema_prefetch_v1_20261027'
HOST='interactive-zk06dlgpxt9z-5c484c5f4-vh46c'
KEY='C:/Users/10651/.codex/private/ch7_ssh_node3107_20261026/interactive_3107.key'
SSH=['ssh','-i',KEY,'-p','3107','-o','BatchMode=yes','-o','ConnectTimeout=10','root@117.50.195.94']
SAMPLING={'model':'ling-3.0-flash-n72-75','temperature':.2,'top_p':.95,'reasoning_effort':'high','max_tokens':8192}
ORDER=['flight_accident','nba','fictional_character','company','geography','movie','politics']

def remote(source,payload=None):
    command="python3 -c \"import base64;exec(base64.b64decode('"+base64.b64encode(source.encode()).decode()+"'))\""
    x=subprocess.run(SSH+[command],input=wire(payload) if payload is not None else None,capture_output=True,timeout=35)
    if x.returncode:raise RuntimeError(x.stderr.decode(errors='replace')[:500])
    return json.loads(x.stdout)

def legacy():
    sys.path.insert(0,str(REPO/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    return AttemptBudget(R/'experiments/ch7_private/continuation_20261021/budget',R/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
        path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',R/'experiments/ch7_private')])

class ReviewPool(PersistentSSH):
    def __init__(self):
        self.clients=[];self.available=queue.Queue()
        try:
            for _ in range(8):
                c=paramiko.SSHClient();self.clients.append(c);c.load_host_keys(str(Path.home()/'.ssh/known_hosts'))
                c.set_missing_host_key_policy(paramiko.RejectPolicy())
                c.connect('117.50.195.94',port=3107,username='root',key_filename=KEY,look_for_keys=False,
                          allow_agent=False,timeout=10,auth_timeout=15,banner_timeout=15)
                c.get_transport().set_keepalive(20)
                for _ in range(8):self.available.put(c)
        except BaseException:self.close();raise

class ReviewTransport(NodeTransport):
    def __init__(self,rows,pool):
        self.ssh=SSH;self.remote_root=REMOTE;self.evidence=RUN/'node_transfers';self.run=pool
        self.identities={hashlib.sha256(wire(r['messages'])).hexdigest():r['path_key'] for r in rows}
        if len(self.identities)!=len(rows):raise ValueError('Review inputs must be unique')


def prepare():
    assert not RUN.exists()
    b=legacy();before=b.inspect()
    core=R/'experiments/public_benchmark_v1/cypherbench'
    tp=core/'runtime/train.jsonl';oldp=R/'experiments/public_benchmark_v1/dev_validation_deepseek_v4_20261024/run_manifest.json'
    old=set(read(oldp)['task_ids']);tasks=[json.loads(line) for line in tp.read_text(encoding='utf-8-sig').splitlines()]
    assert len(tasks)==8534
    canon=R/'code/OSDB_CSTKG_Text2Cypher_publication_t1/experiment-harness/public_benchmark_v1'
    def literal(path,name):
        import ast
        tree=ast.parse(path.read_text(encoding='utf-8-sig'))
        value=next(n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in n.targets))
        return ast.literal_eval(value)
    systems={'direct':literal(canon/'dev_preparation.py','DIRECT_INSTRUCTIONS'),
             'ours':literal(canon/'schema_services.py','PLAN_INSTRUCTIONS')}
    sources={str(p):sha(p) for p in [tp,oldp,canon/'dev_preparation.py',canon/'schema_services.py']}
    rows=[];selected=[]
    for graph in ['biology','soccer','terrorist_attack','art']:
        sp=core/'upstream/graphs/schemas'/(graph+'_schema.json');sources[str(sp)]=sha(sp);schema=read(sp)
        candidates=[v for v in tasks if v['graph']==graph and v['qid'] not in old]
        candidates.sort(key=lambda v:hashlib.sha256(('20261027|'+graph+'|'+v['qid']).encode()).hexdigest())
        chosen=candidates[:512];assert len(chosen)==512
        for task in chosen:
            selected.append({'graph':graph,'qid':task['qid']})
            context=('Graph schema:\n'+json.dumps(schema,ensure_ascii=False)+'\nQuestion:\n'+task['nl_question']+
                     '\nSchema-only developer prefetch: no entity lookup results are supplied to either method.')
            for arm in ('direct','ours'):
                key='cb_train_prefetch_'+hashlib.sha256((graph+'\0'+task['qid']).encode()).hexdigest()[:24]+'__'+arm
                rows.append({'graph':graph,'qid':task['qid'],'arm':arm,'path_key':key,'request_id':key+'__initial',
                             'messages':[{'role':'system','content':systems[arm]},{'role':'user','content':context}]})
    assert len(rows)==4096 and len({v['qid'] for v in selected})==2048
    original=sum(read(p).get('scope_id') is None for p in b.ledger.glob('*.json'))
    assert original+4096<=12000 and not any(v['qid'] in old for v in selected)
    RUN.mkdir();immutable(RUN/'manifest.json',rows);immutable(RUN/'input_sources.json',sources);immutable(RUN/'selected_positions.json',selected)
    previous=R/'experiments/cb_train_capacity64_fresh_v2_20261027/operations/analysis'
    shutil.copyfile(previous/'common.py',RUN/'review_common.py')
    lease=(previous/'parallel_budget.py').read_text(encoding='utf-8-sig').replace('from common import immutable','from review_common import immutable')
    lease=lease.replace('owned_parallel_capacity_lease','owned_CB_TRAIN_prefetch_lease')
    lease=lease.replace('CypherBench TRAIN capacity development ','CypherBench TRAIN schema-only developer prefetch ')
    (RUN/'review_budget.py').write_text(lease,encoding='utf-8')
    worker=node_worker_source().replace('T1 OSDB exposed development, single-shot only','T1 CypherBench TRAIN schema-only developer prefetch')
    owner={'hostname':HOST,'purpose':'T1 CypherBench TRAIN schema-only developer prefetch',
           'worker_sha256':hashlib.sha256(worker.encode()).hexdigest(),
           'private_configuration':'/mnt/data/0/louzehua/ch7_t1_route_repair_v1_20261027/private/connection_louzehua_20261027.json',
           'sampling':SAMPLING,'allowed_request_ids':[r['request_id'] for r in rows]}
    deployment=remote('''from pathlib import Path
import json,sys,socket,hashlib
v=json.load(sys.stdin);assert socket.gethostname()==v['host']
o=Path('/mnt/data/0/louzehua/ch7_cb_posthoc_review_v1_20261027/execution_owner.json')
assert json.loads(o.read_text())['hostname']==socket.gethostname()
r=Path(v['root']);r.mkdir(mode=0o700)
(r/'node_model_once.py').write_text(v['worker'])
owner=v['owner'];p=Path(owner['private_configuration']);c=json.loads(p.read_text());assert c['model']==v['model'] and c.get('api_key')
owner['private_configuration_sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
(r/'execution_owner.json').write_text(json.dumps(owner));print(json.dumps({'hostname':socket.gethostname(),'owner_verified':True,'new_requests':0,'root':str(r)}))
''',{'host':HOST,'root':REMOTE,'worker':worker,'owner':owner,'model':SAMPLING['model']})
    here=Path(__file__).parent
    names=[Path(__file__).name,'osdb_node_transport_live.py','osdb_node_transport_pool_v3.py','osdb_scoped_attempts_dev.py',
           'osdb_budgeted_model_client_dev.py','osdb_budgeted_model_client_live_v2.py','osdb_model_limit_guard_live_v2.py',
           'osdb_live_monitor_bridge.py','osdb_response_boundary_dev.py','osdb_common_input_v3.py',
           'observed_neo4j_services_dev_v2.py','osdb_shared_read_executor_dev.py']
    hashes={n:sha(here/n) for n in names};(RUN/'executed_sources').mkdir()
    for n in names:shutil.copyfile(here/n,RUN/'executed_sources'/n)
    immutable(RUN/'freeze.json',{'kind':'CB_TRAIN_SCHEMA_ONLY_DEVELOPMENT_PREFETCH_NOT_FORMAL_RESULTS','planned':4096,
          'manifest_sha256':sha(RUN/'manifest.json'),'source_sha256':hashes,'lease_sha256':sha(RUN/'review_budget.py'),
          'common_sha256':sha(RUN/'review_common.py'),'input_sources_sha256':sha(RUN/'input_sources.json'),
          'sampling':SAMPLING,'max_global_inflight':64,'CB_pool_cap':12000,'approved_cap':before['cap'],
          'selected_positions':2048,'per_TRAIN_graph':512,'old_exposed12_excluded':True,
          'input_policy':'Same full public schema and original question for both methods; no candidate lookup in either arm. Not the original candidate-equipped formal condition.',
          'gold_or_reference_values_read':False,'models_only_no_training':True,'automatic_retries':0,
          'feedback_calls':0,'new_DB_queries':0,'score_or_formal_generalization_claim':False,
          'authorization':'Latest human request to prioritize potentially reusable Ling outputs while retaining cumulative12096. Uses original CypherBench development pool only.',
          'dependency':'Wait for prior posthoc review to terminate with no unknown/stop and release central lock; no concurrent second controller.',
          'deployment':deployment,'money':None})
    print(json.dumps({'prepared':4096,'TRAIN_positions':2048,'calls_so_far':0,'CB_remaining_before':12000-original,'run':str(RUN)}))


def run():
    frozen=read(RUN/'freeze.json');here=Path(__file__).parent
    assert all(sha(here/n)==h for n,h in frozen['source_sha256'].items())
    assert sha(RUN/'manifest.json')==frozen['manifest_sha256']
    assert sha(RUN/'review_budget.py')==frozen['lease_sha256'] and sha(RUN/'review_common.py')==frozen['common_sha256']
    sources=read(RUN/'input_sources.json');assert all(sha(p)==h for p,h in sources.items())
    immutable(RUN/'run_claim.json',{'pid':os.getpid(),'started_epoch':time.time(),'command':sys.argv,'kind':'ACTUAL_MODEL_REVIEW_RUNNER'})
    sys.path.insert(0,str(RUN));from review_budget import ParallelLease
    rows=read(RUN/'manifest.json');b=legacy();stop=threading.Event();lock=threading.RLock();events=[];active={};peak=0;infra=0;reason=None;pool=None;started=time.time()
    try:
        with CentralModelGate(R/'state/T1_MODEL_CONCURRENCY_POLICY_20261027.json',64) as gate:
            with ParallelLease(b,RUN/'campaign',4096) as lease:
                assert sum(read(p).get('scope_id') is None for p in b.ledger.glob('*.json'))+4096<=12000
                pool=ReviewPool();transport=ReviewTransport(rows,pool)
                def work(row):
                    nonlocal peak
                    rid=row['request_id'];body=dict(SAMPLING,messages=row['messages']);begin=time.time()
                    with gate.slot(wait_seconds=30):
                        if stop.is_set():return {'undispatched':True,'rid':rid}
                        ack=lease.reserve(rid,body);folder=RUN/'campaign/attempts'/rid
                        immutable(folder/'dispatch_claim.json',{'slot':ack['slot'],'request_id':rid,'no_retry':True})
                        with lock:active[rid]={'qid':row['qid'],'graph':row['graph'],'started_epoch':begin};peak=max(peak,len(active))
                        try:terminal=transport(body)
                        except UnknownRemoteAttempt:
                            gate.preserve_unknown(rid);stop.set();return {'rid':rid,'unknown':True,'ended_epoch':time.time()}
                        finally:
                            with lock:active.pop(rid,None)
                        raw=terminal.pop('raw_response_bytes',None)
                        if raw is not None:(folder/'response.raw').write_bytes(raw)
                        record=normalized_reply(terminal,SAMPLING['model']);end=time.time()
                        terminal.update(budget_slot=ack['slot'],scope='CB_TRAIN_SCHEMA_ONLY_DEVELOPMENT_PREFETCH',
                                        started_epoch=begin,ended_epoch=end,automatic_retry=False,normalized_reply=record)
                        lease.terminal(rid,terminal)
                        return {'rid':rid,'seconds':end-begin,'ended_epoch':end,'http_status':terminal.get('http_status'),
                                'complete':not record['transport_error'] and record['finish_reason']=='stop',
                                'infrastructure_failure':bool(record['transport_error']),'finish_reason':record['finish_reason'],**record['usage']}
                def snapshot(final=False):
                    with lock:current=dict(active)
                    value={'kind':'CB_TRAIN_SCHEMA_ONLY_PREFETCH_NOT_SCORES','pid':os.getpid(),'planned':4096,
                           'dispatched':len(lease.reserved),'terminal':len(lease.terminals),'events':len(events),
                           'active':current,'inflight':len(current),'peak_client_inflight':peak,
                           'complete_outputs':sum(v.get('complete',False) for v in events),
                           'state':'TERMINATED' if final else 'RUNNING','stop_reason':reason,
                           'updated_epoch':time.time(),'started_epoch':started,'budget':lease.status(),
                           'new_DB_queries':0,'old_results_unchanged':True}
                    atomic(RUN/'state.json',value);return value
                with ThreadPoolExecutor(max_workers=64) as executor:
                    futures={executor.submit(work,row):row for row in rows}
                    while futures:
                        done,_=wait(futures,timeout=.5,return_when=FIRST_COMPLETED)
                        for f in sorted(done,key=lambda x:0 if x.exception() else x.result().get('ended_epoch',0)):
                            row=futures.pop(f)
                            try:value=f.result()
                            except BaseException as exc:
                                value={'rid':row['request_id'],'unknown':True,'error':type(exc).__name__};reason='Controller failure; stop/drain/audit';stop.set()
                            if not value.get('undispatched'):events.append(value)
                            if value.get('unknown'):reason='Unknown physical request; no resend';stop.set()
                            elif value.get('infrastructure_failure'):
                                infra+=1
                                if infra>=3:reason='Three consecutive infrastructure failures';stop.set()
                            else:infra=0
                        snapshot()
                final=snapshot(True);immutable(RUN/'outcomes.json',events)
                immutable(RUN/'summary.json',dict(final,wall_seconds=time.time()-started,
                          source_files_unchanged=all(sha(p)==h for p,h in sources.items()),
                          prefetched_outputs_not_scores=True,not_second_backbone_result=True,money=None))
    finally:
        if pool:pool.close()
        immutable(RUN/'runner_exit.json',{'pid':os.getpid(),'ended_epoch':time.time(),'summary_exists':(RUN/'summary.json').exists(),'no_auto_restart':True})

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=('prepare','run'));args=parser.parse_args()
    prepare() if args.action=='prepare' else run()
