"""Build a separate bounded TRAIN output-cache driver; never edits active code."""
from pathlib import Path
import ast

HERE=Path(__file__).parent
BASE=HERE/'run_cb_posthoc_review_20261027.py'
TARGET=HERE/'run_cb_train_schema_prefetch_20261027.py'

PREPARE=r"""
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
"""


def main():
    source=BASE.read_text(encoding='utf-8-sig');tree=ast.parse(source)
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='prepare')
    lines=source.splitlines(keepends=True)
    source=''.join(lines[:node.lineno-1])+PREPARE+'\n'+''.join(lines[node.end_lineno:])
    source=source.replace('cb_posthoc_paired_review_v1_20261027','cb_train_schema_prefetch_v1_20261027')
    source=source.replace("REMOTE='/mnt/data/0/louzehua/ch7_cb_posthoc_review_v1_20261027'", "REMOTE='/mnt/data/0/louzehua/ch7_cb_train_schema_prefetch_v1_20261027'")
    source=source.replace('719','4096').replace('CB_POSTHOC_DIAGNOSTIC_NOT_METHOD_PREDICTION','CB_TRAIN_SCHEMA_ONLY_DEVELOPMENT_PREFETCH')
    source=source.replace('CB_POSTHOC_DIAGNOSTIC_NOT_METHOD_RESULTS','CB_TRAIN_SCHEMA_ONLY_PREFETCH_NOT_SCORES')
    source=source.replace('complete_reviews','complete_outputs').replace('review_annotations_not_human_certified=True','prefetched_outputs_not_scores=True')
    source=source.replace('All cases where either frozen final method failed, fixed before new replies.',
                          'Four TRAIN domains,512 fixed new positions per domain, two original method prompts.')
    source=source.replace('Original answers/scores are read only. Single review per case, no retry or DB.',
                          'Only TRAIN questions and public schemas are read. Initial outputs only; no retry, DB or scores.')
    source=source.replace('Bounded4096-case CypherBench posthoc paired-error review, NOT new method EX.',
                          'Bounded4096-request paired TRAIN schema-only development prefetch, NOT formal EX.')
    compile(source,str(TARGET),'exec')
    with TARGET.open('x',encoding='utf-8',newline='\n') as f:f.write(source)
    print(str(TARGET))


if __name__=='__main__':main()
