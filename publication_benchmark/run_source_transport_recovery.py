"""One explicit recovery of Redis source v1, preserving every usable response.

No compiler/prompt/scoring change. Do not use for quality-based rerolls.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time
from types import SimpleNamespace


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def infrastructure(terminal):
    status=terminal.get('http_status')
    return 'transport_error' in terminal or status==429 or (status is not None and status>=500)


def source_calls(source):
    reusable=[];failed=[]
    for folder in sorted((Path(source)/'calls').iterdir()):
        request,terminal=folder/'request.json',folder/'terminal.json'
        if not request.exists() or not terminal.exists():
            raise RuntimeError('Ambiguous pending attempt requires reconciliation')
        (failed if infrastructure(load(terminal)) else reusable).append(folder)
    return reusable,failed


def run(a):
    source,out,engine=map(lambda x:Path(x).resolve(),(a.source_run,a.output,a.engine_root))
    if source.name!='e7_source_redis_v1' or out.name!='e7_source_redis_v2':
        raise ValueError('Only the single declared partial-source recovery is authorized here')
    if out.exists():raise FileExistsError('No duplicate recovery version')
    original=load(source/'manifest.json')
    summary=load(source/'summary.json')
    if summary['status']!='STOPPED_INFRASTRUCTURE' or summary['actual_calls']!=8:
        raise ValueError('Unexpected original batch')
    task=Path(a.task_dir)
    if original['task_sha256']!=sha(task/'specifications.json') or original['schema_sha256']!=sha(task/'shared_schema.json'):
        raise ValueError('Task or schema changed')
    if original['context_variant']!='compact':raise ValueError('Wrong context variant')
    cfg=load(a.config)
    if original['model']!={k:cfg[k] for k in original['model']}:
        raise ValueError('Model settings changed; not transport-only recovery')
    for frozen in (source/'source_snapshot').glob('*.py'):
        current=engine/'publication_benchmark'/frozen.name
        if not current.exists() or sha(current)!=sha(frozen):
            raise ValueError('Frozen engine source differs: '+frozen.name)
    probe=Path(a.probe_terminal)
    terminal=load(probe)
    choices=terminal.get('body',{}).get('choices') or []
    if terminal.get('http_status')!=200 or not choices or choices[0].get('finish_reason')!='stop':
        raise ValueError('A recorded successful connectivity probe is required')
    if not 0<=time.time()-probe.stat().st_mtime<=3600:
        raise ValueError('Connectivity probe is stale')
    reusable,failed=source_calls(source)
    if len(reusable)!=3 or len(failed)!=5:
        raise ValueError('Unexpected cache/failure flow')
    out.mkdir(parents=True)
    copied=[]
    for folder in reusable:
        dest=out/'calls'/folder.name
        dest.mkdir(parents=True)
        for name in ('request.json','terminal.json'):
            shutil.copyfile(folder/name,dest/name)
        copied.append({'stage':folder.name,'request_sha256':sha(dest/'request.json'),
            'terminal_sha256':sha(dest/'terminal.json'),'source':str(folder)})
    def store(name,data):
        with (out/name).open('x',encoding='utf-8') as f:json.dump(data,f,indent=2)
    store('transport_recovery_plan.json',{'source_run':str(source),'original_manifest_sha256':sha(source/'manifest.json'),
        'probe_terminal_sha256':sha(probe),'copied_calls':copied,
        'original_infrastructure_stages':[x.name for x in failed],
        'wrapper_sha256':sha(__file__),
        'limits':'One transport recovery; original failures remain charged. All non-infrastructure responses reused regardless of validity/correctness. No scoring-aware selection. Three consecutive new infrastructure failures stop the batch.'})
    sys.path.insert(0,str(engine/'publication_benchmark'))
    import run_baselines as b
    import run_information_comparison as comparison
    original_dispatch=comparison.dispatch
    def checked_dispatch(output,stage,config,arm,data,budget):
        previous=source/'calls'/stage/'request.json'
        if previous.exists():
            prompt=comparison.s.PROMPTS['G'] if arm=='G' else comparison.p.PROTOCOL_PROMPT
            expected={k:config[k] for k in ('model','temperature','top_p','reasoning_effort')}
            expected.update(max_tokens=6000,messages=[{'role':'system','content':prompt},
                {'role':'user','content':json.dumps(data,ensure_ascii=False)}])
            if load(previous)!=expected:raise ValueError('Recovery would change request payload')
        return original_dispatch(output,stage,config,arm,data,budget)
    before={p.name for p in Path(a.budget_dir).glob('*.json')}
    comparison.dispatch=checked_dispatch
    b.DB_URL='http://127.0.0.1:17475/db/neo4j/tx/commit'
    try:
        comparison.run(SimpleNamespace(task_dir=str(task),project='redis',data=a.data,receipt=a.receipt,
            config=a.config,output=str(out),budget_dir=a.budget_dir,port=17475,context='compact'))
    finally:
        comparison.dispatch=original_dispatch
        after={p.name for p in Path(a.budget_dir).glob('*.json')}
        store('transport_recovery_dispatch_receipt.json',{'new_budget_slots':sorted(after-before),
            'new_attempts':len(after-before),'cached_attempts_not_charged_again':len(copied),
            'warning':'summary actual_calls includes cached calls. Ledger slots are authoritative for new/all-attempt accounting; original failed attempts remain in source v1.'})


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for k in ('engine-root','source-run','task-dir','data','receipt','config','output','budget-dir','probe-terminal'):
        p.add_argument('--'+k,required=True)
    run(p.parse_args())
