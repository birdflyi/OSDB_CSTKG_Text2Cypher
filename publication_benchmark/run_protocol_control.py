"""Bounded paired development control. No correct-answer-aware retries.

Raw private files are immutable. Resuming reuses recorded requests, never
resends an ambiguous pending request, and checks the source/graph identity.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess

import run_baselines as b
import run_strong_baselines as r
import strong_baselines as s
from run_mechanism_pilot import bag
import protocol_control as p


def reserve(budget_dir, request_path, maximum=1200):
    directory = Path(budget_dir)
    directory.mkdir(parents=True, exist_ok=True)
    for number in range(1, maximum + 1):
        try:
            with (directory / f'{number:04d}.json').open('x', encoding='utf-8') as f:
                json.dump({'request_path': str(request_path.resolve()), 'pid': os.getpid()}, f)
            return number
        except FileExistsError:
            continue
    raise RuntimeError('CONTINUATION_CALL_BUDGET_EXHAUSTED')


def dispatch(out, stage, cfg, arm, data, budget):
    req = out / 'calls' / stage / 'request.json'
    if not req.exists():
        reserve(budget, req)
    old_prompt = s.PROMPTS['S']
    try:
        s.PROMPTS['S'] = p.prompt(arm)
        return r.dispatch(out, stage, cfg, 'S', data)
    finally:
        s.PROMPTS['S'] = old_prompt


def grade(spec, reply, observation):
    prediction = reply.get('prediction') or {}
    status = prediction.get('status')
    executed = observation['execution']['status'] == 'EXECUTED'
    correct = (r.score(observation, spec) if spec['expected_status'] == 'ANSWER'
               else status == spec['expected_status'] and not executed)
    return {'correct_task_completion_or_boundary': correct,
            'catalog_decision_correct': status == spec['expected_catalog_status'],
            'safe_catalog_refusal': status == 'UNSUPPORTED' and not spec['service_expressible'],
            'wrong_returned_answer': executed and not correct,
            'answer_decision_outside_catalog': status == 'ANSWER' and not spec['service_expressible']}


def summarize(out, records, total, stopped=False):
    arms = {}
    for arm in ('S0', 'S1', 'P0', 'P1'):
        rows = [x for x in records if x['arm'] == arm]
        supported = [x for x in rows if x['service_expressible']]
        outside = [x for x in rows if not x['service_expressible'] and x['expected_status'] == 'ANSWER']
        arms[arm] = {'planned': total, 'evaluated': len(rows),
            'supported_cases': len(supported),
            'supported_correct': sum(x['grade']['correct_task_completion_or_boundary'] for x in supported),
            'catalog_decisions_correct': sum(x['grade']['catalog_decision_correct'] for x in rows),
            'graph_answerable_catalog_outside_cases': len(outside),
            'outside_safe_refusals': sum(x['grade']['safe_catalog_refusal'] for x in outside),
            'outside_answer_decisions': sum(x['grade']['answer_decision_outside_catalog'] for x in outside),
            'wrong_returned_answers': sum(x['grade']['wrong_returned_answer'] for x in rows),
            'infrastructure_failures': sum(x['failure'] == 'INFRASTRUCTURE' for x in rows),
            'protocol_failures': sum(x['failure'] == 'PROTOCOL_ERROR' for x in rows),
            'repairs': sum(x['repair_used'] for x in rows)}
    terminals = [json.loads(f.read_text(encoding='utf-8')) for f in (out/'calls').glob('*/terminal.json')]
    return {'mode': 'E5_PROTOCOL_ONLY_DEVELOPMENT_CONTROL',
            'status': 'STOPPED_INFRASTRUCTURE' if stopped else
                ('COMPLETED' if all(x['evaluated'] == total for x in arms.values()) else 'RUNNING'),
            'arm_scores': arms, 'actual_calls': len(terminals),
            'known_reported_tokens': sum((x.get('body', {}).get('usage') or {}).get('total_tokens', 0) for x in terminals),
            'unknown_usage_calls': sum(not x.get('body', {}).get('usage') for x in terminals),
            'dispatch_seconds': sum(x['seconds'] for x in terminals),
            'limits': 'Developer-known tasks, one model, no service capability change. Refusal is not completing an answerable task. Shared first-pass views are paired, not independent calls.'}


def run(a):
    out = Path(a.output)
    repo = Path(__file__).resolve().parents[1]
    if subprocess.check_output(['git','status','--porcelain'], cwd=repo, text=True).strip():
        raise ValueError('Commit implementation before predictions')
    commit = subprocess.check_output(['git','rev-parse','HEAD'], cwd=repo, text=True).strip()
    cfg = json.loads(Path(a.config).read_text(encoding='utf-8'))
    root = Path(a.task_dir)
    tasks = json.loads((root/'specifications.json').read_text(encoding='utf-8'))
    schema = json.loads((root/'shared_schema.json').read_text(encoding='utf-8'))
    manifest = json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    receipt = json.loads(Path(a.receipt).read_text(encoding='utf-8'))
    if manifest['source_hashes'] != receipt['source_hashes']:
        raise ValueError('Task source differs from imported graph')
    if any(b.sha(Path(a.data)/name) != digest for name,digest in receipt['source_hashes'].items()):
        raise ValueError('Actual CSV source changed')
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data'] != receipt['database_identity']:
        raise ValueError('Wrong graph identity')
    verified = []
    for spec in tasks:
        if spec['expected_status'] != 'ANSWER':
            continue
        body, seconds, nbytes = s.transaction(spec['reference_query'], spec['reference_parameters'])
        if body.get('errors'):
            raise ValueError(body['errors'])
        result = body['results'][0]
        if result['columns'] != spec['columns'] or bag([x['row'] for x in result['data']]) != bag(spec['expected_rows']):
            raise ValueError('Reference mismatch: '+spec['id'])
        verified.append(spec['id'])
    r.store(out/'manifest.json', {'mode':'E5_PROTOCOL_ONLY_DEVELOPMENT_CONTROL','git_commit':commit,
        'task_sha256': b.sha(root/'specifications.json'), 'schema_sha256':b.sha(root/'shared_schema.json'),
        'source_hashes':receipt['source_hashes'],'database_identity':receipt['database_identity'],
        'reference_verified_ids':verified, 'catalog':s.CATALOG,
        'compiler_sha256':b.sha(Path(__file__).with_name('mechanism_core.py')),
        'prompts':{arm:p.prompt(arm) for arm in ('S','P')},
        'model':{k:cfg[k] for k in ('model','temperature','top_p','reasoning_effort')},
        'max_calls':4*len(tasks),'repair_limit':1,'infra_retries':0,'consecutive_infra_stop':3})
    b.save(out/'active_process.json',{'pid':os.getpid(),'commit':commit})
    records, consecutive, stopped = [], 0, False
    for index,spec in enumerate(tasks):
        for arm in (('S','P') if index%2==0 else ('P','S')):
            data={**s.public_input(spec),'full_graph_metadata':schema}
            reply=dispatch(out,arm+'_'+spec['id']+'_first',cfg,arm,data,a.budget_dir)
            consecutive=consecutive+1 if reply.get('failure')=='INFRASTRUCTURE' else 0
            first=r.observe(reply,'S')
            repair=None
            final=first
            if first['check'].get('repairable') and consecutive<3:
                feedback={**data,'prior_output':reply.get('text'),
                    'structural_feedback':first['check'].get('feedback'),
                    'instruction':'One structural/protocol repair allowed. Preserve every original condition; never answer a nearby task.'}
                repair=dispatch(out,arm+'_'+spec['id']+'_repair',cfg,arm,feedback,a.budget_dir)
                consecutive=consecutive+1 if repair.get('failure')=='INFRASTRUCTURE' else 0
                final=r.observe(repair,'S')
            for suffix,obs,selected,used in [('0',first,reply,False),('1',final,repair or reply,repair is not None)]:
                record={'id':spec['id'],'project':a.project,'family':spec['family'],
                    'arm':arm+suffix,'expected_status':spec['expected_status'],
                    'service_expressible':spec['service_expressible'],'prediction':selected.get('prediction'),
                    'failure':selected.get('failure'),'repair_used':used,'observation':obs,
                    'grade':grade(spec,selected,obs)}
                r.store(out/'records'/(arm+suffix+'_'+spec['id']+'.json'),record)
                records.append(record)
            progress=summarize(out,records,len(tasks))
            progress['status']='RUNNING'
            b.save(out/'progress.json',progress)
            print(a.project,index+1,len(tasks),arm,final['execution']['status'],flush=True)
            if consecutive>=3:
                stopped=True
                break
        if stopped:
            break
    result=summarize(out,records,len(tasks),stopped)
    r.store(out/'summary.json',result)
    b.save(out/'active_process.json',{'pid':None,'status':result['status']})
    print(json.dumps(result,indent=2),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('task-dir','project','data','receipt','config','output','budget-dir'):
        parser.add_argument('--'+key,required=True)
    parser.add_argument('--port',type=int,choices=(17475,17476),required=True)
    args=parser.parse_args()
    b.DB_URL=f'http://127.0.0.1:{args.port}/db/neo4j/tx/commit'
    run(args)
