"""G strong direct vs P protocol-only vs I minimal event-information extension.

Same output/repair protocol for P/I; old compiler untouched. Common graph and
event semantics. This runner also supports separately frozen independent sets.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

import run_baselines as b
import run_strong_baselines as r
import strong_baselines as s
import protocol_control as p
import run_protocol_control as control
import information_interface as info
from run_mechanism_pilot import bag


def dispatch(out,stage,cfg,arm,data,budget):
    if not (out/'calls'/stage/'request.json').exists():
        control.reserve(budget,out/'calls'/stage/'request.json')
    representation='G' if arm=='G' else 'S'
    original=s.PROMPTS['S']
    try:
        if arm!='G':
            s.PROMPTS['S']=p.PROTOCOL_PROMPT
        return r.dispatch(out,stage,cfg,representation,data)
    finally:
        s.PROMPTS['S']=original


def observe(reply,arm):
    original=s.compile_call
    try:
        if arm=='I':
            s.compile_call=info.compile_call
        return r.observe(reply,'G' if arm=='G' else 'S')
    finally:
        s.compile_call=original


def public_input(spec,schema,arm):
    data=s.public_input(spec)
    # Definitions are the same for all arms; only executable catalog differs.
    data.update(full_graph_metadata=schema,observed_event_definitions=info.EVENT_CONTRACT['meaning'])
    if arm in ('G','I'):
        data['contracts']=info.CATALOG
    return data


def grade(spec,reply,obs,arm):
    prediction=reply.get('prediction') or {}
    correct=(r.score(obs,spec) if spec['expected_status']=='ANSWER' else
             prediction.get('status')==spec['expected_status'] and obs['execution']['status']!='EXECUTED')
    expressible=(spec.get('information_expressible',True) if arm=='I' else
                 spec['service_expressible'] if arm=='P' else spec['expected_status']=='ANSWER')
    return {'correct':correct,'executed':obs['execution']['status']=='EXECUTED',
            'wrong_returned_answer':obs['execution']['status']=='EXECUTED' and not correct,
            'safe_catalog_refusal':prediction.get('status')=='UNSUPPORTED' and not expressible,
            'expressible':expressible,'status':prediction.get('status')}


def summary(out,records,total,stopped=False):
    scores={}
    for arm in ('G0','G1','P0','P1','I0','I1'):
        rr=[x for x in records if x['arm']==arm]
        answerable=[x for x in rr if x['expected_status']=='ANSWER']
        scores[arm]={'planned':total,'evaluated':len(rr),
            'correct':sum(x['grade']['correct'] for x in rr),
            'answerable_cases':len(answerable),
            'answerable_correct':sum(x['grade']['correct'] for x in answerable),
            'answerable_executed':sum(x['grade']['executed'] for x in answerable),
            'wrong_returned_answers':sum(x['grade']['wrong_returned_answer'] for x in rr),
            'safe_catalog_refusals':sum(x['grade']['safe_catalog_refusal'] for x in rr),
            'infrastructure_failures':sum(x['failure']=='INFRASTRUCTURE' for x in rr),
            'repairs':sum(x['repair_used'] for x in rr)}
    terminals=[json.loads(f.read_text(encoding='utf-8')) for f in (out/'calls').glob('*/terminal.json')]
    return {'status':'STOPPED_INFRASTRUCTURE' if stopped else
                ('COMPLETED' if all(x['evaluated']==total for x in scores.values()) else 'RUNNING'),
        'arm_scores':scores,'actual_calls':len(terminals),
        'known_reported_tokens':sum((x.get('body',{}).get('usage') or {}).get('total_tokens',0) for x in terminals),
        'unknown_usage_calls':sum(not x.get('body',{}).get('usage') for x in terminals),
        'dispatch_seconds':sum(x['seconds'] for x in terminals),
        'limits':'One provider model. Correct refusal is separate from completing graph-answerable tasks. G/P/I first/repaired views are paired. A capability extension alone is not a novel mechanism or evidence of superiority.'}


def verify(tasks,receipt):
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data']!=receipt['database_identity']:
        raise ValueError('Wrong dedicated database')
    records=[]
    for spec in tasks:
        if spec['expected_status']!='ANSWER':
            continue
        queries=[('independent',{'query':spec['reference_query'],'parameters':spec['reference_parameters'],'columns':spec['columns']})]
        if spec.get('reference_call'):
            queries.append(('service',info.compile_call(spec['reference_call'])))
        for kind,compiled in queries:
            body,seconds,nbytes=s.transaction(compiled['query'],compiled['parameters'])
            if body.get('errors'):
                raise ValueError(body['errors'])
            result=body['results'][0]
            rows=[x['row'] for x in result['data']]
            correct=result['columns']==spec['columns'] and bag(rows)==bag(spec['expected_rows'])
            records.append({'id':spec['id'],'kind':kind,'correct':correct,'seconds':seconds,'bytes':nbytes})
            if not correct:
                raise ValueError('Reference mismatch: '+spec['id']+' '+kind)
    return records


def run(a):
    out,root=Path(a.output),Path(a.task_dir)
    repo=Path(__file__).resolve().parents[1]
    if subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip():
        raise ValueError('Freeze clean implementation before predictions')
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
    tasks=json.loads((root/'specifications.json').read_text(encoding='utf-8'))
    schema=json.loads((root/'shared_schema.json').read_text(encoding='utf-8'))
    task_manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    receipt=json.loads(Path(a.receipt).read_text(encoding='utf-8'))
    cfg=json.loads(Path(a.config).read_text(encoding='utf-8'))
    if task_manifest['source_hashes']!=receipt['source_hashes'] or any(b.sha(Path(a.data)/name)!=digest for name,digest in receipt['source_hashes'].items()):
        raise ValueError('Source hash mismatch')
    refs=verify(tasks,receipt)
    mode=task_manifest['mode']
    r.store(out/'manifest.json',{'mode':mode,'git_commit':commit,'task_sha256':b.sha(root/'specifications.json'),
        'schema_sha256':b.sha(root/'shared_schema.json'),'source_hashes':receipt['source_hashes'],
        'database_identity':receipt['database_identity'],
        'reference_verified':[{k:x[k] for k in ('id','kind','correct')} for x in refs],
        'legacy_compiler_sha256':b.sha(Path(__file__).with_name('mechanism_core.py')),
        'information_compiler_sha256':b.sha(Path(info.__file__)),
        'prompts':{'G':s.PROMPTS['G'],'P':p.PROTOCOL_PROMPT,'I':p.PROTOCOL_PROMPT},
        'catalogs':{'P':s.CATALOG,'G':info.CATALOG,'I':info.CATALOG},
        'model':{k:cfg[k] for k in ('model','temperature','top_p','reasoning_effort')},
        'repair_limit':1,'infra_retries':0,'max_calls':6*len(tasks),'consecutive_infra_stop':3})
    for source in Path(__file__).parent.glob('*.py'):
        target=out/'source_snapshot'/source.name
        if not target.exists():
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(source,target)
    control.derived(out/'active_process.json',{'pid':os.getpid(),'commit':commit})
    records,consecutive,stopped=[],0,False
    orders=(('G','P','I'),('P','I','G'),('I','G','P'))
    for index,spec in enumerate(tasks):
        for arm in orders[index%3]:
            data=public_input(spec,schema,arm)
            stage=arm+'_'+spec['id']
            first_reply=dispatch(out,stage+'_first',cfg,arm,data,a.budget_dir)
            consecutive=consecutive+1 if first_reply.get('failure')=='INFRASTRUCTURE' else 0
            first=observe(first_reply,arm)
            final,repair=first,None
            if first['check'].get('repairable') and consecutive<3:
                feedback={**data,'prior_output':first_reply.get('text'),
                    'structural_feedback':first['check'].get('feedback'),
                    'instruction':'One structural/protocol/execution repair allowed. Preserve every original task condition.'}
                repair=dispatch(out,stage+'_repair',cfg,arm,feedback,a.budget_dir)
                consecutive=consecutive+1 if repair.get('failure')=='INFRASTRUCTURE' else 0
                final=observe(repair,arm)
            for suffix,obs,reply,used in [('0',first,first_reply,False),('1',final,repair or first_reply,repair is not None)]:
                rec={'id':spec['id'],'project':a.project,'family':spec['family'],'group':spec.get('group',spec['family']),
                    'variant_index':spec.get('variant_index',0),'arm':arm+suffix,'expected_status':spec['expected_status'],
                    'prediction':reply.get('prediction'),'failure':reply.get('failure'),'repair_used':used,
                    'observation':obs,'grade':grade(spec,reply,obs,arm)}
                r.store(out/'records'/(arm+suffix+'_'+spec['id']+'.json'),rec)
                records.append(rec)
            result=summary(out,records,len(tasks))
            result['status']='RUNNING'
            control.derived(out/'progress.json',result)
            print(a.project,index+1,len(tasks),arm,final['execution']['status'],flush=True)
            if consecutive>=3:
                stopped=True
                break
        if stopped:
            break
    result=summary(out,records,len(tasks),stopped)
    result['mode']=mode
    r.store(out/'summary.json',result)
    control.derived(out/'active_process.json',{'pid':None,'status':result['status']})
    print(json.dumps(result,indent=2),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('task-dir','project','data','receipt','config','output','budget-dir'):
        parser.add_argument('--'+key,required=True)
    parser.add_argument('--port',type=int,choices=(17475,17476),required=True)
    args=parser.parse_args()
    b.DB_URL=f'http://127.0.0.1:{args.port}/db/neo4j/tx/commit'
    run(args)
