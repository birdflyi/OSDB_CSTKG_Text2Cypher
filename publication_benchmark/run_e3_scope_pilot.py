"""Bounded E3 feasibility run. No catalog extension, no gold-aware repair."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess

import run_baselines as b
import run_strong_baselines as r
import strong_baselines as s


def run(a):
    root,out=Path(a.task_dir),Path(a.output)
    if out.exists():
        raise FileExistsError('Use a fresh experiment directory')
    repo=Path(__file__).resolve().parents[1]
    if subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip():
        raise ValueError('Freeze a clean implementation commit before predictions')
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
    m=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    specs=json.loads((root/'specifications.json').read_text(encoding='utf-8'))
    schema=json.loads((root/'shared_schema.json').read_text(encoding='utf-8'))
    ref=json.loads(Path(a.reference).read_text(encoding='utf-8'))
    receipt=json.loads(Path(a.receipt).read_text(encoding='utf-8'))
    cfg=json.loads(Path(a.config).read_text(encoding='utf-8'))
    if ref['correct']!=len(specs) or ref['specifications_sha256']!=b.sha(root/'specifications.json'):
        raise ValueError('Unverified reference')
    if m['input_hashes']!=receipt['source_hashes'] or any(b.sha(Path(a.data)/k)!=v for k,v in receipt['source_hashes'].items()):
        raise ValueError('Actual source changed')
    if any(b.sha(root/k)!=m[v] for k,v in [('specifications.json','specifications_sha256'),('shared_schema.json','shared_schema_sha256')]):
        raise ValueError('Frozen task/schema changed')
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data']!=receipt['database_identity']:
        raise ValueError('Wrong dedicated database')
    b.save(out/'manifest.json',{'mode':'E3_FEASIBILITY_NOT_FORMAL_TEST','git_commit':commit,'task_manifest':m,
            'reference_sha256':b.sha(a.reference),'database_identity':receipt['database_identity'],
            'max_dispatches':4*len(specs),'infra_retries':0,'model':{k:cfg[k] for k in ('model','temperature','top_p','reasoning_effort')},
            'shared_graph_schema':schema,'catalog':s.CATALOG,'prompts':s.PROMPTS,
            'code_hashes':{p.name:b.sha(p) for p in Path(__file__).parent.glob('*.py')},
            'limits':'Six source-motivated observed-event goals, selected for reference feasibility. A safe capability refusal is not task completion. Full-history metrics and final independent workload are not measured.'})
    b.save(out/'private_specifications.json',specs)
    for p in Path(__file__).parent.glob('*.py'):
        target=out/'source_snapshot'/p.name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(p,target)
    records,consecutive,stopped=[],0,False
    for index,spec in enumerate(specs):
        for representation in (('G','S') if index%2==0 else ('S','G')):
            data={**s.public_input(spec),'full_graph_metadata':schema}
            stage=representation+'_'+spec['id']
            reply=r.dispatch(out,stage+'_first',cfg,representation,data)
            consecutive=consecutive+1 if reply.get('failure')=='INFRASTRUCTURE' else 0
            first=r.observe(reply,representation)
            final,repair_reply=first,None
            if first['check'].get('repairable') and consecutive<3:
                repair_data={**data,'prior_output':reply.get('text'),'structural_feedback':first['check'].get('feedback'),
                             'instruction':'One repair allowed; preserve every condition of the original task.'}
                repair_reply=r.dispatch(out,stage+'_repair',cfg,representation,repair_data)
                consecutive=consecutive+1 if repair_reply.get('failure')=='INFRASTRUCTURE' else 0
                final=r.observe(repair_reply,representation)
            for suffix,obs,used,selected in [('0',first,False,reply),('1',final,repair_reply is not None,repair_reply or reply)]:
                prediction=selected.get('prediction')
                correct=r.score(obs,spec)
                record={'id':spec['id'],'arm':representation+suffix,'family':spec['family'],
                        'prediction':prediction,'observation':obs,'failure':selected.get('failure'),
                        'repair_used':used,'correct':correct,'graph_answerable':True,'service_expressible':False,
                        'safe_catalog_refusal':representation=='S' and prediction is not None and prediction['status']=='UNSUPPORTED',
                        'catalog_false_accept_decision':representation=='S' and prediction is not None and prediction['status']=='ANSWER',
                        'wrong_returned_result':obs['execution']['status']=='EXECUTED' and not correct}
                b.save(out/'records'/(record['arm']+'_'+spec['id']+'.json'),record)
                records.append(record)
            progress=r.summary(out,records,len(specs),False)
            progress['status']='RUNNING'
            (out/'progress.json').write_text(json.dumps(progress,indent=2),encoding='utf-8')
            print(f'{m["project"]} {index+1}/{len(specs)} {representation}: {final["execution"]["status"]}',flush=True)
            if consecutive>=3:
                stopped=True
                break
        if stopped:
            break
    result=r.summary(out,records,len(specs),stopped)
    result['mode']='E3_FEASIBILITY_NOT_FORMAL_TEST'
    for arm,values in result['arm_scores'].items():
        rr=[x for x in records if x['arm']==arm]
        values['safe_catalog_refusals']=sum(x['safe_catalog_refusal'] for x in rr)
        values['catalog_false_accept_decisions']=sum(x['catalog_false_accept_decision'] for x in rr)
        values['wrong_returned_results']=sum(x['wrong_returned_result'] for x in rr)
    b.save(out/'summary.json',result)
    print(json.dumps(result,indent=2),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for k in ('task-dir','reference','receipt','data','config','output'):
        p.add_argument('--'+k,required=True)
    p.add_argument('--port',type=int,choices=(17475,17476),required=True)
    a=p.parse_args()
    b.DB_URL=f'http://127.0.0.1:{a.port}/db/neo4j/tx/commit'
    run(a)
