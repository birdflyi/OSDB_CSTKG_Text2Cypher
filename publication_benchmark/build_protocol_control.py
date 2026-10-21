"""Predeclared development mix: six legacy, six scope and two boundary goals."""
import argparse
import json
from pathlib import Path

import run_baselines as b
from mechanism_core import compile_call


def build(private_root, project, output):
    private_root,out=Path(private_root),Path(output)
    if out.exists():
        raise FileExistsError('Use a new task version')
    prior=json.loads((private_root/'mechanism_start_v1_20261021'/(project+'_specifications.json')).read_text(encoding='utf-8'))
    scope_root=private_root/'e3_scope_tasks_v1_20261021'/project
    scope=json.loads((scope_root/'specifications.json').read_text(encoding='utf-8'))
    prior_manifest=json.loads((scope_root/'manifest.json').read_text(encoding='utf-8'))
    tasks,seen=[],set()
    for spec in prior:
        if spec['variant_index']!=0 or spec['family'] in seen:
            continue
        seen.add(spec['family'])
        compiled=compile_call(spec['reference_call'])
        tasks.append({**spec,'reference_query':compiled['query'],'reference_parameters':compiled['parameters'],
                      'service_expressible':True,'expected_catalog_status':'ANSWER'})
    if len(tasks)!=6:
        raise ValueError('Expected six legacy development families')
    tasks.extend({**spec,'expected_catalog_status':'UNSUPPORTED'} for spec in scope)
    for key,question,status,columns in [
        ('missing','Which domains are linked by the pull request? No pull request identifier or earlier context is available.','NEEDS_INPUT',['domain']),
        ('causality','Which commit actually resolves the issue? Establish a resolving relationship, not merely a reference; no issue identifier or prior context is available.','NEEDS_INPUT',['commit_id'])]:
        tasks.append({'id':project+'_E5_'+key,'project':project,'question':question,'family':key,
            'columns':columns,'expected_status':status,'expected_catalog_status':status,'expected_rows':[],
            'service_expressible':False,'provenance':'Developer-authored boundary diagnostic, not real user evidence'})
    # Use an anchored unsupported question, so missing anchor and unsupported
    # relation are separate, independently scoreable boundary cases.
    issue=next(x['reference_call']['arguments']['issue_id'] for x in tasks if x['family']=='issue_openers')
    tasks[-1].update(question=f'Which Commit actually RESOLVES Issue {issue}? Establish the actual resolving relationship, not merely a reference or link.',
                     expected_status='UNSUPPORTED',expected_catalog_status='UNSUPPORTED')
    b.save(out/'specifications.json',tasks)
    b.save(out/'shared_schema.json',json.loads((scope_root/'shared_schema.json').read_text(encoding='utf-8')))
    b.save(out/'manifest.json',{'mode':'E5_DEVELOPMENT_ONLY','project':project,'cases':len(tasks),
        'source_hashes':prior_manifest['input_hashes'],
        'task_sha256':b.sha(out/'specifications.json'),'builder_sha256':b.sha(__file__),
        'selection':'First canonical request in each of six existing E1 families; all six E3 scope goals; missing-anchor and unsupported-causality developer probes. Fixed before new predictions.'})
    print(project,len(tasks),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('private-root','project','output'):
        parser.add_argument('--'+key,required=True)
    args=parser.parse_args()
    build(args.private_root,args.project,args.output)
