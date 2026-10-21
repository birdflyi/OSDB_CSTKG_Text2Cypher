"""Same existing goal definitions on preselected projects, not new tasks."""
import argparse
import json
from pathlib import Path
import sys


def build(a):
    sys.path.insert(0,str(Path(a.engine_root)/'publication_benchmark'))
    import run_baselines as b
    import mechanism_core as core
    import e3_scope_pilot as scope
    import information_interface as info
    from run_mechanism_pilot import bag
    out=Path(a.output)
    if out.exists():raise FileExistsError('Immutable transfer preparation')
    schema_root=Path(a.source_tasks)
    prior=json.loads((schema_root/'manifest.json').read_text(encoding='utf-8'))
    assert prior['project']==a.project
    assert prior['source_hashes']=={name:b.sha(Path(a.data)/name) for name in ('nodes.csv','edges.csv')}
    repo=prior['repo_id']
    nodes,edges,_=b.load_graph(Path(a.data))
    candidates=core.dataset(a.project,core.Oracle(nodes,edges))
    tasks=[];seen=set()
    for spec in candidates:
        if spec['variant_index']!=0 or spec['family'] in seen:continue
        seen.add(spec['family'])
        q=core.compile_call(spec['reference_call'])
        tasks.append({**spec,'reference_query':q['query'],'reference_parameters':q['parameters'],
            'service_expressible':True,'information_expressible':True})
    assert len(tasks)==6
    event_rows=scope.references(a.data,repo)
    oracle=info.EventOracle(a.data)
    event_map={'issues_new':('Issue','OPENED_BY',False,'entity_count'),
        'issues_closed':('Issue','CLOSED_BY',False,'entity_count'),
        'pr_new':('PullRequest','OPENED_BY',False,'entity_ids'),
        'pr_accepted':('PullRequest','MERGED_BY',False,'entity_count'),
        'pr_declined':('PullRequest','CLOSED_BY',True,'entity_ids'),
        'pr_acceptance_ratio':('PullRequest',None,False,'entity_count_ratio')}
    for family,_,instruction,columns in scope.TASKS:
        label,action,decline,mode=event_map[family]
        prefix=('I_' if label=='Issue' else 'PR_')+repo+'#'
        events=([{'action':'MERGED_BY','explicit_decline':False},{'action':'CLOSED_BY','explicit_decline':True}]
                if mode=='entity_count_ratio' else [{'action':action,'explicit_decline':decline}])
        args={'entity_label':label,'entity_prefix':prefix,'start_date':'2023-07-01','end_date':'2023-08-01',
            'events':events,'mode':mode,'fields':[],'output_columns':columns}
        assert bag(oracle.expected(args))==bag(event_rows[family])
        tasks.append({'id':a.project+'_TRANSFER_'+family,'project':a.project,'family':family,
            'question':'Use only the supplied graph snapshot. Scope '+label+' entity_id by the literal prefix '+prefix+'. The UTC period is [2023-07-01,2023-08-01). '+instruction+' Return the complete result, not a sample. These are observed snapshot facts, not total historical activity.',
            'columns':columns,'expected_status':'ANSWER','expected_rows':event_rows[family],
            'service_expressible':False,'information_expressible':True,
            'reference_query':scope.query(family),'reference_parameters':{'prefix':prefix,'start':'2023-07-01T00:00:00Z','end':'2023-08-01T00:00:00Z'},
            'reference_call':{'operation':'observed_events','arguments':args}})
    issue=next(x['reference_call']['arguments']['issue_id'] for x in tasks if x['family']=='issue_openers')
    for key,text,status,columns in [
        ('missing','Which domains are linked by the pull request? No pull request identifier or earlier context is available.','NEEDS_INPUT',['domain']),
        ('causality',f'Which Commit actually RESOLVES Issue {issue}? Establish the actual resolving relationship, not merely a reference or link.','UNSUPPORTED',['commit_id'])]:
        tasks.append({'id':a.project+'_TRANSFER_'+key,'project':a.project,'family':key,'question':text,
            'columns':columns,'expected_status':status,'expected_rows':[],
            'service_expressible':False,'information_expressible':False})
    for spec in tasks:
        spec.update(group='known:'+spec['family'],semantic_goal='known:'+spec['family'],variant_index=0,
            stratum='known_goal_project_transfer_not_independent_task',
            output_contract_eligible=len(spec['expected_rows'])<=500,
            provenance='Existing developer goal and wording policy rebound on a preselected project. No new independent semantic goal or user study.')
    b.save(out/'all_prepared_specifications.json',tasks)
    selected=[s for s in tasks if s['output_contract_eligible']]
    b.save(out/'specifications.json',selected)
    b.save(out/'shared_schema.json',json.loads((schema_root/'shared_schema.json').read_text(encoding='utf-8')))
    b.save(out/'manifest.json',{'mode':'E8_KNOWN_GOAL_PROJECT_TRANSFER_NOT_TASK_GENERALIZATION',
        'project':a.project,'repo_id':repo,'source_hashes':prior['source_hashes'],
        'prepared':len(tasks),'eligible':len(selected),'excluded_output_budget':[s['family'] for s in tasks if not s['output_contract_eligible']],
        'task_sha256':b.sha(out/'specifications.json'),'all_prepared_sha256':b.sha(out/'all_prepared_specifications.json'),
        'builder_sha256':b.sha(__file__),'limits':'Known developer goal definitions. No independent task count. All14 retained in intake; over500-row outputs excluded without replacement before prediction.'})
    print(json.dumps({'project':a.project,'prepared':len(tasks),'eligible':len(selected),'rows':{s['family']:len(s['expected_rows']) for s in tasks}}),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for k in ('engine-root','source-tasks','data','project','output'):p.add_argument('--'+k,required=True)
    build(p.parse_args())
