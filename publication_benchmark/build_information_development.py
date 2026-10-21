"""Prepare known development goals and independently calculate event references."""
import argparse
import json
from pathlib import Path

import run_baselines as b
import information_interface as i
from run_mechanism_pilot import bag


def build(task_root,data,output):
    root,out=Path(task_root),Path(output)
    if out.exists():
        raise FileExistsError('Do not replace development version')
    tasks=json.loads((root/'specifications.json').read_text(encoding='utf-8'))
    oracle=i.EventOracle(data)
    event_map={'issues_new':('Issue','OPENED_BY',False,'entity_count'),
               'issues_closed':('Issue','CLOSED_BY',False,'entity_count'),
               'pr_new':('PullRequest','OPENED_BY',False,'entity_ids'),
               'pr_accepted':('PullRequest','MERGED_BY',False,'entity_count'),
               'pr_declined':('PullRequest','CLOSED_BY',True,'entity_ids'),
               'pr_acceptance_ratio':('PullRequest',None,False,'entity_count_ratio')}
    for spec in tasks:
        if spec['family'] not in event_map:
            spec['information_expressible']=spec['service_expressible']
            continue
        label,action,decline,mode=event_map[spec['family']]
        old=spec['reference_parameters']
        events=([{'action':'MERGED_BY','explicit_decline':False},{'action':'CLOSED_BY','explicit_decline':True}]
                if mode=='entity_count_ratio' else [{'action':action,'explicit_decline':decline}])
        args={'entity_label':label,'entity_prefix':old['prefix'],'start_date':old['start'][:10],
              'end_date':old['end'][:10],'events':events,'mode':mode,'fields':[],
              'output_columns':spec['columns']}
        if bag(oracle.expected(args))!=bag(spec['expected_rows']):
            raise ValueError('Independent CSV oracle disagrees with prior reference: '+spec['id'])
        spec['reference_call']={'operation':'observed_events','arguments':args}
        spec['information_expressible']=True
    prior=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    b.save(out/'specifications.json',tasks)
    b.save(out/'shared_schema.json',json.loads((root/'shared_schema.json').read_text(encoding='utf-8')))
    b.save(out/'manifest.json',{'mode':'E6_KNOWN_DEVELOPMENT_NOT_INDEPENDENT','project':prior['project'],
        'source_hashes':prior['source_hashes'],'task_sha256':b.sha(out/'specifications.json'),
        'catalog':i.CATALOG,'oracle_sha256':b.sha(Path(i.__file__)),
        'limits':'Old E1/E3 developer-known goals plus two boundary checks. No independent effect estimate.'})
    print(prior['project'],len(tasks),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('task-root','data','output'):
        parser.add_argument('--'+key,required=True)
    args=parser.parse_args()
    build(args.task_root,args.data,args.output)
