"""Offline status/cost audit. Never calls models, databases or changes frozen files."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import random
import sys

GRAPHS={'flight_accident':189,'nba':270,'fictional_character':385,'company':347,
        'geography':366,'movie':401,'politics':390}


def paired_family_interval(rows, groups, counts, repetitions=10000, seed=20261024):
    by_graph=defaultdict(lambda:defaultdict(lambda:[0,0]))
    for qid,graph,difference in rows:
        family=groups[qid]
        by_graph[graph][family][0]+=difference
        by_graph[graph][family][1]+=1
    if {g:sum(v[1] for v in families.values()) for g,families in by_graph.items()}!=counts:
        raise ValueError('No interval for incomplete/nonmatching task coverage')
    rng=random.Random(seed);draws=[];total=sum(counts.values())
    ordered=[(graph,[families[key] for key in sorted(families)]) for graph,families in sorted(by_graph.items())]
    for _ in range(repetitions):
        delta=0.0
        for graph,families in ordered:
            sample=rng.choices(families,k=len(families))
            delta+=(sum(r[0] for r in sample)/sum(r[1] for r in sample))*counts[graph]/total
        draws.append(delta)
    draws.sort()
    def percentile(p):
        index=(len(draws)-1)*p;lo=int(index);hi=min(lo+1,len(draws)-1)
        return draws[lo]+(draws[hi]-draws[lo])*(index-lo)
    return {'lower_95':percentile(.025),'upper_95':percentile(.975),'replicates':repetitions,
            'seed':seed,'unit':'paired within-graph template families; fixed original graph weights'}


def audit(args):
    run=args.run;data=args.data
    runtime=[json.loads(s) for s in (data/'runtime/test.jsonl').read_text(encoding='utf-8').splitlines() if s.strip()]
    expected={(r['qid'],a) for r in runtime for a in ['direct','ours']}
    files=list(run.glob('*/tasks/*/*/record.json'));records=[];partial=[]
    for p in files:
        try:records.append(json.loads(p.read_text(encoding='utf-8')))
        except ValueError:partial.append(str(p))
    ids=[(r['qid'],r['arm']) for r in records]
    if len(ids)!=len(set(ids)) or not set(ids)<=expected:
        raise ValueError('Duplicate or non-frozen prediction identity')
    attempts=[]
    for p in run.glob('*/segments/*/model_calls/physical_*/terminal.json'):
        x=json.loads(p.read_text(encoding='utf-8'));attempts.append((p,x))
    slot_ids=[x['budget_slot'] for _,x in attempts]
    if len(slot_ids)!=len(set(slot_ids)):
        raise ValueError('Repeated physical model attempt')
    known_tokens=Counter();unknown_usage=0;http=Counter();transport_errors=Counter()
    for _,x in attempts:
        http[str(x.get('http_status'))]+=1
        if x.get('transport_error'):transport_errors[x['transport_error']]+=1
        body=x.get('body')
        usage=body.get('usage') if isinstance(body,dict) else None
        if isinstance(usage,dict):
            for k in ['prompt_tokens','completion_tokens','total_tokens']:
                if type(usage.get(k)) is int:known_tokens[k]+=usage[k]
        else:unknown_usage+=1
    traces=list(run.glob('*/tasks/*/*/query_trace/*/*/result.json'))
    db_results=[json.loads(p.read_text(encoding='utf-8'))['result'] for p in traces]
    missing_db_bytes=sum(type(r.get('result_bytes')) is not int for r in db_results)
    unknown_db=list(run.glob('*/tasks/*/*/query_trace/*/*/transport_error.json'))
    statuses=Counter((r['arm'],r['views'][-1]['outcome']['status']) for r in records)
    cells={};paired=[]
    for graph,n in GRAPHS.items():
        score=run/graph/'scores'/(graph+'.json')
        if not score.exists():continue
        rows=json.loads(score.read_text(encoding='utf-8'))
        if len(rows)!=2*n or len({(r['qid'],r['arm']) for r in rows})!=2*n:
            raise ValueError('Cannot report an incomplete graph score')
        methods={}
        for arm in ['direct','ours']:
            subset=[r for r in rows if r['arm']==arm]
            for name,view in ([('Direct',0),('Direct+feedback',-1)] if arm=='direct' else [('Ours',-1)]):
                correct=sum(r['views'][view]['score']['ex'] for r in subset)
                returned=sum(r['views'][view]['score']['returned_answer'] for r in subset)
                methods[name]={'n':n,'correct':correct,'ex_percent':100*correct/n,
                               'coverage_percent':100*returned/n,'wrong_answer_percent':100*(returned-correct)/n}
            if arm=='ours':
                correct=sum(r['fixed_plan_full_detail_score']['ex'] for r in subset)
                returned=sum(r['fixed_plan_full_detail_score']['returned_answer'] for r in subset)
                methods['Full-detail(shared-plan)']={'n':n,'correct':correct,'ex_percent':100*correct/n,
                    'coverage_percent':100*returned/n,'wrong_answer_percent':100*(returned-correct)/n}
        by_qid=defaultdict(dict)
        for row in rows:by_qid[row['qid']][row['arm']]=row['views'][-1]['score']['ex']
        paired += [(qid,graph,arms['ours']-arms['direct']) for qid,arms in by_qid.items()]
        cells[graph]=methods
    result={'kind':'OFFLINE_FORMAL_CORE_AUDIT','planned_primary_paths':4696,'terminal_primary_paths':len(records),
            'dispatched_primary_paths':len(list(run.glob('*/tasks/*/*/intent.json'))),
            'nonparseable_record_files':partial,'final_status_counts':{a+':'+s:n for (a,s),n in statuses.items()},
            'physical_model_attempts':len(attempts),'known_token_sums':dict(known_tokens),
            'attempts_with_unknown_usage':unknown_usage,'http_counts':dict(http),'transport_errors':dict(transport_errors),
            'database_result_traces':len(traces),'known_row_payload_bytes':sum(r['result_bytes'] for r in db_results if type(r.get('result_bytes')) is int),
            'database_results_without_byte_measure':missing_db_bytes,'database_attempts_with_unknown_completion':len(unknown_db),
            'records_with_unknown_primary_wrapper_clock':sum(r.get('model_and_execution_wall_seconds') is None for r in records),
            'graph_scores':cells,'full2348_score_ready':len(cells)==7,'entire_T1_complete':False,
            'monetary_cost':'unknown; no assumed unit prices','historical_root_stop':(run/'queue_stopped.json').exists(),
            'queue_state_directory':args.queue_state,
            'resumed_queue_stopped':(run/args.queue_state/'queue_stopped.json').exists(),
            'resumed_queue_complete':(run/args.queue_state/'queue_complete.json').exists()}
    if len(cells)==7:
        groups={r['qid']:r['family'] for r in map(json.loads,(data/'evaluator/task_groups_v1.jsonl').read_text(encoding='utf-8').splitlines())}
        result['primary_delta_percent_points']=100*sum(x[2] for x in paired)/2348
        result['primary_delta_interval']=paired_family_interval(paired,groups,GRAPHS)
        result['micro_ex_percent']={method:100*sum(cells[g][method]['correct'] for g in GRAPHS)/2348
                                   for method in cells['flight_accident']}
        result['macro_ex_percent']={method:sum(cells[g][method]['ex_percent'] for g in GRAPHS)/7
                                   for method in cells['flight_accident']}
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:result[k] for k in ['terminal_primary_paths','dispatched_primary_paths','physical_model_attempts',
                         'full2348_score_ready','resumed_queue_stopped','graph_scores']}),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for field in ['run','data','output']:p.add_argument('--'+field,type=Path,required=True)
    p.add_argument('--queue-state',default='infra_resume_queue_v1')
    audit(p.parse_args())
