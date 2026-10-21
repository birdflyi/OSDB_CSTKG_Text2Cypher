"""Offline evidence/cost accounting. Unknown usage is never treated as free."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import random
import statistics


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def usage(records):
    totals={'attempts':len(records),'unknown_usage_attempts':0,'pending_attempts':0,
            'known_prompt_tokens':0,'known_completion_tokens':0,'known_total_tokens':0,
            'unknown_prompt_token_attempts':0,'unknown_completion_token_attempts':0,
            'http_or_transport_failures':0,'recorded_dispatch_seconds':0.0}
    for record in records:
        if record is None:
            totals['pending_attempts']+=1
            totals['unknown_usage_attempts']+=1
            totals['unknown_prompt_token_attempts']+=1
            totals['unknown_completion_token_attempts']+=1
            continue
        totals['recorded_dispatch_seconds']+=record.get('seconds',0)
        totals['http_or_transport_failures']+=int('transport_error' in record or record.get('http_status')!=200)
        u=record.get('body',{}).get('usage') or {}
        if not isinstance(u.get('total_tokens'),(int,float)):
            totals['unknown_usage_attempts']+=1
        else:
            totals['known_total_tokens']+=u['total_tokens']
        for source,target in [('prompt_tokens','prompt'),('completion_tokens','completion')]:
            if isinstance(u.get(source),(int,float)):
                totals['known_'+target+'_tokens']+=u[source]
            else:
                totals['unknown_'+target+'_token_attempts']+=1
    totals['total_tokens_complete']=totals['unknown_usage_attempts']==0
    totals['currency_cost']=None
    totals['currency_cost_reason']='No verified provider price or billing record. Known token sums exclude unknown usage; they are not zero-cost estimates.'
    return totals


def ledger(root):
    """Charge each reserved attempt once, including requests copied to new runs."""
    records=[]
    slots=sorted((Path(root)/'budget').glob('*.json'))
    for slot in slots:
        request=Path(load(slot)['request_path'])
        terminal=(request.with_name('connectivity_probe_01.json') if request.name=='connectivity_probe_01_request.json'
                  else request.with_name('terminal.json'))
        records.append(load(terminal) if terminal.exists() else None)
    return {'reserved_slots':len(slots),**usage(records)}


def correct(record):
    grade=record['grade']
    return grade.get('correct',grade.get('correct_task_completion_or_boundary',False))


def terminal_records(root,record):
    stem=record['arm'][0]+'_'+record['id']
    stages=[stem+'_first']
    if record['arm'].endswith('1') and record['repair_used']:
        stages.append(stem+'_repair')
    return [load(root/'calls'/stage/'terminal.json') if (root/'calls'/stage/'terminal.json').exists() else None for stage in stages]


def percentile(values,fraction):
    if not values:
        return None
    ordered=sorted(values)
    return ordered[int((len(ordered)-1)*fraction)]


def run_summary(folder):
    root=Path(folder)
    manifest=load(root/'manifest.json')
    rows=[load(f) for f in sorted((root/'records').glob('*.json'))]
    arms={}
    for arm in sorted({x['arm'] for x in rows}):
        selected=[x for x in rows if x['arm']==arm]
        answerable=[x for x in selected if x['expected_status']=='ANSWER']
        attempts=[call for row in selected for call in terminal_records(root,row)]
        cost=usage(attempts)
        successes=sum(correct(x) for x in answerable)
        network_times=[sum(c.get('seconds',0) for c in terminal_records(root,row) if c) for row in selected]
        arms[arm]={'evaluated_cases':len(selected),'answerable_cases':len(answerable),
            'correct_answerable_cases':successes,
            'correct_boundary_decisions':sum(correct(x) for x in selected if x['expected_status']!='ANSWER'),
            'wrong_returned_answers':sum(x['grade']['wrong_returned_answer'] for x in selected),
            'executed_answerable':sum(x['observation']['execution']['status']=='EXECUTED' for x in answerable),
            'infrastructure_outcomes':sum(x['failure']=='INFRASTRUCTURE' for x in selected),
            'all_attempt_usage':cost,
            'tokens_per_correct_answerable':cost['known_total_tokens']/successes if successes and cost['total_tokens_complete'] else None,
            'median_model_dispatch_seconds_per_case':statistics.median(network_times) if network_times else None,
            'p95_model_dispatch_seconds_per_case':percentile(network_times,.95)}
    paired=[]
    index={(x['id'],x['arm']):x for x in rows}
    ids=sorted({x['id'] for x in rows})
    for left,right in [('P1','S1'),('I1','P1'),('I1','G1')]:
        pairs=[(index[(key,left)],index[(key,right)]) for key in ids if (key,left) in index and (key,right) in index]
        if not pairs:
            continue
        paired.append({'left':left,'right':right,'paired_cases':len(pairs),
            'left_only_correct':sum(correct(a) and not correct(b) for a,b in pairs),
            'right_only_correct':sum(correct(b) and not correct(a) for a,b in pairs),
            'both_correct':sum(correct(a) and correct(b) for a,b in pairs),
            'neither_correct':sum(not correct(a) and not correct(b) for a,b in pairs)})
    return {'folder':root.name,'mode':manifest['mode'],'git_commit':manifest['git_commit'],
        'arms':arms,'paired_correctness':paired,
        'limits':'Retrieval/result-generation stage only, not end-to-end conversational answering or service construction/maintenance. Dispatch-time sums are per-attempt recorded time, not queue wall time or DB time. Arm first/repaired views share calls; do not add their usage. Development requests are not an independent generalization estimate.'}


def goal_bootstrap(differences,repetitions=5000,seed=1701):
    """Bootstrap at semantic-goal level, never at individual paraphrase level.

    Caller must establish independent goal IDs and freeze formal inclusion.
    This conditional interval does not imply representative user sampling.
    """
    groups=defaultdict(list)
    for row in differences:
        groups[row['semantic_goal']].append(row['difference'])
    means=[statistics.mean(x) for x in groups.values()]
    if len(means)<2:
        return {'goals':len(means),'mean':statistics.mean(means) if means else None,'interval':None}
    rng=random.Random(seed)
    draws=[statistics.mean(rng.choices(means,k=len(means))) for _ in range(repetitions)]
    return {'goals':len(means),'mean':statistics.mean(means),
        'interval':[percentile(draws,.025),percentile(draws,.975)],'seed':seed,
        'limits':'Conditional goal-block bootstrap; convenience/synthetic goals and only four projects do not establish population representativeness.'}


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    parser.add_argument('--run',action='append',default=[])
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    result={'unique_continuation_attempts':ledger(args.root),
            'runs':[run_summary(folder) for folder in args.run],
            'acceptance_claim':'None; publication contribution still requires independent useful effects and related-work comparison.'}
    with Path(args.output).open('x',encoding='utf-8') as f:
        json.dump(result,f,ensure_ascii=False,indent=2)
    print(json.dumps({'reserved_attempts':result['unique_continuation_attempts']['reserved_slots'],'runs':len(result['runs'])}))
