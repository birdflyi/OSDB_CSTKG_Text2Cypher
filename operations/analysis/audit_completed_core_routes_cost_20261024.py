"""Observed routes and measured cost components, not a causal speed benchmark."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path


def audit(run, graph):
    folder=run/graph
    rows=json.loads((folder/'scores'/f'{graph}.json').read_text(encoding='utf-8'))
    assert len(rows)==len({(r['qid'],r['arm']) for r in rows})
    ours=[r for r in rows if r['arm']=='ours']
    executed=[r for r in ours if r['views'][-1]['outcome']['status']=='EXECUTED']
    routes=Counter(r['views'][-1]['outcome'].get('representation',r['views'][-1]['outcome']['status']) for r in ours)
    intermediate_equal=Counter()
    for row in executed:
        outcome=row['views'][-1]['outcome'];control=row['fixed_plan_full_detail']
        a=[(x['columns'],x['rows']) for x in outcome['observations']]
        b=[(x['columns'],x['rows']) for x in control['observations']]
        intermediate_equal['identical_intermediate_columns_rows' if a==b else 'different_intermediate_columns_rows']+=1
    per_arm={}
    physical_paths=set()
    for arm in ('direct','ours'):
        subset=[r for r in rows if r['arm']==arm]
        paths={Path(p) for r in subset for v in r['views'] for p in v['reply']['attempt_folders']}
        physical_paths.update(paths)
        calls=[json.loads((p/'terminal.json').read_text(encoding='utf-8')) for p in paths]
        usage=Counter();unknown=0
        for call in calls:
            fields=(call.get('body') or {}).get('usage')
            if not isinstance(fields,dict):unknown+=1;continue
            for key in ('prompt_tokens','completion_tokens','total_tokens'):
                if type(fields.get(key)) is int:usage[key]+=fields[key]
        traced={'primary':Counter(),'shared_plan_full_detail_control':Counter()}
        for record in subset:
            task=folder/'tasks'/record['qid']/arm
            for path in task.glob('query_trace/*/*/result.json'):
                trace=json.loads(path.read_text(encoding='utf-8'))['result']
                key='shared_plan_full_detail_control' if 'full_detail' in path.parent.parent.name else 'primary'
                cost=traced[key];cost['completed_result_traces']+=1
                for field in ('result_bytes','explain_seconds','db_seconds','worker_roundtrip_seconds'):
                    if isinstance(trace.get(field),(int,float)):cost[field]+=trace[field]
                    else:cost[field+'_unknown']+=1
            for path in task.glob('query_trace/*/*/transport_error.json'):
                key='shared_plan_full_detail_control' if 'full_detail' in path.parent.parent.name else 'primary'
                traced[key]['database_attempts_unknown_completion']+=1
        known_wrappers=[r['model_and_execution_wall_seconds'] for r in subset
                        if isinstance(r.get('model_and_execution_wall_seconds'),(int,float))]
        per_arm[arm]=dict(n=len(subset),physical_model_attempts=len(calls),known_usage=dict(usage),
            unknown_usage_attempts=unknown,known_model_transport_seconds=sum(c['seconds'] for c in calls),
            retry_wait_seconds=sum(c['retry_wait_seconds'] for c in calls),
            known_wrapper_seconds_sum=sum(known_wrappers),known_wrapper_records=len(known_wrappers),
            unknown_wrapper_records=len(subset)-len(known_wrappers),
            database_trace_components={k:dict(v) for k,v in traced.items()})
    actual={p.parent for p in folder.glob('segments/*/model_calls/physical_*/terminal.json')}
    assert physical_paths==actual,'Unattributed physical model attempts; do not omit them'
    return dict(kind='COMPLETED_GRAPH_DESCRIPTIVE_ROUTE_AND_COST_COMPONENTS',
        timestamp_utc=datetime.now(timezone.utc).isoformat(),graph=graph,n=len(ours),
        observed_ours_routes=dict(routes),executed_plan_control_comparison=dict(intermediate_equal),
        certified_disjoint_summary_executions=routes['certified_disjoint_counts'],
        per_arm=per_arm,physical_billed_model_attempts=len(actual),
        shared_plan_control_generations_are_reused=True,monetary_cost='unknown',
        this_is_not_complete_T4_acceptance=True,
        caveats=['Observed routes reflect model-produced plans; they are not oracle task applicability labels.',
                 'A zero trigger count provides no empirical evidence of summary-selection savings on this graph.',
                 'Full-detail is always replayed after Ours; cache/order confound prevents a causal speed claim.',
                 'Wrapper sums omit explicitly counted unknown clocks; they are not complete E2E totals.',
                 'Candidate index/lookup, startup, outage and infrastructure costs require the full cost ledger.',
                 'Failed and replayed database attempts remain included in available traces; unknown completions stay unknown.',
                 'No method changes or new model/database calls were performed.'])


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,required=True)
    p.add_argument('--graph',required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();r=audit(a.run,a.graph)
    a.output.write_text(json.dumps(r,indent=2),encoding='utf-8')
    print(json.dumps(r))
