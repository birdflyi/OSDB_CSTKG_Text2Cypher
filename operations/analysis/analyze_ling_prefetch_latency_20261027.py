# -*- coding: utf-8 -*-
"""Offline, read-only latency/usage accounting for both real TRAIN attempts.

One instance means one question x method x initial physical attempt. No model,
DB, evaluator gold, retry, approval change or score update is performed.
"""
from pathlib import Path
from collections import Counter,defaultdict
import datetime,hashlib,json,statistics

ROOT=Path('D:/ch7')
RUNS=[ROOT/'experiments/cb_train_schema_prefetch_direct_v2_20261027',
      ROOT/'experiments/cb_train_schema_prefetch_direct_v3_20261027']
REPORT=ROOT/'reports/ling_prefetch_latency_and_utilization_v1_20261027'

def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def describe(values):
    valid=sorted(v for v in values if type(v) in (int,float))
    def q(p):
        if not valid:return None
        x=(len(valid)-1)*p;lo=int(x);hi=min(lo+1,len(valid)-1)
        return valid[lo]+(valid[hi]-valid[lo])*(x-lo)
    return {'n_known':len(valid),'n_unknown':len(values)-len(valid),
            'mean':statistics.mean(valid) if valid else None,
            'P50':q(.5),'P90':q(.9),'max':max(valid) if valid else None,
            'sum_known':sum(valid) if valid else None}

def aggregate(rows):
    return {'physical_attempts':len(rows),'complete_response_not_correct_answer':sum(r['complete'] for r in rows),
        'length':sum(r['finish_reason']=='length' for r in rows),
        'infrastructure_failure':sum(r['infrastructure_failure'] for r in rows),
        'http_client_wall_seconds':describe([r['HTTP_seconds'] for r in rows]),
        'logical_attempt_wall_seconds':describe([r['ended_epoch']-r['started_epoch'] for r in rows]),
        'input_tokens':describe([r['prompt_tokens'] for r in rows]),
        'output_tokens_including_reported_reasoning':describe([r['completion_tokens'] for r in rows]),
        'reported_reasoning_tokens':describe([r['reasoning_tokens'] for r in rows]),
        'reported_cached_input_tokens':describe([r['cached_tokens'] for r in rows]),
        'input_characters':describe([r['prompt_characters'] for r in rows]),
        'output_content_characters':describe([r['response_characters'] for r in rows]),
        'connected_seconds':describe([r['phases'].get('connected_seconds') for r in rows]),
        'request_to_headers_seconds':describe([
            r['phases']['headers_received_seconds']-r['phases']['request_written_seconds']
            if all(k in r['phases'] for k in ('headers_received_seconds','request_written_seconds')) else None for r in rows]),
        'headers_to_full_body_seconds':describe([
            r['phases']['body_read_seconds']-r['phases']['headers_received_seconds']
            if all(k in r['phases'] for k in ('body_read_seconds','headers_received_seconds')) else None for r in rows]),
        'monetary_cost':None,'server_queue_seconds':None,'pure_decode_tokens_per_second':None}

def occupancy(rows):
    points=sorted([(r['started_epoch'],1) for r in rows]+[(r['ended_epoch'],-1) for r in rows])
    current=0;peak=0;area=0;last=points[0][0];by_count=defaultdict(float)
    for at,delta in points:
        area+=current*(at-last);by_count[current]+=at-last
        current+=delta;peak=max(peak,current);last=at
    assert current==0
    span=points[-1][0]-points[0][0]
    assert abs(area-sum(r['ended_epoch']-r['started_epoch'] for r in rows))<.02
    return {'first_start_epoch':points[0][0],'last_end_epoch':points[-1][0],'observation_span_seconds':span,
        'peak_reconstructed_client_inflight':peak,'time_weighted_mean_client_inflight':area/span,
        'observed_completion_rate_per_minute':len(rows)*60/span,
        'little_law_mean_latency_times_observed_rate':statistics.mean(r['ended_epoch']-r['started_epoch'] for r in rows)*len(rows)/span,
        'seconds_at_least150_client_inflight':sum(v for k,v in by_count.items() if k>=150),
        'seconds_at_least290_client_inflight':sum(v for k,v in by_count.items() if k>=290),
        'includes_startup_and_drain':True,'not_GPU_decoding_concurrency':True}

def windows(rows,seconds):
    ordered=sorted(rows,key=lambda r:r['ended_epoch']);left=0;tokens=0;complete=0;infra=0;result=[]
    first=min(r['started_epoch'] for r in rows)
    for right,row in enumerate(ordered):
        tokens+=row['completion_tokens'] or 0;complete+=row['complete'];infra+=row['infrastructure_failure']
        while left<=right and ordered[left]['ended_epoch']<=row['ended_epoch']-seconds:
            old=ordered[left];tokens-=old['completion_tokens'] or 0;complete-=old['complete'];infra-=old['infrastructure_failure'];left+=1
        if row['ended_epoch']-first>=seconds:
            result.append({'end_epoch':row['ended_epoch'],'window_seconds':seconds,'terminals':right-left+1,
                'complete_responses':complete,'infrastructure_failures':infra,
                'known_completed_output_tokens':tokens,'completed_output_tokens_per_wall_second':tokens/seconds})
    return result

def main():
    assert not REPORT.exists();REPORT.mkdir()
    rows=[];sources={};seen=set();by_run={};seen_500=[]
    for run in RUNS:
        freeze=read(run/'freeze.json');assert sha(run/'manifest.json')==freeze['manifest_sha256']
        manifest={r['request_id']:r for r in read(run/'manifest.json')}
        group=[]
        for folder in sorted((run/'campaign/attempts').iterdir()):
            rid=folder.name;assert rid not in seen;seen.add(rid);m=manifest[rid]
            t=read(folder/'terminal.json');body=read(folder/'request.json');n=t['normalized_reply'];usage=n['usage']
            assert body['messages']==m['messages']
            record={'request_id':rid,'run':run.name,'qid':m['qid'],'graph':m['graph'],'arm':m['arm'],
                'budget_slot':t['budget_slot'],'started_epoch':t['started_epoch'],'ended_epoch':t['ended_epoch'],
                'HTTP_seconds':t['seconds'],'http_status':t.get('http_status'),'finish_reason':n['finish_reason'],
                'complete':not n['transport_error'] and n['finish_reason']=='stop',
                'infrastructure_failure':bool(n['transport_error']),
                'prompt_characters':sum(len(v['content']) for v in body['messages']),
                'response_characters':n['response_characters'],'phases':t.get('network_phase_seconds',{}),
                **usage}
            rows.append(record);group.append(record)
            sources[str(folder/'terminal.json')]=sha(folder/'terminal.json')
            sources[str(folder/'request.json')]=sha(folder/'request.json')
            if t.get('http_status')==500:seen_500.append(t['ended_epoch'])
        expected=207 if 'v2_' in run.name else 2102
        assert len(group)==expected==read(run/'summary.json')['dispatched']
        by_run[run.name]={'all_attempts':aggregate(group),'complete_responses_only':aggregate([v for v in group if v['complete']]),'occupancy':occupancy(group)}
        for seconds in (60,300):
            w=windows(group,seconds)
            with (REPORT/(run.name+f'_rolling_{seconds}s.jsonl')).open('w',encoding='utf-8') as out:
                for item in w:out.write(json.dumps(item,sort_keys=True)+'\n')
            healthy=[v for v in w if v['infrastructure_failures']==0]
            by_run[run.name][f'peak_healthy_completed_output_wall_TPS_{seconds}s']=max(healthy,key=lambda x:x['completed_output_tokens_per_wall_second']) if healthy else None
    assert len(rows)==2309 and sum(v['complete'] for v in rows)==1990
    assert sum(v['infrastructure_failure'] for v in rows)==309
    assert len(seen_500)==302 and len({r['budget_slot'] for r in rows})==2309
    with (REPORT/'attempt_metrics.jsonl').open('w',encoding='utf-8') as out:
        for item in rows:out.write(json.dumps(item,sort_keys=True)+'\n')
    (REPORT/'evidence_hashes.json').write_text(json.dumps(sources,sort_keys=True,indent=2),encoding='utf-8')
    summary={'created_local':datetime.datetime.now().astimezone().isoformat(),'unit':'one question x method x initial physical attempt',
        'all_attempts':aggregate(rows),'complete_responses_only':aggregate([v for v in rows if v['complete']]),
        'by_method':{arm:aggregate([r for r in rows if r['arm']==arm]) for arm in ('direct','ours')},
        'by_run':by_run,'distinct_question_ids_attempted':len({v['qid'] for v in rows}),
        'graph_attempt_counts':dict(Counter(v['graph'] for v in rows)),
        'all4096_planned':4096,'undispatched':1787,'model_calls_in_this_analysis':0,'DB_queries':0,
        'old_predictions_or_scores_modified':False,'hold_still_active':read(ROOT/'state/T1_MODEL_CONCURRENCY_POLICY_20261027.json')['human_dispatch_hold'],
        'evidence_manifest_sha256':sha(REPORT/'evidence_hashes.json'),
        'upstream_failure_first_return_local':datetime.datetime.fromtimestamp(min(seen_500)).astimezone().isoformat(),
        'upstream_failure_last_return_local':datetime.datetime.fromtimestamp(max(seen_500)).astimezone().isoformat(),
        'limitations':['Complete response is not query execution or correctness.',
            'Healthy-window peaks are descriptive selection, not sustained capacity or matched-concurrency experiment.',
            'Timing contains client connect/gateway wait/generation/transfer; no pure decoding or GPU attribution.',
            'Request-size and method mix differ over time; no causal150-versus300 speedup claim.',
            'The source-port constraint and gateway500 are distinct observations; failure cause needs server evidence.']}
    (REPORT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,sort_keys=True,indent=2),encoding='utf-8')
    print(json.dumps({'summary':str(REPORT/'summary.json'),'attempts':len(rows),'complete':1990,
        'latency':summary['all_attempts']['http_client_wall_seconds'],
        'input':summary['all_attempts']['input_tokens'],'output':summary['all_attempts']['output_tokens_including_reported_reasoning'],
        'occupancy_v3':by_run[RUNS[1].name]['occupancy'],'model_calls':0}))

if __name__=='__main__':main()
