"""Remote TRAIN-only capacity campaign. No graph service, scoring or training.

Every POST requires acknowledgement from the local authoritative budget over an
authenticated localhost SSH reverse tunnel. No model retries or hidden SDK.
"""
import argparse
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import time
from urllib.parse import urlsplit

from common import atomic, immutable, read, rates, distribution, valid_token


class Controller:
    def __init__(self, private): self.private = private
    def call(self, route, body):
        wire = json.dumps(body,ensure_ascii=False,separators=(',',':')).encode('utf-8')
        for attempt in range(3):
            connection = http.client.HTTPConnection('127.0.0.1',self.private['control_port'],timeout=30)
            try:
                connection.request('POST',route,wire,{'Content-Type':'application/json',
                    'X-Run-Token':self.private['control_token']})
                response=connection.getresponse(); result=json.loads(response.read())
                if response.status!=200: raise RuntimeError(result.get('error','Coordinator rejected request'))
                return result
            except (OSError,http.client.HTTPException):
                if attempt==2: raise
                time.sleep(1)
            finally: connection.close()


class Pacer:
    """Adaptive estimated-output pacing; delayed usage is not backend QoS."""
    def __init__(self, cap):
        self.cap=cap; self.credit=0.; self.last=time.monotonic()
        self.estimates={'direct':564.,'ours':1018.}
    def refill(self):
        now=time.monotonic();self.credit=min(max(self.cap,8192.),self.credit+(now-self.last)*self.cap);self.last=now
    def delay(self, arm):
        self.refill();return max(0.,(self.estimates[arm]-self.credit)/self.cap)
    def take(self, arm):
        self.refill();estimate=self.estimates[arm];self.credit-=estimate;return estimate
    def settle(self, arm, estimate, actual):
        if valid_token(actual):
            self.refill();self.credit-=actual-estimate
            self.estimates[arm]=max(32.,min(8192.,self.estimates[arm]*.75+actual*.25))


def model_request(private, body, slot, folder):
    endpoint=urlsplit(private['endpoint'])
    connection=(http.client.HTTPSConnection if endpoint.scheme=='https' else http.client.HTTPConnection)(
        endpoint.hostname,endpoint.port,timeout=10)
    wire=json.dumps(body,ensure_ascii=False,separators=(',',':')).encode('utf-8')
    started=time.monotonic();epoch=time.time();phase='connecting';written=False
    try:
        connection.connect();connected=time.monotonic()-started;phase='sending_request'
        headers={'Authorization':'Bearer '+private['api_key'],'Content-Type':'application/json; charset=utf-8'}
        if private.get('host_header'):headers['Host']=private['host_header']
        connection.request('POST',endpoint.path+('?' + endpoint.query if endpoint.query else ''),wire,
            headers)
        written=True;phase='waiting_headers'
        if connection.sock:connection.sock.settimeout(240)
        response=connection.getresponse();phase='reading_body';raw=response.read(4*1024*1024+1)
        if len(raw)>4*1024*1024:raise ValueError('Bounded response size exceeded')
        try:reply=json.loads(raw)
        except (ValueError,UnicodeError):reply={'invalid_response':True}
        terminal={'http_status':response.status,'body':reply,'connect_seconds':connected}
    except (OSError,http.client.HTTPException,ValueError) as exc:
        terminal={'transport_error':type(exc).__name__,'http_status':None}
    finally:connection.close()
    terminal.update(seconds=time.monotonic()-started,retry_wait_seconds=0.,budget_slot=slot,
        physical_attempt_in_request=1,model_dispatched=written,request_write_completed=written,
        last_network_phase=phase,started_epoch=epoch,ended_epoch=time.time(),
        formal_benchmark_prediction=False,scope='CypherBench_TRAIN_capacity_development')
    immutable(Path(folder)/'terminal.json',terminal)
    return terminal


def response_metrics(terminal):
    body=terminal.get('body') or {};choices=body.get('choices') or [{}]
    choice=choices[0];content=choice.get('message',{}).get('content');usage=body.get('usage') or {}
    if isinstance(content,list):content=''.join(r.get('text','') for r in content if isinstance(r,dict))
    return {'complete':terminal.get('http_status')==200 and choice.get('finish_reason')=='stop'
            and isinstance(content,str) and bool(content.strip()),
        'finish_reason':choice.get('finish_reason'),
        **{k:usage.get(k) if valid_token(usage.get(k)) else None for k in ('prompt_tokens','completion_tokens','total_tokens')},
        'reasoning_tokens':usage.get('completion_tokens_details',{}).get('reasoning_tokens'),
        'seconds':terminal['seconds'],'ended_epoch':terminal['ended_epoch'],
        'http_status':terminal.get('http_status')}


def baseline_ready(dispatched, complete, target):
    return dispatched>=min(12,target) and complete==dispatched


def dispatch_window_expired(now, started, seconds=240):
    # This deadline bounds both pacing waits and dispatch. Draining the final
    # response after this deadline is normal and must not become a fatal stall.
    return now-started>seconds


def steady_metrics(rows, concurrency):
    ordered=sorted(rows,key=lambda r:r['ended_epoch'])
    trim=concurrency
    if len(ordered)<max(8,4*concurrency):return {'steady_sample_sufficient':False}
    middle=ordered[trim:len(ordered)-trim]
    seconds=middle[-1]['ended_epoch']-middle[0]['ended_epoch']
    measured=middle[1:]
    if seconds<=0:return {'steady_sample_sufficient':False}
    return {'steady_sample_sufficient':True,'steady_window_seconds':seconds,
            'steady_window_completions':len(measured),'steady_trim_each_end':trim,
            'steady_complete_cases_per_minute':sum(r['complete'] for r in measured)*60/seconds,
            'steady_terminal_cases_per_minute':len(measured)*60/seconds,
            'steady_known_output_tokens_per_second':sum(r['completion_tokens'] for r in measured
                if valid_token(r['completion_tokens']))/seconds}


def recommend(stages):
    candidates=[s for s in stages if s.get('steady_sample_sufficient')
        and s['concurrency']>1 and s.get('completed',0)>=s['concurrency']*4
        and s.get('HTTP200_responses',0)/max(1,s['completed'])>=.99
        and s.get('complete_responses',0)/max(1,s['completed'])>=.95
        and not s.get('stop_reason')]
    if not candidates:return None
    maximum=max(s['steady_complete_cases_per_minute'] for s in candidates)
    return min(s['concurrency'] for s in candidates if s['steady_complete_cases_per_minute']>=maximum*.95)


def campaign(private_path, manifest_path, output):
    private=read(private_path);manifest=read(manifest_path);output=Path(output)
    if socket.gethostname()!=manifest['execution_hostname']:raise RuntimeError('Wrong execution host')
    if output.exists():raise RuntimeError('Existing campaign cannot be resumed or overwritten')
    output.mkdir(parents=True); controller=Controller(private)
    controller.call('/ping',{'hostname':socket.gethostname()})
    immutable(output/'protocol.json',manifest['protocol'])
    stages=list(manifest.get('seed_stages',[]));events=deque();all_rows=[];pacer=Pacer(manifest['protocol']['output_tokens_per_second_target'])
    stop_reason=None;failures=0;started=time.time();recent=deque(maxlen=8);last_metrics=0;cached={}
    snapshot={'kind':'T1_RUNTIME_SNAPSHOT_V1','capacity':True,'model':private['model'],
        'owner_hostname':socket.gethostname(),'campaign_started':started,'job_id':'并发容量验收 / 固定TRAIN题',
        'jobs':manifest['jobs'],'stages':stages,'state':'运行中','stage_name':'正在准备','total':0,
        'concurrency_target':0,'output_budget':pacer.cap,'output_budget_source':'用户提供，估计调速',
        'server_load':None,'server_queue':None,'recent':[],'active':[],'completed':0,'complete_responses':0}
    def publish(active, force=False):
        nonlocal last_metrics,cached
        now=time.time()
        while events and events[0]['ended_epoch']<now-300:events.popleft()
        if force or now-last_metrics>=30:
            cached={'latency':distribution([r['seconds'] for r in all_rows[-1000:]]),
                'tokens':{k:distribution([r[k] for r in all_rows[-1000:] if valid_token(r.get(k))])
                          for k in ('prompt_tokens','completion_tokens','reasoning_tokens')}}
            last_metrics=now
        snapshot.update(updated_epoch=now,active=[{k:r[k] for k in ('trial_id','question','arm','started_epoch')}
                                                   for r in active.values()],recent=list(recent),rates=rates(events,now,snapshot.get('stage_started')),
                        completed=len(stage_rows) if 'stage_rows' in locals() else snapshot.get('completed',0),
                        statistics=cached,remaining_campaign_calls=manifest['protocol']['max_calls']-len(all_rows))
        try:
            ack=controller.call('/snapshot',snapshot);snapshot['budget']=ack['budget']
        except (OSError,http.client.HTTPException):pass
        atomic(output/'state.json',snapshot)
        atomic(output.parent/'monitor/state.json',snapshot)
    for stage in manifest['stages']:
        if stop_reason:break
        if stage['name']=='refine48' and not any(s.get('plateau') and s['concurrency']==64 for s in stages):
            stage['skip_reason']='未出现32到64的平台，不触发48档细测'
        if stage.get('skip_reason'):
            result={'name':stage['name'],'concurrency':stage['concurrency'],'planned':stage['count'],
                    'dispatched':0,'completed':0,'failed_or_truncated':0,'undispatched':stage['count'],
                    'state':'未派发','reason':stage['skip_reason']}
            stages.append(result);controller.call('/stage',result);continue
        count=stage['count'];concurrency=stage['concurrency'];stage_rows=[];active={};sent=0;peak=0
        events.clear()
        stage_start=time.monotonic();stage_epoch=time.time();last_publish=0;dispatch_stop=False
        last_dispatch=time.monotonic()
        snapshot.update(stage_name=stage['name'],total=count,completed=0,complete_responses=0,
                        concurrency_target=concurrency,state='运行中',stage_started=stage_epoch)
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            while sent<count or active:
                now=time.monotonic()
                if dispatch_window_expired(now,stage_start,
                    manifest['protocol']['stage_new_dispatch_time_limit_seconds']):dispatch_stop=True
                if (output/'STOP').exists():stop_reason='用户停止标记';dispatch_stop=True
                while not dispatch_stop and not stop_reason and sent<count and len(active)<concurrency:
                    # Process ready terminals before filling more slots; a burst
                    # of failures must not wait behind another dispatch wave.
                    if any(f.done() for f in active):break
                    trial=stage['trials'][sent];arm=trial['arm']
                    if pacer.delay(arm)>.001:break
                    ack=controller.call('/reserve',{'trial_id':trial['trial_id']})
                    folder=output/'attempts'/trial['trial_id'];folder.mkdir(parents=True)
                    immutable(folder/'request.json',trial['body'])
                    estimate=pacer.take(arm)
                    future=pool.submit(model_request,private,trial['body'],ack['slot'],folder)
                    active[future]={'trial_id':trial['trial_id'],'arm':arm,'question':trial['question'],
                        'started_epoch':time.time(),'estimate':estimate,'folder':folder}
                    sent+=1;peak=max(peak,len(active))
                    last_dispatch=time.monotonic()
                if active:
                    done,_=wait(active,timeout=.25,return_when=FIRST_COMPLETED)
                    for future in sorted(done,key=lambda f:f.result()['ended_epoch']):
                        meta=active.pop(future);terminal=future.result()
                        controller.call('/terminal',{'trial_id':meta['trial_id'],'terminal':terminal})
                        row={'trial_id':meta['trial_id'],'arm':meta['arm'],**response_metrics(terminal)}
                        stage_rows.append(row);all_rows.append(row);events.append(row)
                        pacer.settle(meta['arm'],meta['estimate'],row['completion_tokens'])
                        infra=bool(terminal.get('transport_error')) or terminal.get('http_status')==429 or (
                            isinstance(terminal.get('http_status'),int) and terminal['http_status']>=500)
                        failures=failures+1 if infra else 0
                        if failures>=3:stop_reason='连续3次基础设施失败，停止批次'
                        if terminal.get('http_status') in (400,401,403,404):stop_reason='接口或鉴权配置错误，停止批次'
                        recent.appendleft({'trial_id':meta['trial_id'],'state':'完整响应' if row['complete'] else '失败/截断',
                                           'seconds':round(row['seconds'],2)})
                elif dispatch_stop or stop_reason:break
                else:time.sleep(min(.25,max(.01,pacer.delay(stage['trials'][sent]['arm']))))
                snapshot.update(completed=len(stage_rows),complete_responses=sum(r['complete'] for r in stage_rows),
                    dispatched=sent,undispatched=count-sent,stop_reason=stop_reason,
                    last_progress_epoch=max((r['ended_epoch'] for r in stage_rows),default=stage_epoch),
                    http_counts=dict(Counter(str(r['http_status']) for r in stage_rows)))
                if time.monotonic()-last_publish>=3:publish(active);last_publish=time.monotonic()
                if stop_reason:dispatch_stop=True
        wall=time.monotonic()-stage_start;good=[r for r in stage_rows if r['complete']]
        known=[r for r in stage_rows if valid_token(r['completion_tokens'])]
        result={'name':stage['name'],'concurrency':concurrency,'actual_peak_requests':peak,
            'planned':count,'dispatched':sent,'completed':len(stage_rows),'complete_responses':len(good),
            'failed_or_truncated':len(stage_rows)-len(good),'undispatched':count-sent,'wall_seconds':wall,
            'terminal_cases_per_minute':len(stage_rows)*60/wall,'complete_cases_per_minute':len(good)*60/wall,
            'observed_output_tokens_per_second':sum(r['completion_tokens'] for r in known)/wall,
            'known_output_tokens':sum(r['completion_tokens'] for r in known),'unknown_usage':len(stage_rows)-len(known),
            'latency':distribution([r['seconds'] for r in stage_rows]),'stage_time_limit_hit':dispatch_stop and not stop_reason,
            'model_name_echo_only':True,'plateau':False,'stop_reason':stop_reason,
            'output_pacing':'Adaptive expected-output pacing at target; delayed usage cannot enforce instant backend QoS.'}
        result.update(steady_metrics(stage_rows,concurrency))
        result['HTTP200_responses']=sum(r['http_status']==200 for r in stage_rows)
        previous=[s for s in stages if s['concurrency']>1 and s['name']!='refine48' and s.get('completed',0)>0]
        if previous and concurrency>previous[-1]['concurrency'] and result.get('steady_sample_sufficient'):
            current_rate=result.get('steady_complete_cases_per_minute',result['complete_cases_per_minute'])
            prior_rate=previous[-1].get('steady_complete_cases_per_minute',previous[-1]['complete_cases_per_minute'])
            result['gain_over_previous']=current_rate/max(prior_rate,.001)-1
            result['plateau']=result['gain_over_previous']<.05
        immutable(output/(stage['name']+'_result.json'),result);stages.append(result)
        controller.call('/stage',result)
        print(json.dumps(result,ensure_ascii=True),flush=True)
        snapshot.update(completed=len(stage_rows),complete_responses=len(good),state='阶段完成')
        publish({},True)
        if stop_reason:break
        if result['plateau']:
            # Do not run still-higher load after an observed plateau. A fixed48
            # intermediate stage is allowed only to refine the32/64 bracket.
            for later in manifest['stages']:
                if later['concurrency']>concurrency:later['skip_reason']='吞吐已进入平台，停止增加负载'
        if concurrency==1 and not baseline_ready(sent,len(good),count):
            stop_reason='串行预检失败或完整样本不足，停止扩并发'
    known_names={s['name'] for s in stages}
    for stage in manifest['stages']:
        if stage['name'] not in known_names:
            result={'name':stage['name'],'concurrency':stage['concurrency'],'planned':stage['count'],
                    'dispatched':0,'completed':0,'failed_or_truncated':0,'undispatched':stage['count'],
                    'state':'未派发','reason':stop_reason or '未满足进入条件'}
            stages.append(result);controller.call('/stage',result)
    summary={'kind':'TRAIN_CAPACITY_NOT_FORMAL_T1_OR_SECOND_BACKBONE_RESULT','model':private['model'],
        'stages':stages,'dispatched':len(all_rows),'known_usage':sum(valid_token(r['completion_tokens']) for r in all_rows),
        'recommended_concurrency':recommend(stages),'stop_reason':stop_reason,
        'elapsed_seconds':time.time()-started,'best_point_scope':'Observed fixed TRAIN pack under adaptive3500output pacing; not a backend maximum or accuracy result.',
        'formal_predictions':0,'monetary_cost':None}
    immutable(output/'summary.json',summary)
    snapshot.update(state='已停止' if stop_reason else '已完成',stop_reason=stop_reason,recommendation=summary['recommended_concurrency'])
    publish({},True);controller.call('/done',summary)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--private',required=True)
    parser.add_argument('--manifest',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();campaign(args.private,args.manifest,args.output)
