"""Finite128-cap direct-HTTP TRAIN prefetch. Not formal scores or second-backbone evidence.

Uses the already prepared 1787 immutable paired messages. Rolling refill starts
with two actual unused tasks, then32/64/128 after healthy bounded ramps.
There is one global controller, one physical POST per identity, and no retries.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import argparse, hashlib, http.client, json, os, shutil, socket, sys, threading, time
from urllib.parse import urlsplit
from task_bound_http_v1_20261027 import TaskBoundHTTPConnection
from osdb_scoped_attempts_dev import read, sha, wire, immutable
from osdb_budgeted_model_client_live_v2 import normalized_reply
from osdb_live_monitor_bridge import atomic
from osdb_model_limit_guard_128_v1_20261027 import CentralModelGate

R=Path('D:/ch7'); REPO=R/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
OLD=R/'experiments/cb_train_schema_prefetch_v1_20261027'
RUN=R/'experiments/cb_train_schema_prefetch_direct_v4_20261027'
PRIVATE=Path('C:/Users/10651/.codex/private/ch7_llm/connection_louzehua_20261027.json')
POLICY=R/'state/T1_MODEL_CONCURRENCY_POLICY_20261027.json'
SAMPLING={'model':'ling-3.0-flash-n72-75','temperature':.2,'top_p':.95,'reasoning_effort':'high','max_tokens':8192}

def legacy():
    sys.path.insert(0,str(REPO/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    return AttemptBudget(R/'experiments/ch7_private/continuation_20261021/budget',
        R/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
        path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',R/'experiments/ch7_private')])

class DirectHTTP:
    """A bounded single POST. Timings are client phases, not pure decoding TPS."""
    def __init__(self,private):
        self.private=private; self.url=urlsplit(private['endpoint'])
        assert private['model']==SAMPLING['model']
        assert private['endpoint']=='http://117.50.195.92:8080/gpt-oss-120b/litellm/v1/chat/completions'
    def __call__(self,body):
        endpoint=self.url; conn=TaskBoundHTTPConnection(endpoint.hostname,endpoint.port,timeout=10)
        start=time.monotonic(); phase='connecting'; written=False; timer=None
        expired=threading.Event(); terminal={}; times={}
        try:
            conn.connect(); terminal['task_local_source_port']=conn.sock.getsockname()[1]; times['connected_seconds']=time.monotonic()-start
            owned_socket=conn.sock
            def abort():
                expired.set()
                try: owned_socket.shutdown(socket.SHUT_RDWR)
                except OSError: pass
            timer=threading.Timer(600,abort); timer.daemon=True; timer.start()
            conn.sock.settimeout(600); phase='sending_request'
            conn.request('POST',endpoint.path,body=wire(body),headers={
                'Authorization':'Bearer '+self.private['api_key'],
                'Content-Type':'application/json; charset=utf-8'})
            written=True; times['request_written_seconds']=time.monotonic()-start; phase='waiting_headers'
            response=conn.getresponse(); terminal['http_status']=response.status
            times['headers_received_seconds']=time.monotonic()-start; phase='reading_body'
            raw=response.read(4*1024**2+1); terminal['raw_response_bytes']=raw
            times['body_read_seconds']=time.monotonic()-start
            if len(raw)>4*1024**2: raise ValueError('Response size limit exceeded')
            if getattr(response,'length',None) not in (None,0): raise http.client.IncompleteRead(raw)
            try:
                def reject(value): raise ValueError('Non-finite JSON')
                parsed=json.loads(raw,parse_constant=reject); wire(parsed); terminal['body']=parsed
            except (ValueError,UnicodeError): terminal.update(body=None,response_parse_error=True)
        except (OSError,http.client.HTTPException,ValueError) as exc:
            terminal.update(transport_error=type(exc).__name__,error_errno=getattr(exc,'errno',None),error_winerror=getattr(exc,'winerror',None),error_message=str(exc)[:250])
        finally:
            if timer: timer.cancel()
            conn.close()
        terminal.update(seconds=time.monotonic()-start,network_phase_seconds=times,
            last_network_phase=phase,request_write_completed=written,
            remote_outcome_unknown=bool(terminal.get('transport_error') and phase!='connecting'),
            post_connect_deadline_seconds=600,deadline_expired=expired.is_set(),transport='DIRECT_LOCAL_HTTP_SINGLE_POST')
        return terminal

def prepare():
    raise RuntimeError('Prepared by explicit no-resend v2/v3 reconciliation; do not recreate')

def run():
    f=read(RUN/'freeze.json'); here=Path(__file__).parent
    assert all(sha(here/n)==h for n,h in f['source_sha256'].items())
    assert sha(RUN/'manifest.json')==f['manifest_sha256'] and sha(PRIVATE)==f['private_configuration_sha256']
    assert sha(POLICY)==f['policy_sha256']
    assert sha(RUN/'review_budget.py')==f['lease_sha256'] and sha(RUN/'review_common.py')==f['common_sha256']
    assert all(sha(p)==h for p,h in read(RUN/'input_sources.json').items())
    immutable(RUN/'run_claim.json',{'pid':os.getpid(),'started_epoch':time.time(),
        'command':sys.argv,'transport':'DIRECT_HTTP','source':str(Path(__file__))})
    sys.path.insert(0,str(RUN)); from review_budget import ParallelLease
    rows=read(RUN/'manifest.json'); assert len(rows)==1787
    transport=DirectHTTP(read(PRIVATE)); mutex=threading.RLock(); stop=threading.Event()
    active={}; events=[]; peak=0; infra=0; reason=None; started=time.time(); limit=2; stage=0; stage_epoch=None; stage_baseline=0; first_dispatch=None
    try:
        with CentralModelGate(POLICY,128) as gate:
            with ParallelLease(legacy(),RUN/'campaign',1787) as lease:
                original=sum(read(p).get('scope_id') is None for p in lease.legacy.ledger.glob('*.json'))
                assert original+1787<=12000
                def work(row):
                    nonlocal peak, first_dispatch
                    rid=row['request_id']; body=dict(SAMPLING,messages=row['messages']); queued=time.time()
                    with gate.slot(wait_seconds=30):
                        if stop.is_set(): return {'rid':rid,'undispatched':True}
                        ack=lease.reserve(rid,body); folder=RUN/'campaign/attempts'/rid
                        immutable(folder/'dispatch_claim.json',{'slot':ack['slot'],'request_id':rid,'physical_attempt':1,'retry':False})
                        begin=time.time()
                        with mutex:
                            active[rid]={'qid':row['qid'],'graph':row['graph'],'arm':row['arm'],'started_epoch':begin}
                            peak=max(peak,len(active))
                            if first_dispatch is None:first_dispatch=begin
                        try:
                            t=transport(body); end=time.time(); raw=t.pop('raw_response_bytes',None)
                            if raw is not None:
                                with (folder/'response.raw').open('xb') as out: out.write(raw)
                            reply=normalized_reply(t,SAMPLING['model'])
                            unknown=t.get('remote_outcome_unknown',False)
                            t.update(budget_slot=ack['slot'],scope='CB_TRAIN_SCHEMA_ONLY_DEVELOPMENT_PREFETCH',
                                started_epoch=begin,ended_epoch=end,automatic_retry=False,normalized_reply=reply,
                                prompt_characters=sum(len(m['content']) for m in row['messages']),
                                scheduling_wait_seconds=begin-queued,monetary_cost=None)
                            lease.terminal(rid,t)
                            if unknown:
                                gate.preserve_unknown(rid); stop.set()
                            return {'rid':rid,'seconds':end-begin,'ended_epoch':end,'unknown':unknown,
                                'http_status':t.get('http_status'),'infrastructure_failure':bool(reply['transport_error']),
                                'complete':not reply['transport_error'] and reply['finish_reason']=='stop',
                                'finish_reason':reply['finish_reason'],'prompt_characters':t['prompt_characters'],
                                'response_characters':reply['response_characters'],**reply['usage']}
                        except BaseException:
                            gate.preserve_unknown(rid); stop.set(); raise
                        finally:
                            with mutex: active.pop(rid,None)
                def snapshot(final=False):
                    now=time.time()
                    with mutex: current=dict(active)
                    recent=[e for e in events if now-e.get('ended_epoch',0)<=60]
                    recent5=[e for e in events if now-e.get('ended_epoch',0)<=300]
                    value={'kind':'CB_TRAIN_PREFETCH_DIRECT_V4_NOT_FORMAL_EX','pid':os.getpid(),'planned':1787,
                        'dispatched':len(lease.reserved),'terminal':len(lease.terminals),'active':current,
                        'inflight':len(current),'peak_client_inflight':peak,'global_cap':128,'current_dispatch_target':limit,
                        'completed_last_60s':len(recent),'completed_last_300s':len(recent5),
                        'completed_output_tokens_per_wall_second_60s':sum(e.get('completion_tokens') or 0 for e in recent)/60,
                        'pure_decode_TPS':None,'server_queue':None,'state':'TERMINATED' if final else 'RUNNING',
                        'updated_epoch':now,'started_epoch':started,'stop_reason':reason,'budget':lease.status(),
                        'complete_outputs':sum(e.get('complete',False) for e in events),
                        'infrastructure_failures':sum(e.get('infrastructure_failure',False) for e in events),
                        'unknown':sum(e.get('unknown',False) for e in events),'old_results_unchanged':True}
                    atomic(RUN/'state.json',value); return value
                iterator=iter(rows); exhausted=False
                with ThreadPoolExecutor(max_workers=128) as executor:
                    futures={}
                    while futures or not exhausted:
                        while not exhausted and not stop.is_set() and len(futures)<limit:
                            row=next(iterator,None)
                            if row is None: exhausted=True; break
                            futures[executor.submit(work,row)]=row
                        if stop.is_set(): exhausted=True
                        if not futures: break
                        done,_=wait(futures,timeout=.5,return_when=FIRST_COMPLETED)
                        values=[]
                        for future in done:
                            row=futures.pop(future)
                            try: value=future.result()
                            except BaseException as exc:
                                value={'rid':row['request_id'],'unknown':True,'error':type(exc).__name__,'ended_epoch':time.time()}
                            if not value.get('undispatched'): values.append(value)
                        for value in sorted(values,key=lambda v:v.get('ended_epoch',0)):
                            events.append(value)
                            if value.get('unknown'): reason='Unknown outcome; stopped and drained, never resent'; stop.set()
                            elif value.get('infrastructure_failure'):
                                infra+=1
                                if infra>=3: reason='Three consecutive infrastructure failures'; stop.set()
                            else: infra=0
                        if stage_epoch is None and first_dispatch is not None:stage_epoch=first_dispatch
                        stage_rows=events[stage_baseline:]
                        ready=(stage==0 and len(stage_rows)>=2 and all(v.get('http_status')==200 and not v.get('infrastructure_failure') for v in stage_rows[-2:]))
                        if stage in (1,2) and stage_epoch is not None:
                            required=16 if stage==1 else 32
                            recent=[v for v in stage_rows if time.time()-v.get('ended_epoch',0)<=30]
                            ready=(time.time()-stage_epoch>=30 and len(stage_rows)>=required and recent and not any(v.get('infrastructure_failure') or v.get('unknown') for v in recent))
                        if ready and stage<3 and not stop.is_set():
                            previous=limit;stage+=1;limit=(2,32,64,128)[stage];stage_epoch=time.time();stage_baseline=len(events)
                            immutable(RUN/('ramp_to_'+str(limit)+'.json'),{'epoch':stage_epoch,'terminals':len(events),'initial':previous,'new':limit,'clock_origin':'actual_model_dispatch_not_budget_inspection'})
                        snapshot()
                final=snapshot(True); immutable(RUN/'outcomes.json',events)
                values=sorted(e['seconds'] for e in events if 'seconds' in e)
                def q(p):
                    if not values:return None
                    v=(len(values)-1)*p; lo=int(v); hi=min(lo+1,len(values)-1)
                    return values[lo]+(values[hi]-values[lo])*(v-lo)
                immutable(RUN/'summary.json',dict(final,wall_seconds=time.time()-started,
                    latency_mean_seconds=sum(values)/len(values) if values else None,
                    latency_P50_seconds=q(.5),latency_P90_seconds=q(.9),
                    known_prompt_tokens=sum(e.get('prompt_tokens') or 0 for e in events),
                    known_completion_tokens=sum(e.get('completion_tokens') or 0 for e in events),
                    unknown_usage_count=sum(e.get('completion_tokens') is None for e in events),
                    monetary_cost=None,not_formal_scores=True,not_second_backbone_result=True))
    finally:
        immutable(RUN/'runner_exit.json',{'pid':os.getpid(),'ended_epoch':time.time(),
            'summary_exists':(RUN/'summary.json').exists(),'no_automatic_restart':True})

if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('action',choices=('prepare','run')); a=p.parse_args()
    prepare() if a.action=='prepare' else run()
