"""One idle-gated probe05 and one paced untouched IoTDB source batch."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import requests
from paced_transport import PacedPost
from wait_for_transport_and_continue import latest_terminal


def load(path):return json.loads(Path(path).read_text(encoding='utf-8'))


def run(a):
    root=Path(a.root).resolve();engine=Path(a.engine_root).resolve()
    out=root/'e7_source_iotdb_v1'
    if out.exists():raise FileExistsError('Untouched IoTDB batch already exists')
    with (root/'paced_admission_started.json').open('x',encoding='utf-8') as f:
        json.dump({'pid':os.getpid(),'started_utc':datetime.now(timezone.utc).isoformat(),
            'policy':'One later probe, one fresh paced IoTDB source batch; no model/task/scoring changes, no automatic next batch.'},f,indent=2)
    def state(value):
        value.update(pid=os.getpid(),updated_utc=datetime.now(timezone.utc).isoformat())
        temp=root/'paced_admission_status.tmp';temp.write_text(json.dumps(value,indent=2),encoding='utf-8')
        temp.replace(root/'paced_admission_status.json')
    deadline=time.time()+7200
    while True:
        remaining=latest_terminal(root)+3600-time.time()
        if remaining<=0:break
        if time.time()>deadline:
            state({'status':'STOPPED_IDLE_WAIT_TIMEOUT'});return
        state({'status':'WAITING_ONE_HOUR_IDLE_FOR_PACED_PROBE','seconds_remaining':round(remaining)})
        time.sleep(min(30,max(1,remaining)))
    for folder in ('e7_source_redis_v2','e7_source_duckdb_v1'):
        if load(root/folder/'summary.json')['status']!='STOPPED_INFRASTRUCTURE':
            raise ValueError('Unexpected earlier batch state')
    sys.path.insert(0,str(engine/'publication_benchmark'))
    import run_baselines as b
    import run_protocol_control as control
    import run_information_comparison as comparison
    cfg=load(a.config);probe=root/'connectivity_probe_05'
    if probe.exists():raise FileExistsError('Probe05 already exists')
    payload=load(root/'connectivity_probe_03/request.json')
    if not all(payload[k]==cfg[k] for k in ('model','temperature','top_p','reasoning_effort')):
        raise ValueError('Not the same declared model cohort')
    slot=control.reserve(root/'budget',probe/'request.json');b.save(probe/'request.json',payload)
    state({'status':'RUNNING_SINGLE_PACED_ADMISSION_PROBE','budget_slot':slot})
    started=time.monotonic()
    try:
        response=requests.post(cfg['endpoint'],headers={'Authorization':'Bearer '+cfg['api_key']},json=payload,
            timeout=(10,120),allow_redirects=False)
        try:body=response.json()
        except ValueError:body={'non_json_body':response.text}
        terminal={'http_status':response.status_code,'body':body,'seconds':time.monotonic()-started}
    except requests.RequestException as exc:
        terminal={'transport_error':type(exc).__name__,'seconds':time.monotonic()-started}
    b.save(probe/'terminal.json',terminal)
    choices=terminal.get('body',{}).get('choices') or []
    usable=(terminal.get('http_status')==200 and bool(choices) and choices[0].get('finish_reason')=='stop'
        and isinstance(choices[0].get('message',{}).get('content'),str))
    if not usable:
        state({'status':'STOPPED_PACED_PROBE_FAILED','http_status':terminal.get('http_status')});return
    out.mkdir(parents=True)
    b.save(out/'transport_manifest.json',{'mode':'ONE_UNTOUCHED_SOURCE_BATCH_WITH_DISCLOSED_TRANSPORT_PACING',
        'minimum_request_start_interval_seconds':60,'first_request_wait_after_probe_seconds':60,
        'wrapper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'pacing_module_sha256':hashlib.sha256(Path(__file__).with_name('paced_transport.py').read_bytes()).hexdigest(),
        'probe_terminal_sha256':b.sha(probe/'terminal.json'),'new_operations':0,'new_model_prompt_rules':0,
        'limits':'Capacity/rate hypothesis only; no rate-limit error was observed. Methods/tasks/repair/scoring unchanged. Raw terminal seconds include waiting; separate pacing/network receipts are authoritative. No immediate next batch or retry.'})
    original=comparison.r.requests.post
    pacing=PacedPost(original,cfg['endpoint'],out/'pacing_receipts',interval=60)
    comparison.r.requests.post=pacing
    b.DB_URL='http://127.0.0.1:17477/db/neo4j/tx/commit'
    state({'status':'RUNNING_PACED_IOTDB_SOURCE','output':str(out),'interval_seconds':60})
    try:
        comparison.run(SimpleNamespace(task_dir=str(root/'e7_source_tasks_v1/iotdb'),project='iotdb',
            data=str(root/'e8_iotdb_projection/execution_csv'),receipt=str(root/'e8_iotdb_runtime_v1/import_receipt.json'),
            config=a.config,output=str(out),budget_dir=str(root/'budget'),port=17477,context='compact'))
    except Exception as exc:
        state({'status':'STOPPED_PACED_BATCH_EXCEPTION','exception_class':type(exc).__name__})
        raise
    finally:
        comparison.r.requests.post=original
    summary=load(out/'summary.json')
    state({'status':'PACED_ADMISSION_FINISHED','result_status':summary['status'],
        'actual_calls':summary['actual_calls'],'unknown_usage_calls':summary['unknown_usage_calls']})


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ('root','engine-root','config'):p.add_argument('--'+key,required=True)
    run(p.parse_args())
