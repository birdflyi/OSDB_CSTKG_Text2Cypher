"""One delayed recorded probe, then the already frozen finite queue.

Not a periodic retry loop. Never dispatch until an hour after the last attempt.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import requests


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def latest_terminal(root):
    latest=0
    for slot in (root/'budget').glob('*.json'):
        request=Path(load(slot)['request_path'])
        terminal=(request.with_name('connectivity_probe_01.json') if request.name=='connectivity_probe_01_request.json'
                  else request.with_name('terminal.json'))
        if not terminal.exists():
            raise RuntimeError('Pending attempt: reconcile before any new dispatch')
        latest=max(latest,terminal.stat().st_mtime)
    return latest


def run(a):
    root=Path(a.root).resolve()
    if (root/'remaining_queue_started.json').exists():raise FileExistsError('Remaining queue already started')
    started=root/'delayed_transport_started.json'
    with started.open('x',encoding='utf-8') as f:
        json.dump({'pid':os.getpid(),'started_utc':datetime.now(timezone.utc).isoformat(),
            'policy':'Exactly one probe after at least3600seconds idle; optional single frozen queue; no periodic retries.'},f,indent=2)
    def state(value):
        value.update(pid=os.getpid(),updated_utc=datetime.now(timezone.utc).isoformat())
        temp=root/'delayed_transport_status.tmp'
        temp.write_text(json.dumps(value,indent=2),encoding='utf-8')
        temp.replace(root/'delayed_transport_status.json')
    while True:
        remaining=latest_terminal(root)+3600-time.time()
        if remaining<=0:break
        state({'status':'WAITING_ONE_HOUR_IDLE','seconds_remaining':round(remaining)})
        time.sleep(min(30,max(1,remaining)))
    for filename in ('development_queue_status.json','recovery_queue_status.json','source_queue_status.json'):
        status=load(root/filename)['status']
        if 'RUNNING' in status or 'WAITING' in status:
            raise RuntimeError('Other model queue may be active')
    if (root/'remaining_queue_started.json').exists():raise FileExistsError('Remaining queue already started')
    folder=root/'connectivity_probe_03'
    if folder.exists():raise FileExistsError('Probe03 already exists; no repeat')
    engine=Path(a.engine_root).resolve()
    sys.path.insert(0,str(engine/'publication_benchmark'))
    import run_protocol_control as control
    import run_baselines as b
    cfg=load(a.config)
    payload={k:cfg[k] for k in ('model','temperature','top_p','reasoning_effort')}
    payload.update(max_tokens=6000,messages=[
        {'role':'system','content':'Return only the JSON object {"ok":true}. Do not add other text.'},
        {'role':'user','content':'Connectivity check.'}])
    request=folder/'request.json'
    slot=control.reserve(root/'budget',request)
    b.save(request,payload)
    state({'status':'RUNNING_SINGLE_PROBE','budget_slot':slot})
    begin=time.monotonic()
    try:
        response=requests.post(cfg['endpoint'],headers={'Authorization':'Bearer '+cfg['api_key']},
            json=payload,timeout=(10,120),allow_redirects=False)
        try:body=response.json()
        except ValueError:body={'non_json_body':response.text}
        terminal={'http_status':response.status_code,'body':body,'seconds':time.monotonic()-begin}
    except requests.RequestException as exc:
        terminal={'transport_error':type(exc).__name__,'seconds':time.monotonic()-begin}
    b.save(folder/'terminal.json',terminal)
    choices=terminal.get('body',{}).get('choices') or []
    success=(terminal.get('http_status')==200 and bool(choices) and choices[0].get('finish_reason')=='stop'
             and isinstance(choices[0].get('message',{}).get('content'),str))
    if not success:
        state({'status':'STOPPED_PROBE_FAILED','http_status':terminal.get('http_status'),
            'budget_slot':slot,'next_action':'Offline work; later low-frequency probe only. No queue dispatched.'})
        return
    state({'status':'RUNNING_ALREADY_FROZEN_QUEUE','budget_slot':slot})
    with (root/'remaining_queue_console.log').open('x',encoding='utf-8') as log:
        code=subprocess.call([sys.executable,str(Path(__file__).with_name('run_frozen_remaining_queue.py')),
            '--root',str(root),'--engine-root',str(engine),'--config',a.config,
            '--probe-terminal',str(folder/'terminal.json')],cwd=engine,stdout=log,stderr=subprocess.STDOUT)
    result=load(root/'remaining_queue_status.json') if (root/'remaining_queue_status.json').exists() else {}
    state({'status':'FROZEN_QUEUE_EXITED','exit_code':code,'queue_status':result.get('status')})


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('root','engine-root','config'):parser.add_argument('--'+key,required=True)
    run(parser.parse_args())
