"""Local budget authority; capacity requests execute on an explicitly owned node.

Private inputs stay outside Git. Reverse forwarding binds only localhost. Only
presealed TRAIN trial IDs can reserve model attempts. All raw evidence stays in
the local report directory. No resume, automatic model retry, or old queue use.
"""
import argparse
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit

import paramiko

from common import atomic,immutable,read
from parallel_budget import ParallelLease

ROOT=Path('D:/ch7')
REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def workload():
    source=ROOT/'experiments/public_benchmark_v1/dev_validation_deepseek_v4_20261024'
    manifest=read(source/'run_manifest.json')
    assert manifest['kind']=='TRAIN_PREPARATION_ONLY_NOT_MAIN_TABLE' and len(manifest['task_ids'])==12
    rows=[]
    for index,qid in enumerate(manifest['task_ids']):
        for arm in ('direct','ours'):
            path=source/'progress'/f'{index}_{arm}_0.json';record=read(path)
            assert record['qid']==qid and record['arm']==arm and record['views'][0]['step']==0
            request=Path(record['views'][0]['reply']['attempt_folders'][0])/'request.json'
            body=read(request);assert [m['role'] for m in body['messages']]==['system','user']
            context=body['messages'][1]['content'];question=context.split('Question:\n')[-1].split('\n')[0]
            rows.append({'qid':qid,'arm':arm,'messages':body['messages'],'question':question,
                         'source_path':str(request),'source_sha256':sha(request)})
    return rows,{'run_manifest_sha256':sha(source/'run_manifest.json'),'kind':manifest['kind'],
                 'task_file_sha256':manifest['task_file_sha256'],'task_ids':manifest['task_ids']}


def jobs():
    rows=[]
    for benchmark in ('CypherBench','Text2GQL','MindTheQuery','ZOGRASCOPE','OSDB-STKG'):
        for method in ('直接查询+执行反馈','我们的方法（含共享计划控制）'):
            done=benchmark=='CypherBench'
            rows.append({'benchmark':benchmark,'method':method,'state':'已完成' if done else '准备中',
                         'completed':2348 if done else 0,'total':2348 if done else '待冻结',
                         'reason':'原DeepSeek冻结结果，未修改' if done else '待独立准入/协议/预算，不自动预测'})
    for method in ('Cypher-RI','CypherSmith','CYQUARK'):
        rows.append({'benchmark':'作者方法适用范围待核','method':method,'state':'资源待核',
                     'completed':0,'total':'待定','reason':'权重/推理/对应输出尚未验收'})
    return rows


def ssh_client(node):
    client=paramiko.SSHClient();client.load_host_keys(str(Path.home()/'.ssh/known_hosts'))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(node['host'],port=node['port'],username=node['user'],key_filename=node['key_path'],
                   timeout=10,auth_timeout=10,banner_timeout=10,look_for_keys=False,allow_agent=False)
    return client


def remote_python(client, code):
    stdin,stdout,stderr=client.exec_command('python3 -',timeout=30)
    stdin.write(code);stdin.flush();stdin.channel.shutdown_write()
    out=stdout.read().decode('utf-8');error=stderr.read().decode('utf-8');status=stdout.channel.recv_exit_status()
    if status:raise RuntimeError('Remote preparation failed: '+error[:400])
    return out


def optional_monitor_deploy(client, launch):
    try:
        stdin,stdout,stderr=client.exec_command(launch,timeout=30)
        result=stdout.read().decode();error=stderr.read().decode()
        if stdout.channel.recv_exit_status()!=0:raise RuntimeError('Monitor deployment failed')
        return json.loads(result)
    except Exception as exc:
        # Monitoring is an independent consumer, never a prediction prerequisite.
        return {'monitor_deployed':False,'error_type':type(exc).__name__,
                'experiment_continues':True,'no_model_retry':True}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--node-config',required=True)
    parser.add_argument('--model-config',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();node=read(args.node_config);config=read(args.model_config);out=Path(args.output)
    if out.exists():raise RuntimeError('Existing coordinator output must be audited, never resumed')
    out.mkdir(parents=True);source_dir=Path(__file__).parent
    assert config['model']=='ling-3.0-flash-n72-75'
    pack,lineage=workload();stages=[];expected={}
    stage_plan=node.get('capacity_stage_plan') or [('baseline1',1,24),('c16',16,96),('c32',32,192),('c64',64,384),('c128',128,768),('refine48',48,288)]
    dispatch_seconds=node.get('stage_dispatch_seconds',240)
    assert 1<=dispatch_seconds<=600
    for name,c,n in stage_plan:
        assert isinstance(c,int) and 1<=c<=128 and isinstance(n,int) and 1<=n<=2048
        if name in node.get('completed_stage_names',[]):continue
        if name=='baseline1' and node.get('accepted_baseline_summary'):
            previous=read(node['accepted_baseline_summary']);baseline=previous['stages'][0]
            if baseline['completed']<12 or baseline['complete_responses']!=baseline['dispatched']:
                raise RuntimeError('Prior baseline is not accepted')
            continue
        trials=[]
        for i in range(n):
            original=pack[i%len(pack)];trial_id=f"{node.get('trial_namespace','direct1')}_{name}_{i:04d}"
            messages=deepcopy(original['messages'])
            messages[-1]['content']+='\nCapacity measurement trial '+trial_id+'. Answer the original graph question normally.'
            body={'model':config['model'],'messages':messages,'temperature':config['temperature'],
                  'top_p':config['top_p'],'reasoning_effort':config['reasoning_effort'],'max_tokens':8192}
            trial={'trial_id':trial_id,'arm':original['arm'],'qid':original['qid'],
                   'question':original['question'],'body':body}
            expected[trial_id]=trial;trials.append(trial)
        stages.append({'name':name,'concurrency':c,'count':n,'trials':trials})
    freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
    assert all(sha(CANON/p)==digest for p,digest in freeze['code_files'].items())
    runtime_before={str(p.relative_to(REPO)):sha(p) for p in (REPO/'experiment-harness/public_benchmark_v1').rglob('*.py')}
    protocol={'kind':'USER_AUTHORIZED_CONCURRENCY_AND_CHINESE_MONITOR_DEVELOPMENT',
        'model':config['model'],'max_calls':len(expected),'stages':[{k:s[k] for k in ('name','concurrency','count')} for s in stages],
        'output_tokens_per_second_target':3500,'target_origin':'User estimate of remaining shared output capacity',
        'runtime_sources_sha256':{p.name:sha(p) for p in source_dir.glob('*.py')},'TRAIN_lineage':lineage,
        'workload_source_request_hashes':[{'qid':p['qid'],'arm':p['arm'],'sha256':p['source_sha256']} for p in pack],
        'same_body_model_attempts':1,'automatic_model_retries':0,'read_timeout_seconds':240,
        'stage_new_dispatch_time_limit_seconds':dispatch_seconds,'infrastructure_failure_stop':3,
        'plateau_gain_threshold':.05,'formal_predictions':0,'graph_queries':0,'monetary_cost':None,
        'scope':'CypherBench TRAIN/client capacity development; not a second-backbone quality comparison or extra benchmark.'}
    protocol['model_network_route']='SSH reverse forwarding via local reachable model gateway' if node.get('model_forward_port') else 'Remote direct gateway'
    if node.get('accepted_baseline_summary'):
        protocol['prior_baseline_summary_sha256']=sha(node['accepted_baseline_summary'])
        protocol['prior_baseline_is_partial_not_matched_full_pack']=True
    seeds=[]
    if node.get('accepted_c16_result'):
        from capacity_worker import response_metrics,steady_metrics
        prior=read(node['accepted_c16_result']);raw=[]
        for path in Path(node['accepted_c16_result']).parent.glob('budget_lease/attempts/*c16*/terminal.json'):
            raw.append(response_metrics(read(path)))
        if len(raw)!=prior['completed']:raise RuntimeError('Prior stage raw/count mismatch')
        prior.update(steady_metrics(raw,16));prior['HTTP200_responses']=sum(r['http_status']==200 for r in raw)
        prior['source_result_sha256']=sha(node['accepted_c16_result']);prior['retained_from_prior_version']=True
        seeds.append(prior);protocol['retained_prior_c16_result_sha256']=prior['source_result_sha256']
    for source in node.get('accepted_stage_results',[]):
        from capacity_worker import response_metrics,steady_metrics
        prior=read(source);raw=[]
        for path in Path(source).parent.glob('budget_lease/attempts/*/terminal.json'):
            if ('_'+prior['name']+'_') in path.parent.name:
                raw.append(response_metrics(read(path)))
        if len(raw)!=prior['completed']:raise RuntimeError('Retained stage raw/count mismatch')
        prior.update(steady_metrics(raw,prior['concurrency']))
        prior['source_result_sha256']=sha(source);prior['retained_from_prior_version']=True
        seeds.append(prior)
    protocol['retained_stage_results']=[{'name':s['name'],'sha256':s['source_result_sha256']} for s in seeds]
    protocol['throughput_selection']='Central completion window after trimming one concurrency-wave per end; all finite batch times and failures also retained. Select smallest cap within5% of best, HTTP200>=99% and complete responses>=95%.'
    immutable(out/'protocol.json',protocol)
    manifest={'execution_hostname':node['hostname'],'protocol':protocol,'stages':stages,'jobs':jobs(),'seed_stages':seeds}
    immutable(out/'manifest.json',manifest)
    token=secrets.token_urlsafe(32);done=threading.Event();summary_box={};stage_results=[];snap_lock=threading.Lock()
    sys.path.insert(0,str(CANON/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    legacy=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
        ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',path_rewrites=[
            (r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])
    client=ssh_client(node);remote=node['root'].rstrip('/')
    result=remote_python(client,"import socket,json\nprint(json.dumps({'hostname':socket.gethostname()}))\n")
    assert json.loads(result)['hostname']==node['hostname']
    # Refuse reuse: this is a new isolated operations owner, not migration of a shared graph owner.
    remote_python(client,"from pathlib import Path\nimport json,socket,os\nr=Path("+repr(remote)+")\n"
        "assert not r.exists(),'Preserve existing operations directory'\nr.mkdir(mode=0o700)\n"
        "(r/'code').mkdir()\n(r/'private').mkdir(mode=0o700)\n"
        "(r/'execution_owner.json').write_text(json.dumps({'hostname':socket.gethostname(),'purpose':'T1 monitoring and TRAIN capacity only'}))\n")
    sftp=client.open_sftp()
    for p in source_dir.glob('*.py'):sftp.put(str(p),remote+'/code/'+p.name)
    sftp.put(str(out/'manifest.json'),remote+'/manifest.json');sftp.chmod(remote+'/manifest.json',0o600)
    with ParallelLease(legacy,out/'budget_lease',len(expected)) as lease:
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*unused):pass
            def setup(self):
                super().setup();self.connection.settimeout(30)
            def do_POST(self):
                try:
                    if not secrets.compare_digest(self.headers.get('X-Run-Token',''),token):
                        self.send_error(403);return
                    size=int(self.headers.get('Content-Length','0'))
                    if not 0<size<4*1024*1024:raise ValueError('Invalid bounded control payload')
                    body=json.loads(self.rfile.read(size))
                    if self.path=='/ping':
                        if body['hostname']!=node['hostname']:raise ValueError('Wrong remote execution owner')
                        reply={'ok':True}
                    elif self.path=='/reserve':
                        trial=expected[body['trial_id']]
                        reply=lease.reserve(trial['trial_id'],trial['body'])
                    elif self.path=='/terminal':reply=lease.terminal(body['trial_id'],body['terminal'])
                    elif self.path=='/snapshot':
                        body['budget']=lease.status()
                        with snap_lock:atomic(out/'state.json',body)
                        reply={'accepted':True,'budget':lease.status()}
                    elif self.path=='/stage':
                        with snap_lock:
                            path=out/(body['name']+'_result.json')
                            if path.exists():
                                if read(path)!=body:raise ValueError('Conflicting stage result')
                            else:immutable(path,body);stage_results.append(body)
                        reply={'accepted':True}
                    elif self.path=='/done':
                        if (out/'summary.json').exists():
                            if read(out/'summary.json')!=body:raise ValueError('Conflicting campaign result')
                        else:immutable(out/'summary.json',body)
                        summary_box.update(body);done.set();reply={'accepted':True}
                    else:raise ValueError('Unknown control route')
                    wire=json.dumps(reply,separators=(',',':')).encode()
                    self.send_response(200);self.send_header('Content-Type','application/json')
                    self.send_header('Content-Length',str(len(wire)));self.end_headers();self.wfile.write(wire)
                except Exception as exc:
                    wire=json.dumps({'error':type(exc).__name__}).encode()
                    self.send_response(409);self.send_header('Content-Length',str(len(wire)));self.end_headers();self.wfile.write(wire)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        server.daemon_threads=True
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        private={k:config[k] for k in ('endpoint','api_key','model')}
        if node.get('model_forward_port'):
            origin=urlsplit(config['endpoint'])
            if origin.scheme!='http':raise RuntimeError('Verified TCP forwarding profile admits HTTP origin only')
            private['endpoint']=f"http://127.0.0.1:{node['model_forward_port']}"+origin.path+('?' + origin.query if origin.query else '')
            private['host_header']=origin.netloc
        private.update(control_port=node['control_port'],control_token=token)
        with sftp.file(remote+'/private/connection.json','w') as stream:stream.write(json.dumps(private))
        sftp.chmod(remote+'/private/connection.json',0o600)
        initial={'updated_epoch':time.time(),'state':'正在启动','capacity':True,'jobs':jobs(),
            'stage_name':'固定TRAIN容量验收','job_id':'固定TRAIN容量验收','model':config['model'],
            'budget':lease.status(),'output_budget':3500,'stages':[]}
        atomic(out/'state.json',initial)
        with sftp.file(remote+'/initial_state.json','w') as stream:stream.write(json.dumps(initial,ensure_ascii=False))
        # monitor reads campaign/state.json; create parent before worker, not worker output itself.
        remote_python(client,"from pathlib import Path\nimport shutil\nr=Path("+repr(remote)+")\n"
            "(r/'monitor').mkdir()\nshutil.copyfile(r/'initial_state.json',r/'monitor/state.json')\n")
        # Dedicated monitor path is separately updated below by the worker.
        command=['ssh','-i',node['key_path'],'-p',str(node['port']),'-o','BatchMode=yes',
            '-o','ConnectTimeout=10','-o','ExitOnForwardFailure=yes','-o','ServerAliveInterval=15',
            '-o','ServerAliveCountMax=3','-R',f"127.0.0.1:{node['control_port']}:127.0.0.1:{server.server_port}"]
        if node.get('model_forward_port'):
            origin=urlsplit(config['endpoint'])
            command+=['-R',f"127.0.0.1:{node['model_forward_port']}:{origin.hostname}:{origin.port or 80}"]
        command+=[node['user']+'@'+node['host'],
            f"python3 {remote}/code/capacity_worker.py --private {remote}/private/connection.json --manifest {remote}/manifest.json --output {remote}/campaign"]
        with (out/'worker_stdout.log').open('w',encoding='utf-8') as stdout,(out/'worker_stderr.log').open('w',encoding='utf-8') as stderr:
            process=subprocess.Popen(command,stdout=stdout,stderr=stderr)
            immutable(out/'coordinator_started.json',{'pid':os.getpid(),'ssh_pid':process.pid,'remote_root':remote,
                'hostname':node['hostname'],'control_localhost_port':server.server_port,'budget_before':lease.before,
                'source_sha256':sha(Path(__file__))})
            # Both monitor windows exist independently of the experiment SSH process.
            import shlex
            launch=' '.join(shlex.quote(x) for x in ['python3',remote+'/code/launch_tmux.py','--root',remote,'--hostname',node['hostname']])
            if node.get('previous_root'):launch+=' --previous-root '+shlex.quote(node['previous_root'])
            immutable(out/'monitor_deployment.json',optional_monitor_deploy(client,launch))
            print('remote TRAIN capacity runner and two Chinese tmux monitors started',flush=True)
            hard_deadline=time.monotonic()+len(stages)*(dispatch_seconds+300)+180
            while not done.wait(3):
                if time.monotonic()>hard_deadline:
                    immutable(out/'coordinator_failure.json',{'kind':'BOUNDED_CAMPAIGN_DEADLINE',
                        'budget':lease.status(),'pending_requires_audit':True,'no_automatic_restart':True})
                    process.terminate()  # Only this coordinator's SSH child.
                    break
                if process.poll() is not None:
                    time.sleep(1)
                    if done.is_set():break
                    immutable(out/'coordinator_failure.json',{'kind':'REMOTE_WORKER_EXIT_BEFORE_DONE',
                        'returncode':process.returncode,'budget':lease.status(),'no_automatic_restart':True})
                    break
            process.wait(timeout=300)
        server.shutdown();server.server_close();sftp.close();client.close()
        immutable(out/'final_reservation_status.json',lease.status())
        assert all(sha(CANON/p)==digest for p,digest in freeze['code_files'].items())
        assert runtime_before=={str(p.relative_to(REPO)):sha(p) for p in (REPO/'experiment-harness/public_benchmark_v1').rglob('*.py')}
    print(json.dumps({'complete':done.is_set(),'recommended_concurrency':summary_box.get('recommended_concurrency'),
        'dispatched':summary_box.get('dispatched'),'stop_reason':summary_box.get('stop_reason')},ensure_ascii=True),flush=True)


if __name__=='__main__':main()
