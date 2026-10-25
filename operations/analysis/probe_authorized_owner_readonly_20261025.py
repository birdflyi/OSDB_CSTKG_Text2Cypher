"""One low-frequency read-only SSH check. No model/query/upload/restart/retry."""
from datetime import datetime
import base64
import json
from pathlib import Path
import shlex
import socket
import time
import paramiko

ROOT='/mnt/data/louzehua/ch7_t1_public_benchmark_20261023'
HOST='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
script='''import json,os,socket
from pathlib import Path
root=Path("/mnt/data/louzehua/ch7_t1_public_benchmark_20261023")
owner=json.loads((root/"execution_owner.json").read_text())
workers=[]
for p in Path("/proc").iterdir():
    if not p.name.isdigit() or int(p.name)==os.getpid():continue
    try:command=(p/"cmdline").read_bytes().replace(b"\\0",b" ").decode(errors="replace")
    except (OSError,PermissionError):continue
    if str(root) in command:
        for name in ["public_query_worker.py","run_frozen_main.py","durable_native_journal_v1.py"]:
            if name in command:workers.append({"pid":int(p.name),"kind":name});break
print(json.dumps({"hostname":socket.gethostname(),"owner_hostname":owner.get("hostname"),
"public_runtime_lock_present":(root/".public-runtime.lock").exists(),"matching_workers":workers}))
'''

def main():
    client=paramiko.SSHClient()
    client.load_host_keys(str(Path.home()/'.ssh/known_hosts'))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    started=time.monotonic()
    report={'kind':'SINGLE_READONLY_AUTHORIZED_REMOTE_OWNER_PROBE',
        'time_local':datetime.now().astimezone().isoformat(),'attempts':1,
        'new_model_calls':0,'new_database_queries':0,'remote_files_written':0,
        'queue_started':False,'retry':False,'model_service_health_tested':False}
    try:
        client.connect('117.50.195.94',port=2491,username='root',
            key_filename='C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
            timeout=12,banner_timeout=12,auth_timeout=12,allow_agent=False,look_for_keys=False)
        encoded=base64.b64encode(script.encode()).decode()
        command=shlex.join(['python3','-c',"import base64;exec(base64.b64decode('"+encoded+"'))"])
        _,out,err=client.exec_command(command,timeout=20)
        response=out.read()
        if out.channel.recv_exit_status()!=0:raise RuntimeError('Remote inspection failed')
        value=json.loads(response)
        assert value['hostname']==HOST and value['owner_hostname']==HOST,'Unexpected host/owner'
        report.update(pass_owner_check=True,observation=value)
    except Exception as error:
        report.update(pass_owner_check=False,error_type=type(error).__name__)
    finally:client.close()
    report['seconds']=round(time.monotonic()-started,3)
    target=Path('D:/ch7/reports/t1_readonly_remote_owner_probe_20261025_1200.json')
    target.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))

if __name__=='__main__':main()
