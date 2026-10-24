"""Audited SIGINT of the exact abandoned company worker. Never clear its lock."""
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess

remote=r'''
import json,os,signal,socket,time
from pathlib import Path
root=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
assert socket.gethostname()=='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
assert json.loads((root/'execution_owner.json').read_text())['hostname']==socket.gethostname()
lock=root/'.public-runtime.lock';before=json.loads(lock.read_text())
assert before=={'pid':198329,'graph':'company','state':'PUBLIC_QUERY_WORKER'}
argv=(Path('/proc/198329/cmdline').read_bytes().split(b'\0'))
assert b'--graph' in argv and argv[argv.index(b'--graph')+1]==b'company'
assert str(root/'public_query_core_v1_code/public_query_worker.py').encode() in argv
assert str(root).encode()==argv[argv.index(b'--root')+1]
wchan=Path('/proc/198329/wchan').read_text().strip()
assert 'pipe' in wchan,'Worker not idle in its abandoned input pipe'
print(json.dumps({'state':'OWNED_ORPHAN_VERIFIED','pid':198329,'java_pid':198344,'lock':before,'wchan':wchan}),flush=True)
os.kill(198329,signal.SIGINT)
for _ in range(45):
    if not Path('/proc/198329').exists() and not Path('/proc/198344').exists() and not lock.exists():break
    time.sleep(1)
assert not Path('/proc/198329').exists(),'Worker did not stop within bounded wait'
assert not Path('/proc/198344').exists(),'Owned database did not stop'
assert not lock.exists(),'Lock remains; do not remove it automatically'
with socket.socket() as check:check.bind(('127.0.0.1',17688))
print(json.dumps({'state':'ORPHAN_STOPPED_BY_OWN_FINALLY','public_port_free':True,'lock_removed_by_worker':True,'manual_lock_unlink':False,'model_calls':0}),flush=True)
'''
encoded=base64.b64encode(remote.encode()).decode()
command=['ssh','-i',r'C:\Users\10651\.codex\private\ch7_ssh_20261023\interactive_2491.key',
         '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94',
         'python3 -c "import base64;exec(base64.b64decode(\''+encoded+'\'))"']
result=subprocess.run(command,capture_output=True,text=True,encoding='utf-8',timeout=65)
receipt=dict(timestamp_utc=datetime.now(timezone.utc).isoformat(),action='SIGINT_EXACT_VERIFIED_IDLE_ORPHAN',
             target_pid=198329,returncode=result.returncode,stdout=result.stdout,stderr=result.stderr,
             automatic_lock_removal=False,new_model_calls=0)
Path(r'D:\ch7\reports\t1_company_orphan_stop_receipt_20261025.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
print(json.dumps(receipt))
raise SystemExit(result.returncode)
