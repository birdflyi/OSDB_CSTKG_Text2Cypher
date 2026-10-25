"""Read prepared dependency receipt and run only reviewed --version exit path."""
from pathlib import Path
from datetime import datetime
import json,subprocess

BASE=Path('D:/ch7/reports/text2gql_native_engine_20261026')
code=r'''
from pathlib import Path
from datetime import datetime
import os,platform,json,subprocess,resource
root=Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
assert platform.node()==json.loads((root/'execution_owner.json').read_text())['hostname']
assert not (root/'.public-runtime.lock').exists()
prefix=root/'text2gql_native_v452_prepare_20261026'
receipt=json.loads((prefix/'local_transfer_preparation_receipt.json').read_text())
assert receipt['phase']=='DEPENDENCIES_RESOLVED_NO_SERVER_STARTED'
assert not Path('/proc/'+str(receipt['pid'])).exists(), 'Previous helper still active or pid reused'
resource.setrlimit(resource.RLIMIT_AS,(4*1024**3,4*1024**3))
resource.setrlimit(resource.RLIMIT_CPU,(15,15))
env=dict(os.environ,LD_LIBRARY_PATH=str(prefix/'lib'),OMP_NUM_THREADS='2',OPENBLAS_NUM_THREADS='2')
result=subprocess.run([str(prefix/'bin/lgraph_server'),'--version'],cwd=prefix,env=env,
                      capture_output=True,text=True,timeout=20)
record={'time_iso':datetime.now().astimezone().isoformat(),'hostname':platform.node(),
        'version_returncode':result.returncode,'version_stdout':result.stdout,'version_stderr':result.stderr,
        'server_launch_mode':False,'new_database_queries':0,'new_model_calls':0,
        'runtime_preparation':receipt,'main_runtime_lock_absent':not (root/'.public-runtime.lock').exists()}
(prefix/'version_inspection_receipt.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record))
'''
run=subprocess.run(['ssh','-i','C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
    '-p','2491','-o','BatchMode=yes','-o','ConnectTimeout=15','root@117.50.195.94','python3 -'],
    input=code,capture_output=True,text=True,timeout=40)
assert run.returncode==0, run.stderr[:400]
record=json.loads(run.stdout)
(BASE/'remote_runtime_version_receipt.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'time_local':datetime.now().astimezone().isoformat(),
    'phase':record['runtime_preparation']['phase'],'version_returncode':record['version_returncode'],
    'stdout':record['version_stdout'],'stderr':record['version_stderr'],
    'files':len(record['runtime_preparation']['files']),'server_started':False},ensure_ascii=True))
