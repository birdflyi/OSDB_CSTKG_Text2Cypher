"""Private-prefix Ubuntu Git extraction and finite preflight; no global install."""
import base64
import json
from pathlib import Path
import shlex
import sys

sys.path.insert(0,'D:/ch7/code/OSDB_CSTKG_Text2Cypher_t1_recovery/operations')
from journal_public_worker_v2 import JournalPublicWorker,ROOT,HOST

SCRIPT='''from pathlib import Path
import hashlib,json,os,socket,subprocess,shutil
root=Path("/mnt/data/louzehua/ch7_t1_public_benchmark_20261023")
host="interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj"
assert socket.gethostname()==host and json.loads((root/'execution_owner.json').read_text())['hostname']==host
assert not (root/'.public-runtime.lock').exists() and not (root/'.t1-cutover-preflight.lock').exists()
old=root/'remote_cutover_preflight_v1_20261025'
prior=json.loads((old/'result.json').read_text())
assert prior['pass'] is False and prior['error_type']=='FileNotFoundError' and "'git'" in prior['error']
assert prior['native_child_closed'] and prior['public_runtime_lock_absent']
assert not (old/'native_ipc').exists()
os.umask(0o077)
tool=root/'private_git_noble_20261025';tool.mkdir()
download=subprocess.run(['apt-get','download','git=1:2.43.0-1ubuntu7.4'],cwd=tool,capture_output=True,text=True,timeout=45)
(tool/'download.log').write_text(download.stdout+download.stderr)
download.check_returncode()
debs=list(tool.glob('*.deb'));assert len(debs)==1
expected='c79b7948ef9860c97ec74293a90178676e63368a0711c95c28a55af4edf1adab'
assert hashlib.sha256(debs[0].read_bytes()).hexdigest()==expected
prefix=tool/'extracted';subprocess.run(['dpkg-deb','-x',str(debs[0]),str(prefix)],check=True,timeout=15)
env=dict(os.environ,PATH=str(prefix/'usr/bin')+':'+os.environ['PATH'],GIT_EXEC_PATH=str(prefix/'usr/lib/git-core'),GIT_TEMPLATE_DIR=str(prefix/'usr/share/git-core/templates'))
version=subprocess.check_output(['git','--version'],env=env,text=True).strip()
install={'package':'git','ubuntu_version':'1:2.43.0-1ubuntu7.4','package_sha256':expected,'version':version,'private_prefix':str(prefix),'global_packages_changed':False,'gpu_or_model_services_changed':False}
(tool/'installation.json').write_text(json.dumps(install,indent=2))
base=root/'remote_cutover_preflight_v2_20261025';base.mkdir()
spec=json.loads((old/'spec.json').read_text())
names=['packet.zip','full_history.zip','source.bundle','fixture.json','spec.json']+list(spec['operations_sha256'])
for name in names:
    assert Path(name).name==name
    shutil.copy2(old/name,base/name)
    assert hashlib.sha256((old/name).read_bytes()).digest()==hashlib.sha256((base/name).read_bytes()).digest()
log=(base/'job.log').open('xb')
p=subprocess.Popen([str(root/'.venv/bin/python'),str(base/'native_cutover_preflight_job.py'),'--base',str(base)],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,env=env)
value={'git_installation':install,'launched':{'pid':p.pid,'base':str(base)},'prior_failed_preflight_preserved':True,'new_model_calls':0,'formal_predictions':0,'private_model_configuration_uploaded':False}
(base/'launch.json').write_text(json.dumps(value,indent=2))
print(json.dumps(value))
'''

def main():
    w=JournalPublicWorker.__new__(JournalPublicWorker)
    w.key=Path('C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key')
    client=w.connect()
    try:
        enc=base64.b64encode(SCRIPT.encode()).decode()
        command=shlex.join(['python3','-c',"import base64;exec(base64.b64decode('"+enc+"'))"])
        _,out,err=client.exec_command(command,timeout=60)
        result=out.read();error=err.read();status=out.channel.recv_exit_status()
        if status:
            Path('D:/ch7/reports/t1_private_git_prepare_failure_20261025.log').write_bytes(error)
            raise RuntimeError('Private-prefix Git preparation failed; inspect saved log, do not blindly repeat')
        value=json.loads(result)
        Path('D:/ch7/reports/t1_private_git_and_preflight_v2_launch_20261025.json').write_text(json.dumps(value,indent=2),encoding='utf-8')
        print(json.dumps(value))
    finally:client.close()

if __name__=='__main__':main()
