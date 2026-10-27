"""Separate read-only publisher, not a model runner or a keepalive workload."""
from pathlib import Path
import base64,json,os,time,hashlib
import paramiko

R=Path('D:/ch7'); RUN=R/'experiments/cb_train_schema_prefetch_direct_v3_20261027'
REMOTE='/mnt/data/0/louzehua/ch7_t1_ling300_monitor_v1_20261027'
EXPECTED='interactive-zk06dlgpxt9z-5c484c5f4-vh46c'
KEY='C:/Users/10651/.codex/private/ch7_ssh_node3107_20261026/interactive_3107.key'

def main():
    claim=RUN/'monitor_publisher_claim.json'
    with claim.open('x',encoding='utf-8') as f:json.dump({'pid':os.getpid(),'role':'READ_ONLY_MONITOR_NO_MODEL'},f)
    ssh=paramiko.SSHClient();ssh.load_host_keys(str(Path.home()/'.ssh/known_hosts'))
    ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    ssh.connect('117.50.195.94',port=3107,username='root',key_filename=KEY,
        look_for_keys=False,allow_agent=False,timeout=10,auth_timeout=15,banner_timeout=15)
    ssh.get_transport().set_keepalive(20)
    sftp=ssh.open_sftp()
    try:
        _,out,err=ssh.exec_command('hostname',timeout=10);assert out.read().decode().strip()==EXPECTED
        old=json.loads(sftp.open('/mnt/data/0/louzehua/ch7_cb_train_schema_prefetch_v1_20261027/execution_owner.json').read())
        assert old['hostname']==EXPECTED
        sftp.mkdir(REMOTE,mode=0o700)
        with sftp.open(REMOTE+'/execution_owner.json','x') as f:
            f.write(json.dumps({'hostname':EXPECTED,'role':'READ_ONLY_MONITOR','controller_pid':os.getpid(),'run':str(RUN)}))
        source=Path(__file__).with_name('cb_ling300_chinese_monitor_20261027.py')
        sftp.put(str(source),REMOTE+'/board.py')
        def publish():
            data=(RUN/'state.json').read_bytes()
            with sftp.open(REMOTE+'/state.next','wb') as f:f.write(data)
            sftp.posix_rename(REMOTE+'/state.next',REMOTE+'/state.json')
            state=json.loads(data)
            path=R/'state/T1_BACKGROUND_PLAN_20261027.json';plan=json.loads(path.read_text(encoding='utf-8'))
            plan['runtime_progress']=state;plan['budget_snapshot']=state['budget']
            plan['model_runner']['active']=state['state']!='TERMINATED'
            plan['monitor']={'session':'ch7-t1-ling300','remote_root':REMOTE,'publisher_pid':os.getpid(),'model_calls':0}
            temp=path.with_suffix('.monitor-writing');temp.write_text(json.dumps(plan,ensure_ascii=False,indent=2),encoding='utf-8');os.replace(temp,path)
            return state
        publish()
        command="""import subprocess,sys,json
root=sys.argv[1]
assert subprocess.run(['python3','-c','import rich']).returncode==0
session='ch7-t1-ling300'
base='env LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUTF8=1 python3 '+root+'/board.py '+root+' '
subprocess.run(['tmux','new-session','-d','-s',session,'-n','global',base+'global'],check=True)
subprocess.run(['tmux','new-window','-t',session,'-n','cases',base+'cases'],check=True)
print(json.dumps({'session':session,'model_calls':0}))
"""
        call="python3 -c \"import base64;exec(base64.b64decode('"+base64.b64encode(command.encode()).decode()+"'))\" "+REMOTE
        _,out,err=ssh.exec_command(call,timeout=15);receipt=out.read();error=err.read();code=out.channel.recv_exit_status()
        if code:raise RuntimeError('Own monitor deployment failed: '+error.decode(errors='replace')[:300])
        (RUN/'monitor_deployment.json').write_bytes(receipt)
        failures=0
        while True:
            try:
                state=publish();failures=0
                if state['state']=='TERMINATED':break
            except (OSError,ValueError,paramiko.SSHException):
                failures+=1
                if failures>=3:raise RuntimeError('Read-only publisher stopped after three failures; model runner unaffected')
            time.sleep(10)
    finally:
        sftp.close();ssh.close()
        (RUN/'monitor_publisher_exit.json').write_text(json.dumps({'pid':os.getpid(),'epoch':time.time(),'model_calls':0}),encoding='utf-8')

if __name__=='__main__':main()
