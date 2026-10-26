"""Create only this task's isolated tmux server; never change other services."""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import sys


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--hostname',required=True)
    p.add_argument('--previous-root')
    a=p.parse_args();root=Path(a.root).resolve()
    if socket.gethostname()!=a.hostname:raise RuntimeError('Execution owner hostname mismatch')
    owner=json.loads((root/'execution_owner.json').read_text())
    if owner['hostname']!=socket.gethostname() or owner['purpose']!='T1 monitoring and TRAIN capacity only':
        raise RuntimeError('Wrong isolated operations owner')
    tmux=['tmux','-L','ch7-t1-monitor']
    exists=subprocess.run(tmux+['has-session','-t','ch7-t1'],capture_output=True).returncode==0
    if exists:
        roots=[str(root)]
        if a.previous_root:
            previous=Path(a.previous_root).resolve();old=json.loads((previous/'execution_owner.json').read_text())
            if old['hostname']!=socket.gethostname() or old['purpose']!='T1 monitoring and TRAIN capacity only':
                raise RuntimeError('Previous monitor owner does not match; preserve')
            roots.append(str(previous))
        starts=subprocess.check_output(tmux+['list-panes','-s','-t','ch7-t1','-F','#{pane_start_command}'],text=True)
        if len(starts.splitlines())!=2 or not all(any(r+'/code/monitor.py' in line for r in roots) for line in starts.splitlines()):
            raise RuntimeError('Existing tmux panes not owned by this task; preserve')
    script=root/'code/monitor.py';state=root/'monitor/state.json'
    def command(view):
        import shlex
        return ' '.join(shlex.quote(v) for v in [sys.executable,str(script),'--state',str(state),'--view',view])
    if exists:
        subprocess.run(tmux+['respawn-window','-k','-t','ch7-t1:0',command('pipeline')],check=True)
        subprocess.run(tmux+['respawn-window','-k','-t','ch7-t1:1',command('job')],check=True)
    else:
        subprocess.run(tmux+['new-session','-d','-s','ch7-t1','-n','全局流水线','-x','160','-y','55',command('pipeline')],check=True)
        subprocess.run(tmux+['new-window','-t','ch7-t1:1','-n','逐题监控',command('job')],check=True)
    subprocess.run(tmux+['set-option','-t','ch7-t1','remain-on-exit','on'],check=True)
    print(json.dumps({'session':'ch7-t1','socket':'ch7-t1-monitor','windows':['0 全局流水线','1 逐题监控']},ensure_ascii=True))


if __name__=='__main__':main()
