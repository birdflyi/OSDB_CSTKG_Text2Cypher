"""Asynchronous file-only monitor publisher; never holds model/DB permits."""
from copy import deepcopy
from pathlib import Path
import json,os,threading,time,subprocess
from osdb_scoped_attempts_dev import wire


def atomic(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.writing');temporary.write_bytes(wire(value));os.replace(temporary,path)


class Publisher:
    def __init__(self,directory,ssh,remote_root,plan_path,issue_path,questions=None):
        self.directory=Path(directory);self.ssh=list(ssh);self.remote_root=remote_root
        self.plan_path=Path(plan_path);self.issue_path=Path(issue_path)
        self.lock=threading.Lock();self.cached=None;self.phase='准备开发入口';self.stop=threading.Event()
        self.errors=0;self.thread=threading.Thread(target=self.loop,daemon=True)
        self.questions=questions or {}
    def start(self):self.thread.start()
    def set_phase(self,phase):self.phase=phase
    def offer(self,state):
        state=deepcopy(state)
        for row in state.get('active_paths',[]):row['question']=self.questions.get(row['path_key'],'')
        with self.lock:self.cached=state
    def loop(self):
        last_remote=0
        while not self.stop.wait(3):
            with self.lock:state=deepcopy(self.cached)
            if state is None:continue
            state['heartbeat_epoch']=time.time();state['runtime_phase']=self.phase
            if self.phase:state['state']=self.phase
            try:
                atomic(self.directory/'live_state.json',state)
                plan=json.loads(self.plan_path.read_text(encoding='utf-8'))
                plan['runtime_progress']={k:state.get(k) for k in ('heartbeat_epoch','runtime_phase','current_graph','initial_paths_dispatched','paths_completed','undispatched_paths','model_requests_inflight','budget','stop_reason')}
                atomic(self.plan_path,plan)
                if time.monotonic()-last_remote>=15:
                    last_remote=time.monotonic()
                    q=subprocess.run(self.ssh+['python3 '+self.remote_root+'/publish_state.py'],input=wire(state),capture_output=True,timeout=10)
                    if q.returncode:self.errors+=1
            except Exception:self.errors+=1
    def close(self):
        self.stop.set();self.thread.join(timeout=12)
        with self.lock:state=deepcopy(self.cached)
        if state is not None:
            state['runtime_phase']=self.phase;state['heartbeat_epoch']=time.time()
            atomic(self.directory/'live_state.json',state)
            try:subprocess.run(self.ssh+['python3 '+self.remote_root+'/publish_state.py'],input=wire(state),capture_output=True,timeout=10)
            except Exception:self.errors+=1
