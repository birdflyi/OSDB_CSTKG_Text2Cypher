"""Transport-only alternate client for the unchanged frozen native worker."""
import hashlib
import json
from pathlib import Path
from queue import Queue, Empty
import re
import shlex
from threading import Thread, Lock
import time

import paramiko

ROOT='/mnt/data/louzehua/ch7_t1_public_benchmark_20261023'
HOST='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'


class ParamikoPublicWorker:
    def __init__(self,key,graph,log,remote):
        self.log=Path(log).open('x',encoding='utf-8')
        self.lock=Lock();self.queue=Queue();self.client=paramiko.SSHClient()
        self.channel=None
        self.client.load_host_keys(str(Path.home()/'.ssh/known_hosts'))
        self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            self.client.connect('117.50.195.94',port=2491,username='root',key_filename=str(key),
                timeout=12,banner_timeout=20,auth_timeout=20,allow_agent=False,look_for_keys=False)
            self.client.get_transport().set_keepalive(15)
            _,stdout,stderr=self.client.exec_command('hostname; cat '+ROOT+'/execution_owner.json',timeout=15)
            lines=stdout.read().decode('utf-8').splitlines()
            if stdout.channel.recv_exit_status()!=0 or lines[0]!=HOST or json.loads(lines[1])['hostname']!=HOST:
                raise ValueError('Unexpected remote owner/hostname')
            if not re.fullmatch(r'public_query_[a-z0-9_]+',remote['code_folder']):
                raise ValueError('Unexpected original worker folder')
            folder=ROOT+'/'+remote['code_folder']
            with self.client.open_sftp() as sftp:
                for name,expected in remote['files'].items():
                    if not re.fullmatch(r'[a-z_]+\.py',name):raise ValueError('Unexpected dependency name')
                    with sftp.open(folder+'/'+name,'rb') as stream:
                        actual=hashlib.sha256(stream.read()).hexdigest()
                    if actual!=expected:raise ValueError('Frozen remote dependency changed')
            command=shlex.join([ROOT+'/.venv/bin/python',folder+'/public_query_worker.py',
                               '--root',ROOT,'--graph',graph])
            self.stdin,self.stdout,self.stderr=self.client.exec_command(command,get_pty=False)
            self.channel=self.stdout.channel
            def reader():
                try:
                    for raw in self.stdout:
                        self.queue.put(raw.decode('utf-8') if isinstance(raw,bytes) else raw)
                except Exception as error:
                    with self.lock:
                        self.log.write(json.dumps({'reader_error':type(error).__name__})+'\n');self.log.flush()
                finally:self.queue.put(None)
            def errors():
                try:
                    for raw in self.stderr:
                        text=raw.decode('utf-8',errors='replace') if isinstance(raw,bytes) else raw
                        with self.lock:self.log.write(text);self.log.flush()
                except Exception:pass
            self.reader=Thread(target=reader,daemon=True);self.reader.start()
            self.error_reader=Thread(target=errors,daemon=True);self.error_reader.start()
            self.ready=self.receive(300)
            if self.ready.get('kind')!='READY' or self.ready.get('graph')!=graph:
                raise RuntimeError('Wrong native worker identity')
        except Exception:
            self.close();raise

    def receive(self,timeout=125):
        try:line=self.queue.get(timeout=timeout)
        except Empty as error:raise RuntimeError('Native worker timed out; retain intent') from error
        if line is None:raise RuntimeError('Native worker exited; retain intent')
        return json.loads(line)

    def request(self,value):
        started=time.monotonic()
        self.stdin.write((json.dumps(value,ensure_ascii=False)+'\n').encode('utf-8'))
        self.stdin.flush()
        result=self.receive()
        result['worker_roundtrip_seconds']=time.monotonic()-started
        return result

    def execute(self,query,parameters):
        return self.request({'query':query,'parameters':parameters})

    def close(self):
        try:
            if self.channel:
                try:self.channel.shutdown_write()
                except Exception:pass
                deadline=time.monotonic()+45
                while not self.channel.exit_status_ready() and time.monotonic()<deadline:
                    time.sleep(.1)
                if not self.channel.exit_status_ready():
                    with self.lock:
                        self.log.write('{"close_timeout":true,"needs_remote_audit":true}\n');self.log.flush()
        finally:
            self.client.close()
            for thread in (getattr(self,'reader',None),getattr(self,'error_reader',None)):
                if thread:thread.join(timeout=1)
            if not self.log.closed:self.log.close()
