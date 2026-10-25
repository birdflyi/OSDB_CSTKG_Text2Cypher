"""Local IPC around the ORIGINAL worker. No model, scorer or query rewriting.

Operational candidate: unknown dispatch poisons the channel and is never replayed.
Responses cross the same canonical JSON boundary as the admitted SSH journal.
"""
import hashlib
import json
import os
from pathlib import Path
from queue import Queue, Empty
import re
import socket
import subprocess
from threading import Thread
import time

ROOT = Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
HOST = 'interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
GRAPHS = {'flight_accident','nba','fictional_character','company','geography','movie','politics'}


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def publish(path, value):
    with Path(path).open('xb') as stream:
        stream.write(encode(value))
        stream.flush()
        os.fsync(stream.fileno())


class LocalLineTransport:
    """Generic byte-pipe mechanics; fake subprocesses can test it without a DB."""
    def __init__(self, command, graph, log, *, ready_timeout=300, request_timeout=125):
        self.directory = Path(log).parent / 'native_ipc'
        self.directory.mkdir(exist_ok=False)
        self.log = Path(log).open('xb')
        self.queue, self.sequence = Queue(), 0
        self.broken = self.closed = False
        self.request_timeout = request_timeout
        env=dict(os.environ, PYTHONIOENCODING='utf-8')
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.log, env=env)
        publish(self.directory/'owner.json', {'native_pid':self.process.pid, 'graph':graph,
                                              'command':list(map(str,command))})
        def reader():
            try:
                for line in self.process.stdout:self.queue.put(line)
            finally:self.queue.put(None)
        self.reader=Thread(target=reader, daemon=True)
        self.reader.start()
        try:
            self.ready = self.receive(ready_timeout)
            if self.ready.get('kind')!='READY' or self.ready.get('graph')!=graph:
                raise RuntimeError('Wrong original native worker identity')
            publish(self.directory/'ready.json',self.ready)
        except BaseException:
            self.broken=True
            self.close()
            raise

    def receive(self, timeout):
        try:raw=self.queue.get(timeout=timeout)
        except Empty as exc:
            raise RuntimeError('Native response unknown; audit without replay') from exc
        if raw is None:raise RuntimeError('Native exited; audit without replay')
        # Canonical round trip reproduces old journal RPC's recursive key ordering.
        return json.loads(encode(json.loads(raw)))

    def request(self, value):
        if self.broken or self.closed:
            raise RuntimeError('Native channel unavailable; no replay/restart')
        raw=encode(value)
        if len(raw)>4*1024*1024:
            raise ValueError('Same 4MiB request bound as admitted journal')
        index=self.sequence
        publish(self.directory/f'{index:06d}.request.json',value)
        started=time.monotonic()
        try:
            # Dispatch marker is durable BEFORE a single stdin write.
            publish(self.directory/f'{index:06d}.dispatch.json',{
                'native_pid':self.process.pid,'time_ns':time.time_ns(),
                'request_sha256':hashlib.sha256(encode(value)).hexdigest()})
            self.process.stdin.write(raw+b'\n')
            self.process.stdin.flush()
            result=self.receive(self.request_timeout)
            publish(self.directory/f'{index:06d}.result.json',result)
            publish(self.directory/f'{index:06d}.receipt.json',{
                'native_writes':1,'native_roundtrip_seconds':time.monotonic()-started})
        except BaseException as error:
            self.broken=True
            publish(self.directory/f'{index:06d}.unknown.json',{
                'error_type':type(error).__name__,'completion_unknown':True,
                'automatic_replay':False})
            raise
        self.sequence+=1
        result['worker_roundtrip_seconds']=time.monotonic()-started
        return result

    def execute(self, query, parameters):
        return self.request({'query':query,'parameters':parameters})

    def close(self):
        if self.closed:return
        self.closed=True
        try:
            if self.process.stdin and not self.process.stdin.closed:self.process.stdin.close()
            self.process.wait(timeout=45)
            publish(self.directory/'closed.json',{'native_pid':self.process.pid,
                                                 'returncode':self.process.returncode})
        except BaseException as error:
            publish(self.directory/'close_unknown.json',{'native_pid':self.process.pid,
                'error_type':type(error).__name__,'audit_required':True,'manual_lock_unlink':False})
            # No arbitrary kill or lock removal: original child owns DB cleanup.
        finally:
            if self.process.poll() is not None:
                self.reader.join(timeout=1)
                self.process.stdout.close()
            self.log.close()


class LocalNativePublicWorker(LocalLineTransport):
    def __init__(self, key, graph, log, remote):
        # key is interface compatibility only; no SSH key is read on the server.
        if socket.gethostname()!=HOST or graph not in GRAPHS:
            raise ValueError('Unexpected authorized native runtime')
        owner=json.loads((ROOT/'execution_owner.json').read_text())
        if owner.get('hostname')!=HOST:raise ValueError('Runtime owner mismatch')
        if (ROOT/'.public-runtime.lock').exists():
            raise RuntimeError('Native runtime occupied/unknown; never clear lock')
        if not re.fullmatch(r'public_query_[a-z0-9_]+',remote['code_folder']):
            raise ValueError('Unexpected frozen native code folder')
        code=ROOT/remote['code_folder']
        if not remote['files'] or any(not re.fullmatch(r'[a-z_]+\.py',n) for n in remote['files']):
            raise ValueError('Invalid native dependency list')
        for name,expected in remote['files'].items():
            if hashlib.sha256((code/name).read_bytes()).hexdigest()!=expected:
                raise ValueError('Original native dependency changed')
        super().__init__([str(ROOT/'.venv/bin/python'),str(code/'public_query_worker.py'),
                          '--root',str(ROOT),'--graph',graph],graph,log)
