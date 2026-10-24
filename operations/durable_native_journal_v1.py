"""Infrastructure journal for the unchanged native worker. No model or scorer."""
import argparse
import hashlib
import json
import os
from pathlib import Path
from queue import Queue, Empty
import re
import socket
import subprocess
import sys
from threading import Thread
import time

ROOT = Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')
HOST = 'interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def durable(path, data):
    """Publish once, with content verification on replay; no overwrites."""
    temp = path.with_name(path.name + '.tmp.' + str(os.getpid()))
    with temp.open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temp, path)
        fd = os.open(str(path.parent), os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    except FileExistsError:
        if path.read_bytes() != data:
            raise ValueError('Journal identity/content conflict')
    finally:
        temp.unlink()


def identity(session):
    assert socket.gethostname() == HOST
    assert json.loads((ROOT/'execution_owner.json').read_text())['hostname'] == HOST
    assert re.fullmatch(r'[a-z0-9_]{1,100}', session)
    return ROOT/'transport_journal_v1'/session


def serve(session, graph):
    folder = identity(session)
    spec = json.loads((folder/'spec.json').read_text())
    assert spec['graph'] == graph and spec['session'] == session
    code = ROOT/spec['remote']['code_folder']
    assert re.fullmatch(r'public_query_[a-z0-9_]+', spec['remote']['code_folder'])
    for name, expected in spec['remote']['files'].items():
        assert re.fullmatch(r'[a-z_]+\.py', name)
        assert hashlib.sha256((code/name).read_bytes()).hexdigest() == expected
    # mkdir is the exclusive session owner. Never auto-clear stale ownership.
    (folder/'daemon_owner').mkdir()
    durable(folder/'owner.json', encode({'pid': os.getpid(), 'hostname': HOST, 'graph': graph}))
    child = None
    log = (folder/'native_stderr.log').open('xb')
    try:
        child = subprocess.Popen([str(ROOT/'.venv/bin/python'), str(code/'public_query_worker.py'),
                                  '--root', str(ROOT), '--graph', graph],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log)
        incoming = Queue()
        def read():
            try:
                for line in child.stdout:
                    incoming.put(line)
            finally: incoming.put(None)
        Thread(target=read, daemon=True).start()
        def receive(timeout):
            try: raw = incoming.get(timeout=timeout)
            except Empty as error: raise RuntimeError('Native worker response timeout; no replay') from error
            if raw is None: raise RuntimeError('Native worker exited; no replay')
            json.loads(raw)
            return raw
        ready = receive(300)
        assert json.loads(ready)['graph'] == graph
        durable(folder/'ready.json', ready)
        index = 0
        while True:
            if (folder/'stop.request').exists(): break
            request = folder/(f'{index:06d}.request.json')
            if not request.exists():
                if child.poll() is not None: raise RuntimeError('Native worker died while idle')
                time.sleep(.05)
                continue
            raw = request.read_bytes()
            payload = json.loads(raw)
            digest = hashlib.sha256(raw).hexdigest()
            durable(folder/(f'{index:06d}.dispatch.json'), encode({
                'sha256': digest, 'native_pid': child.pid, 'time_ns': time.time_ns()}))
            # Exactly one write per journal id. A missing result after daemon death
            # is UNKNOWN, never automatically re-executed in a new daemon.
            started = time.monotonic()
            child.stdin.write(encode(payload)+b'\n')
            child.stdin.flush()
            response = receive(125)
            durable(folder/(f'{index:06d}.result.json'), response)
            durable(folder/(f'{index:06d}.receipt.json'), encode({
                'sha256': digest, 'native_pid': child.pid,
                'native_roundtrip_seconds': time.monotonic()-started,
                'native_writes': 1, 'time_ns': time.time_ns()}))
            index += 1
    except BaseException as error:
        durable(folder/'error.json', encode({'type': type(error).__name__, 'error': str(error),
                                           'time_ns': time.time_ns()}))
        raise
    finally:
        if child is not None:
            try:
                child.stdin.close()
                child.wait(timeout=45)
                durable(folder/'closed.json', encode({'returncode': child.returncode,
                                                       'native_pid': child.pid}))
            except Exception as error:
                durable(folder/'close_error.json', encode({'type': type(error).__name__}))
        log.close()


def call(session, index):
    folder = identity(session)
    assert 0 <= index < 1000000
    raw = sys.stdin.buffer.read(4*1024*1024+1)
    assert len(raw) <= 4*1024*1024
    payload = encode(json.loads(raw))
    request = folder/(f'{index:06d}.request.json')
    durable(request, payload)
    deadline = time.monotonic()+135
    while time.monotonic() < deadline:
        result = folder/(f'{index:06d}.result.json')
        receipt = folder/(f'{index:06d}.receipt.json')
        if result.exists() and receipt.exists():
            envelope = {'response': json.loads(result.read_bytes()),
                        'journal': json.loads(receipt.read_bytes()), 'session': session, 'id': index}
            sys.stdout.buffer.write(encode(envelope)+b'\n')
            return
        if (folder/'error.json').exists(): raise RuntimeError((folder/'error.json').read_text())
        if (folder/'closed.json').exists(): raise RuntimeError('Daemon closed before result')
        time.sleep(.05)
    raise TimeoutError('Journal client wait expired; inspect same id, never resend as new id')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['serve', 'call'])
    parser.add_argument('--session', required=True)
    parser.add_argument('--graph')
    parser.add_argument('--index', type=int)
    args = parser.parse_args()
    if args.mode == 'serve': serve(args.session, args.graph)
    else: call(args.session, args.index)
