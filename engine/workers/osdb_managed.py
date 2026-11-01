"""Start one owned imported graph, serve its existing read-only RPC, then stop it.

The existing osdb_read worker still checks identity, receipt, counts and the read
guard. This wrapper only owns process lifetime. It never imports or deletes data.
"""
import argparse
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid
from urllib.parse import urlparse

from engine.config import canonical, digest
from engine.errors import IntegrityError
from engine.workers.osdb_read import checked_file


def validate(config):
    root=Path(config['owned_root']).resolve()
    _,owner=checked_file(config['owner'])
    path,receipt=checked_file(config['import_receipt'])
    instance=Path(config['instance_dir']).resolve()
    if owner['hostname']!=socket.gethostname() or not instance.is_relative_to(root) or not path.is_relative_to(root):
        raise IntegrityError('Managed graph owner mismatch')
    if receipt.get('status')!='COMPLETE' or receipt.get('graph_sha256')!=config['graph_sha256'] or receipt.get('graph')!=config['graph']:
        raise IntegrityError('Managed graph receipt mismatch')
    launch_path,launch=checked_file(config['database_launch'])
    command=launch['command']
    if not launch_path.is_relative_to(root) or not isinstance(command,list) or not all(isinstance(x,str) for x in command):
        raise IntegrityError('Invalid owned launch command')
    if '--config-dir='+str(instance/'conf') not in command:
        raise IntegrityError('Launch command belongs to another instance')
    conf=instance/'conf/neo4j.conf'
    if digest(conf.read_bytes())!=launch['config_sha256']:
        raise IntegrityError('Database configuration changed')
    uri=urlparse(config['bolt_uri'])
    if uri.scheme!='bolt' or uri.hostname!='127.0.0.1' or uri.username or uri.port!=receipt.get('bolt_port'):
        raise IntegrityError('Managed graph route mismatch')
    if receipt.get('instance_dir')!=str(instance):raise IntegrityError('Managed instance mismatch')
    return instance,command,uri.port


def stop(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill();process.wait(timeout=10)


def run(config):
    instance,command,port=validate(config)
    token=uuid.uuid4().hex
    lock=instance/'managed.lock'
    with lock.open('x',encoding='utf8') as f:f.write(token)
    database=worker=None
    try:
        invocation=instance/'invocations'/token;invocation.mkdir(parents=True)
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1',port))==0:raise IntegrityError('Port occupied; do not attach to another process')
        with (invocation/'database.log').open('xb') as log:
            database=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT)
        (invocation/'launch.json').write_bytes(canonical(dict(pid=database.pid,command=command,started_epoch=time.time())))
        import neo4j
        deadline=time.monotonic()+120
        while True:
            if database.poll() is not None:raise RuntimeError('Database exited before READY')
            try:
                with neo4j.GraphDatabase.driver(config['bolt_uri'],auth=None,connection_timeout=3) as driver:driver.verify_connectivity()
                break
            except Exception:
                if time.monotonic()>=deadline:raise TimeoutError('Database READY deadline')
                time.sleep(2)
        settings=invocation/'settings.json'
        settings.write_bytes(canonical(dict(config,database_pid=database.pid)))
        worker=subprocess.Popen([sys.executable,'-m','engine.workers.osdb_read',str(settings)])
        return worker.wait()
    finally:
        stop(worker);stop(database)
        if lock.read_text(encoding='utf8')==token:lock.unlink()


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('settings',type=Path)
    args=parser.parse_args()
    def interrupted(signum,frame):raise SystemExit(128+signum)
    for name in ('SIGTERM','SIGHUP'):
        if hasattr(signal,name):signal.signal(getattr(signal,name),interrupted)
    raise SystemExit(run(json.loads(args.settings.read_bytes())))


if __name__=='__main__':main()
