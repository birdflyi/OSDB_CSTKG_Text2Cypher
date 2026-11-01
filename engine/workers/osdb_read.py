"""Read-only RPC facade for an already imported, owned OSDB Neo4j graph.

Does not import data, start/stop Neo4j, or call models. The operator pins the
import receipt and database PID; counts are an additional check, not a substitute
for the receipt. Use the existing shared read guard and result normalization.
"""
import argparse
import json
from pathlib import Path
import socket
import sys
from urllib.parse import urlparse

from engine.config import canonical, digest
from engine.execution import serve_stream
from engine.errors import IntegrityError
from engine.workers.neo4j import ReadHandler, load_helpers


def checked_file(binding):
    path = Path(binding['path']).resolve()
    raw = path.read_bytes()
    if digest(raw) != binding['sha256']:
        raise IntegrityError('OSDB receipt hash changed')
    return path, json.loads(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('settings', type=Path)
    args = parser.parse_args()
    config = json.loads(args.settings.read_bytes())
    root = Path(config['owned_root']).resolve()
    _, owner = checked_file(config['owner'])
    receipt_path, receipt = checked_file(config['import_receipt'])
    if not receipt_path.is_relative_to(root) or owner['hostname'] != socket.gethostname():
        raise IntegrityError('Wrong owner or receipt directory')
    if receipt.get('graph_sha256') != config['graph_sha256'] or receipt.get('graph') != config['graph']:
        raise IntegrityError('Import receipt belongs to another graph')
    if receipt.get('status') != 'COMPLETE':
        raise IntegrityError('Import is not complete')
    pid = int(config['database_pid'])
    command = Path('/proc', str(pid), 'cmdline').read_bytes().replace(b'\0', b' ').decode()
    instance = Path(config['instance_dir']).resolve()
    if not instance.is_relative_to(root) or str(instance) not in command:
        raise IntegrityError('Database PID does not belong to this graph instance')
    uri = urlparse(config['bolt_uri'])
    if uri.scheme != 'bolt' or uri.hostname not in ('127.0.0.1', 'localhost') or uri.username:
        raise IntegrityError('OSDB worker accepts only owned loopback Bolt')
    if uri.port != receipt.get('bolt_port') or receipt.get('instance_dir') != str(instance):
        raise IntegrityError('Import route differs')
    read_query, _, normalize = load_helpers(Path(config['harness']), config['helper_sha256'])
    import neo4j
    if neo4j.__version__ != '5.28.1':
        raise IntegrityError('OSDB requires pinned neo4j driver 5.28.1')
    with neo4j.GraphDatabase.driver(config['bolt_uri'], auth=None, max_connection_pool_size=4) as driver:
        with driver.session(default_access_mode='READ') as session:
            nodes = session.run('MATCH (n) RETURN count(n)').single()[0]
            edges = session.run('MATCH ()-[r]->() RETURN count(r)').single()[0]
        if [nodes, edges] != config['counts']:
            raise IntegrityError('OSDB node/edge counts changed')
        handler = ReadHandler(driver, None, read_query, normalize, neo4j.Query)
        def execute(request):
            if request.get('kind') == 'candidates':
                raise ValueError('Candidate lookup is not part of this frozen OSDB input')
            return handler(request)
        ready = dict(kind='READY', graph=config['graph'], graph_sha256=config['graph_sha256'],
                     source_sha256=digest(Path(__file__).read_bytes()), max_case_workers=2,
                     hostname=socket.gethostname(), worker_version='osdb-existing-read-v1',
                     import_receipt_sha256=config['import_receipt']['sha256'],
                     database_pid=pid, counts=[nodes, edges])
        def emit(value):
            print(canonical(value).decode('utf8'), flush=True)
        emit(ready)
        serve_stream(sys.stdin, emit, execute, 2)


if __name__ == '__main__':
    main()
