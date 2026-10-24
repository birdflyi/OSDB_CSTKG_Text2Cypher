"""Read-only JSON-lines worker for an already imported owned public graph.

No reference file is loaded. No model credentials or model calls are involved.
The same lexical candidate operation is available to both comparison arms.
"""
import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from neo4j import GraphDatabase, Query

from public_read_guard import read_query
from entity_candidates import EntityCandidates
from model_transport import immutable_json
from public_graph_runtime import ROOT
from smoke_train_database import normalize


def emit(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


def serve(root, graph_name):
    root = root.resolve()
    if root != ROOT or json.loads((root / 'execution_owner.json').read_text())['hostname'] != socket.gethostname():
        raise ValueError('Wrong owned runtime root/host')
    manifest = json.loads((root / 'cypherbench_data/manifest.json').read_text())
    if graph_name not in manifest['splits']['test']['by_graph']:
        raise ValueError('Not a pinned public TEST graph')
    folder = root / 'test_runtime_v1' / graph_name
    validation = json.loads((folder / 'graph_validation.json').read_text())
    if validation.get('all_properties_labels_and_endpoints_match') is not True:
        raise ValueError('Graph import not certified')
    source = root / 'cypherbench_data/upstream/graphs/simplekg' / (graph_name + '_simplekg.json')
    entry = json.loads((root / 'graph_receipts' / (graph_name + '.json')).read_text())
    digest = hashlib.sha256()
    with source.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    if digest.hexdigest() != entry['lfs']['oid'] or source.stat().st_size != entry['size']:
        raise ValueError('Public source changed')
    index_started = time.monotonic()
    graph = json.loads(source.read_text())
    resolver = EntityCandidates(graph['entities'])
    del graph
    index_seconds = time.monotonic() - index_started
    runtime = json.loads((root / 'runtime_receipt.json').read_text())
    credentials = json.loads((folder / 'db_connection.json').read_text())
    if credentials['uri'] != 'bolt://127.0.0.1:17688':
        raise ValueError('Unexpected public database endpoint')
    with socket.socket() as check:
        check.bind(('127.0.0.1', 17688))
    lock = root / '.public-runtime.lock'
    with lock.open('x') as stream:
        json.dump({'pid': os.getpid(), 'graph': graph_name, 'state': 'PUBLIC_QUERY_WORKER'}, stream)
    process = driver = None
    log_file = folder / f'query_worker_{time.time_ns()}.log'
    try:
        env = dict(os.environ, JAVA_HOME=runtime['java_home'], NEO4J_CONF=str(folder / 'conf'))
        with log_file.open('x') as log:
            process = subprocess.Popen([runtime['neo4j_home'] + '/bin/neo4j', 'console'], env=env,
                        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        for _ in range(90):
            if process.poll() is not None:
                raise RuntimeError('Public query database exited')
            try:
                driver = GraphDatabase.driver(credentials['uri'],
                         auth=(credentials['user'], credentials['password']), connection_timeout=2)
                driver.verify_connectivity()
                break
            except Exception:
                if driver:
                    driver.close()
                    driver = None
                time.sleep(1)
        if driver is None:
            raise RuntimeError('Public query DB startup timeout')
        with driver.session() as session:
            counts = [session.run('MATCH (n) RETURN count(n) AS n').single()['n'],
                      session.run('MATCH ()-[r]->() RETURN count(r) AS n').single()['n']]
        if counts != [validation['nodes'], validation['edges']]:
            raise RuntimeError('Public database row identity changed')
        emit({'kind': 'READY', 'graph': graph_name, 'graph_sha256': digest.hexdigest(),
              'graph_validation': validation, 'candidate_index_seconds': index_seconds,
              'pid': process.pid})
        for line in sys.stdin:
            started = time.monotonic()
            try:
                request = json.loads(line)
                if request.get('kind') == 'candidates':
                    candidates = resolver.find(request['question'])
                    emit({'status': 'CANDIDATES', 'candidates': candidates,
                          'lookup_seconds': time.monotonic() - started,
                          'result_bytes': len(json.dumps(candidates, ensure_ascii=False).encode())})
                    continue
                query = read_query(request['query'])
                params = request.get('parameters', {})
                if not isinstance(params, dict):
                    raise ValueError('Expected parameter object')
                with driver.session(default_access_mode='READ') as session:
                    explain = session.run(Query('EXPLAIN ' + query, timeout=30), params).consume()
                    if explain.query_type != 'r':
                        raise ValueError('EXPLAIN did not certify read-only query')
                    explained = time.monotonic()
                    cursor = session.run(Query(query, timeout=60), params)
                    columns, rows, complete, result_bytes = list(cursor.keys()), [], True, 2
                    for row in cursor:
                        values = [normalize(row[c]) for c in columns]
                        row_bytes = len(json.dumps(values, ensure_ascii=False).encode()) + (2 if rows else 0)
                        if len(rows) == 1000000 or result_bytes + row_bytes > 64 * 1024 * 1024:
                            complete = False
                            break
                        rows.append(values)
                        result_bytes += row_bytes
                    cursor.consume()
                emit({'status': 'EXECUTED' if complete else 'TRUNCATED', 'complete': complete,
                      'columns': columns, 'rows': rows,
                      'result_bytes': result_bytes,
                      'explain_seconds': explained - started, 'db_seconds': time.monotonic() - explained})
            except Exception as error:
                emit({'status': 'EXECUTION_ERROR', 'columns': [], 'rows': [], 'complete': False,
                      'error_type': type(error).__name__, 'error': str(error)[:1200],
                      'db_seconds': time.monotonic() - started})
    finally:
        if driver:
            driver.close()
        if process and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        immutable_json(folder / (log_file.stem + '_stopped.json'),
                       {'stopped': True, 'pid': process.pid if process else None})
        lock.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--graph', required=True)
    args = parser.parse_args()
    logging.getLogger('neo4j.notifications').setLevel(logging.ERROR)
    serve(args.root, args.graph)
