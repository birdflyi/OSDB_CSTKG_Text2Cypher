"""Owned TRAIN-only persistent JSON-lines database worker. No model/config secrets.

Run using the existing private remote venv. Starts only its own Neo4j process
and stops it on stdin EOF. Does not load evaluator references or accept test graphs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from neo4j import GraphDatabase, Query

from cypher_read_guard import read_query
from smoke_train_database import normalize, validate_import


def emit(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


def serve(root):
    root = root.resolve()
    if root != Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023'):
        raise ValueError('Unexpected owned runtime root')
    if json.loads((root / 'execution_owner.json').read_text())['hostname'] != socket.gethostname():
        raise ValueError('Wrong execution host on shared mount')
    runtime = json.loads((root / 'runtime_receipt.json').read_text())
    data = root / 'cypherbench_data'
    manifest = json.loads((data / 'manifest.json').read_text())
    graph_name = manifest['dev_smoke_graph']
    if graph_name not in manifest['splits']['train']['by_graph'] or graph_name in manifest['splits']['test']['by_graph']:
        raise ValueError('Development worker refuses test graphs')
    graph_path = data / f'upstream/graphs/simplekg/{graph_name}_simplekg.json'
    relative = str(graph_path.relative_to(data))
    receipt = next(x for x in manifest['artifacts'] if x['path'] == relative)
    if hashlib.sha256(graph_path.read_bytes()).hexdigest() != receipt['sha256']:
        raise ValueError('Training graph changed')
    graph = json.loads(graph_path.read_text())
    credentials = json.loads((root / 'db_connection.json').read_text())
    with socket.socket() as check:
        check.bind(('127.0.0.1', runtime['bolt_port']))
    log_path = root / f'dev_worker_{time.time_ns()}.log'
    env = dict(os.environ, JAVA_HOME=runtime['java_home'])
    driver = process = None
    with log_path.open('w') as log:
        try:
            process = subprocess.Popen([runtime['neo4j_home'] + '/bin/neo4j', 'console'],
                                       env=env, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            (root / 'dev_active_process.json').write_text(json.dumps({
                'pid': process.pid, 'hostname': socket.gethostname(),
                'state': 'RUNNING_PREPARATION_WORKER', 'log': str(log_path)}))
            for _ in range(90):
                if process.poll() is not None:
                    raise RuntimeError('Owned database exited')
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
                raise RuntimeError('Owned development DB startup timeout')
            with driver.session() as session:
                checked = validate_import(session, graph)
            emit({'kind': 'READY', 'graph': graph_name, 'graph_validation': checked})
            for line in sys.stdin:
                start = time.monotonic()
                try:
                    request = json.loads(line)
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
                        columns = list(cursor.keys())
                        rows = []
                        complete = True
                        for row in cursor:
                            if len(rows) == 10000:
                                complete = False
                                break
                            rows.append([normalize(row[column]) for column in columns])
                        cursor.consume()
                    encoded = json.dumps(rows, ensure_ascii=False).encode('utf-8')
                    emit({'status': 'EXECUTED' if complete else 'TRUNCATED',
                          'columns': columns, 'rows': rows, 'complete': complete,
                          'result_bytes': len(encoded),
                          'explain_seconds': explained - start,
                          'db_seconds': time.monotonic() - explained})
                except Exception as error:
                    emit({'status': 'EXECUTION_ERROR', 'columns': [], 'rows': [],
                          'complete': False, 'error_type': type(error).__name__,
                          'error': str(error)[:1200], 'db_seconds': time.monotonic() - start})
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
            if process:
                (root / 'dev_active_process.json').write_text(json.dumps({
                    'pid': process.pid, 'hostname': socket.gethostname(),
                    'state': 'STOPPED_AFTER_PREPARATION_WORKER'}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    serve(parser.parse_args().root)
