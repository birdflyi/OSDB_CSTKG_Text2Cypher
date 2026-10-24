"""Owned per-graph CPU runtime and reference preflight. No model calls.

TEST references are evaluator inputs only. Never use this program as model
context. Partial imports are not cleared/repaired/resumed automatically.
"""
import argparse
from collections import defaultdict
import hashlib
import json
import logging
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time

from neo4j import GraphDatabase, Query

from answer_scoring import load_official_comparator, read_reference_answer, score_executed_answer
from model_transport import immutable_json
from smoke_train_database import properties, normalize, identifier
from schema_identity import canonical_schema
from import_recovery import recovery_evidence


ROOT = Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023')


def chunks(rows, size=1000):
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def import_verify(session, graph, folder, *, verify_existing=False):
    nodes, edges = graph['entities'], graph['relations']
    by_node = {r['eid']: r for r in nodes}
    by_edge = {r['rid']: r for r in edges}
    if len(by_node) != len(nodes) or len(by_edge) != len(edges):
        raise ValueError('Duplicate input identity')
    if any(r['subj_id'] not in by_node or r['obj_id'] not in by_node for r in edges):
        raise ValueError('Dangling input endpoint')
    es = {r['label']: r for r in graph['schema']['entities']}
    rs = {r['label']: r for r in graph['schema']['relations']}
    receipt = folder / 'graph_validation.json'
    if not receipt.exists() and not verify_existing:
        if (folder / 'import_started.json').exists():
            raise RuntimeError('Prior import is incomplete; audit without clearing/resuming')
        if session.run('MATCH (n) RETURN count(n) AS n').single()['n']:
            raise RuntimeError('Refusing an unowned/nonempty store without validation receipt')
        immutable_json(folder / 'import_started.json', {'nodes': len(nodes), 'edges': len(edges)})
        grouped = defaultdict(list)
        for row in nodes:
            grouped[row['label']].append(row)
        for label, rows in grouped.items():
            session.run('CREATE CONSTRAINT IF NOT EXISTS FOR (n:' + identifier(label) +
                        ') REQUIRE n.eid IS UNIQUE').consume()
            for batch in chunks(rows):
                session.run('UNWIND $rows AS row CREATE (n:' + identifier(label) + ') SET n=row',
                            rows=[properties(r, es[label], True) for r in batch]).consume()
        grouped.clear()
        for row in edges:
            signature = (row['label'], by_node[row['subj_id']]['label'], by_node[row['obj_id']]['label'])
            grouped[signature].append(row)
        for (label, subj, obj), rows in grouped.items():
            for batch in chunks(rows):
                values = [{'subj': r['subj_id'], 'obj': r['obj_id'],
                           'props': properties(r, rs[label], False)} for r in batch]
                query = ('UNWIND $rows AS row MATCH (a:' + identifier(subj) +
                         ' {eid:row.subj}), (b:' + identifier(obj) +
                         ' {eid:row.obj}) CREATE (a)-[r:' + identifier(label) + ']->(b) SET r=row.props')
                session.run(query, rows=values).consume()
    # Stream actual values; compare every identity, property, type and endpoint.
    seen = set()
    # Exhaustive offline validation is not a benchmark query. Large complete
    # scans get their own explicit budget without changing the formal 60s cap.
    for row in session.run(Query('MATCH (n) RETURN n.eid AS eid,labels(n) AS labels,properties(n) AS props', timeout=600)):
        expected = by_node.get(row['eid'])
        if (expected is None or row['eid'] in seen or row['labels'] != [expected['label']]
            or normalize(row['props']) != normalize(properties(expected, es[expected['label']], True))):
            raise ValueError('Node import mismatch')
        seen.add(row['eid'])
    if len(seen) != len(nodes):
        raise ValueError('Missing imported nodes')
    seen = set()
    for row in session.run(Query('MATCH (a)-[r]->(b) RETURN r.rid AS rid,type(r) AS label,'
                           'a.eid AS subj,b.eid AS obj,properties(r) AS props', timeout=600)):
        expected = by_edge.get(row['rid'])
        if (expected is None or row['rid'] in seen or row['label'] != expected['label']
            or row['subj'] != expected['subj_id'] or row['obj'] != expected['obj_id']
            or normalize(row['props']) != normalize(properties(expected, rs[expected['label']], False))):
            raise ValueError('Relationship import mismatch')
        seen.add(row['rid'])
    if len(seen) != len(edges):
        raise ValueError('Missing imported edges')
    validation = {'nodes': len(nodes), 'edges': len(edges),
                  'all_properties_labels_and_endpoints_match': True}
    if not receipt.exists():
        immutable_json(receipt, validation)
    return validation


def preflight(root, graph_name, output, only_qids=None, recover_from=None, recovery_reason=''):
    root = root.resolve()
    if root != ROOT or json.loads((root / 'execution_owner.json').read_text())['hostname'] != socket.gethostname():
        raise ValueError('Wrong runtime owner/host')
    data = root / 'cypherbench_data'
    manifest = json.loads((data / 'manifest.json').read_text())
    expected_tasks = manifest['splits']['test']['by_graph'].get(graph_name)
    if expected_tasks is None:
        raise ValueError('Public runtime requires a listed TEST graph')
    entry = json.loads((root / 'graph_receipts' / (graph_name + '.json')).read_text())
    source = data / 'upstream/graphs/simplekg' / (graph_name + '_simplekg.json')
    h = hashlib.sha256()
    with source.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    if source.stat().st_size != entry['size'] or h.hexdigest() != entry['lfs']['oid']:
        raise ValueError('Pinned public graph hash mismatch')
    for relative in ['evaluator/test.jsonl', 'upstream/graphs/schemas/' + graph_name + '_schema.json']:
        expected = next(x['sha256'] for x in manifest['artifacts'] if x['path'] == relative)
        if hashlib.sha256((data / relative).read_bytes()).hexdigest() != expected:
            raise ValueError('Public evaluation artifact changed')
    graph = json.loads(source.read_text())
    standalone_schema = json.loads((data / 'upstream/graphs/schemas' / (graph_name + '_schema.json')).read_text())
    if canonical_schema(graph['schema']) != canonical_schema(standalone_schema):
        raise ValueError('Embedded/public schema mismatch')
    folder = root / 'test_runtime_v1' / graph_name
    folder.mkdir(parents=True, exist_ok=True)
    output = output.resolve()
    if folder not in output.parents:
        raise ValueError('Preflight output must remain in owned graph runtime')
    output.mkdir(parents=True, exist_ok=False)
    if recover_from:
        evidence = recovery_evidence(folder, recover_from, graph, recovery_reason)
        immutable_json(output / 'existing_import_recovery.json', evidence)
    elif recovery_reason:
        raise ValueError('Recovery reason requires a specific prior failed output')
    lock = root / '.public-runtime.lock'
    with lock.open('x') as stream:
        json.dump({'pid': os.getpid(), 'host': socket.gethostname(), 'graph': graph_name}, stream)
    process = driver = None
    try:
        runtime = json.loads((root / 'runtime_receipt.json').read_text())
        port = 17688
        with socket.socket() as check:
            check.bind(('127.0.0.1', port))
        conf = folder / 'conf'
        conf.mkdir(exist_ok=True)
        config = '\n'.join([
            'server.default_listen_address=127.0.0.1', 'server.bolt.enabled=true',
            f'server.bolt.listen_address=127.0.0.1:{port}',
            f'server.bolt.advertised_address=127.0.0.1:{port}',
            'server.http.enabled=false', 'server.https.enabled=false',
            'server.memory.heap.initial_size=512m', 'server.memory.heap.max_size=2g',
            'server.memory.pagecache.size=1g', 'server.jvm.additional=-XX:ActiveProcessorCount=4',
            'db.transaction.timeout=60s', 'dbms.security.auth_enabled=true',
            'dbms.usage_report.enabled=false',
            'server.directories.data=' + str(folder / 'data'),
            'server.directories.logs=' + str(folder / 'logs'),
            'server.directories.run=' + str(folder / 'run'),
            'server.directories.transaction.logs.root=' + str(folder / 'transactions'), ''])
        config_path = conf / 'neo4j.conf'
        if config_path.exists() and config_path.read_text() != config:
            raise RuntimeError('Owned runtime config changed; audit required')
        config_path.write_text(config)
        env = dict(os.environ, JAVA_HOME=runtime['java_home'], NEO4J_CONF=str(conf))
        private = folder / 'db_connection.json'
        if not private.exists():
            if (folder / 'data').exists():
                raise RuntimeError('Store exists without original private identity')
            credentials = {'uri': f'bolt://127.0.0.1:{port}', 'user': 'neo4j',
                           'password': secrets.token_urlsafe(32)}
            fd = os.open(private, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w') as stream:
                json.dump(credentials, stream)
            password_result = subprocess.run([runtime['neo4j_home'] + '/bin/neo4j-admin', 'dbms',
                            'set-initial-password', credentials['password']],
                           env=env, capture_output=True, check=False)
            if password_result.returncode:
                raise RuntimeError('Private database credential initialization failed; inspect privately')
        credentials = json.loads(private.read_text())
        with (output / 'database_console.log').open('x') as log:
            process = subprocess.Popen([runtime['neo4j_home'] + '/bin/neo4j', 'console'],
                                       env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        immutable_json(output / 'active_process.json', {'pid': process.pid, 'hostname': socket.gethostname(),
                       'graph': graph_name, 'port': port})
        for _ in range(90):
            if process.poll() is not None:
                raise RuntimeError('Owned public DB exited; inspect private console log')
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
            raise RuntimeError('Public database startup timeout')
        with driver.session() as session:
            validation = import_verify(session, graph, folder, verify_existing=bool(recover_from))
        immutable_json(output / 'graph_validation.json', validation)
        compare = load_official_comparator(root / 'official_source')
        refs = [json.loads(s) for s in (data / 'evaluator/test.jsonl').read_text().splitlines()
                if s.strip() and json.loads(s)['graph'] == graph_name]
        if len(refs) != expected_tasks:
            raise ValueError('Reference task count mismatch')
        if only_qids:
            if not set(only_qids) <= {r['qid'] for r in refs}:
                raise ValueError('Diagnostic QID outside selected public graph')
            refs = [r for r in refs if r['qid'] in set(only_qids)]
        checked = []
        for row in sorted(refs, key=lambda r: r['qid']):
            started = time.monotonic()
            try:
                with driver.session(default_access_mode='READ') as session:
                    info = session.run(Query('EXPLAIN ' + row['gold_cypher'], timeout=30)).consume()
                    if info.query_type != 'r':
                        raise ValueError('Reference not certified read-only')
                    cursor = session.run(Query(row['gold_cypher'], timeout=60))
                    columns = list(cursor.keys())
                    actual = [[normalize(r[c]) for c in columns] for r in cursor]
                result = score_executed_answer(status='EXECUTED', predicted_rows=actual,
                         reference_rows=read_reference_answer(row['answer_json']),
                         order_matters='order by' in row['gold_cypher'].lower(), qid=row['qid'], compare=compare)
                item = {'qid': row['qid'], 'reference_answer_match': result['ex'],
                        'rows': len(actual), 'seconds': time.monotonic() - started}
                if result['ex'] != 1:
                    item.update(actual_rows=actual, published_rows=read_reference_answer(row['answer_json']))
            except Exception as error:
                item = {'qid': row['qid'], 'reference_answer_match': 0,
                        'error_type': type(error).__name__, 'error': str(error)[:1000],
                        'seconds': time.monotonic() - started}
            checked.append(item)
            immutable_json(output / 'references' / (row['qid'] + '.json'), item)
        summary = {'kind': 'TEST_DATA_REFERENCE_PREFLIGHT_NOT_MODEL_RESULT', 'graph': graph_name,
                   'diagnostic_subset': bool(only_qids), 'official_graph_tasks': expected_tasks,
                   'tasks': len(refs), 'checked': len(checked),
                   'passed': sum(r['reference_answer_match'] == 1 for r in checked),
                   'graph_validation': validation, 'model_calls': 0, 'formal_predictions': 0,
                   'schema_declaration_order_differs': graph['schema'] != standalone_schema,
                   'graph_sha256': h.hexdigest(), 'status': 'PASS' if all(r['reference_answer_match'] == 1 for r in checked) else 'REFERENCE_REVIEW_REQUIRED'}
        immutable_json(output / 'summary.json', summary)
        print(json.dumps(summary), flush=True)
    except Exception as error:
        immutable_json(output / 'failure.json', {'error_type': type(error).__name__, 'error': str(error)[:1000]})
        raise
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
        immutable_json(output / 'process_stopped.json', {'owned_process_stopped': True,
                       'pid': process.pid if process else None})
        lock.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--graph', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--qid', action='append')
    parser.add_argument('--recover-from', type=Path)
    parser.add_argument('--recovery-reason', default='')
    args = parser.parse_args()
    logging.getLogger('neo4j.notifications').setLevel(logging.ERROR)
    preflight(args.root, args.graph, args.output, args.qid, args.recover_from, args.recovery_reason)
