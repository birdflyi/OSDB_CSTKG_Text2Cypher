"""Validate a full TRAIN graph and official references; no model predictions.

Run only on the intended remote instance. Starts/stops its own Neo4j process.
"""
import argparse
from collections import defaultdict
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
from neo4j.time import Date


def identifier(value):
    return '`' + value.replace('`', '``') + '`'


def normalize(value):
    if isinstance(value, Date):
        return value.iso_format()
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value


def properties(record, schema, node):
    fields = ('eid', 'name', 'aliases', 'description', 'provenance') if node else ('rid', 'provenance')
    props = {key: record[key] for key in fields if key in record}
    if props.keys() & record['properties'].keys():
        raise ValueError('Property collides with metadata')
    props.update(record['properties'])
    for key, kind in schema['properties'].items():
        if props.get(key) is not None and kind == 'date':
            props[key] = Date.from_iso_format(props[key])
    return {key: value for key, value in props.items() if value is not None}


def validate_import(session, graph):
    schemas = {x['label']: x for x in graph['schema']['entities']}
    rel_schemas = {x['label']: x for x in graph['schema']['relations']}
    expected_nodes = {r['eid']: {'labels': [r['label']],
                      'props': normalize(properties(r, schemas[r['label']], True))}
                      for r in graph['entities']}
    expected_rels = {r['rid']: {'label': r['label'], 'subj': r['subj_id'], 'obj': r['obj_id'],
                     'props': normalize(properties(r, rel_schemas[r['label']], False))}
                     for r in graph['relations']}
    if len(expected_nodes) != len(graph['entities']) or len(expected_rels) != len(graph['relations']):
        raise ValueError('Duplicate source ids')
    count = session.run('MATCH (n) RETURN count(n) AS n').single()['n']
    if count == 0:
        grouped = defaultdict(list)
        for row in graph['entities']:
            grouped[row['label']].append(properties(row, schemas[row['label']], True))
        for label, rows in grouped.items():
            session.run('CREATE CONSTRAINT IF NOT EXISTS FOR (n:' + identifier(label)
                        + ') REQUIRE n.eid IS UNIQUE').consume()
            session.run('UNWIND $rows AS row CREATE (n:' + identifier(label)
                        + ') SET n = row', rows=rows).consume()
        rels = defaultdict(list)
        for row in graph['relations']:
            rels[row['label']].append({'subj': row['subj_id'], 'obj': row['obj_id'],
                 'props': properties(row, rel_schemas[row['label']], False)})
        for label, rows in rels.items():
            session.run('UNWIND $rows AS row MATCH (a {eid: row.subj}), (b {eid: row.obj}) '
                        'CREATE (a)-[r:' + identifier(label) + ']->(b) SET r = row.props',
                        rows=rows).consume()
    # On reruns, verify existing data. Never clear or silently repair an unknown graph.
    actual_nodes = {r['eid']: {'labels': r['labels'], 'props': normalize(r['props'])}
                    for r in session.run('MATCH (n) RETURN n.eid AS eid, labels(n) AS labels, properties(n) AS props')}
    actual_rels = {r['rid']: {'label': r['label'], 'subj': r['subj'], 'obj': r['obj'],
                  'props': normalize(r['props'])} for r in session.run(
        'MATCH (a)-[r]->(b) RETURN r.rid AS rid, type(r) AS label, a.eid AS subj, '
        'b.eid AS obj, properties(r) AS props')}
    counts = session.run('MATCH ()-[r]->() RETURN count(r) AS n').single()['n']
    node_count = session.run('MATCH (n) RETURN count(n) AS n').single()['n']
    if actual_nodes != expected_nodes or actual_rels != expected_rels:
        raise ValueError('Imported graph is not property/label/direction-equivalent to original JSON')
    if node_count != len(expected_nodes) or counts != len(expected_rels):
        raise ValueError('Graph row counts differ')
    return {'nodes': node_count, 'edges': counts, 'all_properties_labels_and_endpoints_match': True}


def main(root):
    root = root.resolve()
    if root != Path('/mnt/data/louzehua/ch7_t1_public_benchmark_20261023'):
        raise ValueError('Unexpected experiment root')
    if not (root / 'OWNED_CH7_T1').exists():
        raise ValueError('Missing ownership marker')
    runtime = json.loads((root / 'runtime_receipt.json').read_text())
    data = root / 'cypherbench_data'
    manifest = json.loads((data / 'manifest.json').read_text())
    for receipt in manifest['artifacts']:
        if hashlib.sha256((data / receipt['path']).read_bytes()).hexdigest() != receipt['sha256']:
            raise ValueError('Transferred artifact checksum mismatch: ' + receipt['path'])
    graph_name = manifest['dev_smoke_graph']
    if graph_name in manifest['splits']['test']['by_graph']:
        raise ValueError('Refusing test-graph development')
    graph = json.loads((data / ('upstream/graphs/simplekg/' + graph_name + '_simplekg.json')).read_text())
    credentials = json.loads((root / 'db_connection.json').read_text())
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', runtime['bolt_port']))
    env = dict(os.environ, JAVA_HOME=runtime['java_home'])
    log_path = root / ('dev_database_' + str(time.time_ns()) + '.log')
    report = {'kind': 'TRAIN_DATABASE_PREFLIGHT_NOT_MODEL_ACCURACY',
              'hostname': socket.gethostname(), 'graph': graph_name, 'model_calls': 0,
              'source_commit': manifest['official_source_commit'],
              'dataset_revision': manifest['dataset_revision'], 'neo4j_version': runtime['neo4j_version']}
    log = log_path.open('w')
    process = subprocess.Popen([runtime['neo4j_home'] + '/bin/neo4j', 'console'],
        env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (root / 'dev_active_process.json').write_text(json.dumps({
        'hostname': socket.gethostname(), 'pid': process.pid, 'started_by': __file__,
        'log': str(log_path), 'state': 'RUNNING_PREFLIGHT'}, indent=2))
    driver = None
    try:
        for attempt in range(90):
            if process.poll() is not None:
                raise RuntimeError('Owned Neo4j process exited; inspect ' + str(log_path))
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
        else:
            raise RuntimeError('Owned Neo4j did not become ready')
        sys.path.insert(0, str(root / 'official_source'))
        from cypherbench.metrics.execution_accuracy import result_eq, to_hashable, to_tuples
        with driver.session() as session:
            report['graph_validation'] = validate_import(session, graph)
            print(json.dumps(report['graph_validation']), flush=True)
            wanted = set(manifest['dev_smoke_ids'])
            references = [json.loads(line) for line in (data / 'evaluator/train.jsonl').read_text().splitlines()
                          if json.loads(line)['qid'] in wanted]
            results = []
            for row in references:
                start = time.monotonic()
                executed = session.run(Query(row['gold_cypher'], timeout=60)).data()
                target = json.loads(row['answer_json'])
                actual = [{k: to_hashable(v) for k, v in item.items()} for item in executed]
                if not isinstance(target, list) or any(not isinstance(item, list) for item in target):
                    raise ValueError('Expected official answer_json row arrays')
                expected = [tuple(to_hashable(v) for v in item) for item in target]
                actual_tuples = to_tuples(actual) if actual else []
                score = float(result_eq(actual_tuples, expected, 'order by' in row['gold_cypher'].lower()))
                results.append({'qid': row['qid'], 'reference_answer_match': score,
                                'rows': len(actual), 'seconds': time.monotonic() - start})
            report['reference_checks'] = results
            report['passed'] = len(results) == len(wanted) and all(r['reference_answer_match'] == 1 for r in results)
        report['limitations'] = ['Reference query execution only; no model result.',
            'Three development references do not certify every query, graph or official Docker equivalence.']
    except Exception as error:
        report['passed'] = False
        report['error_type'] = type(error).__name__
        raise
    finally:
        if driver:
            driver.close()
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        log.close()
        (root / 'dev_active_process.json').write_text(json.dumps({
            'hostname': socket.gethostname(), 'pid': process.pid, 'state': 'STOPPED_AFTER_PREFLIGHT'}))
        report['database_stopped'] = True
        (root / (log_path.stem + '_report.json')).write_text(json.dumps(report, indent=2))
        (root / 'dev_database_report.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    if not report.get('passed'):
        raise SystemExit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    main(parser.parse_args().root)
