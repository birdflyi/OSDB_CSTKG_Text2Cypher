"""Import trusted projection CSVs into an EMPTY dedicated loopback Neo4j.

Reuses the tagged-node/edge-UID import approach of the earlier local pilot.
Never clears a graph, resumes a partial import, or connects to a remote address.
"""
import argparse
import csv
from collections import defaultdict
import json
from pathlib import Path
import re

import run_baselines as b


def tx(query, parameters=None):
    response = b.LOCAL.post(b.DB_URL, json={'statements': [{'statement': query,
        'parameters': parameters or {}, 'resultDataContents': ['row']}]}, timeout=(3, 60))
    response.raise_for_status()
    body = response.json()
    if body.get('errors'):
        raise RuntimeError(json.dumps(body['errors']))
    return body['results'][0]


def run(data, runtime):
    data, runtime = Path(data).resolve(), Path(runtime).resolve()
    if (runtime / 'import_started.json').exists() or (runtime / 'import_receipt.json').exists():
        raise FileExistsError('Import already attempted; do not replace history')
    nodes, edges = defaultdict(list), defaultdict(list)
    with (data / 'nodes.csv').open(encoding='utf-8-sig', newline='') as h:
        for r in csv.DictReader(h):
            if not re.fullmatch('[A-Za-z][A-Za-z0-9_]*', r['label']):
                raise ValueError('Unexpected label')
            nodes[r['label']].append({'uid': r['node_uid'], 'props': json.loads(r['properties_json'])})
    with (data / 'edges.csv').open(encoding='utf-8-sig', newline='') as h:
        for r in csv.DictReader(h):
            if r['native_rel_type'] not in {'EVENT_ACTION', 'REFERENCE'}:
                raise ValueError('Unexpected relationship type')
            edges[r['native_rel_type']].append({'source': r['source_node_uid'], 'target': r['target_node_uid'],
                                              'props': json.loads(r['properties_json'])})
    node_ids = [r['uid'] for group in nodes.values() for r in group]
    edge_ids = [r['props']['edge_uid'] for group in edges.values() for r in group]
    allowed_nodes = set(node_ids)
    if len(allowed_nodes) != len(node_ids) or len(set(edge_ids)) != len(edge_ids):
        raise ValueError('Duplicate source UID; cannot silently merge records')
    if any(r['source'] not in allowed_nodes or r['target'] not in allowed_nodes for g in edges.values() for r in g):
        raise ValueError('Missing edge endpoints')
    identity = tx('CALL db.info() YIELD id,name RETURN id,name')['data']
    if tx('MATCH (n) RETURN count(n)')['data'][0]['row'][0] != 0:
        raise RuntimeError('Refuse nonempty database')
    hashes = {name: b.sha(data / name) for name in ('nodes.csv', 'edges.csv')}
    b.save(runtime / 'import_started.json', {'url': b.DB_URL, 'database_identity': identity,
            'fresh_empty_database': True, 'source_hashes': hashes, 'code_sha256': b.sha(__file__)})
    tx('CREATE CONSTRAINT pilot_uid_unique IF NOT EXISTS FOR (n:PilotNode) REQUIRE n.__pilot_uid IS UNIQUE')
    for label, records in sorted(nodes.items()):
        for i in range(0, len(records), 300):
            tx('UNWIND $rows AS row CREATE (n:PilotNode) SET n:' + label +
               ' SET n = row.props SET n.__pilot_uid = row.uid', {'rows': records[i:i+300]})
    for rel, records in sorted(edges.items()):
        for i in range(0, len(records), 150):
            tx('UNWIND $rows AS row MATCH (s:PilotNode {__pilot_uid:row.source}),'
               '(t:PilotNode {__pilot_uid:row.target}) CREATE (s)-[r:' + rel + ']->(t) SET r = row.props',
               {'rows': records[i:i+150]})
    actual_n = tx('MATCH (n) RETURN count(n)')['data'][0]['row'][0]
    actual_e, distinct_e = tx('MATCH ()-[r]->() RETURN count(r),count(DISTINCT r.edge_uid)')['data'][0]['row']
    if (actual_n, actual_e, distinct_e) != (len(node_ids), len(edge_ids), len(edge_ids)):
        raise RuntimeError('Imported graph counts mismatch')
    if tx('CALL db.info() YIELD id,name RETURN id,name')['data'] != identity:
        raise RuntimeError('Database identity changed')
    receipt = {'nodes': actual_n, 'edges': actual_e, 'distinct_edge_uids': distinct_e,
               'source_hashes': hashes, 'database_identity': identity,
               'extra_storage_label': 'PilotNode', 'extra_storage_property': '__pilot_uid',
               'import_method': 'CREATE from verified unique node/edge UIDs into verified empty dedicated database',
               'code_sha256': b.sha(__file__)}
    b.save(runtime / 'import_receipt.json', receipt)
    print(json.dumps(receipt, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--data', required=True)
    p.add_argument('--runtime', required=True)
    p.add_argument('--port', type=int, choices=(17475, 17476), default=17475)
    a = p.parse_args()
    b.DB_URL = f'http://127.0.0.1:{a.port}/db/neo4j/tx/commit'
    run(a.data, a.runtime)
