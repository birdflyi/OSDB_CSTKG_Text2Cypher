"""Execute correct reference service calls; no NL mapping score is produced."""
import argparse
import json
from pathlib import Path
import re

import run_baselines as b
from mechanism_core import compile_call


def execute(query, parameters):
    if not isinstance(query, str) or not isinstance(parameters, dict):
        return {'status': 'PROTOCOL_ERROR'}
    query = query.strip().removesuffix(';').strip()
    if not re.match(r'^MATCH\b', query, re.I) or re.search(r';|\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|CALL|LOAD|FOREACH|UNION|USE)\b|\b(apoc|gds|dbms)\s*\.', query, re.I):
        return {'status': 'PRECHECK_ERROR'}
    try:
        def tx(q):
            r = b.LOCAL.post(b.DB_URL, json={'statements': [{'statement': q, 'parameters': parameters,
                'resultDataContents': ['row']}]}, timeout=(3, 45))
            r.raise_for_status()
            body = r.json()
            if body.get('errors'):
                raise ValueError(json.dumps(body['errors']))
            return body['results'][0]
        tx('EXPLAIN ' + query)
        r = tx(query)
        if len(r['data']) > 500:
            return {'status': 'OUTPUT_BUDGET_ERROR'}
        return {'status': 'EXECUTED', 'columns': r['columns'], 'rows': [x['row'] for x in r['data']]}
    except Exception as e:
        return {'status': 'EXECUTION_ERROR', 'reason': str(e)}


def run(specifications, receipt, output):
    specs = json.loads(Path(specifications).read_text(encoding='utf-8'))
    receipt = json.loads(Path(receipt).read_text(encoding='utf-8'))
    manifest = json.loads(Path(specifications).with_name('dataset_manifest.json').read_text(encoding='utf-8'))
    if manifest['projects'][specs[0]['project']] != receipt['source_hashes']:
        raise ValueError('Specification source and graph import differ')
    expected_identity = receipt['database_identity']
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data'] != expected_identity:
        raise ValueError('Wrong dedicated graph')
    cache, records = {}, []
    for s in specs:
        compiled = compile_call(s['reference_call'])
        key = json.dumps(s['reference_call'], sort_keys=True)
        reused = key in cache
        if not reused:
            cache[key] = execute(compiled['query'], compiled['parameters'])
        r = cache[key]
        answer = {'id': s['id'], 'status': 'ANSWER' if r['status'] == 'EXECUTED' else 'ERROR',
                  'rows': r.get('rows', []), 'evidence_ids': ['REF:' + s['id']]}
        correct = b.score([s], [answer], {s['id']: {'REF:' + s['id']}})['summary']['table_and_decision_correct'] == 1
        correct = correct and r.get('columns') == s['columns']
        records.append({'id': s['id'], 'correct_reference_execution': correct, 'reference_cache_reused': reused,
                        'compiled': compiled, 'execution': r})
    result = {'mode': 'REFERENCE_SERVICE_CONFORMANCE_NOT_NL_ACCURACY', 'cases': len(specs),
        'correct': sum(r['correct_reference_execution'] for r in records), 'unique_calls': len(cache),
        'database_identity': expected_identity, 'specifications_sha256': b.sha(specifications),
        'compiler_sha256': b.sha(Path(__file__).with_name('mechanism_core.py')), 'records': records}
    b.save(Path(output), result)
    print(json.dumps({k: v for k, v in result.items() if k != 'records'}, ensure_ascii=False), flush=True)
    if result['correct'] != len(specs):
        raise RuntimeError('Reference mismatch; do not start model comparison')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    for name in ('specifications', 'receipt', 'output'):
        p.add_argument('--' + name, required=True)
    p.add_argument('--port', type=int, choices=(17475, 17476), required=True)
    a = p.parse_args()
    b.DB_URL = f'http://127.0.0.1:{a.port}/db/neo4j/tx/commit'
    run(a.specifications, a.receipt, a.output)
