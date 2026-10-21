"""E2b: real Cypher execution on a dedicated synthetic fixture graph.

The tool never imports or alters a graph. It requires its frozen import receipt.
Reference/mutant checks can run before the model experiment finishes.
"""
import argparse
import json
from pathlib import Path

import run_baselines as b
from mechanism_core import compile_call
from run_mechanism_pilot import bag
import strong_baselines as s


def matches(execution, expected):
    return execution['status'] == 'EXECUTED' and execution['columns'] == expected['columns'] and bag(execution['rows']) == bag(expected['expected_rows'])


def mutations(query):
    lower, upper = ' AND r.source_event_time >= $start_date', ' AND r.source_event_time < $end_date'
    result = {}
    if lower in query:
        result['drop_lower'] = query.replace(lower, '')
    if upper in query:
        result['drop_upper'] = query.replace(upper, '')
    if lower in query and upper in query:
        result['drop_window'] = query.replace(lower, '').replace(upper, '')
    if 'count(r)' in query:
        result['records_to_distinct_targets'] = query.replace('count(r)', 'count(DISTINCT x.entity_id)')
    if ' RETURN ' in query and 'count(' not in query and ' RETURN DISTINCT ' not in query:
        result['drop_duplicate_rows'] = query.replace(' RETURN ', ' RETURN DISTINCT ')
    segment = '(c:IssueComment)-[a:EVENT_ACTION]->(i:Issue)'
    if segment in query:
        result['reverse_comment_direction'] = query.replace(segment, '(c:IssueComment)<-[a:EVENT_ACTION]-(i:Issue)')
    return result


def run(a):
    fixture, source = Path(a.fixture), Path(a.source)
    receipt = json.loads(Path(a.receipt).read_text(encoding='utf-8'))
    expected = {r['id']: r for r in json.loads((fixture / 'expected.json').read_text(encoding='utf-8'))}
    if any(b.sha(fixture / name) != value for name, value in receipt['source_hashes'].items()):
        raise ValueError('Fixture import source differs')
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data'] != receipt['database_identity']:
        raise ValueError('Wrong synthetic graph')
    specs = []
    for project in ('redis', 'duckdb'):
        specs.extend(json.loads((source / (project + '_specifications.json')).read_text(encoding='utf-8')))
    references, mutants, cache = [], [], {}

    def execute(query, parameters):
        key = json.dumps([query, parameters], sort_keys=True)
        if key not in cache:
            prediction = {'status': 'ANSWER', 'cypher': query, 'parameters': parameters}
            cache[key] = s.evaluate(prediction, 'G')['execution']
        return cache[key]

    for spec in specs:
        c = compile_call(spec['reference_call'])
        r = execute(c['query'], c['parameters'])
        references.append({'id': spec['id'], 'correct': matches(r, expected[spec['id']])})
        for kind, query in mutations(c['query']).items():
            r = execute(query, c['parameters'])
            mutants.append({'id': spec['id'], 'kind': kind, 'execution_status': r['status'],
                            'detected_by_result': r['status'] == 'EXECUTED' and not matches(r, expected[spec['id']])})
    if not all(r['correct'] for r in references):
        raise ValueError('Fixture reference/CSV mismatch; do not score predictions')
    predictions, sources = [], []
    for value in a.runs:
        root = Path(value)
        summary = json.loads((root / 'summary.json').read_text(encoding='utf-8'))
        if summary['status'] != 'PLANNED_DISPATCHES_COMPLETED':
            raise ValueError('Model source run incomplete')
        sources.append({'run': root.name, 'summary_sha256': b.sha(root / 'summary.json')})
        for file in sorted((root / 'records').glob('*.json')):
            record = json.loads(file.read_text(encoding='utf-8'))
            pred = record['prediction']
            r = {'status': 'NO_ANSWER'}
            if pred and pred['status'] == 'ANSWER':
                if record['arm'].startswith('S'):
                    try:
                        c = compile_call({k: pred[k] for k in ('operation', 'arguments')})
                        r = execute(c['query'], c['parameters'])
                    except (ValueError, KeyError, TypeError):
                        r = {'status': 'INVALID_CALL'}
                else:
                    r = execute(pred['cypher'], pred['parameters'])
            predictions.append({'id': record['id'], 'arm': record['arm'], 'original_correct': record['correct'],
                                'fixture_correct': matches(r, expected[record['id']]), 'execution': r,
                                'source_record_sha256': b.sha(file)})
    result = {'mode': 'E2B_SYNTHETIC_CYPHER_EXECUTION_NOT_REAL_OSS', 'fixture': fixture.name,
              'references_correct': len(references), 'reference_cases': len(specs),
              'mutants': mutants, 'predictions': predictions, 'prediction_sources': sources,
              'unique_query_executions': len(cache), 'database_identity': receipt['database_identity'],
              'fixture_hashes': {name: b.sha(fixture / name) for name in ('nodes.csv', 'edges.csv', 'expected.json')},
              'implementation_hashes': {name: b.sha(Path(__file__).with_name(name)) for name in
                  ('run_semantic_fixture_checks.py', 'strong_baselines.py', 'mechanism_core.py', 'run_baselines.py')},
              'mutation_detection': {kind: {'applicable_cases': sum(m['kind'] == kind for m in mutants),
                  'detected_cases': sum(m['kind'] == kind and m['detected_by_result'] for m in mutants)} for kind in sorted({m['kind'] for m in mutants})},
              'limitations': 'Finite declared witnesses. Detection is not a model error rate or proof of semantic equivalence. No re-prompting.'}
    b.save(Path(a.output), result)
    print(json.dumps({k: result[k] for k in ('fixture', 'references_correct', 'mutation_detection')}, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    for key in ('fixture', 'source', 'receipt', 'output'):
        p.add_argument('--' + key, required=True)
    p.add_argument('--runs', nargs='*', default=[])
    p.add_argument('--port', type=int, choices=(17477, 17478, 17479), required=True)
    a = p.parse_args()
    b.DB_URL = f'http://127.0.0.1:{a.port}/db/neo4j/tx/commit'
    run(a)
