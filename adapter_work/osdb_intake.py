"""Build an OSDB cohort from independently accepted tasks, without model/DB calls.

Input JSONL: task_id, graph, question, schema (file pin), source (file pin),
reference_query, answer, output_contract, validation (file pin), category.
Validation must contain status=PASS, csv_answer and db_answer with equal values.
The caller must bind these receipts to the same layer/snapshot before intake.
"""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT/'adapter_work')]
from common.cohort import canonical, digest, verify_pin
from cohort_preparation import build_inventory, save, receipt, METHOD_IDS
from osdb_output import normalized
from task_scoring_policy import build_task_policy


def jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as f:
        for row in rows:
            f.write(canonical(row) + b'\n')
    return receipt(path)


def build(input_path, output):
    output = Path(output).resolve()
    output.mkdir(exist_ok=False, parents=True)
    tasks = [json.loads(line) for line in Path(input_path).read_bytes().splitlines() if line]
    if not tasks or len({t['task_id'] for t in tasks}) != len(tasks):
        raise ValueError('Tasks must be nonempty and unique')
    tasks.sort(key=lambda t: t['task_id'])
    publics, references = [], []
    for task in tasks:
        validation = json.loads(verify_pin(task['validation'])[1])
        schema = json.loads(verify_pin(task['schema'])[1])
        verify_pin(task['source'])
        if validation.get('draft_id') != task['task_id'] or validation.get('status') != 'PASS':
            raise ValueError('Missing matching independent validation')
        if 'csv_answer' not in validation or validation['csv_answer'] != validation.get('db_answer'):
            raise ValueError('CSV and graph reference disagree')
        normalized(task['answer'], task['output_contract'])
        values = task['answer']['rows']
        flat = [r[0] for r in values] if task['output_contract']['kind'] == 'list' else values
        validated = validation['csv_answer']
        if task['output_contract']['kind'] == 'count':
            flat = values[0][0]
        if flat != validated:
            raise ValueError('Normalized answer differs from validated answer')
        if task.get('snapshot_sha256') != validation.get('snapshot_sha256') or not task.get('snapshot_sha256'):
            raise ValueError('Validation needs an explicit matching snapshot SHA')
        if task.get('layer') not in ('event', 'reference') or task['layer'] != validation.get('layer'):
            raise ValueError('Validation needs explicit matching source layer')
        pid = 'osdb:' + task['task_id']
        public = dict(question=task['question'], graph=task['graph'], schema=schema)
        publics.append(dict(position_id=pid, task_id=task['task_id'], graph=task['graph'], public_input=public))
        references.append(dict(task_id=task['task_id'], status='VALID', answer=task['answer'],
                               reference_query=task['reference_query'], evidence=task['validation']))
    pub = jsonl(output/'public.jsonl', publics)
    ref = jsonl(output/'references.jsonl', references)
    population = []
    for i, (task, public, reference) in enumerate(zip(tasks, publics, references), 1):
        population.append(dict(position_id=public['position_id'], benchmark='osdb_stkg',
            task_id=task['task_id'], graph=task['graph'], split='engineering_smoke', source_group=task['layer'],
            category=task.get('category'), source_file=task['source']['path'], source_sha256=task['source']['sha256'],
            public_binding=dict(pub, line_1based=i, record_sha256=digest(canonical(public))),
            reference_binding=dict(ref, line_1based=i, record_sha256=digest(canonical(reference))),
            public_input_sha256=digest(canonical(public['public_input'])), exposure_status='development_exposed'))
    population.sort(key=lambda r:r['position_id'])
    source = jsonl(output/'population.jsonl', population)
    selection = jsonl(output/'selection.jsonl', [dict(r, population_row_sha256=digest(canonical(r))) for r in population])
    policy = save(output/'selection_policy.json', dict(benchmark='osdb_stkg', mode='independently_authored', population_sha256=source['sha256']))
    manifest = save(output/'cohort.json', dict(kind='cohort_v1', schema_version='cohort_v1',
        benchmark='osdb_stkg', selection_mode='independently_authored', source=source, policy=policy,
        selection=selection, population_count=len(tasks), selected_count=len(tasks), formal_admitted=False))
    profiles = {m:receipt(ROOT/'adapter_work'/'prompts'/(m+'.json')) for m in ('direct','ours')}
    inventory = build_inventory([manifest], list(METHOD_IDS), output/'inventory', profiles=profiles, bindings={})
    policies = []
    for task, row in zip(tasks, population):
        p = build_task_policy('OSDB-STKG', row['position_id'], task['reference_query'], reference_status='VALID', cohort_sha256=manifest['sha256'])
        p.update(metric_id='project_osdb_explicit_output_v2', output_contract=task['output_contract'],
                 order_rule='ORDERED' if task['output_contract']['order']=='ordered' else 'BAG', order_source='explicit_question_contract')
        policies.append(dict(source_position_id=row['position_id'], task_id=task['task_id'], graph=task['graph'],
                             policy=p, reference_binding=row['reference_binding']))
    scoring = save(output/'scoring.json', dict(positions=policies))
    return dict(inventory=receipt(output/'inventory'/'matrix.json'), scoring_contract=scoring,
                counts=inventory['counts'], model_calls=0, database_calls=0,
                next='bind request config and OSDB-STKG RPC routes; use cohort_pipeline run')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('input')
    p.add_argument('output')
    a = p.parse_args()
    print(json.dumps(build(a.input, a.output), ensure_ascii=False))
