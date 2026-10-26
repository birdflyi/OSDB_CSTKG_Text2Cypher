"""Read-only historical lineage and source-contract preparation, never prediction.

Reads explicitly scoped old task specifications, not their scores/model outputs.
Caches eight commit-pinned public source documents locally. No old queue,
database, model transport, graph files or execution settings are modified.
"""
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import urllib.request

ROOT = Path('D:/ch7')
REPO = ROOT / 'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON = ROOT / 'code/OSDB_CSTKG_Text2Cypher_publication_t1'
BASE = ROOT / 'experiments/ch7_private/continuation_20261021'
OUT = ROOT / 'reports/osdb_task_lineage_v1_20261026'
PIN = 'ada8c11ee342fdc64e94f5f894676565aa2e6506'


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':')).encode()).hexdigest()


def write(path, value):
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write('\n')


def main():
    assert not OUT.exists(), 'One-shot output exists; preserve it and audit before rerunning.'
    OUT.mkdir()
    prior_path = ROOT / 'reports/osdb_prior_task_exposure_20261025.json'
    prior = read(prior_path)
    runtime = REPO / 'experiment-harness/public_benchmark_v1'
    before = {str(p.relative_to(runtime)): sha(p) for p in runtime.rglob('*.py')}
    freeze = read(ROOT / 'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
    for rel, digest in freeze['code_files'].items():
        assert sha(CANON / rel) == digest
    sys.path.insert(0, str(CANON / 'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    budget = AttemptBudget(BASE / 'budget', ROOT / 'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',
        path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private', ROOT / 'experiments/ch7_private')])
    balance = budget.inspect()
    assert balance['reserved'] == 5192 and not balance['pending'] and balance['cap'] == 12000
    write(OUT / 'protocol.json', {
        'kind': 'HISTORICAL_LINEAGE_NOT_NEW_TASK_OR_METHOD_EVALUATION',
        'sealed_local': datetime.now().astimezone().isoformat(),
        'source_sha256': sha(Path(__file__)), 'prior_inventory_sha256': sha(prior_path),
        'scope': 'All 18 packages and all167 positions of the prior scoped inventory; no score/output selection',
        'rules': ['Missing semantic_goal remains missing; do not infer independence from family labels.',
                  'No record does not mean unexposed; all historical positions stay development-only.',
                  'Distinct question hashes, IDs, project IDs or parameter values do not certify structural independence.',
                  'Only the eight pre-existing source proxy families are rechecked against commit-pinned documents.',
                  'Source definitions are motivation, not proof of dataset completeness or exact metric replication.'],
        'confirmation_tasks_admitted': 0, 'new_model_calls': 0, 'new_database_queries': 0})
    rows, input_hashes, sources = [], {}, defaultdict(set)
    by_goal, by_family, by_source, reference_kinds = Counter(), Counter(), Counter(), Counter()
    groups = defaultdict(list)
    for package in prior['inventory']:
        manifest = BASE / package['manifest_path']
        spec = manifest.parent / 'specifications.json'
        assert sha(manifest) == package['manifest_sha256']
        assert sha(spec) == package['specification_sha256']
        meta, tasks = read(manifest), read(spec)
        assert meta['task_sha256'] == sha(spec) and len(tasks) == package['task_count']
        input_hashes[str(manifest)] = sha(manifest)
        input_hashes[str(spec)] = sha(spec)
        receipts = {r['id_sha256']: r for r in package['task_receipts']}
        assert len(receipts) == len(tasks)
        for task in tasks:
            identity = fingerprint(task['id'])
            old = receipts[identity]
            assert old['question_sha256'] == fingerprint(task.get('question'))
            assert old['semantic_goal_label_sha256'] == fingerprint(task.get('semantic_goal'))
            assert task['project'] == package['project']
            family, goal, source = task['family'], task.get('semantic_goal'), task.get('source_url')
            by_family[family] += 1
            if goal is not None: by_goal[goal] += 1
            if source: by_source[source] += 1
            query, call = bool(task.get('reference_query')), bool(task.get('reference_call'))
            kind = 'QUERY_AND_CALL' if query and call else 'QUERY' if query else 'CALL' if call else 'NONE'
            reference_kinds[kind] += 1
            row = {'project': task['project'], 'stratum': package['stratum'],
                'specification_sha256': sha(spec), 'id_sha256': identity,
                'question_sha256': old['question_sha256'], 'family_label': family,
                'declared_semantic_goal': goal, 'source_url': source,
                'query_sha256': fingerprint(task['reference_query']) if query else None,
                'parameter_sha256': fingerprint(task.get('reference_parameters')) if query else None,
                'reference_kind': kind, 'historical_record_present': old['historical_record_present'],
                'disposition': 'HISTORICAL_DEVELOPMENT_ONLY', 'new_confirmation_eligible': False}
            rows.append(row)
            groups[(task['project'], old['question_sha256'])].append(len(rows)-1)
            if package['stratum'] == 'e7_source_tasks_v1':
                sources[family].add(source)
    assert len(rows) == prior['task_positions'] == 167
    assert sum(r['historical_record_present'] for r in rows) == 99
    assert sum(r['declared_semantic_goal'] is None for r in rows) == 56
    repeats = [g for g in groups.values() if len(g) > 1]
    assert len(repeats) == len(prior['same_project_exact_question_repeats']) == 28
    assert len(sources) == 8
    source_receipts = []
    for family, urls in sorted(sources.items()):
        assert len(urls) == 1
        url = next(iter(urls))
        assert url.startswith('https://github.com/chaoss/wg-evolution/blob/' + PIN + '/')
        raw_url = url.replace('https://github.com/chaoss/wg-evolution/blob/',
                              'https://raw.githubusercontent.com/chaoss/wg-evolution/')
        destination = OUT / (family + '.md')
        receipt = {'family': family, 'url': url, 'raw_url': raw_url, 'commit': PIN,
                   'historical_positions': sum(r['family_label'] == family and r['stratum'] == 'e7_source_tasks_v1' for r in rows)}
        try:
            request = urllib.request.Request(raw_url, headers={'User-Agent': 'T1-source-contract-audit'})
            with urllib.request.urlopen(request, timeout=20) as response:
                assert response.status == 200
                assert response.geturl() == raw_url
                body = response.read(512*1024+1)
            assert 0 < len(body) <= 512*1024
            text = body.decode('utf-8-sig')
            assert text.lstrip().startswith('#') and 'Question:' in text
            destination.write_bytes(body)
            receipt.update(status='CACHED_PUBLIC_SOURCE', bytes=len(body), sha256=sha(destination))
        except Exception as error:
            receipt.update(status='SOURCE_FETCH_FAILED', error_type=type(error).__name__)
        source_receipts.append(receipt)
    write(OUT / 'registry.json', rows)
    write(OUT / 'source_receipts.json', source_receipts)
    for path, digest in input_hashes.items(): assert sha(Path(path)) == digest
    assert before == {str(p.relative_to(runtime)): sha(p) for p in runtime.rglob('*.py')}
    for rel, digest in freeze['code_files'].items(): assert sha(CANON / rel) == digest
    after = budget.inspect()
    assert balance == after
    summary = {'kind': 'OSDB_TASK_LINEAGE_AND_SOURCE_CONTRACT_PREPARATION',
        'time_local': datetime.now().astimezone().isoformat(), 'task_packages': len(prior['inventory']),
        'historical_positions': len(rows), 'positions_with_records': 99,
        'positions_without_records_not_certified_unseen': 68,
        'positions_missing_semantic_goal': 56, 'declared_goal_label_count_not_independent_goals': len(by_goal),
        'family_label_counts_not_structural_families': dict(sorted(by_family.items())),
        'reference_kinds_not_reference_validity': dict(reference_kinds),
        'distinct_same_project_question_keys': len(groups), 'repeated_question_keys': len(repeats),
        'source_linked_positions': sum(by_source.values()), 'distinct_historical_source_urls': len(by_source),
        'eight_source_proxy_positions': sum(r['stratum'] == 'e7_source_tasks_v1' for r in rows),
        'eight_source_documents_cached': sum(r['status'] == 'CACHED_PUBLIC_SOURCE' for r in source_receipts),
        'source_receipts': source_receipts, 'registry_sha256': sha(OUT / 'registry.json'),
        'protocol_sha256': sha(OUT / 'protocol.json'), 'frozen_source_files_unchanged': len(freeze['code_files']),
        'method_runtime_files_unchanged': len(before), 'input_files_unchanged': len(input_hashes),
        'actual_attempts': after['reserved'], 'cap': after['cap'], 'remaining': after['remaining'],
        'pending': after['pending'], 'new_model_calls': 0, 'new_database_queries': 0,
        'new_interface_probes': 0, 'confirmation_tasks_admitted': 0,
        'formal_N': None, 'formal_tasks_dispatched': 0, 'formal_tasks_completed': 0,
        'formal_tasks_failed': 0, 'formal_tasks_undispatched': None,
        'T1_complete': False, 'Text2GQL_conditional_budget_activated': False,
        'limits': ['All source families and four projects already have historical exposure.',
                   'Source proxy queries are developer operationalizations, not exact published metric replications.',
                   'No new task/source/structure holdout is certified; task count awaits an actual intake.',
                   'Reference-query presence and goal labels are metadata only; no correctness or EX is established.']}
    write(OUT / 'summary.json', summary)
    write(ROOT / 'reports/t1_osdb_task_lineage_summary_20261026.json', summary)
    print(json.dumps({k: v for k, v in summary.items() if k not in
        ('source_receipts','family_label_counts_not_structural_families','limits')}, ensure_ascii=True))


if __name__ == '__main__':
    main()
