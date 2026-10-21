"""Offline measurement of benchmark discrimination, not model performance.

Known faulty query semantics are evaluated with CSV functions. This neither
executes the generated Cypher nor proves query equivalence. Counterfactual edges
are synthetic evaluator fixtures, isolated from actual project evidence.
"""
import argparse
from collections import Counter
import json
from pathlib import Path

import run_baselines as b


def bag(rows):
    return Counter(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)


def evaluate(nodes, edges, specs, kind, faulty=False):
    pr = specs[1]['question'].split('PullRequest ', 1)[1].split(',', 1)[0]
    prefix = specs[3]['question'].split('literal prefix ', 1)[1].split('?', 1)[0]
    links = [e for e in edges if e['source'] == ('PullRequest', pr)
             and e['target'][0] == 'ExternalResource' and e['physical'] == 'REFERENCE'
             and e['service'] == 'LINKS_TO']
    if kind.startswith('drop_') or kind == 'distinct_count':
        if kind == 'drop_zero_window':
            keep = links if faulty else [e for e in links if e['time'] and
                '2022-01-01T00:00:00Z' <= e['time'] < '2023-01-01T00:00:00Z']
            return [[len(keep)]]
        keep = []
        for e in links:
            low = faulty and kind in {'drop_start', 'drop_both'}
            high = faulty and kind in {'drop_end', 'drop_both'}
            if not e['time']:
                if low and high:
                    keep.append(e)
                continue
            if (low or e['time'] >= '2023-01-01T00:00:00Z') and (high or e['time'] < '2024-01-01T00:00:00Z'):
                keep.append(e)
        if faulty and kind == 'distinct_count':
            pairs = {(e['domain'], e['target']) for e in keep}
            counts = Counter(d for d, target in pairs)
        else:
            counts = Counter(e['domain'] for e in keep)
        return [[d, n] for d, n in counts.items()]
    if kind == 'distinct_rows':
        rows = [[e['domain'], e['target'][1]] for e in links]
        return [list(x) for x in {tuple(r) for r in rows}] if faulty else rows
    if kind == 'reverse_comment_direction':
        comments = Counter()
        for e in edges:
            if e['physical'] != 'EVENT_ACTION' or e['service'] != 'COMMENTED_ON_ISSUE':
                continue
            issue, comment = (e['source'], e['target']) if faulty else (e['target'], e['source'])
            if issue[0] == 'Issue' and issue[1].startswith(prefix) and comment[0] == 'IssueComment':
                comments[comment] += 1
        return [[sum(comments[e['source']] for e in edges if e['source'][0] == 'IssueComment'
            and e['target'][0] == 'Commit' and e['physical'] == 'REFERENCE' and e['service'] == 'REFERENCES')]]
    raise ValueError(kind)


MUTANTS = [('drop_start', 2), ('drop_end', 2), ('drop_both', 2),
           ('distinct_count', 2), ('distinct_rows', 1),
           ('reverse_comment_direction', 4), ('drop_zero_window', 5)]


def audit(data):
    nodes, edges, docs = b.load_graph(data)
    # These diagnostics join by label/entity_id, so refuse ambiguous identities.
    if len(set(nodes.values())) != len(nodes):
        raise ValueError('Identity aliases require UID-aware evaluator')
    specs = b.build_specs(nodes, edges)
    records = []
    for kind, index in MUTANTS:
        gold = evaluate(nodes, edges, specs, kind)
        if bag(gold) != bag(specs[index]['expected_rows']):
            raise ValueError('Offline semantics disagree with existing CSV reference')
        actual = evaluate(nodes, edges, specs, kind, True)
        detected = bag(gold) != bag(actual)
        witness = None
        if not detected:
            pr = specs[1]['question'].split('PullRequest ', 1)[1].split(',', 1)[0]
            original = next(e for e in edges if e['source'] == ('PullRequest', pr)
                            and e['physical'] == 'REFERENCE' and e['service'] == 'LINKS_TO'
                            and e['target'][0] == 'ExternalResource')
            tm = '2024-06-01T00:00:00Z' if kind == 'drop_end' else '2022-06-01T00:00:00Z'
            extra = {**original, 'uid': 'SYNTHETIC_EVALUATOR_WITNESS:' + kind, 'time': tm}
            fixture = edges + [extra]
            witness_gold = evaluate(nodes, fixture, specs, kind)
            witness_bad = evaluate(nodes, fixture, specs, kind, True)
            witness = {'added_edge': extra, 'reference_rows': witness_gold, 'faulty_rows': witness_bad,
                       'discriminates': bag(witness_gold) != bag(witness_bad),
                       'scope': 'Synthetic evaluator fixture; not an observed OSS event'}
        records.append({'mutation': kind, 'task_id': specs[index]['id'], 'reference_rows': gold,
                        'faulty_rows': actual, 'detected_on_original_snapshot': detected, 'witness': witness})
    signatures = Counter((e['source'][0], e['physical'], e['service'], e['target'][0]) for e in edges)
    return {'mode': 'OFFLINE_EVALUATOR_DISCRIMINATION_NOT_MODEL_ACCURACY',
        'source_hashes': {n: b.sha(data / n) for n in ('nodes.csv', 'edges.csv')},
        'profile': {'nodes': len(nodes), 'edges': len(edges), 'node_types': len({x[0] for x in nodes.values()}),
            'physical_edge_types': len({e['physical'] for e in edges}),
            'service_relation_meanings': len({e['service'] for e in edges}), 'typed_signatures': len(signatures),
            'edges_per_node': len(edges) / len(nodes)},
        'records': records, 'fault_models': len(records),
        'detected_on_original_snapshot': sum(r['detected_on_original_snapshot'] for r in records),
        'masked_on_original_snapshot': sum(not r['detected_on_original_snapshot'] for r in records),
        'all_discriminated_with_targeted_fixtures': all(r['detected_on_original_snapshot'] or r['witness']['discriminates'] for r in records),
        'limits': 'Seven predeclared faulty semantics only. No model calls, model success rate, generalization, or new-method advantage measured.'}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--data', required=True)
    p.add_argument('--output', required=True)
    a = p.parse_args()
    result = audit(Path(a.data))
    b.save(Path(a.output), result)
    print(json.dumps({k: v for k, v in result.items() if k != 'records'}, ensure_ascii=False, indent=2))
