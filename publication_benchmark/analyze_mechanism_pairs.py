"""E2a: complete-run paired analysis without any model dispatch or score repair.

This does not replace E2b multi-instance execution. Stable wrong answers are
not counted as robust successes. Missing records never imply correct answers.
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path

import run_baselines as b
from run_mechanism_pilot import bag


ARMS = ('G0', 'G1', 'S0', 'S1')


def analyze(specs, records):
    table = {(r['arm'], r['id']): r for r in records}
    if len(table) != len(records):
        raise ValueError('Duplicate scored record')
    expected = {(arm, s['id']) for arm in ARMS for s in specs}
    if set(table) != expected:
        raise ValueError('Analysis requires every planned record, with no extra records')
    groups = defaultdict(list)
    for s in specs:
        groups[s['group']].append(s)
    for values in groups.values():
        if sorted(s['variant_index'] for s in values) != list(range(6)):
            raise ValueError('Expected four equivalents and two contrasts per group')
        equivalent = [s for s in values if s['variant_index'] < 4]
        canonical = next(s for s in values if s['variant_index'] == 0)
        if any(bag(s['expected_rows']) != bag(canonical['expected_rows']) for s in equivalent):
            raise ValueError('Equivalent references disagree')
        if any(bag(s['expected_rows']) == bag(canonical['expected_rows']) for s in values if s['variant_index'] >= 4):
            raise ValueError('Contrast lacks observable answer difference')
    output = {}
    for arm in ARMS:
        eq_all, eq_identical_wrong, pairs_all, details = 0, 0, 0, []
        for group, values in sorted(groups.items()):
            equivalent = sorted((s for s in values if s['variant_index'] < 4), key=lambda s: s['variant_index'])
            canonical = equivalent[0]
            rr = [table[(arm, s['id'])] for s in equivalent]
            all_correct = all(r['correct'] for r in rr)
            eq_all += all_correct
            executions = [r['observation']['execution'] for r in rr]
            same = all(e['status'] == 'EXECUTED' for e in executions) and all(
                e['columns'] == executions[0]['columns'] and bag(e['rows']) == bag(executions[0]['rows']) for e in executions)
            eq_identical_wrong += same and not all_correct
            pairs = sum(table[(arm, canonical['id'])]['correct'] and table[(arm, s['id'])]['correct']
                        for s in values if s['variant_index'] >= 4)
            pairs_all += pairs
            details.append({'group': group, 'family': canonical['family'], 'equivalent_all_correct': all_correct,
                            'identical_executed_but_wrong': same and not all_correct, 'contrast_pairs_correct': pairs})
        output[arm] = {'groups': len(groups), 'equivalent_groups_all_correct': eq_all,
                       'equivalent_groups_identical_but_wrong': eq_identical_wrong,
                       'contrast_pairs': 2 * len(groups), 'contrast_pairs_both_correct': pairs_all,
                       'details': details}
    strata = {}
    for family in sorted({s['family'] for s in specs}):
        selected = [s for s in specs if s['family'] == family]
        strata[family] = {arm: {'cases': len(selected), 'correct': sum(table[(arm, s['id'])]['correct'] for s in selected)} for arm in ARMS}
    comparison = {}
    for left, right in (('S0', 'G0'), ('S1', 'G1')):
        comparison[left + '_vs_' + right] = {
            'both_correct': sum(table[(left, s['id'])]['correct'] and table[(right, s['id'])]['correct'] for s in specs),
            'left_only_correct': sum(table[(left, s['id'])]['correct'] and not table[(right, s['id'])]['correct'] for s in specs),
            'right_only_correct': sum(not table[(left, s['id'])]['correct'] and table[(right, s['id'])]['correct'] for s in specs),
            'both_wrong': sum(not table[(left, s['id'])]['correct'] and not table[(right, s['id'])]['correct'] for s in specs)}
    return {'mode': 'E2A_DEVELOPMENT_PAIRED_DIAGNOSTIC', 'request_instances': len(specs),
            'base_groups': len(groups), 'task_families': len(strata), 'arms': output,
            'family_strata': strata, 'paired_comparison': comparison,
            'multi_instance_execution': 'NOT_PERFORMED_BY_THIS_ANALYZER',
            'limitations': 'Developer-known templates. Rewordings/project bindings are correlated. No user diversity, semantic equivalence, or population generalization claim.'}


def run(inputs, output):
    specs, records, sources = [], [], []
    for value in inputs:
        root = Path(value).resolve()
        summary = json.loads((root / 'summary.json').read_text(encoding='utf-8'))
        if summary['status'] != 'PLANNED_DISPATCHES_COMPLETED':
            raise ValueError('Source run incomplete: ' + root.name)
        files = sorted((root / 'records').glob('*.json'))
        specs.extend(json.loads((root / 'private_specifications.json').read_text(encoding='utf-8')))
        records.extend(json.loads(p.read_text(encoding='utf-8')) for p in files)
        sources.append({'run': root.name, 'summary_sha256': b.sha(root / 'summary.json'),
                        'manifest_sha256': b.sha(root / 'manifest.json'),
                        'record_hashes': {p.name: b.sha(p) for p in files}})
    result = analyze(specs, records)
    result['sources'] = sources
    b.save(Path(output), result)
    print(json.dumps({k: result[k] for k in ('mode', 'request_instances', 'base_groups', 'paired_comparison')}, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--runs', nargs='+', required=True)
    p.add_argument('--output', required=True)
    a = p.parse_args()
    run(a.runs, a.output)
