"""Synthetic execution fixtures fixed from task scopes, never model outputs.

These are not observed OSS records. References must be recalculated per fixture.
The fixtures target declared omissions; they do not prove query equivalence.
"""
import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import run_baselines as b
from mechanism_core import Oracle


def build(specs, variant):
    nodes, edges = {}, []

    def node(label, identity):
        uid = label + ':' + identity
        nodes[uid] = {'node_uid': uid, 'label': label, 'entity_id': identity,
                      'properties_json': json.dumps({'entity_id': identity})}
        return uid

    def edge(source, target, physical, service, **extra):
        uid = 'E2_' + str(len(edges))
        edges.append({'source_node_uid': source, 'target_node_uid': target,
                      'native_rel_type': physical, 'service_rel_type': service,
                      'properties_json': json.dumps({'edge_uid': uid, 'service_rel_type': service, **extra})})

    issues, prs, windows = set(), set(), set()
    for spec in specs:
        a = spec['reference_call']['arguments']
        if 'issue_id' in a:
            issues.add(a['issue_id'])
        if 'issue_prefix' in a:
            p = a['issue_prefix']
            issues.add(p + ('987' if '#' in p else '900#987'))
        if 'pr_id' in a:
            prs.add(a['pr_id'])
        if 'start_date' in a:
            windows.add((a['pr_id'], a['start_date'], a['end_date']))
    issues.add('I_999999991#987')
    for index, issue in enumerate(sorted(issues)):
        i = node('Issue', issue)
        a = node('Actor', 'A_' + str(88000000 + index))
        edge(i, a, 'EVENT_ACTION', 'OPENED_BY')
        if variant == 'multiplicity':
            edge(i, a, 'EVENT_ACTION', 'OPENED_BY')
            edge(i, node('Actor', 'A_' + str(89000000 + index)), 'EVENT_ACTION', 'CLOSED_BY')
        commits = [node('Commit', 'C_E2_' + str(index) + '_' + str(j)) for j in range(2)]
        for j in range(3):
            c = node('IssueComment', 'IC_E2_' + str(index) + '_' + str(j))
            edge(c, i, 'EVENT_ACTION', 'COMMENTED_ON_ISSUE')
            edge(c, commits[j % 2], 'REFERENCE', 'REFERENCES')
            if variant == 'multiplicity' and j == 0:
                edge(c, commits[0], 'REFERENCE', 'REFERENCES')
                edge(c, commits[1], 'REFERENCE', 'REFERENCES')
    for index, pr in enumerate(sorted(prs)):
        p = node('PullRequest', pr)
        targets = [node('ExternalResource', 'E_E2_' + str(index) + '_' + str(j)) for j in range(2)]
        selected = [(lo, hi) for anchor, lo, hi in sorted(windows) if anchor == pr]
        if not selected:
            selected = [('2023-01-01', '2024-01-01')]
        for lo, hi in selected:
            if variant == 'base' and lo < '2023-01-01':
                continue
            lower = datetime.fromisoformat(lo).replace(tzinfo=timezone.utc)
            upper = datetime.fromisoformat(hi).replace(tzinfo=timezone.utc)
            dates = [lower]
            if variant != 'base':
                dates += [lower - timedelta(seconds=1), upper, lower + (upper - lower) / 2]
            for j, date in enumerate(dates):
                stamp = date.isoformat(timespec='seconds').replace('+00:00', 'Z')
                edge(p, targets[j % 2], 'REFERENCE', 'LINKS_TO', source_event_time=stamp,
                     url_domain_etld1='alpha.example' if j % 2 == 0 else 'beta.example')
                if variant == 'multiplicity':
                    edge(p, targets[j % 2], 'REFERENCE', 'LINKS_TO', source_event_time=stamp,
                         url_domain_etld1='alpha.example' if j % 2 == 0 else 'beta.example')
            if variant == 'multiplicity':
                edge(p, targets[0], 'REFERENCE', 'LINKS_TO', url_domain_etld1='alpha.example')
                edge(p, targets[0], 'REFERENCE', 'MENTIONS', source_event_time=lo + 'T00:00:00Z', url_domain_etld1='decoy.example')
    return list(nodes.values()), edges


def run(source, output):
    source, output = Path(source), Path(output)
    if output.exists():
        raise FileExistsError('Never overwrite frozen fixtures')
    specs = []
    for project in ('redis', 'duckdb'):
        specs.extend(json.loads((source / (project + '_specifications.json')).read_text(encoding='utf-8')))
    manifest = {'mode': 'SYNTHETIC_E2_EXECUTION_NOT_OBSERVED_OSS',
                'selection': 'Task anchors and scopes only; no model predictions consulted',
                'source_hashes': {p.name: b.sha(p) for p in source.glob('*_specifications.json')}, 'fixtures': {}}
    for variant in ('base', 'boundary', 'multiplicity'):
        nodes, edges = build(specs, variant)
        folder = output / variant
        folder.mkdir(parents=True)
        for filename, rows in (('nodes.csv', nodes), ('edges.csv', edges)):
            with (folder / filename).open('x', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0]))
                w.writeheader()
                w.writerows(rows)
        nn, ee, _ = b.load_graph(folder)
        oracle = Oracle(nn, ee)
        b.save(folder / 'expected.json', [{'id': spec['id'], 'columns': spec['columns'],
               'expected_rows': oracle.expected(spec['reference_call'])} for spec in specs])
        manifest['fixtures'][variant] = {'nodes': len(nodes), 'edges': len(edges),
            'hashes': {name: b.sha(folder / name) for name in ('nodes.csv', 'edges.csv', 'expected.json')}}
    b.save(output / 'manifest.json', manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source', required=True)
    p.add_argument('--output', required=True)
    a = p.parse_args()
    run(a.source, a.output)
