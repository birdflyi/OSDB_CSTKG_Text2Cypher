"""Read-only deterministic semantic checks, independent of model predictions.

Two checks enumerate the pinned TRAIN graph; two execute compiler return/filter
fragments over synthetic UNWIND values. No source graph is mutated.
"""
from collections import Counter, defaultdict

from schema_services import compile_selection, execute_plan


def check_rows(result, expected):
    if result.get('status') != 'EXECUTED' or result.get('complete', True) is not True:
        raise RuntimeError('Semantic conformance query did not execute completely')
    if sorted(result['rows']) != sorted(expected):
        raise RuntimeError('Compiled semantics differ from independent reference')


def graph_checks(graph, schema, worker):
    nodes = {r['eid']: r for r in graph['entities']}
    signatures = sorted(schema['relations'], key=lambda r: (r['label'], r['subj_label'], r['obj_label']))
    result = []
    for relation in signatures:
        edges = [r for r in graph['relations'] if r['label'] == relation['label']
                 and nodes[r['subj_id']]['label'] == relation['subj_label']
                 and nodes[r['obj_id']]['label'] == relation['obj_label']]
        shared = defaultdict(list)
        for row in edges:
            shared[row['obj_id']].append(row)
        eligible = sorted(k for k, rows in shared.items() if len({r['subj_id'] for r in rows}) >= 2)
        if not eligible:
            continue
        anchor = min(r['subj_id'] for r in shared[eligible[0]])
        expected_ids = {other['subj_id'] for edge in edges if edge['subj_id'] == anchor
                        for other in shared[edge['obj_id']] if other['rid'] != edge['rid']}
        call = {'nodes': [{'id': 'a', 'label': relation['subj_label']},
                          {'id': 't', 'label': relation['obj_label']},
                          {'id': 'b', 'label': relation['subj_label']}],
                'edges': [{'id': 'x', 'source': 'a', 'target': 't', 'label': relation['label']},
                          {'id': 'y', 'source': 'b', 'target': 't', 'label': relation['label']}],
                'where': [{'field': 'a.eid', 'op': 'eq', 'value': anchor}],
                'returns': [{'field': 'b.eid', 'alias': 'id'}, {'field': 'b.name', 'alias': 'name'}],
                'distinct': True}
        plan = {'status': 'ANSWER', 'calls': [call], 'combine': {'op': 'single'}}
        expected = [[eid, nodes[eid]['name']] for eid in sorted(expected_ids)]
        outcomes = {}
        for strategy in ['contract', 'full_detail']:
            outcome = execute_plan(plan, schema, worker.execute, strategy=strategy)
            check_rows(outcome, expected)
            outcomes[strategy] = outcome
        result.append({'id': 'single_match_relationship_uniqueness', 'kind': 'INDEPENDENT_TRAIN_ENUMERATION',
                       'plan': plan, 'expected_rows': expected, 'outcomes': outcomes})
        break
    if not result:
        raise RuntimeError('Pinned TRAIN graph lacks edge-identity witness')
    for relation in signatures:
        subjects = {r['eid'] for r in graph['entities'] if r['label'] == relation['subj_label']}
        neighbors = defaultdict(set)
        for row in graph['relations']:
            if (row['label'] == relation['label'] and row['subj_id'] in subjects
                and nodes[row['obj_id']]['label'] == relation['obj_label']):
                neighbors[row['subj_id']].add(row['obj_id'])
        if not subjects - neighbors.keys():
            continue
        call = {'nodes': [{'id': 'n', 'label': relation['subj_label']},
                          {'id': 't', 'label': relation['obj_label']}],
                'edges': [{'id': 'r', 'source': 'n', 'target': 't', 'label': relation['label'], 'optional': True}],
                'where': [], 'returns': [{'field': 'n.eid', 'alias': 'id'},
                          {'field': 'n.name', 'alias': 'name'},
                          {'field': 't.eid', 'agg': 'count', 'distinct': True, 'alias': 'degree'}],
                'group_by': ['n.eid']}
        plan = {'status': 'ANSWER', 'calls': [call], 'combine': {'op': 'single'}}
        expected = [[eid, nodes[eid]['name'], len(neighbors[eid])] for eid in sorted(subjects)]
        outcomes = {}
        for strategy in ['contract', 'full_detail']:
            outcome = execute_plan(plan, schema, worker.execute, strategy=strategy)
            check_rows(outcome, expected)
            outcomes[strategy] = outcome
        result.append({'id': 'optional_degree_preserves_zero_groups', 'kind': 'INDEPENDENT_TRAIN_ENUMERATION',
                       'plan': plan, 'expected_rows': expected, 'outcomes': outcomes})
        break
    if len(result) != 2:
        raise RuntimeError('Pinned TRAIN graph lacks optional-zero witness')
    return result


def synthetic_fragment_checks(worker):
    schema = {'entities': [{'label': 'Person', 'properties': {}}], 'relations': []}
    cases = []
    null_rows = [{'eid': 'blocked', 'aliases': ['blocked']}, {'eid': 'empty', 'aliases': []},
                 {'eid': 'other', 'aliases': ['other']}, {'eid': 'null', 'aliases': None},
                 {'eid': 'missing'}]
    call = {'nodes': [{'id': 'n', 'label': 'Person'}], 'edges': [],
            'where': [{'field': 'n.aliases', 'op': 'contains', 'value': 'blocked', 'negate': True}],
            'returns': [{'field': 'n.eid', 'alias': 'id'}]}
    expected = [[r['eid']] for r in null_rows if r.get('aliases') is not None and 'blocked' not in r['aliases']]
    cases.append(('nullable_not_keeps_unknown_separate', call, null_rows, expected))
    grouped_rows = [{'eid': 'a', 'name': 'Same'}, {'eid': 'a', 'name': 'Same'},
                    {'eid': 'b', 'name': 'Same'}]
    call = {'nodes': [{'id': 'n', 'label': 'Person'}], 'edges': [], 'where': [],
            'returns': [{'field': 'n.name', 'alias': 'name'},
                        {'field': '*', 'agg': 'count', 'alias': 'count'}], 'group_by': ['n.eid']}
    counts = Counter(r['eid'] for r in grouped_rows)
    expected = [['Same', count] for count in counts.values()]
    cases.append(('entity_grain_not_display_name_grain', call, grouped_rows, expected))
    result = []
    for label, call, data, expected in cases:
        compiled = compile_selection(call, schema)
        prefix = 'MATCH (n:`Person`) '
        if not compiled['query'].startswith(prefix):
            raise RuntimeError('Synthetic expression binding no longer matches compiler entry')
        query = 'UNWIND $fixture_rows AS n ' + compiled['query'][len(prefix):]
        actual = worker.execute(query, {**compiled['parameters'], 'fixture_rows': data})
        check_rows(actual, expected)
        result.append({'id': label, 'kind': 'CONSTRUCTED_READ_ONLY_EXPRESSION_CHECK',
                       'compiled_fragment': query, 'expected_rows': expected, 'actual': actual})
    return result


def semantic_checks(graph, schema, worker):
    return graph_checks(graph, schema, worker) + synthetic_fragment_checks(worker)
