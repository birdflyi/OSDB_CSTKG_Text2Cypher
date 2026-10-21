"""Development service contracts and independent CSV references.

New matched-capability prototype, not the historical production NL engine.
The compiler does not inspect raw NL or prove that all NL conditions were mapped.
"""
from collections import Counter, defaultdict
from datetime import date
import hashlib
import json
import re

import run_baselines as b


CATALOG = [
    {'operation': 'issue_openers', 'arguments': {'issue_id': 'I_<number>#<number>'},
     'meaning': 'Actor identifiers on Issue -> EVENT_ACTION[OPENED_BY] -> Actor. Retain event records.',
     'columns': ['actor_id']},
    {'operation': 'pr_links', 'arguments': {'pr_id': 'PR_<number>#<number>',
        'fields': 'ordered nonempty subset of [domain,external_resource_id]', 'distinct': 'boolean'},
     'meaning': 'Outgoing PullRequest -> REFERENCE[LINKS_TO] -> ExternalResource. Domain is an edge property. DISTINCT applies to the selected row fields.',
     'columns': 'same ordered fields'},
    {'operation': 'pr_domain_counts', 'arguments': {'pr_id': 'PR_<number>#<number>',
        'start_date': 'YYYY-MM-DD', 'end_date': 'YYYY-MM-DD', 'grain': 'records|distinct_targets'},
     'meaning': 'Outgoing LINKS_TO grouped by edge domain, inclusive start / exclusive end UTC. Count edge records or distinct ExternalResource IDs per domain. Empty input has no groups.',
     'columns': ['domain', 'count']},
    {'operation': 'issue_count', 'arguments': {'issue_prefix': 'literal Issue.entity_id string prefix'},
     'meaning': 'Count all Issue nodes with this literal prefix. Prefix is not graph repository membership.',
     'columns': ['count']},
    {'operation': 'issue_commit_path_count', 'arguments': {'issue_prefix': 'literal Issue.entity_id string prefix',
        'grain': 'paths|distinct_comments|distinct_commits'},
     'meaning': 'IssueComment -> EVENT_ACTION[COMMENTED_ON_ISSUE] -> Issue, and the same comment -> REFERENCE[REFERENCES] -> Commit. Scope the Issue by prefix; count all path records or distinct comment/commit identifiers.',
     'columns': ['count']},
    {'operation': 'pr_link_count', 'arguments': {'pr_id': 'PR_<number>#<number>',
        'start_date': 'YYYY-MM-DD', 'end_date': 'YYYY-MM-DD', 'grain': 'records|distinct_targets'},
     'meaning': 'Count outgoing LINKS_TO records or distinct ExternalResource identifiers in the half-open UTC time window. Return zero when no records match.',
     'columns': ['count']},
]
CONTRACTS = {x['operation']: x for x in CATALOG}


def compile_call(call):
    if not isinstance(call, dict) or set(call) != {'operation', 'arguments'}:
        raise ValueError('Expected operation and arguments only')
    op, a = call['operation'], call['arguments']
    if op not in CONTRACTS or not isinstance(a, dict) or set(a) != set(CONTRACTS[op]['arguments']):
        raise ValueError('Unknown operation or missing/extra arguments')
    for key in ('issue_id', 'pr_id', 'issue_prefix'):
        if key not in a:
            continue
        pattern = {'issue_id': r'I_\d+#\d+', 'pr_id': r'PR_\d+#\d+',
                   'issue_prefix': r'I_(?:\d+(?:#\d*)?)?'}[key]
        if not isinstance(a[key], str) or not re.fullmatch(pattern, a[key]):
            raise ValueError('Invalid identifier or prefix')
    if 'start_date' in a:
        for k in ('start_date', 'end_date'):
            if not isinstance(a[k], str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', a[k]):
                raise ValueError('Invalid date')
            date.fromisoformat(a[k])
        if a['start_date'] >= a['end_date']:
            raise ValueError('Invalid window')
    if 'grain' in a:
        allowed = {'paths', 'distinct_comments', 'distinct_commits'} if op == 'issue_commit_path_count' else {'records', 'distinct_targets'}
        if a['grain'] not in allowed:
            raise ValueError('Invalid grain')
    params = {k: v for k, v in a.items() if k not in {'fields', 'distinct', 'grain'}}
    for k in ('start_date', 'end_date'):
        if k in params:
            params[k] += 'T00:00:00Z'
    if op == 'issue_openers':
        query = "MATCH (i:Issue {entity_id:$issue_id})-[r:EVENT_ACTION]->(a:Actor) WHERE r.service_rel_type='OPENED_BY' RETURN a.entity_id AS actor_id"
        columns = ['actor_id']
    elif op == 'issue_count':
        query = 'MATCH (i:Issue) WHERE i.entity_id STARTS WITH $issue_prefix RETURN count(i) AS count'
        columns = ['count']
    elif op == 'issue_commit_path_count':
        value = {'paths': 'count(*)', 'distinct_comments': 'count(DISTINCT c.entity_id)',
                 'distinct_commits': 'count(DISTINCT k.entity_id)'}[a['grain']]
        query = "MATCH (c:IssueComment)-[a:EVENT_ACTION]->(i:Issue),(c)-[r:REFERENCE]->(k:Commit) WHERE i.entity_id STARTS WITH $issue_prefix AND a.service_rel_type='COMMENTED_ON_ISSUE' AND r.service_rel_type='REFERENCES' RETURN " + value + ' AS count'
        columns = ['count']
    else:
        query = "MATCH (p:PullRequest {entity_id:$pr_id})-[r:REFERENCE]->(x:ExternalResource) WHERE r.service_rel_type='LINKS_TO'"
        if op == 'pr_links':
            fields = a['fields']
            if not isinstance(fields, list) or not fields or len(fields) != len(set(fields)) or set(fields) - {'domain', 'external_resource_id'} or type(a['distinct']) is not bool:
                raise ValueError('Invalid projection or multiplicity')
            expressions = {'domain': 'r.url_domain_etld1', 'external_resource_id': 'x.entity_id'}
            query += ' RETURN ' + ('DISTINCT ' if a['distinct'] else '') + ','.join(expressions[f] + ' AS ' + f for f in fields)
            columns = fields
        else:
            query += ' AND r.source_event_time >= $start_date AND r.source_event_time < $end_date'
            count = 'count(r)' if a['grain'] == 'records' else 'count(DISTINCT x.entity_id)'
            query += ' RETURN ' + ('r.url_domain_etld1 AS domain,' if op == 'pr_domain_counts' else '') + count + ' AS count'
            columns = ['domain', 'count'] if op == 'pr_domain_counts' else ['count']
    return {'query': query, 'parameters': params, 'columns': columns}


class Oracle:
    def __init__(self, nodes, edges):
        self.nodes, self.edges = nodes, edges
        if len(set(nodes.values())) != len(nodes):
            raise ValueError('Reference needs unique typed entity identities')
        self.openers, self.links = defaultdict(list), defaultdict(list)
        comments = defaultdict(list)
        for e in edges:
            if e['source'][0] == 'Issue' and e['target'][0] == 'Actor' and e['physical'] == 'EVENT_ACTION' and e['service'] == 'OPENED_BY':
                self.openers[e['source'][1]].append(e['target'][1])
            if e['source'][0] == 'PullRequest' and e['target'][0] == 'ExternalResource' and e['physical'] == 'REFERENCE' and e['service'] == 'LINKS_TO':
                self.links[e['source'][1]].append(e)
            if e['source'][0] == 'IssueComment' and e['target'][0] == 'Issue' and e['physical'] == 'EVENT_ACTION' and e['service'] == 'COMMENTED_ON_ISSUE':
                comments[e['source_uid']].append(e['target'][1])
        self.paths = []
        for e in edges:
            if e['source_uid'] in comments and e['target'][0] == 'Commit' and e['physical'] == 'REFERENCE' and e['service'] == 'REFERENCES':
                self.paths.extend((issue, e['source'][1], e['target'][1]) for issue in comments[e['source_uid']])

    def expected(self, call):
        op, a = call['operation'], call['arguments']
        if op == 'issue_openers':
            return [[actor] for actor in self.openers[a['issue_id']]]
        if op == 'issue_count':
            return [[sum(typ == 'Issue' and eid.startswith(a['issue_prefix']) for typ, eid in self.nodes.values())]]
        if op == 'issue_commit_path_count':
            paths = [p for p in self.paths if p[0].startswith(a['issue_prefix'])]
            n = len(paths) if a['grain'] == 'paths' else len({p[1 if a['grain'] == 'distinct_comments' else 2] for p in paths})
            return [[n]]
        links = self.links[a['pr_id']]
        if op == 'pr_links':
            rows = [[{'domain': e['domain'], 'external_resource_id': e['target'][1]}[f] for f in a['fields']] for e in links]
            if a['distinct']:
                rows = [list(r) for r in sorted(set(tuple(r) for r in rows), key=str)]
            return rows
        lo, hi = a['start_date'] + 'T00:00:00Z', a['end_date'] + 'T00:00:00Z'
        selected = [e for e in links if e['time'] and lo <= e['time'] < hi]
        if op == 'pr_link_count':
            return [[len(selected) if a['grain'] == 'records' else len({e['target'][1] for e in selected})]]
        if op == 'pr_domain_counts':
            grouped = defaultdict(list)
            for e in selected:
                grouped[e['domain']].append(e['target'][1])
            return [[domain, len(rows) if a['grain'] == 'records' else len(set(rows))] for domain, rows in grouped.items()]
        raise ValueError(op)


def wording(call, style):
    op, a = call['operation'], call['arguments']
    if op == 'issue_openers':
        core = f"Who opened Issue {a['issue_id']}? Return Actor identifiers."
        alternate = f"For Issue {a['issue_id']}, give me the IDs of its opening actors."
    elif op == 'pr_links':
        fields = ', then '.join(a['fields'])
        grain = 'remove duplicate returned rows' if a['distinct'] else 'keep repeated link records'
        core = f"List outgoing LINKS_TO entries for PullRequest {a['pr_id']}. Return {fields}; {grain}."
        alternate = f"Which external resources does PullRequest {a['pr_id']} link to? Give {fields}, and {grain}."
    elif op == 'issue_count':
        core = f"Count every Issue node whose entity_id starts with the literal prefix {a['issue_prefix']}. Return the total count."
        alternate = f"How many Issue identifiers begin with {a['issue_prefix']}? I need the complete node count, not a sample."
    elif op == 'issue_commit_path_count':
        what = {'paths': 'matching path records', 'distinct_comments': 'distinct IssueComment identifiers', 'distinct_commits': 'distinct Commit identifiers'}[a['grain']]
        core = f"For Issue entity_id prefix {a['issue_prefix']}, find IssueComments that COMMENTED_ON_ISSUE the Issue and whose same comment REFERENCES a Commit. Count {what}."
        alternate = f"Under the literal Issue ID prefix {a['issue_prefix']}, follow a comment to its Issue and that same comment to a referenced Commit. How many {what} are there?"
    else:
        what = 'link records' if a['grain'] == 'records' else 'distinct ExternalResource identifiers'
        group = ' per link domain' if op == 'pr_domain_counts' else ''
        core = f"For PullRequest {a['pr_id']}, count outgoing LINKS_TO {what}{group} on or after {a['start_date']} and before {a['end_date']} UTC."
        alternate = f"How many outgoing LINKS_TO {what} does PullRequest {a['pr_id']} have{group} in [{a['start_date']}, {a['end_date']}) UTC?"
        if op == 'pr_link_count':
            core += ' Include zero.'
            alternate += ' Return zero if none match.'
    return [core, alternate, 'For my repository activity check: ' + alternate,
            'I am preparing a collaboration report. ' + core][style]


def dataset(project, oracle):
    # Deterministic discriminability selection, independent of any model output.
    actors, issues = set(), []
    for issue in sorted(oracle.openers):
        rows = oracle.openers[issue]
        if len(rows) == 1 and rows[0] not in actors:
            issues.append(issue)
            actors.add(rows[0])
        if len(issues) == 4:
            break
    prs = []
    for pr in sorted(oracle.links):
        links = oracle.links[pr]
        inside = [e for e in links if e['time'] and '2023-01-01T00:00:00Z' <= e['time'] < '2024-01-01T00:00:00Z']
        if 2 <= len(links) <= 50 and len(inside) > len({e['target'][1] for e in inside}):
            prs.append(pr)
    ids = sorted(eid for typ, eid in oracle.nodes.values() if typ == 'Issue')
    prefixes = {eid.split('#')[0] for eid in ids}
    prefixes.update(eid.split('#')[0] + '#' + eid.split('#')[1][0] for eid in ids)
    counts = {p: sum(eid.startswith(p) for eid in ids) for p in prefixes}
    prefixes = sorted((p for p in prefixes if 1 < counts[p] < len(ids)), key=lambda p: (-counts[p], p))
    path_prefixes = {issue.split('#')[0] for issue, c, k in oracle.paths}
    path_prefixes.update(issue.split('#')[0] + '#' + issue.split('#')[1][0] for issue, c, k in oracle.paths)
    path_prefixes.update(issue for issue, c, k in oracle.paths)
    valid_paths = []
    for p in sorted(path_prefixes, key=lambda p: (len(p), p)):
        x = [row for row in oracle.paths if row[0].startswith(p)]
        if len(x) > len({r[1] for r in x}) and len(x) > len({r[2] for r in x}):
            valid_paths.append(p)
    if min(len(issues) // 2, len(prs), len(prefixes), len(valid_paths)) < 2:
        raise ValueError('Insufficient discriminating anchors; do not fill by guessing')
    records = []
    for family, op in enumerate(CONTRACTS):
        for anchor in range(2):
            pr = prs[anchor]
            if op == 'issue_openers':
                a = {'issue_id': issues[anchor]}
                contrasts = [({'issue_id': issues[2]}, 'anchor'), ({'issue_id': issues[3]}, 'anchor')]
            elif op == 'pr_links':
                a = {'pr_id': pr, 'fields': ['domain', 'external_resource_id'], 'distinct': False}
                contrasts = [({**a, 'fields': ['domain']}, 'projection'), ({**a, 'distinct': True}, 'multiplicity')]
            elif op == 'issue_count':
                p = prefixes[anchor]
                narrow = next(i for i in ids if i.startswith(p) and len(i) > len(p)
                              and sum(x.startswith(i) for x in ids) < counts[p])
                a = {'issue_prefix': p}
                contrasts = [({'issue_prefix': narrow}, 'scope'), ({'issue_prefix': 'I_'}, 'scope')]
            elif op == 'issue_commit_path_count':
                a = {'issue_prefix': valid_paths[anchor], 'grain': 'paths'}
                contrasts = [({**a, 'grain': 'distinct_comments'}, 'grain'), ({**a, 'grain': 'distinct_commits'}, 'grain')]
            else:
                a = {'pr_id': pr, 'start_date': '2023-01-01', 'end_date': '2024-01-01', 'grain': 'records'}
                contrasts = [({**a, 'start_date': '2022-01-01', 'end_date': '2023-01-01'}, 'time_window'),
                             ({**a, 'grain': 'distinct_targets'}, 'grain')]
            base = {'operation': op, 'arguments': a}
            group = f'{project}:{family}:{anchor}'
            base_rows = oracle.expected(base)
            variants = [(base, style, 'equivalent', 'none') for style in range(4)]
            variants += [({'operation': op, 'arguments': other}, 0, 'contrast', dim) for other, dim in contrasts]
            for index, (call, style, variant, dim) in enumerate(variants):
                compiled, expected = compile_call(call), oracle.expected(call)
                if variant == 'contrast' and Counter(map(json.dumps, expected)) == Counter(map(json.dumps, base_rows)):
                    raise ValueError(f'Contrast not discriminating: {op}, anchor={anchor}, dimension={dim}')
                request_id = 'M' + hashlib.sha256((group + ':' + str(index)).encode()).hexdigest()[:12]
                records.append({'id': request_id, 'question': wording(call, style), 'columns': compiled['columns'],
                    'expected_status': 'ANSWER', 'expected_rows': expected, 'reference_call': call,
                    'project': project, 'family': op, 'group': group, 'variant_index': index,
                    'variant': variant, 'changed_dimension': dim,
                    'provenance': 'Program-instantiated developer-authored English development probe; not real-user data or held-out formal test'})
    return records
