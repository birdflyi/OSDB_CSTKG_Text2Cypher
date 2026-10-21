"""Source-motivated E3 feasibility cases, not the final independent workload.

Definitions restrict claims to observed events in a supplied snapshot. They do
not estimate complete GitHub community metrics. The frozen six-service catalog
has no time-filtered Issue/PR lifecycle operations; correct refusal is separate
from completing the requested task.
"""
import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path

import run_baselines as b


TASKS = [
    ('issues_new', 'SRC032', 'Count distinct Issue identifiers with an observed initial OPENED_BY event in the stated period. Do not count reopening events.', ['count']),
    ('issues_closed', 'SRC031', 'Count distinct Issue identifiers with at least one observed CLOSED_BY event in the stated period. Do not infer their current state or invalidate a close because of a later reopening.', ['count']),
    ('pr_new', 'SRC020', 'List the distinct PullRequest identifiers with an observed initial OPENED_BY event in the stated period.', ['pr_id']),
    ('pr_accepted', 'SRC016', 'Count distinct PullRequest identifiers with an observed MERGED_BY event in the stated period.', ['count']),
    ('pr_declined', 'SRC017', 'List distinct PullRequest identifiers with an observed CLOSED_BY event whose raw_event_trigger explicitly contains pull_merged=False in the stated period. Absence of a merge event is not enough.', ['pr_id']),
    ('pr_acceptance_ratio', 'SRC015', 'Count distinct accepted PullRequests using MERGED_BY and distinct declined PullRequests using CLOSED_BY with explicit pull_merged=False, within the stated period. Return accepted_count, declined_count, and accepted_count divided by declined_count rounded to 6 decimals; return null for the ratio when declined_count is zero.', ['accepted_count', 'declined_count', 'ratio']),
]


def references(data, repo_id, start='2023-07-01', end='2023-08-01'):
    """Independent CSV predicate/sets; no compiler calls or model output."""
    data = Path(data)
    nodes = {}
    with (data / 'nodes.csv').open(encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f):
            nodes[r['node_uid']] = (r['label'], r['entity_id'])
    sets = defaultdict(set)
    with (data / 'edges.csv').open(encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f):
            if r['native_rel_type'] != 'EVENT_ACTION':
                continue
            label, entity = nodes[r['source_node_uid']]
            if nodes[r['target_node_uid']][0] != 'Actor':
                continue
            p = json.loads(r['properties_json'])
            stamp = p.get('source_event_time')
            if not stamp or not start+'T00:00:00Z' <= stamp < end+'T00:00:00Z':
                continue
            service = r['service_rel_type']
            if label == 'Issue' and entity.startswith('I_'+repo_id+'#'):
                if service == 'OPENED_BY':
                    sets['issues_new'].add(entity)
                if service == 'CLOSED_BY':
                    sets['issues_closed'].add(entity)
            if label == 'PullRequest' and entity.startswith('PR_'+repo_id+'#'):
                if service == 'OPENED_BY':
                    sets['pr_new'].add(entity)
                if service == 'MERGED_BY':
                    sets['pr_accepted'].add(entity)
                if service == 'CLOSED_BY' and 'pull_merged=False' in p.get('raw_event_trigger', ''):
                    sets['pr_declined'].add(entity)
    accepted, declined = len(sets['pr_accepted']), len(sets['pr_declined'])
    return {'issues_new': [[len(sets['issues_new'])]], 'issues_closed': [[len(sets['issues_closed'])]],
            'pr_new': [[x] for x in sorted(sets['pr_new'])], 'pr_accepted': [[accepted]],
            'pr_declined': [[x] for x in sorted(sets['pr_declined'])],
            'pr_acceptance_ratio': [[accepted, declined, round(accepted/declined, 6) if declined else None]]}


def query(task):
    label = 'Issue' if task.startswith('issues_') else 'PullRequest'
    q = f'MATCH (n:{label})-[r:EVENT_ACTION]->(a:Actor) WHERE n.entity_id STARTS WITH $prefix AND r.source_event_time >= $start AND r.source_event_time < $end'
    if task == 'pr_acceptance_ratio':
        return q + " WITH count(DISTINCT CASE WHEN r.service_rel_type='MERGED_BY' THEN n.entity_id END) AS accepted_count,count(DISTINCT CASE WHEN r.service_rel_type='CLOSED_BY' AND r.raw_event_trigger CONTAINS 'pull_merged=False' THEN n.entity_id END) AS declined_count RETURN accepted_count,declined_count,CASE WHEN declined_count=0 THEN null ELSE round(1.0*accepted_count/declined_count,6) END AS ratio"
    service = {'issues_new': 'OPENED_BY', 'issues_closed': 'CLOSED_BY', 'pr_new': 'OPENED_BY', 'pr_accepted': 'MERGED_BY', 'pr_declined': 'CLOSED_BY'}[task]
    q += " AND r.service_rel_type='" + service + "'"
    if task == 'pr_declined':
        q += " AND r.raw_event_trigger CONTAINS 'pull_merged=False'"
    return q + (' RETURN DISTINCT n.entity_id AS pr_id' if task in {'pr_new','pr_declined'} else ' RETURN count(DISTINCT n.entity_id) AS count')


def schema(data):
    data = Path(data)
    nodes, node_props, signatures, triggers = {}, defaultdict(set), set(), set()
    with (data / 'nodes.csv').open(encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f):
            nodes[r['node_uid']] = r['label']
            node_props[r['label']].update(json.loads(r['properties_json']))
    edge_props = defaultdict(set)
    with (data / 'edges.csv').open(encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f):
            p = json.loads(r['properties_json'])
            signatures.add((nodes[r['source_node_uid']], r['native_rel_type'], r['service_rel_type'], nodes[r['target_node_uid']]))
            edge_props[r['native_rel_type']].update(p)
            if p.get('raw_event_trigger'):
                triggers.add(p['raw_event_trigger'])
    return {'node_property_names': {k: sorted(v) for k,v in node_props.items()},
            'edge_property_names': {k: sorted(v) for k,v in edge_props.items()},
            'typed_relationship_signatures': sorted(signatures), 'raw_event_trigger_values': sorted(triggers),
            'interpretation': 'A signature is (source label, physical type, service_rel_type, target label). Match service_rel_type as an edge property, not Cypher type syntax. source_event_time is the event producing the edge; it is not commit authored date. A source snapshot is not a complete GitHub history. Do not infer fix/introduce-bug causality from references.'}


def build(data, project, repo_id, output, source_census):
    out = Path(output)
    if out.exists():
        raise FileExistsError('Do not overwrite feasibility data')
    census = json.loads(Path(source_census).read_text(encoding='utf-8'))
    sources = {x['source_entry_id']: x for x in census['candidates']}
    rows = references(data, repo_id)
    specifications = []
    for index,(task,source_id,instruction,columns) in enumerate(TASKS,1):
        prefix = ('I_' if task.startswith('issues_') else 'PR_') + repo_id + '#'
        question = ('Use only the provided graph snapshot. Scope the '+('Issue' if task.startswith('issues_') else 'PullRequest')+
                    ' entity_id by the literal prefix '+prefix+'. The UTC period is [2023-07-01,2023-08-01). '+instruction+
                    ' Return the complete result, not a sample. These are observed snapshot facts, not total historical GitHub activity.')
        specifications.append({'id': project+'_E3_'+str(index).zfill(2), 'project': project, 'family': task,
              'group': project+':E3:'+task, 'variant_index': 0, 'question': question, 'columns': columns,
              'expected_status': 'ANSWER', 'expected_rows': rows[task],
              'service_expressible': False,
              'service_capability_reason': 'Frozen six-service catalog has no time-filtered Issue/PR lifecycle retrieval or ratio operation.',
              'reference_query': query(task), 'reference_parameters': {'prefix': prefix,'start':'2023-07-01T00:00:00Z','end':'2023-08-01T00:00:00Z'},
              'source_entry': source_id, 'source_url': sources[source_id]['url'],
              'adaptation': 'Explicit period, distinct identities, snapshot observation scope, and closure/merge policy. Not an exact full-history CHAOSS metric reproduction.'})
    b.save(out/'specifications.json', specifications)
    b.save(out/'public_tasks.json',[{k:x[k] for k in ('id','question','columns')} for x in specifications])
    b.save(out/'shared_schema.json', schema(data))
    b.save(out/'manifest.json',{'mode':'E3_FEASIBILITY_SUBSET_NOT_FORMAL_WORKLOAD','project':project,'repo_id':repo_id,
            'cases':6,'source_census_sha256':b.sha(source_census),'input_hashes':{n:b.sha(Path(data)/n) for n in ('nodes.csv','edges.csv')},
            'specifications_sha256':b.sha(out/'specifications.json'),'public_tasks_sha256':b.sha(out/'public_tasks.json'),
            'shared_schema_sha256':b.sha(out/'shared_schema.json'),'builder_sha256':b.sha(__file__),
            'selection':'Six external metric goals with explicit auditable event-count/list definitions. Chosen for reference feasibility before predictions; not a representative sample.',
            'protocol_revision':'Full shared schema metadata supplied equally to G/S; unlike E1 the task can be outside the service catalog. No new service, example or repair budget.'})
    print(json.dumps({'project':project,'cases':6,'rows_per_case':{x['family']:len(x['expected_rows']) for x in specifications}},indent=2))


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    for name in ('data','project','repo-id','output','source-census'):
        p.add_argument('--'+name, required=True)
    a=p.parse_args()
    build(a.data,a.project,a.repo_id,a.output,a.source_census)
