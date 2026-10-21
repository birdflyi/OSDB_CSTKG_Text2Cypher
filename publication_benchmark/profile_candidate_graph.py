"""Prediction-free graph profiling with the frozen typed projection.

Raw candidates are profiled without exporting/importing large graphs. Existing
execution CSVs use the same feature accumulator. No model response is read.
"""
import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
import sys


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def quantiles(values):
    values = sorted(values)
    if not values:
        return {'n': 0, 'p50': None, 'p90': None, 'max': None}
    return {'n': len(values), 'p50': values[int((len(values)-1)*.5)],
            'p90': values[int((len(values)-1)*.9)], 'max': values[-1]}


class Profile:
    def __init__(self, nodes):
        self.nodes = nodes
        self.labels = Counter(n['label'] for n in nodes.values())
        self.physical, self.services, self.signatures = Counter(), Counter(), Counter()
        self.node_properties, self.edge_properties = defaultdict(Counter), defaultdict(Counter)
        for node in nodes.values():
            self.node_properties[node['label']].update(node['properties'])
        self.time_known, self.time_missing, self.triggers = Counter(), Counter(), Counter()
        self.time_min, self.time_max = None, None
        self.link_pairs, self.link_degree, self.domains = Counter(), Counter(), Counter()
        self.out_degree, self.reference_degree = Counter(), Counter()
        self.domain_missing, self.reference_edges, self.edges = 0, 0, 0

    def add(self, source, target, physical, props):
        s, t = self.nodes[source], self.nodes[target]
        service = props['service_rel_type']
        self.edges += 1
        self.physical[physical] += 1
        self.services[service] += 1
        self.signatures['|'.join((s['label'], physical, service, t['label']))] += 1
        self.edge_properties[physical].update(props.keys())
        self.out_degree[source] += 1
        self.triggers[service + '|' + str(props.get('raw_event_trigger', ''))] += 1
        time = props.get('source_event_time')
        if time:
            self.time_known[physical] += 1
            self.time_min = min(self.time_min, time) if self.time_min else time
            self.time_max = max(self.time_max, time) if self.time_max else time
        else:
            self.time_missing[physical] += 1
        if physical == 'REFERENCE':
            self.reference_edges += 1
            self.reference_degree[source] += 1
        if s['label'] == 'PullRequest' and t['label'] == 'ExternalResource' and physical == 'REFERENCE' and service == 'LINKS_TO':
            self.link_pairs[(source, target)] += 1
            self.link_degree[source] += 1
            domain = props.get('url_domain_etld1')
            if domain:
                self.domains[domain] += 1
            else:
                self.domain_missing += 1

    def result(self):
        return {'nodes': len(self.nodes), 'edges': self.edges, 'node_types': len(self.labels),
                'physical_types': len(self.physical), 'service_meanings': len(self.services),
                'typed_signatures': len(self.signatures), 'node_counts': dict(self.labels),
                'physical_counts': dict(self.physical), 'service_counts': dict(self.services),
                'signature_counts': dict(self.signatures), 'node_property_presence': dict(self.node_properties),
                'edge_property_presence': dict(self.edge_properties),
                'time_present': dict(self.time_known), 'time_missing': dict(self.time_missing),
                'event_time_min': self.time_min, 'event_time_max': self.time_max,
                'raw_trigger_counts': dict(self.triggers), 'reference_edges': self.reference_edges,
                'reference_edge_fraction': self.reference_edges/self.edges if self.edges else 0,
                'active_source_out_degree': quantiles(self.out_degree.values()),
                'reference_source_out_degree': quantiles(self.reference_degree.values()),
                'pr_external_link_records': sum(self.link_degree.values()),
                'pr_external_link_degree': quantiles(self.link_degree.values()),
                'repeated_link_pairs': sum(n > 1 for n in self.link_pairs.values()),
                'parallel_link_excess_records': sum(n-1 for n in self.link_pairs.values()),
                'distinct_domains': len(self.domains), 'missing_domain_link_records': self.domain_missing,
                'cautions': 'Event times describe source events, not commit authored dates. Project source scope is not graph repository membership. Degree statistics condition on active sources.'}


def run(a):
    source, out = Path(a.input), Path(a.output)
    if out.exists():
        raise FileExistsError('Do not overwrite candidate profile')
    if a.kind == 'raw':
        repo = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo / 'graph-migration'))
        from projection.d1_typed_projection import D1TypedProjection, VERSION
        manifest = json.loads(Path(a.manifest).read_text(encoding='utf-8'))
        if manifest['status'] != 'COMPLETE_REQUESTED_MEMBERS' or sha(source) != manifest['files'][source.name]['sha256']:
            raise ValueError('Raw source manifest mismatch')
        projector = D1TypedProjection()
        with source.open(encoding='utf-8-sig', newline='') as f:
            graph = projector.project(csv.DictReader(f))
        nodes = {uid: {'label': n.label, 'entity_id': n.entity_id, 'properties': list(n.properties)}
                 for uid, n in graph.nodes.items() if n.properties['query_eligible']}
        p = Profile(nodes)
        for e in graph.edges:
            if e.properties['query_eligible']:
                p.add(e.source_node_uid, e.target_node_uid, e.rel_type, e.properties)
        provenance = {'raw_sha256': sha(source), 'source_manifest_sha256': sha(a.manifest),
                      'raw_records': projector.counters['input_rows'],
                      'quarantined_records': len(projector.quarantined_records),
                      'projection_version': VERSION, 'projection_sha256': sha(repo / 'graph-migration/projection/d1_typed_projection.py')}
    else:
        nodes = {}
        with (source / 'nodes.csv').open(encoding='utf-8-sig', newline='') as f:
            for r in csv.DictReader(f):
                nodes[r['node_uid']] = {'label': r['label'], 'entity_id': r['entity_id'],
                                         'properties': list(json.loads(r['properties_json']))}
        p = Profile(nodes)
        with (source / 'edges.csv').open(encoding='utf-8-sig', newline='') as f:
            for r in csv.DictReader(f):
                p.add(r['source_node_uid'], r['target_node_uid'], r['native_rel_type'], json.loads(r['properties_json']))
        provenance = {'execution_csv_hashes': {name: sha(source / name) for name in ('nodes.csv', 'edges.csv')}}
    result = {'mode': 'E4_CANDIDATE_PROFILE_NO_PREDICTIONS', 'project': a.project,
              'kind': a.kind, 'features': p.result(), 'provenance': provenance, 'profiler_sha256': sha(__file__)}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({'project': a.project, **{k: result['features'][k] for k in ('nodes','edges','reference_edges','typed_signatures','repeated_link_pairs')}}, indent=2), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    for key in ('input','project','output'):
        p.add_argument('--'+key, required=True)
    p.add_argument('--kind', choices=('raw','execution'), required=True)
    p.add_argument('--manifest')
    a = p.parse_args()
    if a.kind == 'raw' and not a.manifest:
        p.error('Raw candidates require a source manifest')
    run(a)
