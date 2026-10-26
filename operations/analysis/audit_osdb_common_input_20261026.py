"""One offline census and actual-compiler admission diagnostic, not predictions.

Never starts a service, sends requests or changes frozen runtime source. Existing
output must be retained; failures cannot be silently retried into the same path.
"""
from collections import Counter, defaultdict
from copy import deepcopy
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

from osdb_common_input import common_input, observed_type, property_kind, VERSION
from osdb_actor_reference import utc_microseconds
from build_osdb_actor_intake_20261026 import GRAPHS, ROOT, REPO, CANON, inspect_budget, runtime, read, sha, write, stamp

OUT = ROOT/'reports/osdb_common_input_dev_v1_20261026'


def add(stats, properties):
    if type(properties) is not dict:
        raise ValueError('Property map required')
    for key, value in properties.items():
        stats[key][observed_type(value)] += 1


def declarations(stats):
    return {key: property_kind(types) for key, types in sorted(stats.items())}


def census(folder):
    nodes = {}; identities = set(); edge_ids = set()
    ns = defaultdict(lambda: defaultdict(Counter)); es = defaultdict(lambda: defaultdict(Counter))
    nc = Counter(); ec = Counter(); timestamps = Counter()
    for row in csv.DictReader((folder/'nodes.csv').open(encoding='utf-8-sig', newline='')):
        props = json.loads(row['properties_json'])
        label, uid, entity = row['label'], row['node_uid'], props.get('entity_id')
        if type(entity) is not str or not entity or entity != row['entity_id']:
            raise ValueError('Missing/conflicting typed identity')
        if uid in nodes or (label, entity) in identities:
            raise ValueError('Duplicate storage or typed entity identity')
        if label == 'PilotNode' or '__pilot_uid' in props:
            raise ValueError('Semantic source unexpectedly contains importer metadata')
        nodes[uid] = label; identities.add((label, entity)); nc[label] += 1
        add(ns[label], props)
    for row in csv.DictReader((folder/'edges.csv').open(encoding='utf-8-sig', newline='')):
        props = json.loads(row['properties_json'])
        edge = props.get('edge_uid')
        if type(edge) is not str or not edge or edge in edge_ids:
            raise ValueError('Missing/duplicate physical edge identity')
        edge_ids.add(edge)
        signature = (row['native_rel_type'], nodes[row['source_node_uid']], nodes[row['target_node_uid']])
        if props.get('native_rel_type') != signature[0] or props.get('service_rel_type') != row['service_rel_type']:
            raise ValueError('Relationship/property discriminator differs from CSV columns')
        ec[signature] += 1; add(es[signature], props)
        when = props.get('source_event_time')
        if when is None:
            timestamps['null_or_missing'] += 1
        else:
            try: utc_microseconds(when)
            except ValueError: timestamps['not_canonical_UTC'] += 1
            else: timestamps['canonical_UTC'] += 1
    schema = {'entities': [{'label': label, 'identity_property': 'entity_id',
                           'properties': declarations(ns[label])} for label in sorted(ns)],
              'relations': [{'label': rel, 'subj_label': a, 'obj_label': b,
                             'properties': declarations(es[(rel, a, b)])}
                            for rel, a, b in sorted(es)]}
    # Counts/type frequencies stay outside the model schema.
    evidence = {'nodes': sum(nc.values()), 'edges': sum(ec.values()),
        'typed_identity_unique_nonempty': True, 'edge_identity_unique_nonempty': True,
        'node_counts': dict(sorted(nc.items())),
        'relation_counts': [{'signature': list(k), 'count': v} for k, v in sorted(ec.items())],
        'node_property_observations': {k: dict(v) for k, v in sorted(ns.items())},
        'relation_property_observations': [{'signature': list(k), 'properties': dict(v)} for k, v in sorted(es.items())],
        'source_event_time_observations': dict(timestamps),
        'schema_origin': 'Full semantic CSV census, not a fresh live database catalog',
        'storage_metadata': {'additional_label': 'PilotNode', 'additional_property': '__pilot_uid',
                             'access_policy': 'UNRESOLVED_NOT_ENFORCED_BY_THIS_PURE_BUILDER'}}
    return schema, evidence


def compiler_diagnostic(schema):
    from schema_services import compile_selection, execute_plan, UnsupportedPlan
    call = {'nodes': [{'id': 'a', 'label': 'Actor'}], 'edges': [], 'where': [],
            'returns': [{'field': 'a.entity_id', 'alias': 'id'}]}
    calls_to_executor = []
    def no_database(query, params):
        calls_to_executor.append(query)
        raise AssertionError('Rejected admission must precede executor dispatch')
    pair = {'status': 'ANSWER', 'calls': [deepcopy(call), deepcopy(call)],
            'combine': {'op': 'difference', 'key': 'id', 'output': 'count', 'alias': 'n'}}
    try: execute_plan(pair, schema, no_database)
    except UnsupportedPlan as exc: identity_error = str(exc)
    else: raise AssertionError('Actual entity_id composition unexpectedly admitted')
    assert 'eid' in identity_error and not calls_to_executor
    phantom = deepcopy(call); phantom['returns'][0]['field'] = 'a.eid'
    phantom_query = compile_selection(phantom, schema)['query']
    assert 'eid' not in next(r['properties'] for r in schema['entities'] if r['label'] == 'Actor')
    assert '`eid`' in phantom_query
    temporal = {'nodes': [{'id': 'i', 'label': 'Issue'}, {'id': 'a', 'label': 'Actor'}],
        'edges': [{'id': 'r', 'source': 'i', 'target': 'a', 'label': 'EVENT_ACTION'}],
        'where': [{'field': 'r.source_event_time', 'op': 'lt', 'value': '2023-07-01T00:00:00.1Z'}],
        'returns': [{'field': 'a.entity_id', 'alias': 'id'}]}
    compiled = compile_selection(temporal, schema)
    assert 'datetime(' not in compiled['query']
    earlier, later = '2023-07-01T00:00:00Z', '2023-07-01T00:00:00.1Z'
    assert utc_microseconds(earlier) < utc_microseconds(later) and not earlier < later
    too_many = deepcopy(pair); too_many['calls'] *= 2
    try: execute_plan(too_many, schema, no_database)
    except UnsupportedPlan as exc: arity_error = str(exc)
    else: raise AssertionError('Four leaf selections unexpectedly admitted')
    assert not calls_to_executor
    return {'actual_identity_composition_rejected_before_dispatch': identity_error,
        'undeclared_eid_compiles_due_to_legacy_metadata_injection': True,
        'compiled_temporal_property_is_lexical_not_datetime': True,
        'fixed_UTC_fractional_counterexample': {'earlier': earlier, 'later': later,
            'instant_order': True, 'lexical_order': False},
        'four_leaf_plan_rejected_before_dispatch': arity_error,
        'database_dispatches': len(calls_to_executor),
        'limits': 'Developer witnesses, not a complete expressibility proof or measured task coverage. '
                  'Four leaf Issue/PR-by-window construction exceeds current arity; a generic polymorphic '
                  'storage view could reduce arity, but its schema/access semantics are not admitted. '
                  'Never invent eid, collapse labels, or compare UTC strings to evade these gates.'}


def main():
    assert not OUT.exists(), 'Preserve an existing attempt, including failure artifacts'
    checks = subprocess.run([sys.executable, '-m', 'unittest', 'test_osdb_common_input', '-v'],
        cwd=Path(__file__).parent, capture_output=True, text=True)
    assert checks.returncode == 0, checks.stderr
    budget = inspect_budget(); assert not budget['pending']
    before = runtime()
    frozen = read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')['code_files']
    assert all(sha(CANON/p) == h for p, h in frozen.items())
    registry_path = ROOT/'reports/osdb_actor_candidate_intake_v1_20261026/candidate_registry.json'
    assert sha(registry_path) == '26f24882852e446ae0d28957d3f60b0edaf5d2ec68e66d275d0159a83ac183b3'
    registry = read(registry_path); assert len(registry) == 24
    OUT.mkdir()
    for p in (Path(__file__), Path(__file__).with_name('osdb_common_input.py'),
              Path(__file__).with_name('test_osdb_common_input.py')):
        (OUT/'executed_sources').mkdir(exist_ok=True)
        (OUT/'executed_sources'/p.name).write_bytes(p.read_bytes())
    write(OUT/'protocol.json', {'sealed_local': stamp(), 'version': VERSION,
        'planned_projects': list(GRAPHS), 'planned_common_inputs': 24,
        'registry_sha256': sha(registry_path), 'budget_before': budget, 'runtime_before': before,
        'canonical_frozen': frozen, 'checks': checks.stderr,
        'sources': {p.name: sha(p) for p in (Path(__file__), Path(__file__).with_name('osdb_common_input.py'),
                                          Path(__file__).with_name('test_osdb_common_input.py'))},
        'rules': ['No model, graph, SSH or network request; no old service/queue resume.',
                  'All 24 exposed developer positions retained; no formal task denominator or coverage claim.',
                  'Full semantic CSV schema for both arms; no gold/reference/provenance/status in user input.',
                  'Storage metadata policy and runtime integration remain pending, not implicitly restricted.',
                  'Observed types are not coerced; mixed/null-only properties retained and explicitly typed.',
                  'Frozen32 and active46 runtime bytes remain unchanged.']})
    sys.path.insert(0, str(REPO/'experiment-harness/public_benchmark_v1'))
    csv.field_size_limit(64*1024**2)
    summaries = {}; inputs = []
    for project, (folder, receipt_path) in GRAPHS.items():
        started = time.monotonic(); receipt = read(receipt_path)
        hashes = {name: sha(folder/name) for name in ('nodes.csv', 'edges.csv')}
        assert hashes == receipt['source_hashes']
        schema, evidence = census(folder)
        assert (evidence['nodes'], evidence['edges']) == (receipt['nodes'], receipt['edges'])
        witness = compiler_diagnostic(schema)
        for row in registry:
            if row['project'] != project: continue
            left = common_input(task_id=row['task_id'], graph=project, question=row['question'], schema=schema)
            right = common_input(task_id=row['task_id'], graph=project, question=row['question'], schema=deepcopy(schema))
            assert left['serialized'] == right['serialized'] and left['sha256'] == right['sha256']
            write(OUT/'common_inputs'/(row['task_id']+'.json'), left)
            inputs.append({'task_id': row['task_id'], 'sha256': left['sha256'],
                           'utf8_bytes': len(left['serialized'].encode('utf-8')),
                           'both_arms_identical': True})
        assert {name: sha(folder/name) for name in hashes} == hashes
        write(OUT/(project+'_schema.json'), schema)
        write(OUT/(project+'_census.json'), evidence)
        write(OUT/(project+'_compiler_diagnostic.json'), witness)
        summaries[project] = {'nodes': evidence['nodes'], 'edges': evidence['edges'],
            'entity_labels': len(schema['entities']), 'relation_signatures': len(schema['relations']),
            'property_kinds': dict(Counter(p for r in schema['entities']+schema['relations'] for p in r['properties'].values())),
            'source_event_time_observations': evidence['source_event_time_observations'],
            'schema_sha256': sha(OUT/(project+'_schema.json')), 'source_hashes': hashes,
            'offline_scan_and_checks_seconds': round(time.monotonic()-started, 3)}
        print(json.dumps({'project_complete': project, 'nodes': evidence['nodes'], 'edges': evidence['edges']}), flush=True)
    assert len(inputs) == 24 and len({r['task_id'] for r in inputs}) == 24
    assert before == runtime()
    assert all(sha(CANON/p) == h for p, h in frozen.items())
    assert sha(registry_path) == read(OUT/'protocol.json')['registry_sha256']
    after_budget = inspect_budget(); assert after_budget == budget
    write(OUT/'common_input_manifest.json', inputs)
    summary = {'kind': 'OSDB_COMMON_INPUT_AND_ACTUAL_COMPILER_DEVELOPMENT_ADMISSION_AUDIT',
        'completed_local': stamp(), 'version': VERSION, 'projects': summaries,
        'semantic_schemas_planned': 4, 'semantic_schemas_completed': 4,
        'developer_common_inputs_planned': 24, 'developer_common_inputs_prepared': len(inputs),
        'common_user_bytes_equal_both_arms': True, 'input_candidates_each_arm': 0,
        'actual_compiler_diagnostic_projects': 4, 'diagnostic_executor_dispatches': 0,
        'unit_checks_passed': 6, 'formal_N': None, 'formal_model_dispatched': 0,
        'formal_model_completed': 0, 'formal_model_failed': 0, 'formal_model_undispatched': None,
        'model_requests': 0, 'graph_requests': 0, 'network_requests': 0,
        'budget': after_budget, 'frozen32_unchanged': True, 'runtime46_unchanged': True,
        'formal_admission': False, 'entire_T1_complete': False,
        'blocking': ['Actual-property identity and zoned string-time compiler profile not implemented/accepted.',
                     'Polymorphic label/storage-view access policy and full runtime enforcement not accepted.',
                     'Sufficient independent task scope, development method, feedback/scoring/controls freeze and separate OSDB budget missing.'],
        'not_claimed': ['Current task EX or complete compiler coverage', 'Live database full schema introspection',
                        'Independent confirmation tasks', 'A four-leaf construction is a general impossibility proof',
                        'Fewer tokens, cheaper execution, or completed T4'],
        'local_evidence': str(OUT), 'input_manifest_sha256': sha(OUT/'common_input_manifest.json')}
    write(OUT/'summary.json', summary)
    print(json.dumps({'completed': True, 'inputs': len(inputs), 'budget': after_budget}), flush=True)


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        if OUT.exists() and not (OUT/'summary.json').exists() and not (OUT/'failure.json').exists():
            write(OUT/'failure.json', {'failed_local': stamp(), 'type': type(exc).__name__,
                                      'message': str(exc), 'model_requests': 0, 'graph_requests': 0})
        raise
