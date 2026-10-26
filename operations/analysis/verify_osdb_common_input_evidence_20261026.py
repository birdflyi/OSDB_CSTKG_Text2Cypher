"""Independent serialized-input checks and fixed-bound time-order diagnostic.

The generic fractional-time counterexample is synthetic. Explicitly distinguish
it from an observed error at the three existing candidate cutoffs.
"""
from collections import Counter
import csv
from datetime import datetime
import hashlib
import json
from pathlib import Path

from build_osdb_actor_intake_20261026 import GRAPHS, PARAMS, read, sha, write, stamp
from audit_osdb_common_input_20261026 import OUT


def main():
    target = OUT/'independent_verification.json'
    assert not target.exists()
    protocol = read(OUT/'protocol.json'); summary = read(OUT/'summary.json')
    manifest = read(OUT/'common_input_manifest.json')
    assert len(manifest) == 24 and sha(OUT/'common_input_manifest.json') == summary['input_manifest_sha256']
    allowed = {'task_id', 'graph', 'question', 'schema', 'entity_candidates', 'database_notes'}
    for entry in manifest:
        value = read(OUT/'common_inputs'/(entry['task_id']+'.json'))
        assert set(value['payload']) == allowed and not value['formal_admission']
        assert json.loads(value['serialized']) == value['payload']
        assert hashlib.sha256(value['serialized'].encode('utf-8')).hexdigest() == value['sha256'] == entry['sha256']
        assert len(value['serialized'].encode('utf-8')) == entry['utf8_bytes']
        assert value['payload']['entity_candidates'] == []
        assert value['payload']['schema'] == read(OUT/(value['payload']['graph']+'_schema.json'))
    for name, digest in protocol['sources'].items():
        assert sha(OUT/'executed_sources'/name) == digest
    csv.field_size_limit(64*1024**2)
    times = {}
    for project, (folder, _) in GRAPHS.items():
        assert {name: sha(folder/name) for name in ('nodes.csv', 'edges.csv')} == summary['projects'][project]['source_hashes']
        precision = Counter(); mismatches = Counter(); count = 0
        with (folder/'edges.csv').open(encoding='utf-8-sig', newline='') as handle:
            for row in csv.DictReader(handle):
                value = json.loads(row['properties_json']).get('source_event_time')
                assert type(value) is str and value.endswith('Z')
                parsed = datetime.fromisoformat(value[:-1]+'+00:00')
                precision[len(value.split('.')[1][:-1]) if '.' in value else 0] += 1
                for name, bound in PARAMS.items():
                    parsed_bound = datetime.fromisoformat(bound[:-1]+'+00:00')
                    if (value < bound, value >= bound) != (parsed < parsed_bound, parsed >= parsed_bound):
                        mismatches[name] += 1
                count += 1
        assert count == summary['projects'][project]['edges']
        assert {name: sha(folder/name) for name in ('nodes.csv', 'edges.csv')} == summary['projects'][project]['source_hashes']
        times[project] = {'records': count, 'fractional_digits': dict(sorted(precision.items())),
                         'lexical_vs_instant_mismatches_at_fixed_candidate_bounds': dict(mismatches)}
        print(project, count, dict(mismatches), flush=True)
    write(target, {'verified_local': stamp(), 'script_sha256': sha(Path(__file__)),
        'common_input_records': 24, 'manifest_payload_schema_hashes_agree': True,
        'executed_sources_match_protocol': True, 'source_csv_hashes_unchanged': True,
        'timestamps_at_actual_fixed_bounds': times, 'bounds': PARAMS,
        'interpretation': 'Synthetic UTC fractional ordering counterexample is not an observed task failure. '
            'Zero mismatches here certify only these stored strings against these three fixed bounds, '
            'not general timestamp literals, future inputs, aggregation or actual method predictions.',
        'model_requests': 0, 'graph_requests': 0})


if __name__ == '__main__': main()
