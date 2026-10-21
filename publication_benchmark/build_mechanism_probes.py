"""Reproducible builder for the bounded two-project development probe set."""
import argparse
from pathlib import Path

import run_baselines as b
from mechanism_core import Oracle, dataset


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--redis-data', required=True)
    p.add_argument('--duckdb-data', required=True)
    p.add_argument('--output', required=True)
    a = p.parse_args()
    out = Path(a.output).resolve()
    if out.exists():
        raise FileExistsError('Use a fresh external artifact folder')
    if Path(__file__).resolve().parents[1] in out.parents:
        raise ValueError('Keep data artifacts outside checkout')
    paths = {'redis': Path(a.redis_data), 'duckdb': Path(a.duckdb_data)}
    hashes = {}
    for name, data in paths.items():
        nodes, edges, docs = b.load_graph(data)
        records = dataset(name, Oracle(nodes, edges))
        b.save(out / (name + '_specifications.json'), records)
        b.save(out / (name + '_public_tasks.json'), [{k: r[k] for k in ('id', 'question', 'columns')} for r in records])
        hashes[name] = {n: b.sha(data / n) for n in ('nodes.csv', 'edges.csv')}
    b.save(out / 'dataset_manifest.json', {'mode': 'DEVELOPMENT_MECHANISM_PROBES_NOT_FORMAL_TEST',
        'core_sha256': b.sha(Path(__file__).with_name('mechanism_core.py')), 'builder_sha256': b.sha(__file__),
        'projects': hashes, 'schema_families': 6, 'instantiated_base_groups': 24,
        'project_request_instances': 144, 'model_generated_requests': False, 'real_users': 0,
        'wording_origin': 'AI-assisted development templates; programmatic slot instantiation; no external LLM generation call or human-user collection',
        'specification_hashes': {name: b.sha(out / (name + '_specifications.json')) for name in paths},
        'selection': 'Deterministic original-data contrast discrimination; no model response used'})
    print('Built 144 development request instances; not a real-user or held-out benchmark.', flush=True)
