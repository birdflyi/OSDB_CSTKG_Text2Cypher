"""Run the full seven-graph frozen core; stop on any interrupted graph.

No implicit retry or recovery. Completed graph records remain immutable.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import os

from model_transport import immutable_json
from run_frozen_main import GRAPHS, sha


def run(args):
    freeze = json.loads(args.freeze.read_text(encoding='utf-8'))
    if freeze['graph_order'] != list(GRAPHS):
        raise ValueError('Graph order changed')
    args.output.mkdir(parents=True, exist_ok=False)
    immutable_json(args.output/'queue_plan.json', {'pid': os.getpid(), 'freeze_sha256': sha(args.freeze),
                   'graphs': list(GRAPHS), 'planned_primary_paths': 4696,
                   'no_automatic_failed_batch_restart': True})
    results = []
    for graph, count in GRAPHS.items():
        graph_output = args.output / graph
        command = [sys.executable, str(Path(__file__).with_name('run_frozen_main.py')),
                   '--graph', graph, '--segment', 'initial_v1', '--output', str(graph_output),
                   '--max-physical-calls', str(count * 4 + 128)]
        for field in ['freeze', 'admission', 'data', 'model_config', 'ssh_key', 'budget',
                      'approval', 'private_data', 'official_source']:
            command += ['--'+field.replace('_', '-'), str(getattr(args, field))]
        with (args.output/(graph+'_runner.log')).open('xb') as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        summary_path = graph_output/'segments/initial_v1/summary.json'
        summary = json.loads(summary_path.read_text(encoding='utf-8')) if summary_path.exists() else None
        if result.returncode or not summary or summary['state'] != 'COMPLETE' or summary['remaining_paths']:
            immutable_json(args.output/'queue_stopped.json', {'graph': graph, 'returncode': result.returncode,
                           'summary': summary, 'completed_graphs': results, 'automatic_retry': False})
            print(json.dumps({'state': 'STOPPED', 'graph': graph}), flush=True)
            return
        results.append(summary)
        immutable_json(args.output/(graph+'_completed.json'), summary)
        print(json.dumps({'state': 'GRAPH_COMPLETE', 'graph': graph, 'paths': count*2}), flush=True)
    immutable_json(args.output/'queue_complete.json', {'state': 'CYPHERBENCH_CORE_PREDICTIONS_COMPLETE',
                   'graphs': results, 'entire_T1_complete': False})


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    for field in ['freeze', 'admission', 'data', 'output', 'model-config', 'ssh-key', 'budget',
                  'approval', 'private-data', 'official-source']:
        p.add_argument('--'+field, type=Path, required=True)
    run(p.parse_args())
