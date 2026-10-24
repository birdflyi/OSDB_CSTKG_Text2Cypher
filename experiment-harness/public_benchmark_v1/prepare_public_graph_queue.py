"""Sequential, resumeless public graph data preflights; no model/GPU calls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

from model_transport import immutable_json


REMOTE = '/mnt/data/louzehua/ch7_t1_public_benchmark_20261023'


def run(args):
    manifest = json.loads((args.data / 'manifest.json').read_text(encoding='utf-8'))
    entries = {r['path']: r for r in json.loads((args.metadata / 'tree.json').read_text(encoding='utf-8'))}
    jobs = sorted(manifest['splits']['test']['by_graph'],
                  key=lambda name: entries[f'graphs/simplekg/{name}_simplekg.json']['size'])
    if args.skip_flight:
        jobs.remove('flight_accident')
    if args.start_at:
        jobs = jobs[jobs.index(args.start_at):]
    if args.reuse_verified_assets and not args.recovery_reason:
        raise ValueError('Verified asset reuse requires an explicit recovery reason')
    if any(not re.fullmatch(r'[a-z_]+', name) for name in jobs):
        raise ValueError('Unexpected graph name')
    args.output.mkdir(parents=True, exist_ok=False)
    source_dir = Path(__file__).resolve().parent
    remote_hashes = {name: hashlib.sha256((source_dir / name).read_bytes()).hexdigest()
                     for name in ['public_graph_runtime.py', 'answer_scoring.py',
                                  'smoke_train_database.py', 'model_transport.py', 'attempt_budget.py',
                                  'schema_identity.py']}
    immutable_json(args.output / 'plan.json', {'kind': 'PUBLIC_DATA_ONLY_NO_MODEL_QUEUE',
                   'pid': os.getpid(), 'graphs': jobs, 'model_calls': 0,
                   'remote_module_hashes': remote_hashes,
                   'recovery_reason': args.recovery_reason,
                   'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', '-p', '2491',
           '-i', str(args.ssh_key), 'root@117.50.195.94']
    scp = ['scp', '-P', '2491', '-i', str(args.ssh_key)]
    records = []
    for name in jobs:
        stage = args.output / name
        stage.mkdir()
        entry = entries[f'graphs/simplekg/{name}_simplekg.json']
        source = args.data / 'upstream' / entry['path']
        if not source.is_file():
            raise RuntimeError('Pinned local graph download missing')
        local_entry = stage / (name + '.json')
        immutable_json(local_entry, entry)
        remote_graph = REMOTE + '/cypherbench_data/upstream/' + entry['path']
        remote_entry = REMOTE + '/graph_receipts/' + name + '.json'
        output = REMOTE + '/test_runtime_v1/' + name + '/reference_preflight_v1'
        # Refuse overwrite/re-execution of any prior graph attempt in this queue.
        owner_code = ('import json,socket;from pathlib import Path;'
                      'assert json.loads(Path(' + repr(REMOTE + '/execution_owner.json') +
                      ').read_text())["hostname"]==socket.gethostname()')
        owner_check = ' '.join(shlex.quote(x) for x in [REMOTE + '/.venv/bin/python', '-c', owner_code])
        precheck = (owner_check + ' && test ! -e ' + shlex.quote(remote_graph + '.partial') +
                    ' && test ! -e ' + shlex.quote(output))
        try:
            hash_command = 'sha256sum ' + ' '.join(shlex.quote(REMOTE + '/public_runtime_v1_code/' + name)
                                                  for name in remote_hashes)
            remote_output = subprocess.check_output(ssh + [hash_command], text=True)
            actual_hashes = {Path(line.split(None, 1)[1].strip()).name: line.split()[0]
                             for line in remote_output.splitlines()}
            if actual_hashes != remote_hashes:
                raise RuntimeError('Remote preflight source changed; do not run under stale freeze')
            subprocess.run(ssh + [precheck], check=True, capture_output=True)
            source_exists = subprocess.run(ssh + ['test -e ' + shlex.quote(remote_graph)],
                                           capture_output=True).returncode == 0
            if source_exists and not args.reuse_verified_assets:
                raise RuntimeError('Existing source requires documented verified reuse')
            with (stage / 'transfer.log').open('xb') as log:
                if not source_exists:
                    subprocess.run(scp + [str(source), 'root@117.50.195.94:' + remote_graph + '.partial'],
                                   check=True, stdout=log, stderr=subprocess.STDOUT)
                subprocess.run(scp + [str(local_entry), 'root@117.50.195.94:' + remote_entry],
                               check=True, stdout=log, stderr=subprocess.STDOUT)
            promote_code = ('from pathlib import Path\nimport hashlib\n'
                'p=Path(' + repr(remote_graph + '.partial') + ')\n'
                'q=Path(' + repr(remote_graph) + ')\n'
                'assert not q.exists()\nassert p.stat().st_size==' + str(entry['size']) + '\n'
                'h=hashlib.sha256()\nwith p.open("rb") as f:\n'
                ' for b in iter(lambda:f.read(1048576),b""): h.update(b)\n'
                'assert h.hexdigest()==' + repr(entry['lfs']['oid']) + '\np.rename(q)')
            promote = ' '.join(shlex.quote(x) for x in [REMOTE + '/.venv/bin/python', '-c', promote_code])
            if source_exists:
                reused_hash = subprocess.check_output(ssh + ['sha256sum ' + shlex.quote(remote_graph)], text=True).split()[0]
                if reused_hash != entry['lfs']['oid']:
                    raise RuntimeError('Previously transferred graph is not the pinned asset')
                immutable_json(stage / 'verified_source_reuse.json', {'sha256': reused_hash,
                               'reason': args.recovery_reason, 'database_import_reused': False})
            else:
                subprocess.run(ssh + [promote], check=True, capture_output=True)
            command = ' '.join(shlex.quote(x) for x in [REMOTE + '/.venv/bin/python',
                       REMOTE + '/public_runtime_v1_code/public_graph_runtime.py', '--root', REMOTE,
                       '--graph', name, '--output', output])
            with (stage / 'remote_preflight.log').open('xb') as log:
                subprocess.run(ssh + [command], stdout=log, stderr=subprocess.STDOUT, check=True)
            subprocess.run(scp + ['-r', 'root@117.50.195.94:' + output, str(stage / 'result')],
                           check=True, capture_output=True)
            result = json.loads((stage / 'result/summary.json').read_text(encoding='utf-8'))
            if result['checked'] != manifest['splits']['test']['by_graph'][name]:
                raise RuntimeError('Public graph reference count incomplete')
            immutable_json(stage / 'receipt.json', result)
            records.append(result)
            print(json.dumps({'graph': name, 'state': result['status'], 'checked': result['checked'],
                              'passed': result['passed']}), flush=True)
        except Exception as error:
            immutable_json(stage / 'failure.json', {'error_type': type(error).__name__,
                           'error': str(error)[:1000], 'no_automatic_retry': True})
            immutable_json(args.output / 'stopped.json', {'failed_graph': name, 'completed': records})
            raise
    immutable_json(args.output / 'summary.json', {'kind': 'DATA_PREFLIGHT_ONLY', 'completed': records,
                   'model_calls': 0, 'formal_predictions': 0})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ['metadata', 'data', 'output', 'ssh-key']:
        parser.add_argument('--' + field, type=Path, required=True)
    parser.add_argument('--skip-flight', action='store_true')
    parser.add_argument('--start-at')
    parser.add_argument('--reuse-verified-assets', action='store_true')
    parser.add_argument('--recovery-reason')
    run(parser.parse_args())
