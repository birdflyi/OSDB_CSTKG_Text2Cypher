"""Pinned graph download only. No reference answers, predictions or DB starts."""
import argparse
import hashlib
import json
from pathlib import Path
import time
from urllib.request import urlopen

from model_transport import immutable_json
from prepare_cypherbench import REVISION, BASE_URL


def verify_file(path, entry):
    size = path.stat().st_size
    if size != entry['size']:
        raise ValueError('Graph size mismatch: ' + entry['path'])
    h = hashlib.sha256() if 'lfs' in entry else hashlib.sha1()
    if 'lfs' not in entry:
        h.update(b'blob ' + str(size).encode() + b'\0')
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    expected = entry['lfs']['oid'] if 'lfs' in entry else entry['oid']
    if h.hexdigest() != expected:
        raise ValueError('Graph hash mismatch: ' + entry['path'])
    return {'bytes': size, 'upstream_digest': h.hexdigest(), 'verified': True}


def fetch_one(entry, destination, *, opener=urlopen):
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return {**verify_file(destination, entry), 'downloaded': False}
    partial = destination.with_name(destination.name + '.partial')
    # Failed partials are evidence, never implicitly reused as complete files.
    if partial.exists():
        raise RuntimeError('Partial download exists; audit it before another download')
    with opener(BASE_URL + REVISION + '/' + entry['path'], timeout=90) as response:
        with partial.open('xb') as stream:
            size = 0
            while True:
                block = response.read(1024 * 1024)
                if not block:
                    break
                size += len(block)
                if size > entry['size']:
                    raise ValueError('Download exceeds pinned graph size')
                stream.write(block)
    checked = verify_file(partial, entry)
    if destination.exists():
        raise RuntimeError('Another downloader created destination; do not overwrite')
    partial.rename(destination)
    return {**checked, 'downloaded': True}


def run(metadata, data, output):
    inventory = json.loads((metadata / 'inventory.json').read_text(encoding='utf-8'))
    if inventory['revision'] != REVISION:
        raise ValueError('Unpinned graph inventory')
    entries = {x['path']: x for x in json.loads((metadata / 'tree.json').read_text(encoding='utf-8'))
               if x['type'] == 'file'}
    manifest = json.loads((data / 'manifest.json').read_text(encoding='utf-8'))
    graphs = list(manifest['splits']['test']['by_graph'])
    jobs = sorted((entries[f'graphs/simplekg/{g}_simplekg.json'] for g in graphs),
                  key=lambda x: (x['size'], x['path']))
    output.mkdir(parents=True, exist_ok=False)
    immutable_json(output / 'plan.json', {'revision': REVISION, 'graphs': jobs,
                   'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   'model_calls': 0, 'database_starts': 0, 'formal_predictions': 0})
    lock = data / '.graph-download.lock'
    with lock.open('x', encoding='utf-8') as stream:
        import os
        json.dump({'pid': os.getpid(), 'output': str(output)}, stream)
    completed = []
    try:
        for entry in jobs:
            started = time.monotonic()
            result = fetch_one(entry, data / 'upstream' / entry['path'])
            receipt = {'path': entry['path'], 'seconds': time.monotonic() - started, **result}
            immutable_json(output / (Path(entry['path']).stem + '.json'), receipt)
            completed.append(receipt)
            print(json.dumps(receipt), flush=True)
        immutable_json(output / 'summary.json', {'state': 'COMPLETE', 'graphs': completed,
                       'model_calls': 0, 'formal_predictions': 0})
    except Exception as error:
        immutable_json(output / 'failure.json', {'error_type': type(error).__name__,
                       'error': str(error), 'completed': completed})
        raise
    finally:
        lock.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ['metadata', 'data', 'output']:
        parser.add_argument('--' + key, type=Path, required=True)
    args = parser.parse_args()
    run(args.metadata, args.data, args.output)
