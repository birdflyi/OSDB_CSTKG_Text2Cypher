"""Prepare pinned official data without model calls or database mutations.

Question-only runtime records and evaluator-only references are separate files.
This separation is not an OS security boundary; runners must only read runtime/.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
from urllib.request import urlopen

REVISION = 'fd80c26a9f5447e4c9ba952cc70575544117306c'
TEST_SHA256 = 'e55cb853bde5ad893a5dcfa2ba38118051c7d8faa9a0d91b3d77a097c13f72f4'
BASE_URL = 'https://huggingface.co/datasets/megagonlabs/cypherbench/resolve/'


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def verify_asset(data, entry):
    if len(data) != entry['size']:
        raise ValueError('Size mismatch: ' + entry['path'])
    if 'lfs' in entry:
        actual = sha256(data)
        expected = entry['lfs']['oid']
    else:
        actual = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        expected = entry['oid']
    if actual != expected:
        raise ValueError('Hash mismatch: ' + entry['path'])


def save_once(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != data:
            raise ValueError('Refusing to replace different artifact: ' + str(path))
        return
    with path.open('xb') as stream:
        stream.write(data)


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode('utf-8')


def jsonl_bytes(rows):
    return ''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows).encode('utf-8')


def separate_records(rows):
    runtime, references, seen = [], [], set()
    for row in rows:
        for field in ('qid', 'graph', 'nl_question', 'gold_cypher', 'answer_json'):
            if not isinstance(row.get(field), str) or not row[field].strip():
                raise ValueError('Invalid required string field: ' + field)
        if row['qid'] in seen:
            raise ValueError('Duplicate question id: ' + row['qid'])
        seen.add(row['qid'])
        # Explicit allowlist. Never pass the full original record to a model.
        runtime.append({key: row[key] for key in ('qid', 'graph', 'nl_question')})
        references.append({key: row[key] for key in
                           ('qid', 'graph', 'gold_cypher', 'answer_json', 'from_template')
                           if key in row})
    return runtime, references


def validate_splits(train, test):
    if {r['qid'] for r in train} & {r['qid'] for r in test}:
        raise ValueError('Train/test question ids overlap')
    if {r['graph'] for r in train} & {r['graph'] for r in test}:
        raise ValueError('Train/test graphs overlap; investigate before development')


def prepare(metadata_dir, output_dir, official_source):
    inventory = json.loads((metadata_dir / 'inventory.json').read_text(encoding='utf-8'))
    if inventory['revision'] != REVISION:
        raise ValueError('Unexpected dataset revision')
    tree = json.loads((metadata_dir / 'tree.json').read_text(encoding='utf-8'))
    entries = {row['path']: row for row in tree if row['type'] == 'file'}
    source_commit = subprocess.check_output(
        ['git', '-C', str(official_source), 'rev-parse', 'HEAD'], text=True).strip()
    source_dirty = subprocess.check_output(
        ['git', '-C', str(official_source), 'status', '--porcelain'], text=True).strip()
    if source_dirty:
        raise ValueError('Official source checkout has local changes')
    receipts = []

    def asset(name, cached=None):
        destination = output_dir / 'upstream' / name
        entry = entries[name]
        if cached is not None:
            data = cached.read_bytes()
        elif destination.exists():
            data = destination.read_bytes()
        else:
            with urlopen(BASE_URL + REVISION + '/' + name, timeout=90) as response:
                data = response.read(entry['size'] + 1)
        verify_asset(data, entry)
        save_once(destination, data)
        receipts.append({'path': 'upstream/' + name, 'bytes': len(data), 'sha256': sha256(data)})
        return json.loads(data)

    test_bytes = (metadata_dir / 'test.json').read_bytes()
    if sha256(test_bytes) != TEST_SHA256:
        raise ValueError('Existing official test snapshot was changed')
    test = asset('test.json', metadata_dir / 'test.json')
    train = asset('train.json')
    validate_splits(train, test)
    split_stats = {}
    runtime_by_split = {}
    for split, rows in [('train', train), ('test', test)]:
        runtime, references = separate_records(rows)
        runtime_by_split[split] = runtime
        split_stats[split] = {'tasks': len(rows), 'by_graph': dict(sorted(Counter(
            r['graph'] for r in rows).items()))}
        for relative, data in [
            ('runtime/' + split + '.jsonl', jsonl_bytes(runtime)),
            ('evaluator/' + split + '.jsonl', jsonl_bytes(references)),
        ]:
            save_once(output_dir / relative, data)
            receipts.append({'path': relative, 'bytes': len(data), 'sha256': sha256(data)})

    graph_names = sorted({r['graph'] for r in train + test})
    for graph in graph_names:
        asset('graphs/schemas/' + graph + '_schema.json')
    # Choose by file size, before any method predictions. Full TRAIN graph only.
    train_graphs = sorted({r['graph'] for r in train})
    smoke_graph = min(train_graphs, key=lambda graph: (
        entries['graphs/simplekg/' + graph + '_simplekg.json']['size'], graph))
    asset('graphs/simplekg/' + smoke_graph + '_simplekg.json')
    smoke_tasks = sorted((row for row in runtime_by_split['train']
                          if row['graph'] == smoke_graph), key=lambda row: row['qid'])[:3]
    save_once(output_dir / 'runtime/dev_smoke.jsonl', jsonl_bytes(smoke_tasks))
    receipts.append({'path': 'runtime/dev_smoke.jsonl',
                     'bytes': len(jsonl_bytes(smoke_tasks)),
                     'sha256': sha256(jsonl_bytes(smoke_tasks))})
    manifest = {
        'status': 'OFFLINE_DATA_PREPARATION_ONLY_NOT_PREDICTION_FREEZE',
        'dataset': 'megagonlabs/cypherbench', 'dataset_revision': REVISION,
        'official_source_url': 'https://github.com/megagonlabs/cypherbench',
        'official_source_commit': source_commit,
        'splits': split_stats, 'dev_smoke_graph': smoke_graph,
        'dev_smoke_ids': [r['qid'] for r in smoke_tasks],
        'test_graph_downloads': 0, 'train_full_graph_downloads': 1,
        'model_calls': 0, 'database_starts': 0,
        'formal_scope_frozen': False,
        'limitations': [
            'No database execution or model accuracy measured.',
            'Test metadata was inspected in previous planning; not claimed unseen by authors.',
            'Runtime/evaluator separation is a runner contract, not OS access isolation.',
            'Public-schema method adaptation and final-answer evaluator adapter still pending.',
        ],
        'artifacts': receipts,
    }
    save_once(output_dir / 'manifest.json', json_bytes(manifest))
    return {key: manifest[key] for key in ('status', 'splits', 'dev_smoke_graph',
            'model_calls', 'database_starts', 'official_source_commit')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--metadata-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--official-source', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.metadata_dir, args.output_dir, args.official_source), indent=2))
