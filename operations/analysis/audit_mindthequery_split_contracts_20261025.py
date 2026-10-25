"""Pinned public data audit. No author code, model calls or database queries.

Download only the declared 65 TRAIN JSON assets, verifying original Git blobs.
Read all corresponding pinned TEST files. Do not relabel/filter or tune methods.
"""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
from pathlib import Path
import requests

BASE = Path('D:/ch7/literature/public_candidate_metadata_20261024/MindTheQuery')
REV = 'ec4054a1b7df7e6310efa95c56bb6e7cebc4d161'
OUT = Path('D:/ch7/reports/mindthequery_split_contracts_20261025.json')


def verify(raw, entry):
    assert len(raw) == entry['size'], entry['path']
    blob = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
    assert blob == entry['sha'], entry['path']
    return hashlib.sha256(raw).hexdigest()


def download(entry):
    relative = entry['path']
    assert relative.startswith('Train_Test_Splits/') and '/train/' in relative
    assert relative.endswith('.json') and '..' not in Path(relative).parts
    p = BASE / 'upstream' / relative
    if p.exists():
        return {'path': relative, 'sha256': verify(p.read_bytes(), entry), 'cached': True}
    url = 'https://raw.githubusercontent.com/endeavorXx/Mind-the-Query/' + REV + '/' + relative
    try:
        response = requests.get(url, timeout=(10, 35))
        response.raise_for_status()
        raw = response.content
        digest = verify(raw, entry)
        json.loads(raw)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('xb') as stream:
            stream.write(raw)
        return {'path': relative, 'sha256': digest, 'bytes': len(raw), 'cached': False, 'url': url}
    except Exception as exc:
        return {'path': relative, 'error': type(exc).__name__ + ': ' + str(exc)}


def field(row, candidates):
    for name in candidates:
        if isinstance(row.get(name), str):
            return row[name]
    return None


def key(row, which):
    question = field(row, ['NL Question', 'nl_question', 'question'])
    query = field(row, ['Cypher', 'cypher', 'query'])
    if which == 'question':
        return question if question else None
    if which == 'query':
        return query if query else None
    return (question, query) if question and query else None


def run():
    assert json.loads((BASE / 'commit.json').read_bytes())['sha'] == REV
    tree = json.loads((BASE / 'tree.json').read_bytes())
    entries = [x for x in tree['tree'] if x['type'] == 'blob'
               and x['path'].startswith('Train_Test_Splits/')
               and len(x['path'].split('/')) > 4 and x['path'].endswith('.json')
               and x['path'].split('/')[3] in ('train', 'test')]
    train = [x for x in entries if '/train/' in x['path']]
    assert len(train) == 65 and sum(x['size'] for x in train) == 23764577
    with ThreadPoolExecutor(max_workers=3) as pool:
        receipts = list(pool.map(download, train))
    errors = [x for x in receipts if 'error' in x]
    if errors:
        OUT.write_text(json.dumps({'commit': REV, 'download_errors': errors,
                                   'download_receipts': receipts, 'audit_complete': False}, indent=2),
                       encoding='utf-8')
        raise RuntimeError('Pinned TRAIN retrieval incomplete; errors preserved, no automatic retries')
    files = []
    groups = defaultdict(list)
    for e in entries:
        raw = (BASE / 'upstream' / e['path']).read_bytes()
        sha = verify(raw, e)
        data = json.loads(raw)
        assert isinstance(data, list) and all(isinstance(r, dict) for r in data)
        parts = e['path'].split('/')
        track, graph, split = parts[1:4]
        assert all(key(row, 'pair') is not None for row in data), 'Unrecognized/empty input fields: ' + e['path']
        files.append({'path': e['path'], 'sha256': sha, 'git_blob': e['sha'], 'rows': len(data)})
        groups[(track, graph, split)].extend(data)
    split_summary = {}
    overlaps = {}
    for (track, graph, split), rows in sorted(groups.items()):
        tag = '/'.join([track, graph, split])
        d = {'rows': len(rows)}
        for name in ['question', 'query', 'pair']:
            values = [key(r, name) for r in rows]
            valid = [v for v in values if v is not None]
            counts = Counter(valid)
            d[name] = {'missing_or_empty': len(rows) - len(valid),
                       'unique_nonempty': len(counts),
                       'duplicate_row_excess': sum(n - 1 for n in counts.values())}
        if track == 'Manual':
            d['logical_labels'] = dict(Counter(str(r.get('logical [0/1]', '<MISSING>')) for r in rows))
            d['result_labels'] = dict(Counter(str(r.get('result [0/1]', '<MISSING>')) for r in rows))
            d['logical_vs_result_labels'] = dict(Counter(
                str((r.get('logical [0/1]', '<MISSING>'), r.get('result [0/1]', '<MISSING>'))) for r in rows))
            d['automated_flags_present'] = sum('schema_validation_passed' in r for r in rows)
        split_summary[tag] = d
    for track, graph in sorted({k[:2] for k in groups}):
        training = groups[(track, graph, 'train')]
        testing = groups[(track, graph, 'test')]
        result = {}
        for which in ['question', 'query', 'pair']:
            seen = {key(r, which) for r in training} - {None}
            hits = [i for i, r in enumerate(testing) if key(r, which) is not None and key(r, which) in seen]
            result[which] = {'test_rows_with_exact_value_in_train': len(hits), 'test_row_indices': hits}
        for identity in ['id', 'unique_id']:
            seen = {str(r[identity]) for r in training if r.get(identity) not in (None, '')}
            result[identity] = {'test_rows_with_same_nonempty_id_in_train':
                               sum(str(r.get(identity)) in seen for r in testing),
                               'warning': 'ID collision is not semantic leakage; verify scope and provenance.'}
        overlaps[track + '/' + graph] = result
    # Different directory names can denote the same source graph; keep mapping explicit.
    aliases = {'bloom': 'bloom50', 'covid': 'covid', 'er': 'er', 'healthcare': 'healthcare', 'wwc': 'wwc'}
    cross_tracks = {}
    for manual, automated in aliases.items():
        assert all((track, graph, split) in groups for track, graph in
                   [('Automated', automated), ('Manual', manual)] for split in ['train', 'test'])
        result = {}
        for source in ['train', 'test']:
            for target in ['train', 'test']:
                a = groups[('Automated', automated, source)]
                m = groups[('Manual', manual, target)]
                seen = {key(r, 'pair') for r in a} - {None}
                result['Automated_' + source + '__Manual_' + target] = sum(
                    key(r, 'pair') is not None and key(r, 'pair') in seen for r in m)
        cross_tracks[manual] = result
    out = {'kind': 'PINNED_MINDTHEQUERY_SPLIT_CONTRACT_AUDIT',
           'time_local': datetime.now().astimezone().isoformat(), 'commit': REV,
           'files': files, 'train_download_receipts': receipts, 'split_summary': split_summary,
           'within_track_train_test_exact_overlap': overlaps,
           'cross_track_exact_question_query_pair_overlap': cross_tracks,
           'cross_track_graph_aliases': aliases,
           'identity_definition': 'Exact decoded strings. No whitespace, identifier, literal or semantic normalization.',
           'audit_complete': True, 'new_model_calls': 0, 'new_database_queries': 0,
           'author_code_executed': False, 'changes_to_labels_splits_queries_or_frozen_method': False,
           'formal_denominator_accepted': False,
           'limitations': 'Overlap is source-data provenance, not evidence of any actual model training exposure. '
                          'Label values inventoried, never imputed from automated flags or nonempty results.'}
    OUT.write_text(json.dumps(out, indent=2), encoding='utf-8')
    print(json.dumps({'audit_complete': True, 'files': len(files),
                      'rows': sum(x['rows'] for x in files), 'cross_tracks': cross_tracks,
                      'within_track': {k: {n: v[n].get('test_rows_with_exact_value_in_train')
                                         for n in ['question', 'query', 'pair']} for k, v in overlaps.items()}}))


if __name__ == '__main__':
    run()
