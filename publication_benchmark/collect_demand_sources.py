"""Source-first E3 collection. No service compiler, graph, gold, or model import.

Keep the complete CHAOSS Evolution metric census and one representative item
from each of the ten RepoQA task groups. Published grouping/ambiguity labels
are provenance only, never trusted semantic gold. No query/results are copied.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import zipfile

import requests


def digest(data):
    return hashlib.sha256(data).hexdigest()


def question(text):
    lines = text.splitlines()
    for i, line in enumerate(lines):
        clean = re.sub(r'[*#_]', '', line).strip()
        if clean.lower().startswith('question:'):
            value = clean.split(':', 1)[1].strip()
            if value:
                return value
            return next((x.strip() for x in lines[i+1:] if x.strip()), '')
    return None


def collect(a):
    out = Path(a.output)
    if out.exists():
        raise FileExistsError('Do not overwrite source collection')
    out.mkdir(parents=True)
    commit = json.loads(Path(a.chaoss_commit).read_text(encoding='utf-8'))['sha']
    tree = json.loads(Path(a.chaoss_tree).read_text(encoding='utf-8'))
    # Same immutable revision must back both the file list and downloaded texts.
    if tree['sha'] != commit:
        raise ValueError('Source tree revision mismatch; reconcile before fetching')
    paths = sorted(x['path'] for x in tree['tree'] if x['path'].startswith('focus-areas/')
                   and x['path'].endswith('.md') and not x['path'].endswith('README.md'))

    def download(path):
        url = f'https://raw.githubusercontent.com/chaoss/wg-evolution/{commit}/{path}'
        r = requests.get(url, timeout=(10, 45))
        r.raise_for_status()
        target = out / 'chaoss' / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(r.content)
        return {'source_kind': 'CHAOSS_EVOLUTION_METRIC', 'source_key': path,
                'url': f'https://github.com/chaoss/wg-evolution/blob/{commit}/{path}',
                'source_commit': commit, 'file_sha256': digest(r.content),
                'title': path.rsplit('/', 1)[1].removesuffix('.md'),
                'source_question': question(r.text)}

    with ThreadPoolExecutor(max_workers=4) as pool:
        metric_rows = list(pool.map(download, paths))
    if any(not x['source_question'] for x in metric_rows):
        raise ValueError('Missing source question; preserve files and inspect')
    archive = Path(a.repoqa_archive)
    with zipfile.ZipFile(archive) as z:
        member = next(n for n in z.namelist() if n.endswith('data/ground_truth/complete_150_set/AutoGPT.json'))
        raw = z.read(member)
        rows = json.loads(raw)
        selected = []
        for task in range(1, 11):
            group = [r for r in rows if r['task'] == task]
            frequencies = Counter(r['query_no'] for r in group)
            modal = min(frequencies, key=lambda q: (-frequencies[q], q))
            selected.append(next(r for r in group if r['query_no'] == modal))
        peer_rows = [{'source_kind': 'REPOQA_PUBLISHED_TASK', 'source_key': 'task_' + str(r['task']),
                      'url': 'https://zenodo.org/records/17162221', 'archive_member': member,
                      'member_sha256': digest(raw), 'archive_sha256': digest(archive.read_bytes()),
                      'source_row_number': r['number'], 'source_query_no': r['query_no'],
                      'source_ambiguity_annotation': r['isAmbiguous'],
                      'title': 'RepoQA published task ' + str(r['task']), 'source_question': r['question']}
                     for r in selected]
    candidates = peer_rows + metric_rows
    for i, row in enumerate(candidates, 1):
        row['source_entry_id'] = 'SRC' + str(i).zfill(3)
    manifest = {'mode': 'E3_SOURCE_CENSUS_BEFORE_LOCAL_CAPABILITY_LABELS',
                'entries': len(candidates), 'repoqa_published_task_groups': len(peer_rows),
                'chaoss_metric_files': len(metric_rows), 'chaoss_commit': commit,
                'selection': 'All non-README Evolution focus-area metric files. Per RepoQA task 1..10 choose most frequent query_no (smallest on ties), then first row. No filtering by local graph or service capability.',
                'limits': 'Convenience source census, not random real-user sample. Labels and task grouping may contain errors. Entries are not yet deduplicated independent goals.',
                'collector_sha256': digest(Path(__file__).read_bytes()), 'candidates': candidates}
    (out / 'source_candidates.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'entries': len(candidates), 'source_questions': [{'id': x['source_entry_id'], 'title': x['title'], 'question': x['source_question']} for x in candidates]}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    for key in ('repoqa-archive', 'chaoss-commit', 'chaoss-tree', 'output'):
        p.add_argument('--' + key, required=True)
    collect(p.parse_args())
