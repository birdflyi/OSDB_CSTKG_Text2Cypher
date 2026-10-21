"""Frozen G0/G1/S0/S1 developer run; two shared first passes, bounded repair.

Raw API records are private. A request without a terminal record is never
automatically repeated. Scoring is outside repair decisions and prompts.
"""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import time

import requests

import run_baselines as b
from run_mechanism_pilot import bag
import strong_baselines as s


def store(path, value):
    if path.exists():
        if json.loads(path.read_text(encoding='utf-8')) != value:
            raise ValueError('Immutable record differs: ' + path.name)
    else:
        b.save(path, value)


def dispatch(out, stage, cfg, representation, data):
    folder = out / 'calls' / stage
    payload = {k: cfg[k] for k in ('model', 'temperature', 'top_p', 'reasoning_effort')}
    payload.update(max_tokens=6000, messages=[{'role': 'system', 'content': s.PROMPTS[representation]},
                   {'role': 'user', 'content': json.dumps(data, ensure_ascii=False)}])
    req, terminal = folder / 'request.json', folder / 'terminal.json'
    if req.exists():
        if json.loads(req.read_text(encoding='utf-8')) != payload:
            raise ValueError('Changed request; start a different run')
        if not terminal.exists():
            raise RuntimeError('Unrecorded prior dispatch; manual reconciliation required')
        record = json.loads(terminal.read_text(encoding='utf-8'))
    else:
        b.save(req, payload)
        started = time.monotonic()
        try:
            r = requests.post(cfg['endpoint'], headers={'Authorization': 'Bearer ' + cfg['api_key']},
                              json=payload, timeout=(10, 120), allow_redirects=False)
            try:
                body = r.json()
            except ValueError:
                body = {'non_json_body': r.text}
            record = {'http_status': r.status_code, 'body': body, 'seconds': time.monotonic() - started}
        except requests.RequestException as e:
            record = {'transport_error': type(e).__name__, 'seconds': time.monotonic() - started}
        b.save(terminal, record)
    http = record.get('http_status')
    if 'transport_error' in record or http == 429 or (http is not None and http >= 500):
        return {'failure': 'INFRASTRUCTURE', 'repairable': False, 'raw': record}
    choices = record.get('body', {}).get('choices') or []
    if http != 200 or not choices or choices[0].get('finish_reason') != 'stop':
        return {'failure': 'INCOMPLETE_OR_HTTP_RESPONSE', 'repairable': False, 'raw': record}
    text = choices[0].get('message', {}).get('content')
    if not isinstance(text, str):
        return {'failure': 'PROTOCOL_ERROR', 'repairable': True, 'feedback': 'Expected JSON content', 'text': text, 'raw': record}
    try:
        return {'prediction': s.parse(text, representation), 'raw': record, 'text': text}
    except (ValueError, TypeError, KeyError) as e:
        return {'failure': 'PROTOCOL_ERROR', 'repairable': True, 'feedback': str(e), 'text': text, 'raw': record}


def observe(reply, representation):
    if 'prediction' not in reply:
        return {'check': {k: reply[k] for k in ('repairable', 'feedback') if k in reply},
                'execution': {'status': reply['failure']}}
    return s.evaluate(reply['prediction'], representation)


def score(observation, spec):
    e = observation['execution']
    return e['status'] == 'EXECUTED' and e['columns'] == spec['columns'] and bag(e['rows']) == bag(spec['expected_rows'])


def summary(out, records, total, stopped):
    arms = {}
    for arm in ('G0', 'G1', 'S0', 'S1'):
        rows = [r for r in records if r['arm'] == arm]
        arms[arm] = {'planned_cases': total, 'evaluated_cases': len(rows),
                     'correct': sum(r['correct'] for r in rows),
                     'executed': sum(r['observation']['execution']['status'] == 'EXECUTED' for r in rows),
                     'repairs_used': sum(r['repair_used'] for r in rows),
                     'infrastructure_failures': sum(r['failure'] == 'INFRASTRUCTURE' for r in rows)}
    terminals = [json.loads(p.read_text(encoding='utf-8')) for p in (out / 'calls').glob('*/terminal.json')]
    complete = all(value['evaluated_cases'] == total for value in arms.values())
    return {'status': 'STOPPED_INFRASTRUCTURE' if stopped else ('PLANNED_DISPATCHES_COMPLETED' if complete else 'RUNNING'),
            'arm_scores': arms, 'terminal_dispatches': len(terminals),
            'known_reported_tokens': sum((r.get('body', {}).get('usage') or {}).get('total_tokens', 0) for r in terminals),
            'unknown_usage_dispatches': sum(not r.get('body', {}).get('usage') for r in terminals),
            'dispatch_seconds': sum(r['seconds'] for r in terminals),
            'interpretation': 'Development only. G1/S1 include shared first passes, not independent samples. No whole-system or human-study claim.'}


def run(a):
    out, source = Path(a.output).resolve(), Path(a.source).resolve()
    cfg = json.loads(Path(a.config).read_text(encoding='utf-8'))
    specs_path = source / (a.project + '_specifications.json')
    specs = json.loads(specs_path.read_text(encoding='utf-8'))
    receipt = json.loads(Path(a.receipt).read_text(encoding='utf-8'))
    dm = json.loads((source / 'dataset_manifest.json').read_text(encoding='utf-8'))
    ref = json.loads(Path(a.reference).read_text(encoding='utf-8'))
    if ref['correct'] != len(specs) or ref['specifications_sha256'] != b.sha(specs_path):
        raise ValueError('Reference execution missing/stale')
    if ref['compiler_sha256'] != b.sha(Path(__file__).with_name('mechanism_core.py')):
        raise ValueError('Compiler differs from verified reference')
    if receipt['source_hashes'] != dm['projects'][a.project]:
        raise ValueError('Graph/spec source mismatch')
    for name, digest in receipt['source_hashes'].items():
        if b.sha(Path(a.data) / name) != digest:
            raise ValueError('Actual input file changed')
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data'] != receipt['database_identity']:
        raise ValueError('Wrong dedicated database')
    if a.selection == 'canonical':
        specs = [x for x in specs if x['variant_index'] == 0]
    specs = sorted(specs, key=lambda x: (x['variant_index'], x['id']))
    repo = Path(__file__).resolve().parents[1]
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo, text=True).strip()
    if dirty:
        raise ValueError('Commit the exact implementation before dispatch')
    hashes = {p.name: b.sha(p) for p in Path(__file__).parent.glob('*.py')}
    manifest = {'mode': 'DEVELOPMENT_G0_G1_S0_S1', 'project': a.project, 'selection': a.selection,
                'cases': len(specs), 'max_dispatches': 4 * len(specs), 'max_completion_tokens': 6000,
                'http_timeout_seconds': 120, 'infra_retries': 0, 'consecutive_infra_stop': 3,
                'model': {k: cfg[k] for k in ('model', 'temperature', 'top_p', 'reasoning_effort')},
                'git_commit': commit, 'dirty': False, 'code_hashes': hashes,
                'spec_sha256': b.sha(specs_path), 'reference_sha256': b.sha(a.reference),
                'source_hashes': receipt['source_hashes'], 'database_identity': receipt['database_identity'],
                'prompts': s.PROMPTS, 'examples': s.examples(),
                'repair_policy': 'Exactly one on structural/protocol/DB error, never on result correctness; same first pass reused',
                'limitations': 'Six developer-known families; no semantic obligation checker; snapshot equality not equivalence'}
    store(out / 'manifest.json', manifest)
    store(out / 'private_specifications.json', specs)
    for name in hashes:
        target = out / 'source_snapshot' / name
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(Path(__file__).with_name(name), target)
    records, consecutive, stopped = [], 0, False
    for index, spec in enumerate(specs):
        representations = ('G', 'S') if index % 2 == 0 else ('S', 'G')
        for representation in representations:
            data = s.public_input(spec)
            stage = representation + '_' + spec['id']
            reply = dispatch(out, stage + '_first', cfg, representation, data)
            consecutive = consecutive + 1 if reply.get('failure') == 'INFRASTRUCTURE' else 0
            first = observe(reply, representation)
            check = first['check']
            repair_reply, final = None, first
            if check.get('repairable') and consecutive < 3:
                repair_data = {**data, 'prior_output': reply.get('text'),
                               'structural_feedback': check.get('feedback'),
                               'instruction': 'One repair allowed. Correct only using the supplied task, schema, contracts and structural feedback.'}
                repair_reply = dispatch(out, stage + '_repair', cfg, representation, repair_data)
                consecutive = consecutive + 1 if repair_reply.get('failure') == 'INFRASTRUCTURE' else 0
                final = observe(repair_reply, representation)
            # Gold is first consulted here, after generation and repair decisions.
            for suffix, observation, used, selected in (
                ('0', first, False, reply), ('1', final, repair_reply is not None, repair_reply or reply)):
                record = {'id': spec['id'], 'project': a.project, 'family': spec['family'],
                          'variant_index': spec['variant_index'], 'arm': representation + suffix,
                          'prediction': selected.get('prediction'), 'failure': selected.get('failure'),
                          'repair_used': used, 'observation': observation, 'correct': score(observation, spec)}
                store(out / 'records' / (representation + suffix + '_' + spec['id'] + '.json'), record)
                records.append(record)
            # Replace only the derived progress view; raw and scored records remain immutable.
            (out / 'progress.json').write_text(json.dumps(summary(out, records, len(specs), False), indent=2), encoding='utf-8')
            print(f'{a.project} {index+1}/{len(specs)} {representation}: first={first["execution"]["status"]} repair={repair_reply is not None}', flush=True)
            if consecutive >= 3:
                stopped = True
                break
        if stopped:
            break
    result = summary(out, records, len(specs), stopped)
    store(out / 'summary.json', result)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    for key in ('source', 'project', 'receipt', 'reference', 'data', 'config', 'output'):
        p.add_argument('--' + key, required=True)
    p.add_argument('--port', type=int, choices=(17475, 17476), required=True)
    p.add_argument('--selection', choices=('canonical', 'all'), default='all')
    a = p.parse_args()
    b.DB_URL = f'http://127.0.0.1:{a.port}/db/neo4j/tx/commit'
    run(a)
