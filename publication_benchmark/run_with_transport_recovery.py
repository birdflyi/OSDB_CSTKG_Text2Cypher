"""Bounded infrastructure recovery, never retry a complete semantic answer.

Up to three transport attempts; immutable records and first-success selection.
Completed source responses must have byte-identical request payloads to be reused.
"""
import argparse
import json
from pathlib import Path
import shutil
import time

import run_baselines as b
import run_comparison as comparison

SOURCE = None


class RecoveryClient(b.Client):
    def __init__(self, config, out):
        super().__init__(config, out)
        b.save(out / 'recovery_policy.json', {'driver_sha256': b.sha(__file__),
            'source_run': str(SOURCE) if SOURCE else None, 'max_attempts_per_stage': 3,
            'wait_seconds_after_failure': [30, 60], 'retry_only': 'transport error or HTTP 5xx/429',
            'selection': 'First HTTP success; do not retry parse/truncation/semantic errors',
            'source_cache_rule': 'Exact payload equality checked by original Client before reuse',
            'failed_attempt_billing': 'Unknown unless provider reports usage; never assume zero'})

    def call(self, stage, prompt, data, ids, keys, max_tokens=12000):
        final_folder = self.out / 'calls' / stage
        for number in range(1, 4):
            folder = self.out / 'attempts' / stage / f'a{number}'
            dispatch = folder / 'calls' / 'd'
            source = SOURCE / 'calls' / stage if SOURCE else None
            if number == 1 and source and (source / 'response.json').exists():
                dispatch.mkdir(parents=True, exist_ok=False)
                for name in ('request.json', 'response.json'):
                    shutil.copy2(source / name, dispatch / name)
                b.save(dispatch / 'reuse.json', {'source': str(source),
                    'request_sha256': b.sha(source / 'request.json'), 'response_sha256': b.sha(source / 'response.json')})
            retryable, result = False, None
            try:
                result = b.Client(self.config, folder).call('d', prompt, data, ids, keys, max_tokens)
            except (RuntimeError, ValueError, TypeError, KeyError) as exc:
                if str(exc).startswith('Cached inputs changed'):
                    raise
                response, transport = dispatch / 'response.json', dispatch / 'transport_error.json'
                if not response.exists() and not transport.exists():
                    raise
                status = json.loads(response.read_text(encoding='utf-8'))['http_status'] if response.exists() else None
                retryable = transport.exists() or status == 429 or (status is not None and 500 <= status <= 599)
                b.save(dispatch / 'outcome.json', {'retryable_infrastructure_failure': retryable,
                    'error_type': type(exc).__name__, 'attempt': number})
            if result is not None or not retryable or number == 3:
                final_folder.mkdir(parents=True, exist_ok=False)
                for name in ('request.json', 'response.json', 'transport_error.json'):
                    if (dispatch / name).exists():
                        shutil.copy2(dispatch / name, final_folder / name)
                b.save(final_folder / 'selection.json', {'selected_attempt': number,
                    'attempt_path': str(dispatch), 'rule': 'first success or terminal failure; no semantic selection'})
                print(stage + ': ' + ('COMPLETE' if result is not None else 'FAILED') + f' attempt={number}', flush=True)
                if result is not None:
                    return result
                b.save(final_folder / 'failure.json', {'ids': ids, 'generated_by': 'RUNNER_NOT_MODEL', 'attempts': number})
                if 'cypher' in keys:
                    return [{'id': i, 'status': 'ERROR', 'cypher': None, 'reason': 'Recorded dispatch/protocol failure'} for i in ids]
                return [{'id': i, 'status': 'ERROR', 'rows': [], 'evidence_ids': []} for i in ids]
            # Cached failures can already be older than the required backoff.
            last = dispatch / ('response.json' if (dispatch / 'response.json').exists() else 'transport_error.json')
            delay = max(0, [30, 60][number - 1] - (time.time() - last.stat().st_mtime))
            if delay:
                print(stage + f': infrastructure backoff {delay:.0f}s', flush=True)
                time.sleep(min(60, delay))


def audit(out):
    records = []
    for p in sorted((out / 'attempts').glob('*/a*/calls/d')):
        response = p / 'response.json'
        r = json.loads(response.read_text(encoding='utf-8')) if response.exists() else {}
        usage = r.get('body', {}).get('usage')
        records.append({'path': str(p), 'reused': (p / 'reuse.json').exists(),
            'http_status': r.get('http_status'), 'transport_error': (p / 'transport_error.json').exists(),
            'usage': usage, 'seconds': r.get('seconds'), 'response_sha256': b.sha(response) if response.exists() else None})
    fresh = [r for r in records if not r['reused']]
    b.save(out / 'recovery_usage.json', {'attempts': records,
        'fresh_dispatches': len(fresh), 'reused_records': len(records) - len(fresh),
        'fresh_known_reported_tokens': sum((r['usage'] or {}).get('total_tokens', 0) for r in fresh),
        'fresh_attempts_with_unknown_usage': sum(not r['usage'] for r in fresh),
        'note': 'Base summary counts selected responses only. Use this audit for all fresh dispatch costs and failures.'})


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    for name in ('config', 'data', 'graph-receipt', 'output'):
        p.add_argument('--' + name, required=True)
    p.add_argument('--source-run')
    p.add_argument('--port', type=int, choices=(17475, 17476), default=17475)
    args = p.parse_args()
    b.DB_URL = f'http://127.0.0.1:{args.port}/db/neo4j/tx/commit'
    SOURCE = Path(args.source_run).resolve() if args.source_run else None
    if SOURCE and not (SOURCE / 'summary.json').exists():
        raise RuntimeError('Source run must be confirmed terminal')
    comparison.RecordedClient = RecoveryClient
    try:
        comparison.run(args)
    finally:
        if Path(args.output).exists():
            audit(Path(args.output))
