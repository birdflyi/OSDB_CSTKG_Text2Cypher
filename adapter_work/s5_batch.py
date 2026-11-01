"""Offline S5 orchestration. Pinned reference and graph programs own semantics.

Each stage is argv (no shell), with {job} and {out} placeholders. It must save
an answer with columns, rows and complete=true to {out}. No model dispatch.
New material requires a new manifest/output, not edits to a running batch.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import threading
import time

from osdb_output import normalized, compare


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_bytes())


def save(path, value):
    path = Path(path)
    with path.open('x', encoding='utf8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
    return {'path': str(path.resolve()), 'sha256': sha(path)}


def verify(pin):
    path = Path(pin['path'])
    if sha(path) != pin['sha256']:
        raise ValueError('Changed input: ' + str(path))
    return path


def stage(spec, job_path, directory, timeout):
    for pin in spec['inputs']:
        verify(pin)
    result = directory / 'answer.json'
    command = [arg.replace('{job}', str(job_path)).replace('{out}', str(result))
               for arg in spec['argv']]
    with (directory/'stdout.log').open('wb') as stdout, (directory/'stderr.log').open('wb') as stderr:
        process = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=timeout,
                                 shell=False, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if process.returncode:
        raise RuntimeError('Stage exit code ' + str(process.returncode))
    answer = read(result)
    if answer.get('complete') is not True or answer.get('status') != 'EXECUTED':
        raise ValueError('Stage did not return a complete executed answer')
    return answer, {'path': str(result), 'sha256': sha(result)}


def process_job(job, root, timeout, retry_failed=False):
    if not job.get('blocked_reasons'):
        for pin in [job['source'], *job['graph_evidence'],
                    *job['reference']['inputs'], *job['graph_query']['inputs']]:
            verify(pin)
    key = hashlib.sha256(job['task_id'].encode()).hexdigest()[:24]
    directory = root / key
    directory.mkdir(exist_ok=True)
    completed = sorted(directory.glob('attempt-*/result.json'))
    if completed:
        last = read(completed[-1])
        for pin in last.get('evidence', []):
            verify(pin)
        if last['status'] == 'PASS' or not retry_failed:
            return last
    attempts = list(directory.glob('attempt-*'))
    if attempts and not (sorted(attempts)[-1]/'result.json').exists():
        # A crash may have left a read running remotely. Never silently resend.
        return dict(task_id=job['task_id'], status='INTERRUPTED',
                    reason='Audit unfinished stage before preparing a recovery manifest')
    if len(attempts) >= 2:
        return dict(task_id=job['task_id'], status='RETRY_EXHAUSTED')
    attempt = directory / f'attempt-{len(attempts)+1:02}'
    attempt.mkdir()
    job_path = attempt/'job.json'
    save(job_path, job)
    start = time.monotonic()
    result = dict(task_id=job['task_id'], graph=job.get('graph'), status='BLOCKED', evidence=[])
    try:
        if job.get('blocked_reasons'):
            result['reason'] = job['blocked_reasons']
        else:
            verify(job['source'])
            for pin in job['graph_evidence']:
                verify(pin)
            if job['layer'] not in ('event', 'reference') or not job['snapshot_sha256']:
                raise ValueError('Explicit source layer and snapshot required')
            answers = []
            for name in ('reference', 'graph_query'):
                target = attempt/name
                target.mkdir()
                answer, pin = stage(job[name], job_path, target, timeout)
                normalized(answer, job['output_contract'])
                answers.append(answer)
                result['evidence'].append(pin)
            result['status'] = 'PASS' if compare(answers[0], answers[1], job['output_contract']) else 'REFERENCE_MISMATCH'
    except Exception as exc:
        result.update(status='ERROR', error_type=type(exc).__name__, reason=str(exc))
    result['seconds'] = time.monotonic()-start
    save(attempt/'result.json', result)
    return result


def _run(manifest, output, workers=2, timeout=600, retry_failed=False):
    root = Path(output).resolve()
    jobs = [json.loads(line) for line in Path(manifest).read_bytes().splitlines() if line]
    if not jobs or len({job['task_id'] for job in jobs}) != len(jobs):
        raise ValueError('Manifest must have unique, nonempty task IDs')
    root.mkdir(parents=True, exist_ok=True)
    binding = root/'manifest.json'
    identity = dict(path=str(Path(manifest).resolve()), sha256=sha(manifest))
    if binding.exists():
        if read(binding) != identity:
            raise ValueError('Manifest changed; use a new output directory')
    else:
        save(binding, identity)
    graph_locks = {job.get('graph'): threading.Lock() for job in jobs}
    write_lock = threading.Lock()
    def work(job):
        with graph_locks[job.get('graph')]:
            value = process_job(job, root, timeout, retry_failed)
        with write_lock, (root/'events.jsonl').open('a', encoding='utf8') as stream:
            stream.write(json.dumps(value, ensure_ascii=False)+'\n')
        return value
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(work, jobs))
    from collections import Counter
    summary = dict(planned=len(jobs), counts=dict(Counter(r['status'] for r in results)),
                   results=results, model_calls=0, formal_admitted=False)
    # Each invocation has a separate summary; previous evidence is immutable.
    index = len(list(root.glob('summary-*.json'))) + 1
    save(root/f'summary-{index:04}.json', summary)
    return summary


def run(manifest, output, workers=2, timeout=600, retry_failed=False):
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock = root/'RUNNING.lock'
    # Fail closed after an unclean exit; never remove another invocation's lock.
    with lock.open('x') as stream:
        import os
        stream.write(str(os.getpid()))
    try:
        return _run(manifest, root, workers, timeout, retry_failed)
    finally:
        lock.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--workers', type=int, choices=(1,2,3,4), default=2)
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--retry-failed', action='store_true')
    args = parser.parse_args()
    print(json.dumps(run(args.manifest,args.out,args.workers,args.timeout,args.retry_failed),ensure_ascii=False))
