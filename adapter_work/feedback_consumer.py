"""Bounded, read-only recovery for already generated Direct-feedback replies.

One worker per graph, one case at a time. Transport failures reopen the worker;
completed answers and scores survive cleanup failures. No model client is used.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT/'adapter_work')]
from engine.config import canonical, digest
from engine.execution import UnknownExecution
from engine.errors import IntegrityError
from offline_pipeline import HARD_ERRORS, load_selection, read_pin, run_group, save, source_manifest

TRANSPORT = (UnknownExecution, TimeoutError, ConnectionError)
TRANSPORT_NAMES = {t.__name__ for t in TRANSPORT}


def incomplete_transport(result):
    if result.get('score', {}).get('error_type') in TRANSPORT_NAMES:
        return True
    if result.get('final'):
        return read_pin(result['final']).get('error_type') in TRANSPORT_NAMES
    return result.get('error_type') in TRANSPORT_NAMES


def consume_graph(benchmark, graph, rows, sources, settings, output, factory, sleep=time.sleep):
    output.mkdir(parents=True, exist_ok=False)
    stack, backend = None, None
    results, starts, startup_failures = [], 0, 0

    def event(kind, **fields):
        with (output/'events.jsonl').open('ab') as f:
            f.write(canonical(dict(kind=kind, epoch=time.time(), **fields))+b'\n')

    def close():
        nonlocal stack, backend
        try:
            if stack is not None:
                stack.close()
        except TRANSPORT as exc:
            event('channel_close_failed', error_type=type(exc).__name__, error=str(exc))
        finally:
            stack, backend = None, None

    def open_worker():
        nonlocal stack, backend, starts, startup_failures
        for wait in (0, 5, 10):
            if wait:
                sleep(wait)
            starts += 1
            child = output/f'worker-{starts}'
            child.mkdir()
            candidate = ExitStack()
            event('worker_start', attempt=starts)
            try:
                backend = candidate.enter_context(factory(benchmark, graph, settings, child))
                stack = candidate
                event('worker_ready', attempt=starts)
                return True
            except TRANSPORT as exc:
                candidate.close()
                backend = None
                startup_failures += 1
                event('worker_start_failed', error_type=type(exc).__name__, error=str(exc))
            except BaseException:
                candidate.close()
                raise
        return False

    def record(value):
        results.append(value)
        with (output/'results.jsonl').open('ab') as f:
            f.write(canonical(value)+b'\n')

    try:
        for index, row in enumerate(rows):
            for attempt in (1, 2):
                if backend is None and not open_worker():
                    for pending in rows[index:]:
                        record(dict(cell_id=pending['cell_id'],position_id=pending['position_id'],
                            method=pending['method'],benchmark=benchmark,graph=graph,
                            status='graph_paused',final=None,score=None,model_calls=0))
                    return results
                folder = output/'cases'/str(index)/str(attempt)
                folder.mkdir(parents=True)
                captured, failure = [], None
                try:
                    run_group([row],sources,backend,folder,on_result=captured.append)
                except TRANSPORT as exc:
                    failure = exc
                    event('channel_failed',position_id=row['position_id'],attempt=attempt,
                          error_type=type(exc).__name__,error=str(exc))
                # Integrity/persistence failures are intentionally not caught.
                result = captured[0] if captured else None
                retry = result is None or incomplete_transport(result)
                if failure is not None or retry:
                    close()
                if result is not None and not retry:
                    record(dict(result,offline_attempts=attempt,cleanup_transport_failed=failure is not None))
                    break
                if attempt == 1:
                    event('case_retry',position_id=row['position_id'],wait_seconds=5)
                    sleep(5)
                else:
                    result = result or dict(cell_id=row['cell_id'],position_id=row['position_id'],
                        method=row['method'],benchmark=benchmark,graph=graph,status='UNKNOWN_EXECUTION',
                        final=None,score=None,error_type=type(failure).__name__ if failure else 'MissingResult',model_calls=0)
                    record(dict(result,offline_attempts=2,recovery_exhausted=True))
    finally:
        close()
        save(output/'summary.json',dict(planned=len(rows),results=results,
            startup_failures=startup_failures,worker_starts=starts,model_calls=0))
    return results


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--offline',required=True)
    p.add_argument('--receipt',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--graphs',type=int,choices=(1,2),default=2)
    a=p.parse_args()
    config=json.loads(Path(a.offline).read_bytes())
    if config.get('execute_authorized') is not True:
        raise IntegrityError('Offline execution is not authorized')
    receipt=json.loads(Path(a.receipt).read_bytes())
    selected={}
    for pin in receipt['source_finals']:
        final=read_pin(pin)
        if final['method']!='direct-feedback':
            raise IntegrityError('Only saved Direct-feedback replies are allowed')
        selected.setdefault(final['cell_id'],[]).append(final['position_id'])
    if not selected:
        raise ValueError('Empty selection; no fallback to full cohort')
    config.update(source=source_manifest(),source_finals=receipt['source_finals'],output=str(Path(a.out).resolve()))
    config['selection']=dict(config['selection'],selected_positions=selected)
    groups,sources=load_selection(config)
    output=Path(config['output']);output.mkdir(parents=True,exist_ok=False)
    save(output/'config.json',config)
    from offline_backends import open_backend
    def execute(item):
        (benchmark,graph),rows=item
        return consume_graph(benchmark,graph,rows,sources,config['backends'][benchmark],
            output/digest(canonical([benchmark,graph]))[:16],open_backend)
    results=[]
    with ThreadPoolExecutor(max_workers=a.graphs) as pool:
        for values in pool.map(execute,groups.items()):
            results.extend(values)
    planned=sum(len(rows) for rows in groups.values())
    if len(results)!=planned:
        raise IntegrityError('Recovery denominator changed')
    save(output/'summary.json',dict(planned=planned,results=results,counts=dict(Counter(r['status'] for r in results)),model_calls=0))
    print(json.dumps(dict(planned=planned,counts=dict(Counter(r['status'] for r in results)),model_calls=0)))

if __name__=='__main__':main()
