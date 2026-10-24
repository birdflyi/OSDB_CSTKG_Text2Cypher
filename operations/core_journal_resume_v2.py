"""Audited infrastructure-only continuation of the immutable core comparison.

Imports the ORIGINAL frozen implementation. Changes only the transport through a durable remote journal.
The original native worker, interpreter, limits and data are unchanged. Never changes prompts, plans, scoring or model
settings, and never regenerates a cached model answer.
"""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from journal_public_worker_v1 import JournalPublicWorker


def keepalive_command(command):
    if isinstance(command, list) and Path(command[0]).stem.lower() == 'ssh':
        return [command[0], '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3'] + command[1:]
    return command


def safe_close(self):
    try:
        try:
            if self.process.stdin and not self.process.stdin.closed:
                self.process.stdin.close()
        except (OSError, BrokenPipeError):
            # The disconnected SSH process is still waited for. A broken pipe
            # must not mask the original error or prevent the segment receipt.
            pass
        try:
            self.process.wait(timeout=45)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=10)
    finally:
        if not self.log.closed:
            self.log.close()


def load_original(root):
    folder = root / 'experiment-harness/public_benchmark_v1'
    sys.path.insert(0, str(folder))
    main = importlib.import_module('run_frozen_main')
    if Path(main.__file__).resolve().parent != folder.resolve():
        raise ValueError('Expected original frozen implementation, not recovery worktree copy')
    def popen(command, *args, **kwargs):
        return subprocess.Popen(keepalive_command(command), *args, **kwargs)
    class SubprocessProxy:
        Popen = staticmethod(popen)
        def __getattr__(self, name):
            return getattr(subprocess, name)
    main.subprocess = SubprocessProxy()
    main.PublicWorker.close = safe_close
    return main


def recovery_identity(main, args):
    return {'kind': 'INFRASTRUCTURE_DURABLE_NATIVE_JOURNAL_V1', 'freeze_sha256': main.sha(args.freeze),
            'adapter_sha256': main.sha(__file__), 'pid': os.getpid(),
            'changes': ['Short SSH connections with durable stable request ids; native worker independent of connection',
                        'original native worker/source/limits unchanged'],
            'transport_helper_sha256':main.sha(Path(__file__).with_name('journal_public_worker_v1.py')),
            'method_prompt_model_metric_changes': False,
            'old_failed_trace_preserved': True, 'automatic_queue_restart': False, 'stable_id_rpc_attempts': 3, 'stable_id_rpc_retry_wait_seconds': 10}


def recover_cached(main, args):
    freeze, admission, _ = main.check_admission(args.freeze, args.admission, args.data,
                                               args.frozen_code, args.graph)
    run = args.output
    folder = run / args.graph / 'tasks' / args.qid / args.arm
    if (folder / 'record.json').exists():
        raise ValueError('Completed prediction cannot be replaced')
    intent = json.loads((folder / 'intent.json').read_text(encoding='utf-8'))
    reply = json.loads((folder / 'reply_0.json').read_text(encoding='utf-8'))
    if intent['freeze_sha256'] != main.sha(args.freeze) or (folder/'reply_1.json').exists():
        raise ValueError('Recovery requires one unchanged cached first reply')
    # Check the cached reply against the successful, budgeted physical response.
    if not 1 <= len(reply['attempt_folders']) <= 3:
        raise ValueError('Unexpected physical retry history')
    attempts = [Path(p) for p in reply['attempt_folders']]
    for previous in attempts[:-1]:
        prior = json.loads((previous/'terminal.json').read_text(encoding='utf-8'))
        if not (prior.get('transport_error') or prior.get('http_status') == 429 or
                isinstance(prior.get('http_status'), int) and prior['http_status'] >= 500):
            raise ValueError('Earlier physical attempt was not an admissible transport retry')
        if (previous/'request.json').read_bytes() != (attempts[-1]/'request.json').read_bytes():
            raise ValueError('Physical retry payload changed')
    attempt = attempts[-1]
    terminal = json.loads((attempt/'terminal.json').read_text(encoding='utf-8'))
    if terminal.get('http_status') != 200 or terminal['body']['choices'][0]['message']['content'] != reply['content']:
        raise ValueError('Cached response lacks immutable successful attempt evidence')
    wire = json.loads((attempt/'request.json').read_text(encoding='utf-8'))
    if wire['messages'] != intent['messages']:
        raise ValueError('Cached physical request differs from original task intent')
    old_queries = sorted((folder/'query_trace/step_0').glob('*/intent.json'))
    if len(old_queries) != 1:
        raise ValueError('Expected the one audited interrupted direct query')
    expected_query = json.loads(old_queries[0].read_text(encoding='utf-8'))
    class CaptureOnly:
        def execute(self, query, parameters):
            if {'query': query, 'parameters': parameters} != expected_query:
                raise RuntimeError('Cached output compiles to a changed query')
            return {'status': 'EXECUTED', 'rows': [], 'columns': [], 'complete': True}
    schema = json.loads((args.data/'upstream/graphs/schemas'/(args.graph+'_schema.json')).read_text(encoding='utf-8'))
    class CapturedFirstQuery(RuntimeError):pass
    class FirstOnly(CaptureOnly):
        def execute(self,query,parameters):
            super().execute(query,parameters)
            raise CapturedFirstQuery()
    try:
        main.interpret(reply['content'],reply['usable'],args.arm,schema,FirstOnly())
    except CapturedFirstQuery:pass
    else:raise ValueError('Cached plan did not produce the audited first query')
    recovery = run/args.recovery_id
    recovery.mkdir(exist_ok=False)
    main.immutable_json(recovery/'plan.json', {**recovery_identity(main, args),
                          'qid': args.qid, 'arm': args.arm, 'new_model_calls': 0,
                          'original_budget_slot': terminal['budget_slot'],
                          'all_physical_budget_slots': [json.loads((p/'terminal.json').read_text(encoding='utf-8'))['budget_slot'] for p in attempts]})
    worker = None
    started = time.monotonic()
    try:
        worker = main.PublicWorker(args.ssh_key, args.graph, recovery/'worker_stderr.log', freeze['remote_worker'])
        if worker.ready['graph_sha256'] != admission['admitted_graphs'][args.graph]['graph_sha256']:
            raise ValueError('Recovery graph identity differs')
        main.immutable_json(recovery/'worker_ready.json', worker.ready)
        schema = json.loads((args.data/'upstream/graphs/schemas'/(args.graph+'_schema.json')).read_text(encoding='utf-8'))
        traced = main.TracedExecutor(worker, folder/'query_trace/recovered_step_0')
        outcome = main.interpret(reply['content'], reply['usable'], args.arm, schema, traced)
        view = {'step': 0, 'reply': reply, 'outcome': outcome, 'generation_wall_seconds': None,
                'original_transport_seconds': terminal['seconds'],
                'all_original_physical_seconds': sum(json.loads((p/'terminal.json').read_text(encoding='utf-8'))['seconds'] for p in attempts),
                'generation_clock_note': 'Original wrapper clock was not persisted before the SSH failure; unknown, not zero.'}
        main.immutable_json(recovery/'recovered_view.json', view)
        if outcome['status'] in ('MODEL_ERROR', 'EXECUTION_ERROR'):
            raise RuntimeError('Cached reply now requires the frozen feedback step; retain view, do not mark complete')
        record = {'qid': args.qid, 'graph': args.graph, 'arm': args.arm,
                  'freeze_sha256': main.sha(args.freeze), 'views': [view],
                  'model_and_execution_wall_seconds': None,
                  'elapsed_since_original_intent_including_outage_seconds': time.time()- (folder/'intent.json').stat().st_mtime,
                  'infrastructure_recovery': str(recovery), 'recovery_wall_seconds': time.monotonic()-started}
        if args.arm == 'ours' and outcome['status'] == 'EXECUTED':
            traced = main.TracedExecutor(worker, folder/'query_trace/recovered_full_detail')
            record['fixed_plan_full_detail'] = main.execute_plan(outcome['plan'], schema, traced.execute,
                                                                 strategy='full_detail')
        main.immutable_json(folder/'record.json', record)
        main.immutable_json(recovery/'complete.json', {'state': 'CACHED_REPLY_EXECUTION_RECOVERED',
                             'new_model_calls': 0, 'status': outcome['status'],
                             'prior_database_completion_unknown_preserved': True})
        print(json.dumps({'state': 'CACHED_REPLY_EXECUTION_RECOVERED', 'status': outcome['status'],
                          'new_model_calls': 0}), flush=True)
    finally:
        if worker:
            worker.close()


def dispatch_graph(main, args):
    config = SimpleNamespace(**vars(args))
    config.output = args.output / args.graph
    main.run(config)


def queue(main, args):
    folder = args.output/args.queue_id
    folder.mkdir(exist_ok=False)
    main.immutable_json(folder/'queue_plan.json', {**recovery_identity(main, args),
                        'graphs': list(main.GRAPHS), 'planned_primary_paths': 4696,
                        'original_queue_stop_preserved': True})
    completed=[]
    for graph,count in main.GRAPHS.items():
        score = args.output/graph/'scores'/(graph+'.json')
        if score.exists():
            rows=json.loads(score.read_text(encoding='utf-8'))
            if len(rows)!=count*2 or len({(r['qid'],r['arm']) for r in rows})!=count*2:
                raise ValueError('Existing graph score is incomplete')
            completed.append(graph)
            continue
        command=[sys.executable,str(Path(__file__).resolve()),'--mode','graph','--graph',graph,
                 '--segment',args.segment,'--max-physical-calls',str(count*4+128)]
        for field in ['frozen_code','freeze','admission','data','output','model_config','ssh_key',
                      'budget','approval','private_data','official_source']:
            command += ['--'+field.replace('_','-'),str(getattr(args,field))]
        with (folder/(graph+'_runner.log')).open('xb') as log:
            result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
        receipt=args.output/graph/'segments'/args.segment/'summary.json'
        summary=json.loads(receipt.read_text(encoding='utf-8')) if receipt.exists() else None
        if result.returncode or not summary or summary['state']!='COMPLETE' or summary['remaining_paths']:
            main.immutable_json(folder/'queue_stopped.json',{'graph':graph,'returncode':result.returncode,
                                'summary':summary,'completed_graphs':completed,'automatic_retry':False})
            print(json.dumps({'state':'STOPPED','graph':graph}),flush=True)
            return
        completed.append(graph)
        main.immutable_json(folder/(graph+'_completed.json'),summary)
        print(json.dumps({'state':'GRAPH_COMPLETE','graph':graph,'paths':count*2}),flush=True)
    main.immutable_json(folder/'queue_complete.json',{'state':'CORE_COMPLETE','graphs':completed,
                                                    'entire_T1_complete':False})


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for field in ['frozen-code','freeze','admission','data','output','model-config','ssh-key',
                  'budget','approval','private-data','official-source']:
        p.add_argument('--'+field,type=Path,required=True)
    p.add_argument('--mode',choices=['cached','graph','queue'],required=True)
    p.add_argument('--graph')
    p.add_argument('--qid')
    p.add_argument('--arm',choices=['direct','ours'])
    p.add_argument('--segment', default='journal_after_model_timeout_v2')
    p.add_argument('--queue-id', default='infra_journal_after_timeout_queue_v2_20261025')
    p.add_argument('--recovery-id', default='infra_recovery_company_ours_journal_v1_20261025')
    p.add_argument('--max-physical-calls',type=int)
    args=p.parse_args()
    for value in (args.segment,args.queue_id,args.recovery_id):
        if not value or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in value):
            p.error('Operational receipt identifiers must be simple directory names')
    main=load_original(args.frozen_code)
    main.PublicWorker=JournalPublicWorker
    if args.mode=='cached':recover_cached(main,args)
    elif args.mode=='graph':dispatch_graph(main,args)
    else:queue(main,args)
