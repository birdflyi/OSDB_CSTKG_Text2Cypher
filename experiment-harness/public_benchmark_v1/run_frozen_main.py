"""One graph of the frozen CypherBench core comparison. Fail-closed admission.

All seven graph task lists must be frozen before the first formal prediction.
Run only after a separately recorded admission review. A complete task record
can be reused; an ambiguous interrupted intent blocks automatic resubmission.
"""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
from queue import Queue, Empty
import re
import subprocess
from threading import Thread
import time

from answer_scoring import load_official_comparator, read_reference_answer, score_executed_answer
from attempt_budget import AttemptBudget
from dev_preparation import DIRECT_INSTRUCTIONS, REMOTE_ROOT, interpret, lines
from entity_candidates import CANDIDATE_NOTICE
from model_transport import ModelTransport, BatchStopped, immutable_json
from schema_services import PLAN_INSTRUCTIONS, execute_plan


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check_admission(freeze_path, admission_path, data, code_root, graph):
    freeze = json.loads(freeze_path.read_text(encoding='utf-8'))
    admission = json.loads(admission_path.read_text(encoding='utf-8'))
    if (admission.get('formal_run_ready') is not True or
        admission.get('freeze_sha256') != sha(freeze_path)):
        raise ValueError('Formal comparison is not admitted for this freeze')
    if graph not in freeze['graphs'] or graph not in admission.get('admitted_graphs', {}):
        raise ValueError('Graph has not passed its data/reference admission review')
    for relative, digest in freeze['code_files'].items():
        path = (code_root / relative).resolve()
        if code_root.resolve() not in path.parents or sha(path) != digest:
            raise ValueError('Frozen source file changed')
    for relative, digest in freeze['data_files'].items():
        path = (data / relative).resolve()
        if data.resolve() not in path.parents or sha(path) != digest:
            raise ValueError('Frozen task/schema/evaluator file changed')
    runtime = lines(data / 'runtime/test.jsonl')
    if len(runtime) != 2348 or len({r['qid'] for r in runtime}) != 2348:
        raise ValueError('Full official task denominator changed')
    actual = Counter(r['graph'] for r in runtime)
    if dict(actual) != freeze['graphs'] or sum(freeze['graphs'].values()) != 2348:
        raise ValueError('Frozen graph/task matrix changed')
    if any(set(r) != {'qid', 'graph', 'nl_question'} for r in runtime):
        raise ValueError('Forbidden fields in runtime context')
    tasks = sorted((r for r in runtime if r['graph'] == graph), key=lambda r: r['qid'])
    return freeze, admission, tasks


class PublicWorker:
    def __init__(self, key, graph, log):
        self.queue = Queue()
        self.log = log.open('x', encoding='utf-8')
        command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', '-p', '2491',
                   '-i', str(key), 'root@117.50.195.94', REMOTE_ROOT + '/.venv/bin/python',
                   REMOTE_ROOT + '/public_runtime_v1_code/public_query_worker.py',
                   '--root', REMOTE_ROOT, '--graph', graph]
        options = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                       stderr=self.log, text=True, encoding='utf-8', **options)
        def reader():
            for line in self.process.stdout:
                self.queue.put(line)
            self.queue.put(None)
        Thread(target=reader, daemon=True).start()
        try:
            self.ready = self.receive(300)
            if self.ready.get('kind') != 'READY' or self.ready.get('graph') != graph:
                raise RuntimeError('Wrong public worker identity')
        except Exception:
            self.close()
            raise

    def receive(self, timeout=125):
        try:
            row = self.queue.get(timeout=timeout)
        except Empty as error:
            raise RuntimeError('Public worker timed out; retain intent and audit') from error
        if row is None:
            raise RuntimeError('Public worker exited; retain intent and audit')
        return json.loads(row)

    def request(self, value):
        started = time.monotonic()
        self.process.stdin.write(json.dumps(value, ensure_ascii=False) + '\n')
        self.process.stdin.flush()
        result = self.receive()
        result['worker_roundtrip_seconds'] = time.monotonic() - started
        return result

    def execute(self, query, parameters):
        return self.request({'query': query, 'parameters': parameters})

    def close(self):
        try:
            if self.process.stdin and not self.process.stdin.closed:
                self.process.stdin.close()
            self.process.wait(timeout=45)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=10)
        finally:
            self.log.close()


def run(args):
    code_root = Path(__file__).resolve().parents[2]
    freeze, admission, tasks = check_admission(args.freeze, args.admission, args.data, code_root, args.graph)
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=code_root, text=True).strip():
        raise ValueError('Source checkout is dirty')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', args.segment):
        raise ValueError('Invalid segment identity')
    config = json.loads(args.model_config.read_text(encoding='utf-8'))
    for field in ['model', 'temperature', 'top_p', 'reasoning_effort']:
        if config[field] != freeze['model_settings'][field]:
            raise ValueError('Private model settings differ from freeze')
    config.update(max_tokens=freeze['model_settings']['max_tokens'],
                  read_timeout_seconds=freeze['model_settings']['read_timeout_seconds'])
    budget = AttemptBudget(args.budget, args.approval, path_rewrites=[
             (r'C:\Users\10651\Documents\trae_projects\ch7_private', args.private_data)])
    snapshot = budget.inspect()
    if snapshot['pending']:
        raise ValueError('Pending model attempt; audit before dispatch')
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    saved_freeze = output / 'freeze.json'
    if saved_freeze.exists() and saved_freeze.read_bytes() != args.freeze.read_bytes():
        raise ValueError('Output directory belongs to another freeze')
    if not saved_freeze.exists():
        with saved_freeze.open('xb') as stream:
            stream.write(args.freeze.read_bytes())
    segment = output / 'segments' / args.segment
    segment.mkdir(parents=True, exist_ok=False)
    immutable_json(segment / 'admission.json', admission)
    transport = worker = None
    stop = None
    try:
        worker = PublicWorker(args.ssh_key, args.graph, segment / 'worker_stderr.log')
        if worker.ready['graph_sha256'] != admission['admitted_graphs'][args.graph]['graph_sha256']:
            raise ValueError('Execution graph differs from admitted graph')
        immutable_json(segment / 'worker_ready.json', worker.ready)
        transport = ModelTransport(config, budget, segment / 'model_calls',
                    max_physical_calls=min(args.max_physical_calls, snapshot['remaining']))
        schema = json.loads((args.data / 'upstream/graphs/schemas' /
                             (args.graph + '_schema.json')).read_text(encoding='utf-8'))
        for index, task in enumerate(tasks):
            if (output / 'STOP_MAIN').exists():
                raise BatchStopped('Explicit STOP_MAIN marker observed')
            shared_path = output / 'shared_inputs' / (task['qid'] + '.json')
            if shared_path.exists():
                shared = json.loads(shared_path.read_text(encoding='utf-8'))
            else:
                candidate_result = worker.request({'kind': 'candidates', 'question': task['nl_question']})
                if candidate_result.get('status') != 'CANDIDATES':
                    raise RuntimeError('Shared entity lookup failed')
                context = ('Graph schema:\n' + json.dumps(schema, ensure_ascii=False) +
                           '\nQuestion:\n' + task['nl_question'] + '\n' + CANDIDATE_NOTICE +
                           '\nCandidates:\n' + json.dumps(candidate_result['candidates'], ensure_ascii=False))
                shared = {'context': context, 'lookup': candidate_result, 'qid': task['qid'],
                          'graph': args.graph, 'freeze_sha256': sha(args.freeze)}
                immutable_json(shared_path, shared)
            for arm in (['direct', 'ours'] if index % 2 == 0 else ['ours', 'direct']):
                folder = output / 'tasks' / task['qid'] / arm
                if (folder / 'record.json').exists():
                    continue
                if (folder / 'intent.json').exists():
                    raise BatchStopped('Interrupted intent requires trace recovery; do not resend')
                messages = [{'role': 'system', 'content': DIRECT_INSTRUCTIONS if arm == 'direct' else PLAN_INSTRUCTIONS},
                            {'role': 'user', 'content': shared['context']}]
                immutable_json(folder / 'intent.json', {'qid': task['qid'], 'arm': arm,
                               'freeze_sha256': sha(args.freeze), 'messages': messages})
                record = {'qid': task['qid'], 'graph': args.graph, 'arm': arm,
                          'freeze_sha256': sha(args.freeze), 'views': []}
                started = time.monotonic()
                for step in range(2):
                    generation_started = time.monotonic()
                    reply = transport.complete(messages, purpose=(f'CypherBench FORMAL {sha(args.freeze)[:12]} '
                               f'{args.graph} {task["qid"]} {arm} step{step}'))
                    generation_seconds = time.monotonic() - generation_started
                    immutable_json(folder / f'reply_{step}.json', reply)
                    outcome = interpret(reply['content'], reply['usable'], arm, schema, worker)
                    record['views'].append({'step': step, 'reply': reply, 'outcome': outcome,
                                            'generation_wall_seconds': generation_seconds})
                    immutable_json(folder / f'view_{step}.json', record['views'][-1])
                    if outcome['status'] not in ('MODEL_ERROR', 'EXECUTION_ERROR'):
                        break
                    messages += [{'role': 'assistant', 'content': reply['content']},
                                 {'role': 'user', 'content': 'Repair once using only this format/compile/execution '
                                  'diagnostic, not correctness feedback: ' + outcome.get('error', 'Execution failed')[:1000]}]
                record['model_and_execution_wall_seconds'] = time.monotonic() - started
                if arm == 'ours' and outcome['status'] == 'EXECUTED':
                    try:
                        record['fixed_plan_full_detail'] = execute_plan(outcome['plan'], schema, worker.execute,
                                                                       strategy='full_detail')
                    except Exception as error:
                        record['fixed_plan_full_detail'] = {'status': 'EXECUTION_ERROR', 'rows': None,
                                                             'error_type': type(error).__name__}
                immutable_json(folder / 'record.json', record)
        # No evaluator references were read while making predictions.
        refs = {r['qid']: r for r in lines(args.data / 'evaluator/test.jsonl') if r['graph'] == args.graph}
        compare = load_official_comparator(args.official_source)
        results = []
        for task in tasks:
            for arm in ['direct', 'ours']:
                record = json.loads((output / 'tasks' / task['qid'] / arm / 'record.json').read_text(encoding='utf-8'))
                ref = refs[task['qid']]
                for view in record['views']:
                    view['score'] = score_executed_answer(status=view['outcome']['status'],
                        predicted_rows=view['outcome']['rows'], reference_rows=read_reference_answer(ref['answer_json']),
                        order_matters='order by' in ref['gold_cypher'].lower(), qid=task['qid'], compare=compare)
                if arm == 'ours':
                    control = record.get('fixed_plan_full_detail', record['views'][-1]['outcome'])
                    record['fixed_plan_full_detail_score'] = score_executed_answer(
                        status=control['status'], predicted_rows=control['rows'],
                        reference_rows=read_reference_answer(ref['answer_json']),
                        order_matters='order by' in ref['gold_cypher'].lower(), qid=task['qid'], compare=compare)
                    record['static_safe_policy_equivalent_to_full_detail_in_v01'] = True
                results.append(record)
        immutable_json(output / 'scores' / (args.graph + '.json'), results)
    except BatchStopped as error:
        stop = str(error)
    finally:
        if worker:
            worker.close()
        completed = sum((output / 'tasks' / t['qid'] / arm / 'record.json').exists()
                        for t in tasks for arm in ['direct', 'ours'])
        import sys
        summary = {'graph': args.graph, 'planned_paths': len(tasks) * 2, 'terminal_paths': completed,
                   'remaining_paths': len(tasks) * 2 - completed, 'stop_reason': stop,
                   'new_physical_attempts': transport.physical_calls if transport else 0,
                   'state': 'STOPPED' if stop else ('ERROR' if sys.exc_info()[0] else 'COMPLETE')}
        immutable_json(segment / 'summary.json', summary)
        print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ['freeze', 'admission', 'data', 'output', 'model-config', 'ssh-key',
                  'budget', 'approval', 'private-data', 'official-source']:
        parser.add_argument('--' + field, type=Path, required=True)
    parser.add_argument('--graph', required=True)
    parser.add_argument('--segment', required=True)
    parser.add_argument('--max-physical-calls', type=int, required=True)
    run(parser.parse_args())
