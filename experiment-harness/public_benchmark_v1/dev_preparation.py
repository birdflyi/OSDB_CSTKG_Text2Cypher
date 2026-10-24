"""Development-only preparation runner. There is intentionally no test mode.

Runs on the fixed training smoke tasks, optionally with model predictions.
Reference answers are loaded only after prediction; no gold-driven feedback.
"""
import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from queue import Queue, Empty
import subprocess
import sys
from threading import Thread
import time

from answer_scoring import load_official_comparator, read_reference_answer, score_executed_answer
from attempt_budget import AttemptBudget
from model_transport import ModelTransport, immutable_json, BatchStopped
from schema_services import (PLAN_INSTRUCTIONS, VERSION, execute_plan, UnsupportedPlan,
                             IncompleteEvidence, ServiceExecutionError)


REMOTE_ROOT = '/mnt/data/louzehua/ch7_t1_public_benchmark_20261023'
DIRECT_INSTRUCTIONS = """Translate the question into a single read-only Neo4j Cypher query.
Return only the query, with no explanation. Use the supplied complete graph schema.
Return entity names/properties, not node or path objects. Do not add a default
LIMIT; use a limit only if the question asks for one. Preserve requested DISTINCT,
time filters, grouping and ordering. Database-side aggregation is allowed.
Node metadata: eid,name,aliases,description; relationship metadata: rid.
No write/admin/procedure statements. Read subqueries CALL { ... } are allowed.
"""


def lines(path):
    return [json.loads(line) for line in Path(path).read_text(encoding='utf-8').splitlines() if line.strip()]


def unfence(text):
    text = text.strip()
    if text.startswith('```') and text.endswith('```'):
        text = text.split('\n', 1)[1].rsplit('```', 1)[0].strip()
    return text


class RemoteWorker:
    def __init__(self, key, log):
        self.queue = Queue()
        self.log = Path(log).open('x', encoding='utf-8')
        command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12',
                   '-p', '2491', '-i', str(key), 'root@117.50.195.94',
                   REMOTE_ROOT + '/.venv/bin/python',
                   REMOTE_ROOT + '/preparation_v1_code/dev_database_worker.py',
                   '--root', REMOTE_ROOT]
        options = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.log, text=True, encoding='utf-8', **options)

        def read():
            for line in self.process.stdout:
                self.queue.put(line)
            self.queue.put(None)
        Thread(target=read, daemon=True).start()
        try:
            self.ready = self.receive(125)
            if self.ready.get('kind') != 'READY':
                raise RuntimeError('Database worker did not return ready')
        except Exception:
            self.close()
            raise

    def receive(self, timeout=125):
        try:
            line = self.queue.get(timeout=timeout)
        except Empty as error:
            raise RuntimeError('Remote worker response timeout') from error
        if line is None:
            raise RuntimeError('Remote worker exited; inspect private stderr log')
        return json.loads(line)

    def execute(self, query, parameters):
        start = time.monotonic()
        self.process.stdin.write(json.dumps({'query': query, 'parameters': parameters}, ensure_ascii=False) + '\n')
        self.process.stdin.flush()
        result = self.receive()
        result['worker_roundtrip_seconds'] = time.monotonic() - start
        return result

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


def interpret(content, usable, arm, schema, worker):
    if not usable:
        return {'status': 'MODEL_ERROR', 'rows': None, 'error': 'Missing or truncated final content'}
    observations = []

    def measured(query, parameters):
        result = worker.execute(query, parameters)
        observations.append(result)
        return result

    try:
        text = unfence(content)
        if arm == 'direct':
            result = measured(text, {})
            return {**result, 'observations': [deepcopy(result)]}
        plan = json.loads(text)
        result = execute_plan(plan, schema, measured)
        result['plan'] = plan
        return result
    except IncompleteEvidence as error:
        return {'status': 'TRUNCATED', 'rows': None, 'error': str(error), 'observations': observations}
    except ServiceExecutionError as error:
        return {'status': 'EXECUTION_ERROR', 'rows': None, 'error': str(error)[:1000],
                'observations': observations}
    except (UnsupportedPlan, ValueError, TypeError, KeyError) as error:
        return {'status': 'MODEL_ERROR', 'rows': None,
                'error': type(error).__name__ + ': ' + str(error)[:1000],
                'observations': observations}


def fixtures(data, schema, worker):
    # Constructed training-only workloads; independent Python graph reference.
    graph = json.loads((data / 'upstream/graphs/simplekg/terrorist_attack_simplekg.json').read_text(encoding='utf-8'))
    countries = {row['eid']: row['name'] for row in graph['entities'] if row['label'] == 'Country'}

    def select(value=None):
        return {'nodes': [{'id': 'n', 'label': 'Country'}], 'edges': [],
                'where': [] if value is None else [{'field': 'n.name', 'op': 'eq', 'value': value}],
                'returns': [{'field': 'n.eid', 'alias': 'id'}, {'field': 'n.name', 'alias': 'name'}]}
    rows = []
    for label, left, right, names in [
        ('disjoint_equality', 'Denmark', 'Iran', {'Denmark', 'Iran'}),
        ('overlap_all_and_subset', None, 'Denmark', None),
    ]:
        expected = len(countries) if names is None else sum(value in names for value in countries.values())
        plan = {'status': 'ANSWER', 'calls': [select(left), select(right)],
                'combine': {'op': 'union', 'key': 'id', 'output': 'count', 'alias': 'count'}}
        outcomes = {}
        for strategy in ['contract', 'full_detail']:
            result = execute_plan(plan, schema, worker.execute, strategy=strategy)
            if result['rows'] != [[expected]]:
                raise RuntimeError('Real TRAIN fixture differs from independent Python reference')
            outcomes[strategy] = result
        rows.append({'id': label, 'kind': 'CONSTRUCTED_TRAIN_COMPILER_CHECK',
                     'expected_rows': [[expected]], 'outcomes': outcomes})
    return rows


def run(args):
    code_root = Path(__file__).resolve().parents[2]
    dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=code_root, text=True).strip()
    if dirty:
        raise RuntimeError('Commit preparation implementation before runtime/model checks')
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=code_root, text=True).strip()
    data, output = args.data, args.output
    if output.exists():
        raise RuntimeError('Existing preparation run is immutable; audit rather than rerun')
    manifest = json.loads((data / 'manifest.json').read_text(encoding='utf-8'))
    tasks = lines(data / 'runtime/dev_smoke.jsonl')
    if len(tasks) != 3 or {x['qid'] for x in tasks} != set(manifest['dev_smoke_ids']):
        raise ValueError('Only the fixed three TRAIN smoke tasks are admitted')
    if any(t['graph'] != manifest['dev_smoke_graph'] or t['graph'] in manifest['splits']['test']['by_graph'] for t in tasks):
        raise ValueError('Development runner refuses test tasks/graphs')
    for relative in ['runtime/dev_smoke.jsonl', 'upstream/graphs/schemas/terrorist_attack_schema.json']:
        expected = next(x['sha256'] for x in manifest['artifacts'] if x['path'] == relative)
        if hashlib.sha256((data / relative).read_bytes()).hexdigest() != expected:
            raise ValueError('Pinned development artifact changed')
    output.mkdir(parents=True)
    schema = json.loads((data / 'upstream/graphs/schemas/terrorist_attack_schema.json').read_text(encoding='utf-8'))
    immutable_json(output / 'run_manifest.json', {
        'kind': 'TRAIN_PREPARATION_ONLY_NOT_MAIN_TABLE', 'git_commit': commit,
        'method_version': VERSION, 'task_ids': [t['qid'] for t in tasks],
        'models_enabled': args.models, 'maximum_new_physical_calls': args.max_model_calls if args.models else 0,
        'feedback': 'At most once per path for invalid format/compile/execution only; no gold feedback.',
        'limits': 'One TRAIN graph; schema finite, no formal test mode. Timings include SSH worker. Not a generalization estimate.'})
    worker = None
    records, stopped = [], None
    transport = None
    try:
        worker = RemoteWorker(args.ssh_key, output / 'worker_stderr.log')
        immutable_json(output / 'worker_ready.json', worker.ready)
        checks = fixtures(data, schema, worker)
        immutable_json(output / 'compiler_fixture_results.json', checks)
        if args.models:
            config = json.loads(args.model_config.read_text(encoding='utf-8'))
            budget = AttemptBudget(args.budget, args.approval, path_rewrites=[
                (r'C:\Users\10651\Documents\trae_projects\ch7_private', args.private_data)])
            transport = ModelTransport(config, budget, output / 'model_calls', max_physical_calls=args.max_model_calls)
            for index, task in enumerate(tasks):
                arms = ['direct', 'ours'] if index % 2 == 0 else ['ours', 'direct']
                for arm in arms:
                    messages = [{'role': 'system', 'content': DIRECT_INSTRUCTIONS if arm == 'direct' else PLAN_INSTRUCTIONS},
                                {'role': 'user', 'content': 'Graph schema:\n' + json.dumps(schema, ensure_ascii=False) +
                                 '\nQuestion:\n' + task['nl_question']}]
                    record = {'qid': task['qid'], 'arm': arm, 'views': []}
                    for step in range(2):
                        reply = transport.complete(messages, purpose=f'cypherbench TRAIN preparation {arm} step{step}')
                        outcome = interpret(reply['content'], reply['usable'], arm, schema, worker)
                        record['views'].append({'step': step, 'reply': reply, 'outcome': outcome})
                        immutable_json(output / 'progress' / f'{index}_{arm}_{step}.json', record)
                        if outcome['status'] not in ('MODEL_ERROR', 'EXECUTION_ERROR'):
                            break
                        messages += [{'role': 'assistant', 'content': reply['content']},
                                     {'role': 'user', 'content': 'The output could not be executed under the declared rules. '
                                      'Repair once using only this diagnostic: ' + outcome.get('error', 'Execution failed')[:1000]}]
                    if arm == 'ours' and outcome['status'] == 'EXECUTED':
                        record['full_detail_control'] = execute_plan(outcome['plan'], schema, worker.execute,
                                                                      strategy='full_detail')
                    records.append(record)
                    immutable_json(output / 'predictions' / f'{task["qid"]}_{arm}.json', record)
        # Evaluator references are intentionally loaded after prediction.
        references = {row['qid']: row for row in lines(data / 'evaluator/train.jsonl')
                      if row['qid'] in {t['qid'] for t in tasks}}
        compare = load_official_comparator(args.official_source)
        reference_checks = []
        for task in tasks:
            reference = references[task['qid']]
            expected = read_reference_answer(reference['answer_json'])
            actual = worker.execute(reference['gold_cypher'], {})
            validation = score_executed_answer(status=actual['status'], predicted_rows=actual['rows'],
                           reference_rows=expected, order_matters='order by' in reference['gold_cypher'].lower(),
                           qid=task['qid'], compare=compare)
            if validation['ex'] != 1:
                raise RuntimeError('Training reference no longer matches imported database')
            reference_checks.append({'qid': task['qid'], 'validation': validation,
                                     'executed_result': actual})
        immutable_json(output / 'reference_execution_checks.json', reference_checks)
        for record in records:
            reference = references[record['qid']]
            for view in record['views']:
                view['score'] = score_executed_answer(
                    status=view['outcome']['status'], predicted_rows=view['outcome']['rows'],
                    reference_rows=read_reference_answer(reference['answer_json']),
                    order_matters='order by' in reference['gold_cypher'].lower(),
                    qid=record['qid'], compare=compare)
        immutable_json(output / 'evaluated_records.json', records)
    except BatchStopped as error:
        stopped = str(error)
    finally:
        if worker:
            worker.close()
        summary = {'kind': 'DEVELOPMENT_ONLY_NOT_MAIN_TABLE', 'git_commit': commit,
                   'status': 'STOPPED' if stopped else ('COMPLETED' if not sys.exc_info()[0] else 'ERROR'),
                   'stop_reason': stopped, 'evaluated_paths': len(records),
                   'physical_model_calls': transport.physical_calls if transport else 0,
                   'outcome_counts': dict(Counter(r['views'][-1]['outcome']['status'] for r in records)),
                   'formal_test_predictions': 0, 'full_main_table_ready': False}
        immutable_json(output / 'summary.json', summary)
        print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    for option in ['data', 'output', 'ssh-key', 'official-source']:
        p.add_argument('--' + option, type=Path, required=True)
    p.add_argument('--models', action='store_true')
    p.add_argument('--model-config', type=Path)
    p.add_argument('--budget', type=Path)
    p.add_argument('--approval', type=Path)
    p.add_argument('--private-data', type=Path)
    p.add_argument('--max-model-calls', type=int, default=12, choices=range(1, 13))
    a = p.parse_args()
    if a.models and any(getattr(a, k) is None for k in ['model_config', 'budget', 'approval', 'private_data']):
        p.error('Model preparation requires private config, canonical budget and approval')
    run(a)
