"""Transparent retry cohort. Frozen prompts/compiler/grading imported read-only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import requests
from ten_second_retry import infrastructure_failure, retry_delay


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)


class RetryCoordinator:
    def __init__(self, output, physical, clock=time.monotonic, sleep=time.sleep):
        self.output = Path(output)
        self.physical = physical
        self.clock, self.sleep = clock, sleep
        self.consecutive = 0
        self.stopped = False
        self.total_wait = 0.0

    def dispatch(self, logical_stage, arm, data):
        if self.stopped:
            raise RuntimeError('Three physical failures already stopped this batch')
        attempts, replies, waits = [], [], []
        for index in range(1, 4):
            if index > 1:
                intended = retry_delay(replies[-1]['raw'])
                if not 0 <= intended < float('inf'):
                    raise ValueError('Invalid server retry delay')
                start = self.clock()
                while self.clock() - start < intended:
                    self.sleep(min(10.0, intended - (self.clock() - start)))
                actual = self.clock() - start
                self.total_wait += actual
                waits.append({'before_attempt': index, 'requested_seconds': intended,
                              'actual_seconds': actual})
            stage = logical_stage + '_attempt' + str(index)
            reply = self.physical(stage, arm, data)
            failed = infrastructure_failure(reply['raw'])
            self.consecutive = self.consecutive + 1 if failed else 0
            attempts.append(stage)
            replies.append(reply)
            if self.consecutive >= 3:
                self.stopped = True
            if not failed or self.stopped:
                break
        receipt = {'logical_stage': logical_stage, 'physical_stages': attempts,
                   'first_stage': attempts[0], 'selected_stage': attempts[-1],
                   'selection': 'First non-infrastructure response; otherwise final failure',
                   'waits': waits, 'consecutive_physical_failures': self.consecutive,
                   'batch_stopped': self.stopped}
        save(self.output / 'logical' / (logical_stage + '.json'), receipt)
        return replies[0], replies[-1], receipt


def physical_dispatch(out, stage, arm, data, cfg, budget, comparison):
    """Charge one network request, then use the original cached response parser."""
    folder = Path(out) / 'calls' / stage
    if folder.exists():
        raise FileExistsError('Never overwrite or silently resume an actual attempt')
    if (Path(budget).parent / 'STOP_BACKGROUND').exists() or (Path(budget).parent / 'EXPERIMENT_DONE').exists():
        raise RuntimeError('Stop marker')
    payload = {k: cfg[k] for k in ('model', 'temperature', 'top_p', 'reasoning_effort')}
    prompt = comparison.s.PROMPTS['G'] if arm == 'G' else comparison.p.PROTOCOL_PROMPT
    payload.update(max_tokens=6000, messages=[{'role': 'system', 'content': prompt},
        {'role': 'user', 'content': json.dumps(data, ensure_ascii=False)}])
    slot = comparison.control.reserve(Path(budget), folder / 'request.json')
    save(folder / 'request.json', payload)
    prepared = requests.Request('POST', cfg['endpoint'], json=payload).prepare()
    wire = prepared.body
    if isinstance(wire, str): wire = wire.encode('utf-8')
    (folder / 'request_wire.bin').write_bytes(wire)
    save(folder / 'transport.json', {'budget_slot': slot, 'connect_read_timeout': [10, 120],
         'prepared_wire_sha256': hashlib.sha256(wire).hexdigest(),
         'route': 'Original Python requests.post; child-only direct proxy environment'})
    start = time.monotonic()
    try:
        response = requests.post(cfg['endpoint'], headers={'Authorization': 'Bearer ' + cfg['api_key']},
                                 json=payload, timeout=(10, 120), allow_redirects=False)
        try: body = response.json()
        except ValueError: body = {'non_json_body': response.text}
        record = {'http_status': response.status_code, 'body': body, 'seconds': time.monotonic() - start}
        if response.headers.get('Retry-After'): record['retry_after'] = response.headers['Retry-After']
        sent = response.request.body
        if isinstance(sent, str): sent = sent.encode('utf-8')
        if sent != wire:
            save(folder / 'terminal.json', record)
            raise RuntimeError('Unexpected request-body serialization difference')
    except requests.RequestException as exc:
        record = {'transport_error': type(exc).__name__, 'seconds': time.monotonic() - start}
    save(folder / 'terminal.json', record)
    # Both files exist: the unchanged decoder can only read the cache, never call.
    return comparison.dispatch(Path(out), stage, cfg, arm, data, Path(budget))


def cohort_summary(out, records, planned, coordinator, accounting):
    terms = {p.parent.name: json.loads(p.read_text(encoding='utf-8'))
             for p in (out / 'calls').glob('*/terminal.json')}
    scores = {}
    for arm in ('G', 'P', 'I'):
        for view in ('FIRST_PHYSICAL', 'AFTER_TRANSPORT', 'AFTER_STRUCTURAL'):
            selected = [r for r in records if r['arm'] == arm and r['view'] == view]
            answerable = [r for r in selected if r['expected_status'] == 'ANSWER']
            scores[arm + '_' + view] = {'planned': planned, 'evaluated': len(selected),
                'answerable_cases': len(answerable),
                'answerable_correct': sum(r['grade']['correct'] for r in answerable),
                'correct_boundary': sum(r['grade']['correct'] for r in selected if r['expected_status'] != 'ANSWER'),
                'wrong_returned_answers': sum(r['grade']['wrong_returned_answer'] for r in selected),
                'safe_catalog_refusals': sum(r['grade']['safe_catalog_refusal'] for r in selected),
                'infrastructure_outcomes': sum(r['failure'] == 'INFRASTRUCTURE' for r in selected)}
    return {'status': 'STOPPED_INFRASTRUCTURE' if coordinator.stopped else 'COMPLETED',
            'scores': scores, 'actual_calls': len(terms), 'physical_usage': accounting.usage(list(terms.values())),
            'arm_physical_costs': {arm: accounting.usage([value for key,value in terms.items() if key.startswith(arm+'_')]) for arm in ('G','P','I')},
            'retry_wait_seconds': coordinator.total_wait,
            'limits': 'New retry cohort, not pooled with zero-retry cohorts. Views share physical attempts; do not add their costs. No inference of rate-limit causality.'}


def run(a):
    engine = Path(a.engine_root).resolve()
    sys.path.insert(0, str(engine / 'publication_benchmark'))
    import run_information_comparison as c
    import analyze_continuation as accounting
    out = Path(a.output)
    if out.exists(): raise FileExistsError('Use an untouched output; never silently rerun')
    for repo in (engine, Path(__file__).resolve().parents[1]):
        if subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip():
            raise RuntimeError('Freeze clean source before predictions')
    root = Path(a.task_dir)
    load = lambda path: json.loads(Path(path).read_text(encoding='utf-8'))
    tasks, schema = load(root/'specifications.json'), load(root/'shared_schema.json')
    task_manifest, receipt, cfg = load(root/'manifest.json'), load(a.receipt), load(a.config)
    if task_manifest['project'] != a.project: raise ValueError('Project binding mismatch')
    if task_manifest['task_sha256'] != c.b.sha(root/'specifications.json'): raise ValueError('Task freeze mismatch')
    if task_manifest['source_hashes'] != receipt['source_hashes']: raise ValueError('Source mismatch')
    for name,digest in receipt['source_hashes'].items():
        if c.b.sha(Path(a.data)/name) != digest: raise ValueError('CSV source mismatch')
    if accounting.ledger(Path(a.budget_dir).parent)['pending_attempts']: raise RuntimeError('Pending call')
    c.b.DB_URL = 'http://127.0.0.1:' + str(a.port) + '/db/neo4j/tx/commit'
    refs = c.verify(tasks,receipt)
    engine_commit = subprocess.check_output(['git','rev-parse','HEAD'],cwd=engine,text=True).strip()
    wrapper_commit = subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).resolve().parents[1],text=True).strip()
    save(out/'manifest.json', {'mode':'DECLARED_TEN_SECOND_RETRY_COHORT','task_mode':task_manifest['mode'],
         'project':a.project,'engine_commit':engine_commit,'wrapper_commit':wrapper_commit,
         'task_sha256':c.b.sha(root/'specifications.json'),'schema_sha256':c.b.sha(root/'shared_schema.json'),
         'source_hashes':receipt['source_hashes'],'database_identity':receipt['database_identity'],
         'reference_verified':refs,'model':{k:cfg[k] for k in ('model','temperature','top_p','reasoning_effort')},
         'maximum_physical_attempts':18*len(tasks),'max_transport_retries_per_logical_request':2,
         'minimum_retry_wait_seconds':10,'global_consecutive_physical_failure_stop':3,
         'structural_repair_limit':1,'context':'compact','port':a.port,
         'runner_sha256':c.b.sha(__file__),'engine_python_sha256':{p.name:c.b.sha(p) for p in (engine/'publication_benchmark').glob('*.py')}})
    for name,folder in [('engine_snapshot',engine/'publication_benchmark'),('runner_snapshot',Path(__file__).parent)]:
        for source in folder.glob('*.py'):
            target=out/name/source.name
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(source,target)
    c.control.derived(out/'active_process.json', {'pid':os.getpid(),'wrapper_commit':wrapper_commit})
    def physical(stage,arm,data): return physical_dispatch(out,stage,arm,data,cfg,a.budget_dir,c)
    coordinator=RetryCoordinator(out,physical)
    records=[]
    orders=(('G','P','I'),('P','I','G'),('I','G','P'))
    for index,spec in enumerate(tasks):
        for arm in orders[index%3]:
            data=c.compact_context.public_input(spec,schema,arm)
            stage=arm+'_'+spec['id']
            first_reply,transport_reply,first_receipt=coordinator.dispatch(stage+'_first',arm,data)
            first_obs=c.observe(first_reply,arm)
            transport_obs=first_obs if first_reply is transport_reply else c.observe(transport_reply,arm)
            final_reply,final_obs=transport_reply,transport_obs
            used=False
            if transport_obs['check'].get('repairable') and not coordinator.stopped:
                feedback={**data,'prior_output':transport_reply.get('text'),
                    'structural_feedback':transport_obs['check'].get('feedback'),
                    'instruction':'One structural/protocol/execution repair allowed. Preserve every original task condition.'}
                _,final_reply,repair_receipt=coordinator.dispatch(stage+'_repair',arm,feedback)
                final_obs=c.observe(final_reply,arm)
                used=True
            for view,reply,obs in [('FIRST_PHYSICAL',first_reply,first_obs),('AFTER_TRANSPORT',transport_reply,transport_obs),('AFTER_STRUCTURAL',final_reply,final_obs)]:
                record={'id':spec['id'],'semantic_goal':spec.get('semantic_goal',spec['family']),
                    'project':a.project,'family':spec['family'],'arm':arm,'view':view,
                    'expected_status':spec['expected_status'],'prediction':reply.get('prediction'),
                    'failure':reply.get('failure'),'structural_repair_used':used if view=='AFTER_STRUCTURAL' else False,
                    'observation':obs,'grade':c.grade(spec,reply,obs,arm)}
                save(out/'records'/(stage+'_'+view+'.json'),record)
                records.append(record)
            progress=cohort_summary(out,records,len(tasks),coordinator,accounting)
            progress['status']='RUNNING'
            c.control.derived(out/'progress.json',progress)
            print(a.project,index+1,len(tasks),arm,final_obs['execution']['status'],len(first_receipt['physical_stages']),flush=True)
            if coordinator.stopped: break
        if coordinator.stopped: break
    summary=cohort_summary(out,records,len(tasks),coordinator,accounting)
    save(out/'summary.json',summary)
    c.control.derived(out/'active_process.json',{'pid':None,'status':summary['status']})
    print(json.dumps(summary),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for key in ('engine-root','task-dir','project','data','receipt','config','output','budget-dir'):
        parser.add_argument('--'+key,required=True)
    parser.add_argument('--port',type=int,choices=(17475,17476,17477,17478),required=True)
    run(parser.parse_args())
