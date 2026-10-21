"""E1 A-vs-C development pilot. Shared semantics, different output spaces.

No obligation gate or repair. No claimed user study, held-out evaluation, or
full end-to-end answer cost. Every complete prediction is scored once.
"""
import argparse
from collections import Counter
import json
import math
from pathlib import Path
import re
import shutil

import run_baselines as b
from mechanism_core import CATALOG, compile_call
from verify_mechanism_references import execute


COMMON = """Answer the supplied repository task by retrieving from the provided graph.
Use the schema and business contracts supplied to both experimental arms.
Preserve requested scope, fields, record multiplicity and aggregation grain.
Keep reason brief. Return ONLY a JSON array containing one object.
Status options: ANSWER,NEEDS_INPUT,UNSUPPORTED,INSUFFICIENT_EVIDENCE,ERROR.
Do not guess missing identifiers. Do not replace unsupported conditions with a
nearby supported task. Do not impose LIMIT on a requested full list or count.
"""
PROMPTS = {
    'A_DIRECT_CYPHER': COMMON + """Object keys exactly: id,status,cypher,parameters,reason.
For ANSWER produce one read-only Cypher query and a parameters object.
Query parameters may be empty if literals are used. Return columns in the task's
specified order. For other statuses use cypher null and parameters null.
You may implement the business contracts directly in Cypher; you have the same
business definitions as the service-mapping arm.
""",
    'C_TYPED_SERVICE': COMMON + """Object keys exactly: id,status,operation,arguments,reason.
For ANSWER select one operation from the supplied contracts and supply ALL its
arguments, and only those arguments. For other statuses use operation null and
arguments null. The operation is compiled deterministically; it does not check
whether you omitted conditions from the original natural-language request.
""",
}

ORIGINAL_PARSE = b.parse_array


def parse_single_prediction(text, ids, keys):
    """Lossless singleton object/array adapter; never fill or modify any value."""
    if len(ids) != 1:
        raise ValueError('This adapter accepts one task only')
    text = text.strip()
    if '```' in text:
        blocks = re.findall(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
        if len(blocks) != 1:
            raise ValueError('Ambiguous JSON blocks')
        text = blocks[0]
    value = json.loads(text)
    if isinstance(value, dict):
        value = [value]
    return ORIGINAL_PARSE(json.dumps(value, ensure_ascii=False), ids, keys)


def bag(rows):
    def canon(x):
        if isinstance(x, float) and math.isfinite(x) and x.is_integer():
            return int(x)
        if isinstance(x, list):
            return [canon(v) for v in x]
        return x
    return Counter(json.dumps(canon(row), ensure_ascii=False, sort_keys=True) for row in rows)


def run(args):
    out, source = Path(args.output).resolve(), Path(args.source).resolve()
    if out.exists():
        raise FileExistsError('Do not overwrite an experiment')
    cfg = json.loads(Path(args.config).read_text(encoding='utf-8'))
    specs = json.loads((source / (args.project + '_specifications.json')).read_text(encoding='utf-8'))
    receipt = json.loads(Path(args.receipt).read_text(encoding='utf-8'))
    reference = json.loads((source / (args.project + '_reference_execution.json')).read_text(encoding='utf-8'))
    dm = json.loads((source / 'dataset_manifest.json').read_text(encoding='utf-8'))
    if reference['correct'] != len(specs) or reference['specifications_sha256'] != b.sha(source / (args.project + '_specifications.json')):
        raise ValueError('Reference verification missing or stale')
    if dm['projects'][args.project] != receipt['source_hashes'] or reference['compiler_sha256'] != b.sha(Path(__file__).with_name('mechanism_core.py')):
        raise ValueError('Source/compiler changed since conformance verification')
    if b.db('CALL db.info() YIELD id,name RETURN id,name')['data'] != receipt['database_identity']:
        raise ValueError('Wrong dedicated graph')
    chosen = [s for s in specs if s['variant_index'] == 0]
    reused = Path(args.reuse_predictions).resolve() if args.reuse_predictions else None
    if reused:
        prior = json.loads((reused / 'summary.json').read_text(encoding='utf-8'))
        if prior['status'] != 'PLANNED_DISPATCHES_COMPLETED' or json.loads((reused / 'private_specifications.json').read_text(encoding='utf-8')) != chosen:
            raise ValueError('Reuse requires identical completed planned predictions')
    b.save(out / 'manifest.json', {'mode': 'E1_CANONICAL_DEVELOPMENT_A_VS_C', 'project': args.project,
        'model': cfg['model'], 'generation': {k: cfg[k] for k in ('temperature', 'top_p', 'reasoning_effort')},
        'cases': len(chosen), 'max_model_calls': 2 * len(chosen), 'max_completion_tokens': 6000,
        'presentation_policy': 'One complete JSON object or singleton array; exact keys and ID; no field/value repair',
        'reused_predictions': str(reused) if reused else None,
        'batch_size': 1, 'automatic_retries': 0, 'circuit_breaker_consecutive_infrastructure_failures': 3,
        'shared_information': 'Identical schema, complete business contracts, public task and output columns',
        'arms': list(PROMPTS), 'prompts': PROMPTS, 'catalog': CATALOG, 'schema': b.SCHEMA,
        'reference_input_sha256': b.sha(source / (args.project + '_specifications.json')),
        'source_hashes': receipt['source_hashes'], 'database_identity': receipt['database_identity'],
        'code_hashes': {n: b.sha(Path(__file__).with_name(n)) for n in
            ('run_mechanism_pilot.py', 'mechanism_core.py', 'verify_mechanism_references.py', 'run_baselines.py')},
        'limits': 'Six known task families; canonical wording only in this first pilot; no obligation checker or repair.'})
    b.save(out / 'private_specifications.json', chosen)
    if reused:
        for s in chosen:
            for arm in PROMPTS:
                stage = arm + '_' + s['id']
                old = reused / 'calls' / stage
                if not (old / 'request.json').exists() or not ((old / 'response.json').exists() or (old / 'transport_error.json').exists()):
                    raise ValueError('Source stage not terminal; no new dispatch allowed')
                target = out / 'calls' / stage
                target.mkdir(parents=True)
                for name in ('request.json', 'response.json', 'transport_error.json'):
                    if (old / name).exists():
                        shutil.copy2(old / name, target / name)
        b.save(out / 'prediction_reuse.json', {'source_run': str(reused), 'fresh_dispatches': 0,
            'rule': 'All planned predictions copied, not selected by correctness. Request equality checked by original Client.',
            'source_response_hashes': {str(p.relative_to(reused)): b.sha(p) for p in (reused / 'calls').glob('*/response.json')}})
    snapshots = out / 'source_snapshot'
    snapshots.mkdir()
    for name in ('run_mechanism_pilot.py', 'mechanism_core.py', 'verify_mechanism_references.py', 'run_baselines.py'):
        (snapshots / name).write_bytes(Path(__file__).with_name(name).read_bytes())
    client, records, consecutive, stopped = b.Client(cfg, out), [], 0, False
    for index, s in enumerate(chosen):
        arms = list(PROMPTS) if index % 2 == 0 else list(reversed(PROMPTS))
        for arm in arms:
            task = {k: s[k] for k in ('id', 'question', 'columns')}
            keys = {'id', 'status', 'cypher', 'parameters', 'reason'} if arm.startswith('A_') else {'id', 'status', 'operation', 'arguments', 'reason'}
            stage = arm + '_' + s['id']
            prediction, result, failure, exact_call = None, {'status': 'NOT_EXECUTED'}, None, None
            infrastructure = False
            try:
                prediction = client.call(stage, PROMPTS[arm], {'schema': b.SCHEMA,
                    'service_contracts': CATALOG, 'task': task}, [s['id']], keys, 6000)[0]
            except (RuntimeError, ValueError, KeyError, TypeError) as exc:
                folder = out / 'calls' / stage
                response, transport = folder / 'response.json', folder / 'transport_error.json'
                if not response.exists() and not transport.exists():
                    raise
                status = json.loads(response.read_text(encoding='utf-8'))['http_status'] if response.exists() else None
                infrastructure = transport.exists() or status == 429 or (status is not None and status >= 500)
                failure = 'INFRASTRUCTURE' if infrastructure else 'MODEL_RESPONSE_PROTOCOL'
                b.save(folder / 'failure.json', {'type': failure, 'exception_type': type(exc).__name__, 'retry': False})
            consecutive = consecutive + 1 if infrastructure else 0
            if prediction and prediction['status'] == 'ANSWER':
                if arm.startswith('A_'):
                    result = execute(prediction['cypher'], prediction['parameters'])
                else:
                    call = {'operation': prediction['operation'], 'arguments': prediction['arguments']}
                    exact_call = call == s['reference_call']
                    try:
                        compiled = compile_call(call)
                        result = execute(compiled['query'], compiled['parameters'])
                        result['compiled'] = compiled
                    except (ValueError, TypeError, KeyError):
                        result = {'status': 'INVALID_SERVICE_CALL'}
            answered = result['status'] == 'EXECUTED'
            correct = answered and bag(result['rows']) == bag(s['expected_rows'])
            record = {'id': s['id'], 'arm': arm, 'family': s['family'], 'prediction': prediction,
                'execution': result, 'failure': failure, 'answered': answered,
                'result_correct': correct, 'exact_reference_call_match': exact_call}
            records.append(record)
            b.save(out / 'records' / (stage + '.json'), record)
            print(f'{index+1}/{len(chosen)} {arm}: ' + ('RESULT_MATCH' if correct else (failure or result['status'])), flush=True)
            if consecutive >= 3:
                stopped = True
                break
        if stopped:
            break
    summaries = {}
    for arm in PROMPTS:
        r = [r for r in records if r['arm'] == arm]
        responses = [json.loads(p.read_text(encoding='utf-8')) for p in (out / 'calls').glob(arm + '_*/response.json')]
        summaries[arm] = {'planned_cases': len(chosen), 'attempted_cases': len(r),
            'not_run_cases': len(chosen) - len(r), 'result_correct': sum(x['result_correct'] for x in r),
            'answered': sum(x['answered'] for x in r), 'wrong_executed_results': sum(x['answered'] and not x['result_correct'] for x in r),
            'infrastructure_failures': sum(x['failure'] == 'INFRASTRUCTURE' for x in r),
            'response_protocol_failures': sum(x['failure'] == 'MODEL_RESPONSE_PROTOCOL' for x in r),
            'exact_reference_calls': sum(x['exact_reference_call_match'] is True for x in r) if arm.startswith('C_') else None,
            'known_reported_tokens': sum((x.get('body', {}).get('usage') or {}).get('total_tokens', 0) for x in responses),
            'missing_usage_responses': sum(not x.get('body', {}).get('usage') for x in responses),
            'unknown_usage_dispatches': len(r) - sum(bool(x.get('body', {}).get('usage')) for x in responses),
            'reported_dispatch_seconds': sum(x['seconds'] for x in responses)}
    summary = {'mode': 'E1_CANONICAL_DEVELOPMENT_NO_NEW_METHOD_ADVANTAGE_CLAIM',
        'status': 'STOPPED_INFRASTRUCTURE' if stopped else 'PLANNED_DISPATCHES_COMPLETED',
        'project': args.project, 'cases': len(chosen), 'arm_scores': summaries,
        'fresh_model_dispatches': 0 if reused else len(records),
        'reported_usage_origin': 'Original prediction responses; reused usage is not new billing' if reused else 'Fresh prediction responses',
        'interpretation': 'Result agreement on this snapshot is not full query semantic equivalence. C call match is a diagnostic unavailable for A, not a shared accuracy metric.'}
    b.save(out / 'summary.json', summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    for name in ('source', 'project', 'receipt', 'config', 'output'):
        p.add_argument('--' + name, required=True)
    p.add_argument('--port', type=int, choices=(17475, 17476), required=True)
    p.add_argument('--reuse-predictions')
    args = p.parse_args()
    b.DB_URL = f'http://127.0.0.1:{args.port}/db/neo4j/tx/commit'
    b.parse_array = parse_single_prediction
    try:
        run(args)
    finally:
        b.parse_array = ORIGINAL_PARSE
