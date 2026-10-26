"""Read-only latency/token diagnosis from sealed completed CypherBench paths.

No endpoint probing, model calls, method changes, re-scoring or dispatch. Keep
case/attempt joins local; publish aggregates only. Quantiles use linear R7.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
import time

from reconcile_t1_cost_evidence_20261026 import usage_object, evidence, identity, sha, read, write

ROOT = Path('D:/ch7')
REPO = ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON = ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
RUN = ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024'
OLD = ROOT/'reports/all_attempt_cost_reconciliation_v2_20261026'
OUT = ROOT/'reports/t1_latency_token_diagnosis_20261026'
FIELDS = ('prompt_tokens', 'completion_tokens', 'total_tokens',
          'completion_tokens_details.reasoning_tokens')


def valid(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def quantile(values, probability):
    rows = sorted(values)
    if not rows: return None
    at = (len(rows)-1)*probability
    lower, upper = math.floor(at), math.ceil(at)
    return rows[lower] + (rows[upper]-rows[lower])*(at-lower)


def stats(values, expected=None):
    rows = [v for v in values if valid(v)]
    return {'known_n': len(rows), 'unknown_n': (expected-len(rows)) if expected is not None else None,
            'sum': math.fsum(rows), 'mean': statistics.mean(rows) if rows else None,
            'p10': quantile(rows, .1), 'p50': quantile(rows, .5),
            'p90': quantile(rows, .9), 'p95': quantile(rows, .95),
            'p99': quantile(rows, .99), 'min': min(rows) if rows else None,
            'max': max(rows) if rows else None}


def sum_complete(values):
    return math.fsum(values) if values and all(valid(v) for v in values) else None


def content_chars(value):
    if type(value) is str: return len(value)
    if type(value) is list and all(type(v) is dict and v.get('type') == 'text'
                                  and type(v.get('text')) is str for v in value):
        return sum(len(v['text']) for v in value)
    return None


def aggregate(rows):
    names = ('network_seconds', 'retry_wait_seconds', 'network_plus_retry_seconds',
             'generation_wall_seconds', 'local_generation_residual_seconds',
             'model_and_execution_wall_seconds', 'execution_residual_seconds',
             'control_wall_seconds', 'prompt_tokens', 'completion_tokens', 'total_tokens',
             'reasoning_tokens', 'input_message_chars', 'visible_output_chars')
    result = {'paths': len(rows), 'final_status': dict(Counter(r['final_status'] for r in rows)),
              'logical_requests': sum(r['logical_requests'] for r in rows),
              'physical_attempts': sum(r['physical_attempts'] for r in rows),
              'feedback_paths': sum(r['logical_requests'] > 1 for r in rows),
              'retry_paths': sum(r['physical_attempts'] > r['logical_requests'] for r in rows)}
    for key in names: result[key] = stats([r[key] for r in rows], len(rows))
    result['known_token_subtotals_including_partially_known_paths'] = {
        key: sum(r['known_'+key] for r in rows)
        for key in ('prompt_tokens', 'completion_tokens', 'total_tokens', 'reasoning_tokens')}
    joint = [r for r in rows if valid(r['generation_wall_seconds']) and
             valid(r['model_and_execution_wall_seconds']) and valid(r['local_generation_residual_seconds'])]
    result['same_sample_clock_decomposition'] = {'n': len(joint),
        **{key: stats([r[key] for r in joint], len(joint)) for key in (
            'network_seconds', 'retry_wait_seconds', 'local_generation_residual_seconds',
            'execution_residual_seconds', 'model_and_execution_wall_seconds')}}
    return result


def main():
    assert not OUT.exists(), 'Keep prior diagnosis, never overwrite'
    assert quantile([0, 10], .9) == 9 and sum_complete([1, None]) is None
    assert not valid(True) and not valid(float('nan'))
    assert stats([1, None], 2)['unknown_n'] == 1
    sys.path.insert(0, str(CANON/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    budget = AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
        ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json', path_rewrites=[
            (r'C:\Users\10651\Documents\trae_projects\ch7_private', ROOT/'experiments/ch7_private')])
    balance = budget.inspect(); balance = {k: balance[k] for k in ('reserved','cap','remaining','pending')}
    assert not balance['pending']
    frozen = read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
    assert all(sha(CANON/k) == v for k, v in frozen['code_files'].items())
    runtime_dir = REPO/'experiment-harness/public_benchmark_v1'
    runtime_before = {str(p.relative_to(runtime_dir)): sha(p) for p in runtime_dir.rglob('*.py')}
    predictions = read(OLD/'prediction_hash_manifest.json')
    physical = {row[0]: row for row in read(OLD/'physical_hash_manifest.json') if row[4] == 'formal_core'}
    assert len(predictions) == 4696 and len(physical) == 4756
    OUT.mkdir()
    write(OUT/'protocol.json', {'created_local': datetime.now(timezone(timedelta(hours=8))).isoformat(),
        'script_sha256': sha(Path(__file__)), 'original_prediction_manifest_sha256': sha(OLD/'prediction_hash_manifest.json'),
        'original_physical_manifest_sha256': sha(OLD/'physical_hash_manifest.json'), 'budget': balance,
        'quantile': 'R7: linear interpolation of sorted values at (n-1)*p, not nearest-rank',
        'case': 'One (graph,qid,arm) final path, all outcomes retained, including refusals/errors.',
        'paired_instance': 'Same graph/qid, both Direct+feedback and Ours paths, no duplicate model bill for shared controls.',
        'network_seconds': 'Original client complete HTTP roundtrip seconds; includes connection, server queue/prefill/decode and response transfer. Excludes retry sleep and budget reservation.',
        'generation_wall_seconds': 'Original per-view complete() wall clock; includes reserve/IO/retry, not graph interpretation.',
        'path_wall': 'Original model_and_execution_wall_seconds; excludes shared lookup, full-detail control, scoring, graph import, phase downtime.',
        'effective_tokens_per_second': 'Completion usage / whole client HTTP seconds, not isolated backend decode throughput. No TTFT or streaming token timestamps available.',
        'tokens': 'Literal usage; missing attempts make full-case token total unknown. Known subtotals retained separately. Reasoning is inside completion, not added again.',
        'endpoint_global_concurrency': 'Unobserved; serial client code is evidence about our scheduling only.',
        'model_or_network_requests': 0, 'runtime_before': runtime_before})
    paths = []; attempts = []; joins = []; seen = set(); clock_anomalies = []
    for name, digest in predictions:
        file = RUN/name; assert sha(file) == digest
        record = read(file); local_attempts = []; generation = []
        for view in record['views']:
            generation.append(view.get('generation_wall_seconds'))
            for folder in view['reply']['attempt_folders']:
                request_path = budget._request(str(Path(folder)/'request.json')).resolve()
                terminal_path = request_path.with_name('terminal.json')
                terminal = read(terminal_path); number = terminal['budget_slot']
                assert number in physical and number not in seen
                seen.add(number); expected = physical[number]
                assert sha(request_path) == expected[2] and sha(terminal_path) == expected[3]
                request = read(request_path); assert request['model'] == 'deepseek-v4.1-flash-n24-131'
                usage = usage_object(terminal); tokens = {}
                for field in FIELDS:
                    state, value = evidence(usage, field)
                    assert state != 'invalid'
                    tokens[field.split('.')[-1]] = value
                message = terminal.get('body', {}).get('choices', [{}])[0].get('message', {})
                a = {'slot': number, 'graph': record['graph'], 'arm': record['arm'], 'qid': record['qid'],
                     'step': view['step'], 'http_status': terminal.get('http_status'),
                     'finish_reason': terminal.get('body', {}).get('choices', [{}])[0].get('finish_reason'),
                     'network_seconds': terminal.get('seconds'),
                     'retry_wait_seconds': terminal.get('retry_wait_seconds'), **tokens,
                     'input_message_chars': sum(len(m['content']) for m in request['messages']),
                     'visible_output_chars': content_chars(message.get('content')),
                     'requested_max_tokens': request['max_tokens'],
                     'effective_completion_tps': tokens['completion_tokens']/terminal['seconds']
                         if valid(tokens['completion_tokens']) and valid(terminal.get('seconds')) and terminal['seconds'] > 0 else None}
                assert valid(a['network_seconds']) and valid(a['retry_wait_seconds'])
                attempts.append(a); local_attempts.append(a)
                joins.append({'slot': number, 'request_sha256': expected[2], 'terminal_sha256': expected[3],
                              'record_path': name, 'record_sha256': digest, 'step': view['step']})
        network = sum_complete([a['network_seconds'] for a in local_attempts])
        waits = sum_complete([a['retry_wait_seconds'] for a in local_attempts])
        gen = sum_complete(generation); wall = record.get('model_and_execution_wall_seconds')
        local = gen-network-waits if valid(gen) else None
        execute = wall-gen if valid(wall) and valid(gen) else None
        for key, value in (('local', local), ('execute', execute)):
            if value is not None and value < -0.01:
                clock_anomalies.append({'record': name, 'field': key, 'value': value})
                if key == 'local': local = None
                else: execute = None
            elif value is not None and value < 0:
                if key == 'local': local = 0
                else: execute = 0
        row = {'graph': record['graph'], 'qid': record['qid'], 'arm': record['arm'],
               'final_status': record['views'][-1]['outcome']['status'],
               'logical_requests': len(record['views']), 'physical_attempts': len(local_attempts),
               'network_seconds': network, 'retry_wait_seconds': waits,
               'network_plus_retry_seconds': network+waits,
               'generation_wall_seconds': gen, 'local_generation_residual_seconds': local,
               'model_and_execution_wall_seconds': wall, 'execution_residual_seconds': execute,
               'control_wall_seconds': record.get('fixed_plan_full_detail_wall_seconds')}
        for key in ('prompt_tokens', 'completion_tokens', 'total_tokens', 'reasoning_tokens',
                    'input_message_chars', 'visible_output_chars'):
            row[key] = sum_complete([a[key] for a in local_attempts])
            if key.endswith('tokens'): row['known_'+key] = sum(a[key] for a in local_attempts if valid(a[key]))
        paths.append(row)
    assert len(paths) == 4696 and len(seen) == len(attempts) == 4756
    # Cross-check against the independently sealed previous aggregate, not just new calculations.
    old = read(OLD/'summary.json')
    by_arm = {arm: aggregate([r for r in paths if r['arm'] == arm]) for arm in ('direct','ours')}
    assert all(x['paths'] == 2348 for x in by_arm.values())
    for arm, values in by_arm.items():
        for field in ('prompt_tokens', 'completion_tokens', 'total_tokens'):
            assert values['known_token_subtotals_including_partially_known_paths'][field] == old['formal_by_arm'][arm]['tokens'][field]['known_sum']
        assert math.isclose(values['network_seconds']['sum'],old['formal_by_arm'][arm]['clock_fields']['seconds']['known_seconds_sum'], abs_tol=.00001)
    grouped = defaultdict(dict)
    for row in paths: grouped[(row['graph'], row['qid'])][row['arm']] = row
    paired = []
    for (graph, qid), arms in grouped.items():
        assert set(arms) == {'direct','ours'}
        paired.append({'graph': graph, 'qid': qid, **{
            field: sum_complete([row[field] for row in arms.values()]) for field in (
                'network_seconds','generation_wall_seconds','model_and_execution_wall_seconds',
                'prompt_tokens','completion_tokens','total_tokens','reasoning_tokens')}})
    assert len(paired) == 2348
    physical_stats = {}
    for arm in ('direct', 'ours'):
        rows = [a for a in attempts if a['arm'] == arm]
        success = [a for a in rows if a['http_status'] == 200]
        speed_rows = [a for a in success if valid(a['effective_completion_tps'])]
        physical_stats[arm] = {'attempts': len(rows), 'HTTP200': len(success),
            'finish_reasons': dict(Counter(str(a['finish_reason']) for a in rows)),
            'all_attempt_latency_seconds': stats([a['network_seconds'] for a in rows], len(rows)),
            'HTTP200_latency_seconds': stats([a['network_seconds'] for a in success], len(success)),
            'effective_completion_tps_per_HTTP200': stats([a['effective_completion_tps'] for a in speed_rows], len(success)),
            'aggregate_effective_completion_tps_HTTP200': sum(a['completion_tokens'] for a in speed_rows)/sum(a['network_seconds'] for a in speed_rows),
            'HTTP200_reasoning_fraction_of_completion': sum(a['reasoning_tokens'] for a in speed_rows)/sum(a['completion_tokens'] for a in speed_rows),
            'reasoning_excluded_visible_token_estimate': stats([a['completion_tokens']-a['reasoning_tokens'] for a in speed_rows],len(success)),
            'HTTP200_visible_output_chars': stats([a['visible_output_chars'] for a in success],len(success)),
            'feedback_attempts': sum(a['step'] > 0 for a in rows),
            'length_finish_attempts': sum(a['finish_reason'] == 'length' for a in rows),
            'known_output_ge_8000_attempts': sum(valid(a['completion_tokens']) and a['completion_tokens'] >= 8000 for a in rows)}
    benchmark = []
    for _ in range(3):
        start = time.perf_counter(); current = budget.inspect(); elapsed = time.perf_counter()-start
        assert {k: current[k] for k in balance} == balance
        benchmark.append(elapsed)
    code = CANON/'experiment-harness/public_benchmark_v1/model_transport.py'
    runner = CANON/'experiment-harness/public_benchmark_v1/run_frozen_main.py'
    policies = list(RUN.glob('*/segments/*/model_calls/transport_policy.json'))
    assert len(policies) == 15
    serial_evidence = {'frozen_transport_sha256': sha(code), 'frozen_runner_sha256': sha(runner),
        'segment_policy_files': len(policies),
        'model_names': sorted({read(p)['model'] for p in policies}),
        'client_implementation': 'Synchronous HTTP request/response inside serial per-task per-arm loop. Budget reserve blocks if ANY unterminalized attempt exists. No client worker pool.',
        'global_endpoint_load_or_concurrency': None,
        'literal_transport_parallelism_policy_values': dict(Counter(str(read(p).get('transport_parallelism','absent_legacy_field')) for p in policies))}
    assert all(sha(CANON/k) == v for k, v in frozen['code_files'].items())
    assert runtime_before == {str(p.relative_to(runtime_dir)):sha(p) for p in runtime_dir.rglob('*.py')}
    summary = {'kind': 'COMPLETED_FORMAL_CASE_LATENCY_TOKEN_DIAGNOSIS_NOT_NEW_EXPERIMENT',
        'created_local': datetime.now(timezone(timedelta(hours=8))).isoformat(),
        'model': 'deepseek-v4.1-flash-n24-131', 'unique_questions': 2348,
        'terminated_method_paths': 4696, 'formal_physical_attempts': 4756,
        'case_definition': 'Each method path includes its initial request, any feedback, all physical retries; terminal failure/refusal retained, not correctness-filtered.',
        'by_arm': by_arm, 'by_graph_arm': {graph+':'+arm: aggregate([r for r in paths if r['graph']==graph and r['arm']==arm])
            for graph in sorted({r['graph'] for r in paths}) for arm in ('direct','ours')},
        'complete_answer_subset_by_arm': {arm: aggregate([r for r in paths if r['arm']==arm and r['final_status']=='EXECUTED']) for arm in ('direct','ours')},
        'paired_both_methods_per_question': {field:stats([r[field] for r in paired],len(paired)) for field in (
            'network_seconds','generation_wall_seconds','model_and_execution_wall_seconds','prompt_tokens','completion_tokens','total_tokens','reasoning_tokens')},
        'physical_attempts_by_arm': physical_stats, 'serial_client_evidence':serial_evidence,
        'current_readonly_ledger_inspect_microbenchmark_seconds':stats(benchmark,3),
        'microbenchmark_limits': 'Current local filesystem at5195 slots, 3 cached/warm reads. Not a historical runtime estimate or reserve() benchmark; no ledger mutation.',
        'clock_anomalies_n': len(clock_anomalies), 'budget':balance,
        'sealed_originals_checked': {'prediction_records':4696,'formal_request_terminal_pairs':4756},
        'new_model_calls':0,'new_DB_calls':0,'model_endpoint_or_SSH_requests':0,
        'entire_T1_complete':False,
        'limits':['No TTFT, streaming decode times, backend GPU/queue/concurrency or cache hit instrumentation.',
                  'HTTP200 is response completion, not EX correctness; EXECUTED subset separately reported.',
                  'Unknown clocks/usages excluded from their distribution only, with denominator retained and unknown counts.',
                  'Reasoning_tokens not added again to completion or total. Token/second ratios include prefill/queue/network.',
                  'Per-path wall excludes shared lookup/full-detail controls/scoring/import/outages; not a complete T4.',
                  'Current Bailing has only one tiny resource probe; formal DeepSeek speed cannot be assigned to Bailing.']}
    write(OUT/'case_rows.json', paths); write(OUT/'physical_rows.json', attempts)
    write(OUT/'paired_rows.json', paired); write(OUT/'source_join_manifest.json', joins)
    write(OUT/'clock_anomalies.json', clock_anomalies); write(OUT/'summary.json', summary)
    print(json.dumps({'by_arm':by_arm,'physical':physical_stats,
                     'paired':summary['paired_both_methods_per_question'],
                     'ledger_microbenchmark':summary['current_readonly_ledger_inspect_microbenchmark_seconds']},ensure_ascii=True),flush=True)


if __name__ == '__main__': main()
