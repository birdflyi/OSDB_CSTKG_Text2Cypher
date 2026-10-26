"""Read-only extension of sealed5192-attempt evidence, no dispatch/replay.

All old slot/request/terminal and prediction hashes must equal the prior audit.
Only nonsecret fields are aggregated; no error bodies, prompts or credentials
are emitted. Runtime sources and original accounting artifacts remain intact.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT=Path('D:/ch7')
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
OUT=ROOT/'reports/all_attempt_cost_reconciliation_v2_20261026'
FIELDS=('prompt_tokens','completion_tokens','total_tokens',
    'completion_tokens_details.reasoning_tokens','prompt_tokens_details.cached_tokens',
    'prompt_cache_hit_tokens','prompt_cache_miss_tokens','cache_read_input_tokens')


def read(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def seal(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def identity(path):return str(path.resolve()).casefold()
def write(path,value):
    with path.open('x',encoding='utf-8',newline='\n') as f:
        json.dump(value,f,ensure_ascii=False,indent=2);f.write('\n')
def now():return datetime.now(timezone(timedelta(hours=8))).isoformat()


def usage_object(terminal):
    body=terminal.get('body')
    nested=body.get('usage') if isinstance(body,dict) else None
    direct=terminal.get('usage')
    if nested is not None and direct is not None and nested!=direct:
        raise ValueError('Conflicting usage schemas; audit before aggregating')
    value=nested if nested is not None else direct
    if value is not None and type(value) is not dict:
        raise ValueError('Malformed usage object')
    return value


def evidence(usage,field):
    value=usage
    for part in field.split('.'):
        if not isinstance(value,dict) or part not in value:return 'unknown',None
        value=value[part]
    if value is None:return 'unknown',None
    if type(value) is int and value>=0:return 'known',value
    return 'invalid',None


def new_bucket():
    return {'attempts':0,'usage_object_present':0,'usage_object_unknown':0,
        'all_three_standard_fields_known':0,'http_status':Counter(),'transport_error_classes':Counter(),
        'tokens':{f:{'known_attempts':0,'unknown_attempts':0,'invalid_attempts':0,'known_sum':0} for f in FIELDS},
        'clock_fields':{f:{'known_attempts':0,'unknown_attempts':0,'known_seconds_sum':0.0}
            for f in ('seconds','elapsed_seconds','retry_wait_seconds')}}


def add(bucket,terminal):
    bucket['attempts']+=1
    bucket['http_status'][str(terminal.get('http_status'))]+=1
    if terminal.get('transport_error'):bucket['transport_error_classes'][terminal['transport_error']]+=1
    usage=usage_object(terminal)
    bucket['usage_object_present' if usage is not None else 'usage_object_unknown']+=1
    if all(evidence(usage,f)[0]=='known' for f in FIELDS[:3]):bucket['all_three_standard_fields_known']+=1
    for field in FIELDS:
        state,value=evidence(usage,field);group=bucket['tokens'][field]
        group[state+'_attempts']+=1
        if state=='known':group['known_sum']+=value
    for field,group in bucket['clock_fields'].items():
        value=terminal.get(field)
        if type(value) in (int,float) and math.isfinite(value) and value>=0:
            group['known_attempts']+=1;group['known_seconds_sum']+=value
        else:group['unknown_attempts']+=1


def main():
    assert not OUT.exists(),'One-shot audit output exists; preserve, do not overwrite.'
    OUT.mkdir()
    baseline_path=ROOT/'reports/t1_all_attempt_ledger_reconciliation_20261025.json'
    manifest_path=ROOT/'reports/t1_all_attempt_hash_manifest_20261025.json'
    baseline=read(baseline_path);old_manifest=read(manifest_path)
    assert baseline['cumulative_reserved']==len(old_manifest)==5192
    assert seal(old_manifest)==baseline['physical_evidence_seal']
    assert baseline['all_formal_attempts_attributed_once'] and baseline['formal_physical_attempts']==4756
    source_sha={Path(__file__).name:sha(Path(__file__)),
        'test_t1_cost_evidence.py':sha(Path(__file__).with_name('test_t1_cost_evidence.py'))}
    runtime=REPO/'experiment-harness/public_benchmark_v1'
    runtime_before={str(p.relative_to(runtime)):sha(p) for p in runtime.rglob('*.py')}
    freeze_path=ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
    freeze=read(freeze_path);assert sha(freeze_path)==baseline['freeze_sha256']
    assert freeze['budget']['historical_attempts_before_formal']==434
    for name,digest in freeze['code_files'].items():assert sha(CANON/name)==digest
    sys.path.insert(0,str(CANON/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    ledger=ROOT/'experiments/ch7_private/continuation_20261021/budget'
    budget=AttemptBudget(ledger,ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',path_rewrites=[
        (r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])
    balance=budget.inspect();assert balance['reserved']==5194 and not balance['pending']
    write(OUT/'protocol.json',{'kind':'SEALED_BASELINE_EXTENSION_AND_COST_COMPLETENESS_NOT_NEW_EXPERIMENT',
        'sealed_local':now(),'source_sha256':source_sha,'baseline_summary_sha256':sha(baseline_path),
        'baseline_manifest_sha256':sha(manifest_path),'expected_counts':{'baseline':5192,'new_probes':2,'all':5194},
        'old_scope_and_retry_checks':'Reuse only if all old slot/request/terminal and prediction record hashes exactly match.',
        'new_scope':'Only exact local generation_probe_01 and generation_probe_02_literal requests, slots5193/5194.',
        'usage':'Read each literal field; bool/negative/noninteger invalid, missing/null unknown; no imputation.',
        'optional_tokens':'Keep reasoning/cache fields separate; never add them to completion or total.',
        'clock':'Keep old seconds, new remote HTTP elapsed_seconds and retry waits separate. No invented E2E total.',
        'outcome_selection':False,'new_model_or_DB_or_network_requests':0})
    (OUT/'baseline_summary.json').write_bytes(baseline_path.read_bytes())
    (OUT/'baseline_manifest.json').write_bytes(manifest_path.read_bytes())
    old={row[0]:row for row in old_manifest};all_manifest=[];prefix=[]
    total=new_bucket();scopes=defaultdict(new_bucket);terminals_by_request={};slot_by_request={}
    expected_new={5193:ROOT/'reports/node111_endpoint_probe_20261026/generation_probe_01/request.json',
        5194:ROOT/'reports/node111_endpoint_probe_20261026/generation_probe_02_literal/request.json'}
    for slot in sorted(ledger.glob('*.json'),key=lambda p:int(p.stem)):
        number=int(slot.stem);reservation=read(slot);request=budget._request(reservation['request_path']).resolve()
        terminal_path=budget.terminal_path(request);terminal=read(terminal_path)
        if number<=5192:
            scope=old[number][4];row=[number,sha(slot),sha(request),sha(terminal_path),scope]
            assert row==old[number],'Old physical evidence changed; do not inherit prior audit'
            prefix.append(row)
        else:
            assert request==expected_new[number].resolve() and terminal['budget_slot']==number
            payload=read(request);assert payload['body']['model']=='deepseek-v4.1-flash-n24-112'
            assert payload['physical_attempt']==1 and payload['automatic_retry'] is False
            assert terminal['http_status']==401 and terminal['physical_POST_attempts']==1
            assert terminal['formal_benchmark_prediction'] is False and terminal['automatic_retry'] is False
            scope='user_authorized_retired_service_probe'
            row=[number,sha(slot),sha(request),sha(terminal_path),scope]
        assert identity(request) not in terminals_by_request
        terminals_by_request[identity(request)]=terminal;slot_by_request[identity(request)]=number
        add(total,terminal);add(scopes[scope],terminal);all_manifest.append(row)
    assert seal(prefix)==baseline['physical_evidence_seal']
    for scope,stats in baseline['scopes'].items():
        actual=scopes[scope]
        assert actual['attempts']==stats['attempts'] and actual['usage_object_present']==stats['usage_known']
        assert actual['usage_object_unknown']==stats['usage_unknown']
        assert dict(actual['http_status'])==stats['http_status']
        for field,value in stats['known_tokens'].items():assert actual['tokens'][field]['known_sum']==value
    run=ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024'
    records=[];attributed=Counter();arm=defaultdict(new_bucket);graph_arm=defaultdict(new_bucket);logical=Counter()
    for path in sorted(run.glob('*/tasks/*/*/record.json')):
        record=read(path);assert record['freeze_sha256']==sha(freeze_path)
        records.append([str(path.relative_to(run)),sha(path)])
        for view in record['views']:
            logical[record['arm']+':'+('feedback' if view['step'] else 'initial')]+=1
            for folder in view['reply']['attempt_folders']:
                request_key=identity(Path(folder)/'request.json')
                attributed[request_key]+=1
                terminal=terminals_by_request[request_key]
                add(arm[record['arm']],terminal);add(graph_arm[record['graph']+':'+record['arm']],terminal)
    assert len(records)==4696 and seal(records)==baseline['prediction_evidence_seal']
    formal_requests={identity(p.parent/'request.json') for p in run.glob('*/segments/*/model_calls/physical_*/terminal.json')}
    assert set(attributed)==formal_requests and len(formal_requests)==4756 and all(v==1 for v in attributed.values())
    assert sum(logical.values())==4750 and sum(v for k,v in logical.items() if k.endswith(':feedback'))==54
    for name,values in baseline['per_graph_arm_physical'].items():assert graph_arm[name]['attempts']==values['physical_attempts']
    # Optional field aggregates are literal observed schemas; their subcomponents
    # may overlap. No model/backbone or provider comparability is inferred.
    assert total['attempts']==5194 and total['usage_object_unknown']==101
    assert sum(b['attempts'] for b in scopes.values())==5194
    assert all(v['known_attempts']+v['unknown_attempts']+v['invalid_attempts']==5194 for v in total['tokens'].values())
    assert sha(baseline_path)==sha(OUT/'baseline_summary.json') and sha(manifest_path)==sha(OUT/'baseline_manifest.json')
    for number,a,b,c,scope in all_manifest:
        slot=ledger/f'{number:04d}.json';reservation=read(slot);request=budget._request(reservation['request_path']).resolve()
        assert sha(slot)==a and sha(request)==b and sha(budget.terminal_path(request))==c
    assert balance==budget.inspect()
    for name,digest in freeze['code_files'].items():assert sha(CANON/name)==digest
    assert runtime_before=={str(p.relative_to(runtime)):sha(p) for p in runtime.rglob('*.py')}
    detail={'total':total,'scopes':dict(scopes),'formal_by_arm':dict(arm),'formal_by_graph_arm':dict(graph_arm),
        'logical_receipts_by_arm':dict(logical)}
    write(OUT/'aggregate_cost_fields.json',detail);write(OUT/'physical_hash_manifest.json',all_manifest)
    write(OUT/'prediction_hash_manifest.json',records)
    summary={'kind':'ALL_ATTEMPT_COST_COMPLETENESS_EXTENSION_NOT_FULL_T4','time_local':now(),
        'actual_cumulative_attempts':5194,'approved_cap':balance['cap'],'remaining_original_CB_scope':balance['remaining'],
        'pending':balance['pending'],'scope_counts':{k:v['attempts'] for k,v in scopes.items()},
        'usage_objects_known':total['usage_object_present'],'usage_objects_unknown':total['usage_object_unknown'],
        'all_three_standard_fields_known':total['all_three_standard_fields_known'],
        'tokens':total['tokens'],'http_status_counts':dict(total['http_status']),
        'formal_physical_attempts':4756,'formal_primary_paths':4696,'formal_logical_receipts':4750,
        'formal_feedback_receipts':54,'formal_extra_physical_retries':6,
        'formal_by_arm':dict(arm),'logical_receipts_by_arm':dict(logical),
        'old5192_physical_evidence_exactly_unchanged':True,'old4696_prediction_records_exactly_unchanged':True,
        'old_formal_freeze_retry_and_feedback_checks_reused_by_exact_evidence_identity':True,
        'all4756_formal_attempts_attributed_once_again':True,'all_raw_hashes_verified_again_at_end':True,
        'new_probe_attempts':2,'new_probe_http_statuses':[401,401],'new_probe_responses_model_successes':0,
        'new_probe_usage_unknown':2,'new_probe_remote_HTTP_seconds_sum':scopes['user_authorized_retired_service_probe']['clock_fields']['elapsed_seconds']['known_seconds_sum'],
        'new_probe_E2E_or_SSH_clock_known':False,'money_cost':None,'unknown_usage_not_filled_zero':True,
        'optional_reasoning_cache_tokens_not_added_to_total':True,'formal_vs_mixed_history_kept_separate':True,
        'development_unit_checks_passed':6,'canonical_frozen_files_unchanged':len(freeze['code_files']),
        'method_runtime_files_unchanged':len(runtime_before),'new_model_calls':0,'new_database_queries':0,'network_requests':0,
        'formal_metric_or_method_changed':False,'entire_T1_complete':False,'complete_T4':False,
        'baseline_summary_sha256':sha(baseline_path),'baseline_manifest_sha256':sha(manifest_path),
        'protocol_sha256':sha(OUT/'protocol.json'),'all_physical_evidence_seal':seal(all_manifest),
        'source_sha256':source_sha,
        'limits':['Hash-identical prior formal evidence reuses prior conformance checks, not a new model replication.',
            'History mixes developer models; never attribute the global total to a formal arm.',
            'Unknown usage and prices remain unknown; optional token details can overlap standard counts.',
            'Remote HTTP elapsed, model-client seconds and retry waits are different clock boundaries.',
            'No complete DB/candidate/compile/transport/outage/scale end-to-end T4 is established.',
            'Retired10.244.171.88 service not contacted; no new node/SSH/model/DB operation.']}
    write(OUT/'summary.json',summary);write(ROOT/'reports/t1_all_attempt_cost_completeness_20261026.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ('tokens','formal_by_arm','limits','source_sha256')}))


if __name__=='__main__':main()
