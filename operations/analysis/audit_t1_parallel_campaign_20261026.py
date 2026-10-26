"""Post-run aggregate evidence audit; no model, node, service or query calls.

Historical5195 evidence and4696 formal records remain immutable. New capacity
attempts are attributed exactly once to local authority leases; retained stage
metrics are not double charged. Run only after the active coordinator closes.
"""
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
import hashlib
import json
from pathlib import Path
import sys

from reconcile_t1_cost_evidence_20261026 import read,sha,seal,new_bucket,add,write

ROOT=Path('D:/ch7')
REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OUT=ROOT/'reports/t1_parallel_capacity_integrity_20261026'
RUNS={
    'direct_v1':ROOT/'reports/t1_parallel_capacity_and_monitor_20261026',
    'forwarded_v2':ROOT/'reports/t1_parallel_capacity_sshroute_v2_20261026',
    'forwarded_v3':ROOT/'reports/t1_parallel_capacity_sshroute_v3_20261026',
    'forwarded_v4':ROOT/'reports/t1_parallel_capacity_sshroute_v4_20261026',
    'forwarded_v5':ROOT/'reports/t1_parallel_capacity_sshroute_v5_20261026',
    'forwarded_v6':ROOT/'reports/t1_parallel_capacity_sshroute_v6_20261026'}


def main():
    assert not OUT.exists(),'Preserve prior audit'
    assert all((r/'budget_lease/lease_close.json').exists() for r in RUNS.values())
    sys.path.insert(0,str(CANON/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    budget=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
        ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',path_rewrites=[
            (r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])
    before=budget.inspect();assert not before['pending']
    old=read(ROOT/'reports/bailing_probe_ledger_delta_20261026/physical_hash_manifest.json')
    old_summary=read(ROOT/'reports/bailing_probe_ledger_delta_20261026/summary.json')
    assert len(old)==5195 and seal(old)==old_summary['physical_evidence_seal']
    freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
    assert all(sha(CANON/name)==digest for name,digest in freeze['code_files'].items())
    OUT.mkdir();write(OUT/'protocol.json',{'created_local':datetime.now(timezone(timedelta(hours=8))).isoformat(),
        'script_sha256':sha(Path(__file__)),'old5195_manifest_seal':seal(old),
        'budget_before':{k:before[k] for k in ('reserved','cap','remaining','pending')},
        'capacity_versions':{k:str(v) for k,v in RUNS.items()},'new_network_or_model_calls':0})
    aggregate=new_bucket();versions=defaultdict(new_bucket);manifest=[];new_rows=[]
    for number in range(1,before['reserved']+1):
        slot=budget.ledger/f'{number:04d}.json';record=read(slot)
        request=budget._request(record['request_path']).resolve();terminal=budget.terminal_path(request)
        if number<=5195:
            row=[number,sha(slot),sha(request),sha(terminal),old[number-1][4]]
            assert row==old[number-1],'Historical evidence changed'
        else:
            names=[key for key,root in RUNS.items() if root.resolve() in request.parents]
            assert len(names)==1,'Unattributed new attempt'
            name=names[0];body=read(request);t=read(terminal)
            assert record['request_sha256']==sha(request)
            assert body['model']=='ling-3.0-flash-n72-75'
            assert body['max_tokens']==8192 and body['temperature']==.2 and body['top_p']==.95
            assert body['reasoning_effort']=='high' and len(body['messages'])==2
            assert t['budget_slot']==number and t['physical_attempt_in_request']==1
            assert t['retry_wait_seconds']==0 and t['formal_benchmark_prediction'] is False
            assert len(request.parent.name)>0
            add(aggregate,t);add(versions[name],t)
            row=[number,sha(slot),sha(request),sha(terminal),'TRAIN_capacity_'+name]
            new_rows.append({'slot':number,'version':name,'trial_id':request.parent.name,
                'http_status':t.get('http_status'),'transport_error':t.get('transport_error'),
                'seconds':t.get('seconds'),'last_network_phase':t.get('last_network_phase'),
                'request_write_completed':t.get('request_write_completed'),
                'request_sha256':sha(request),'terminal_sha256':sha(terminal)})
        manifest.append(row)
    assert len(new_rows)==before['reserved']-5195
    summaries={}
    for name,folder in RUNS.items():
        summary=read(folder/'summary.json');protocol=read(folder/'protocol.json')
        assert summary['dispatched']==versions[name]['attempts']
        assert summary['formal_predictions']==0
        # Snapshot the exact executed operations version, including old failures.
        source=folder/'executed_sources'
        if not source.exists():
            source.mkdir()
            for filename,digest in protocol['runtime_sources_sha256'].items():
                file=REPO/'operations/t1_runtime'/filename
                assert sha(file)==digest,'Executed source missing; do not silently use a later file'
                (source/filename).write_bytes(file.read_bytes())
        assert all(sha(source/filename)==digest for filename,digest in protocol['runtime_sources_sha256'].items())
        summaries[name]={'attempts':summary['dispatched'],'stop_reason':summary['stop_reason'],
                         'summary_sha256':sha(folder/'summary.json'),'protocol_sha256':sha(folder/'protocol.json')}
    predictions=read(ROOT/'reports/all_attempt_cost_reconciliation_v2_20261026/prediction_hash_manifest.json')
    run=ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024'
    assert len(predictions)==4696 and all(sha(run/name)==digest for name,digest in predictions)
    runtime=read(ROOT/'reports/osdb_common_input_dev_v1_20261026/protocol.json')['runtime_before']
    assert all(sha(REPO/'experiment-harness/public_benchmark_v1'/name)==digest for name,digest in runtime.items())
    after=budget.inspect();assert after==before
    current=read(RUNS['forwarded_v6']/'summary.json')
    c64=read(RUNS['forwarded_v4']/'c64_result.json')
    assert c64['actual_peak_requests']==64 and c64['HTTP200_responses']==c64['completed']==363
    assert c64['steady_sample_sufficient'] and c64['complete_responses']/c64['completed']>=.95
    route_errors={}
    for version in ('forwarded_v5','forwarded_v6'):
        lines=(RUNS[version]/'worker_stderr.log').read_text(encoding='utf-8').splitlines()
        route_errors[version]=sum('connect to 117.50.195.92 port 8080 failed' in line for line in lines)
    summary={'kind':'PARALLEL_TRAIN_CAPACITY_AND_CHINESE_MONITOR_DELIVERY_NOT_FORMAL_T1',
        'completed_local':datetime.now(timezone(timedelta(hours=8))).isoformat(),
        'budget':{k:after[k] for k in ('reserved','cap','remaining','pending')},
        'new_capacity_physical_attempts':len(new_rows),'capacity_versions':summaries,
        'new_attempt_cost_fields':aggregate,'new_cost_by_version':dict(versions),
        'known_global_standard_token_subtotal':{k:old_summary['global_known_token_subtotal'][k]+
            aggregate['tokens'][k]['known_sum'] for k in ('prompt_tokens','completion_tokens','total_tokens')},
        'global_unknown_usage':old_summary['global_usage_unknown']+aggregate['usage_object_unknown'],
        'recommended_concurrency':current['recommended_concurrency'],'capacity_stages':current['stages'],
        'provisional_operational_cap':64,
        'provisional_cap_reason':'64 had363 HTTP200 and a sufficient steady window. Its archived fatal flag came from the client idle-guard after the dispatch deadline, not an endpoint failure. Original automatic recommendation32 and all raw flags remain unchanged;64 is an audited conservative cap, not a certified saturation point.',
        'ssh_gateway_connect_errors':route_errors,
        'first128_confirmation_required':read(RUNS['forwarded_v5']/'c128_result.json'),
        'old5195_physical_evidence_unchanged':True,'old4696_formal_records_unchanged':True,
        'canonical32_and_adapter46_unchanged':True,'new_manifest_seal':seal(manifest),
        'retained_stages_charged_once':True,'monetary_cost':None,'new_formal_predictions':0,
        'entire_T1_complete':False,'complete_T4':False,
        'limits':['TRAIN-only fixed pack; response completeness is not EX or accuracy.',
                  '3500output pacing is based on a user resource estimate, not server telemetry or guaranteed instant QoS.',
                  'Remote worker uses SSH reverse forwarding through a local reachable model gateway and local budget authority.',
                  'Version1 request-write stage was not instrumented; do not assert its POSTs reached the model.',
                  'All version1/2/3/4 failures and control corrections retained; no old batch overwritten.',
                  'Version5 completed499 of768 planned128-cap trials, below the prespecified512-sample steady window threshold. Version6 is a separate512-trial bounded confirmation, not replacement or retry.',
                  'Version6 stopped on consecutive infrastructure failures; no further model probes or automatic restart. SSH logs show gateway connection failures, not proof that model GPU throughput is saturated.',
                  'Central-window throughput is supplemented by complete finite-batch counts/times/errors and literal usage.']}
    write(OUT/'physical_hash_manifest.json',manifest);write(OUT/'new_attempt_metadata.json',new_rows)
    write(OUT/'summary.json',summary)
    write(ROOT/'reports/t1_parallel_capacity_and_monitor_delivery_20261026.json',summary)
    print(json.dumps({'new_attempts':len(new_rows),'budget':summary['budget'],
                      'recommended_concurrency':summary['recommended_concurrency']},ensure_ascii=True))


if __name__=='__main__':main()
