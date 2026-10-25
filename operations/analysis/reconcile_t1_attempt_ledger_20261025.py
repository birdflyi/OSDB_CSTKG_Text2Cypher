"""Read-only physical-attempt reconciliation; no network, model or DB calls."""
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path('D:/ch7')
CODE=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
sys.path.insert(0,str(CODE/'experiment-harness/public_benchmark_v1'))
from attempt_budget import AttemptBudget


def read(path):return json.loads(path.read_text(encoding='utf-8'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def key(path):return str(path.resolve()).casefold()
def seal(values):
    return hashlib.sha256(json.dumps(values,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def reconcile():
    run=ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024'
    ledger=ROOT/'experiments/ch7_private/continuation_20261021/budget'
    approval=ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json'
    freeze_path=ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
    freeze=read(freeze_path);freeze_sha=sha(freeze_path)
    budget=AttemptBudget(ledger,approval,path_rewrites=[
        (r'C:\Users\10651\Documents\trae_projects\ch7_private',str(ROOT/'experiments/ch7_private'))])
    before=budget.inspect()
    assert not before['pending'],'Pending attempt: stop reconciliation, never resend'
    for name,expected in freeze['code_files'].items():assert sha(CODE/name)==expected
    formal={key(p.parent/'request.json'):p for p in run.glob('*/segments/*/model_calls/physical_*/terminal.json')}
    scopes=defaultdict(lambda:{'attempts':0,'usage_known':0,'usage_unknown':0,
        'http_status':Counter(),'transport_errors':Counter(),'known_tokens':Counter(),
        'attempt_seconds_known':0.0,'attempt_seconds_unknown':0,
        'retry_wait_seconds_known':0.0,'retry_wait_seconds_unknown':0})
    manifest=[];by_path={};unclassified=[]
    for slot in sorted(ledger.glob('*.json'),key=lambda p:int(p.stem)):
        number=int(slot.stem);reservation=read(slot)
        request=budget._request(reservation['request_path']).resolve()
        terminal_path=budget.terminal_path(request)
        terminal=read(terminal_path)
        if number<=freeze['budget']['historical_attempts_before_formal']:
            scope='historical_pre_freeze'
        elif key(request) in formal:
            scope='formal_core'
            assert terminal.get('budget_slot')==number
        elif 'endpoint_recovery_probe_' in str(request):
            scope='post_freeze_connectivity_probe'
            assert terminal.get('budget_slot')==number
        else:
            scope='UNCLASSIFIED';unclassified.append(number)
        group=scopes[scope];group['attempts']+=1
        group['http_status'][str(terminal.get('http_status'))]+=1
        if terminal.get('transport_error'):group['transport_errors'][terminal['transport_error']]+=1
        usage=(terminal.get('body') or {}).get('usage')
        if isinstance(usage,dict):
            group['usage_known']+=1
            for field in ('prompt_tokens','completion_tokens','total_tokens'):
                if type(usage.get(field)) is int:group['known_tokens'][field]+=usage[field]
        else:group['usage_unknown']+=1
        for field,label in [('seconds','attempt_seconds'),('retry_wait_seconds','retry_wait_seconds')]:
            if isinstance(terminal.get(field),(int,float)):
                group[label+'_known']+=terminal[field]
            else:group[label+'_unknown']+=1
        by_path[key(request)]={'number':number,'terminal':terminal,'request':request,
                              'scope':scope,'reservation':reservation}
        manifest.append([number,sha(slot),sha(request),sha(terminal_path),scope])
    assert not unclassified,'Unclassified new attempt scope'
    assert len(formal)==scopes['formal_core']['attempts']
    assert set(formal)<=set(by_path),'Uncharged formal request'
    attributed=Counter();logical_generations=0;feedback=0;no_response_receipts=0;normalized_empty_replies=0
    per_graph_arm=defaultdict(lambda:Counter())
    record_seals=[]
    for record_path in sorted(run.glob('*/tasks/*/*/record.json')):
        record=read(record_path);intent=read(record_path.with_name('intent.json'))
        assert record['freeze_sha256']==intent['freeze_sha256']==freeze_sha
        assert record['qid']==intent['qid'] and record['arm']==intent['arm']
        views=record['views'];assert len(views) in (1,2)
        expected_messages=intent['messages']
        for step,view in enumerate(views):
            assert step==view['step']
            paths=[Path(p)/'request.json' for p in view['reply']['attempt_folders']]
            assert 1<=len(paths)<=3
            wires=[p.read_bytes() for p in paths]
            assert all(w==wires[0] for w in wires),'Same-request retry changed bytes'
            payload=json.loads(wires[0])
            assert payload['messages']==expected_messages
            for field in ('model','temperature','top_p','reasoning_effort','max_tokens'):
                assert payload[field]==freeze['model_settings'][field]
            terminals=[]
            for index,p in enumerate(paths):
                entry=by_path[key(p)]
                assert entry['scope']=='formal_core'
                terminal=entry['terminal'];terminals.append(terminal)
                assert terminal['physical_attempt_in_request']==index+1
                if index:assert terminal['retry_wait_seconds']>=10
                attributed[key(p)]+=1
                bucket=per_graph_arm[record['graph']+':'+record['arm']]
                bucket['physical_attempts']+=1
                if not isinstance((terminal.get('body') or {}).get('usage'),dict):bucket['unknown_usage']+=1
            if view['reply'].get('response_available') is False:
                assert len(paths)==3 and all(t.get('transport_error') for t in terminals)
                assert view['outcome']['status']=='INFRASTRUCTURE_ERROR'
                no_response_receipts+=1
            else:
                assert terminals[-1].get('http_status')==200
                choice=terminals[-1]['body']['choices'][0]
                raw_content=choice.get('message',{}).get('content')
                expected_content=raw_content if isinstance(raw_content,str) else ''
                assert expected_content==view['reply']['content']
                expected_usable=bool(choice.get('finish_reason')=='stop' and isinstance(raw_content,str) and raw_content.strip())
                assert expected_usable==view['reply']['usable']
                if not isinstance(raw_content,str):normalized_empty_replies+=1
            logical_generations+=1
            if step:feedback+=1
            if step==0 and len(views)==2:
                assert view['outcome']['status'] in ('MODEL_ERROR','EXECUTION_ERROR')
                expected_messages=expected_messages+[
                    {'role':'assistant','content':view['reply']['content']},
                    {'role':'user','content':'Repair once using only this format/compile/execution '
                     'diagnostic, not correctness feedback: '+view['outcome'].get('error','Execution failed')[:1000]}]
        record_seals.append([str(record_path.relative_to(run)),sha(record_path)])
    assert set(attributed)==set(formal),'Formal terminal attempt missing from saved prediction receipt'
    assert all(n==1 for n in attributed.values()),'Attempt attributed more than once'
    after=budget.inspect();assert before==after,'Ledger changed during offline audit'
    out=dict(kind='ALL_ATTEMPT_LEDGER_RECONCILIATION',time_local=datetime.now().astimezone().isoformat(),
        cumulative_reserved=before['reserved'],approved_cap=before['cap'],remaining=before['remaining'],
        pending=before['pending'],scope_counts={name:v['attempts'] for name,v in scopes.items()},
        scopes={name:{k:dict(v) if isinstance(v,Counter) else v for k,v in values.items()} for name,values in scopes.items()},
        formal_physical_attempts=len(formal),formal_prediction_receipts=len(record_seals),
        logical_generation_receipts=logical_generations,feedback_generation_receipts=feedback,
        exhausted_no_model_response_receipts=no_response_receipts,
        nonstring_response_content_normalized_to_empty_by_frozen_transport=normalized_empty_replies,
        extra_physical_retry_attempts=len(formal)-logical_generations,
        per_graph_arm_physical={k:dict(v) for k,v in per_graph_arm.items()},
        unique_budget_slot_per_physical_request=True,all_formal_attempts_attributed_once=True,
        retry_bodies_identical=True,retry_waits_at_least10s=True,max3_attempts_per_request=True,
        saved_successful_replies_match_budgeted_responses_after_frozen_normalization=True,
        formal_model_parameters_match_freeze=True,feedback_messages_match_frozen_non_gold_rule=True,
        frozen_source_hashes_unchanged=True,source_files_verified=len(freeze['code_files']),
        unclassified_new_slots=unclassified,ledger_unchanged_during_audit=True,
        physical_evidence_seal=seal(manifest),prediction_evidence_seal=seal(record_seals),
        freeze_sha256=freeze_sha,new_model_calls=0,new_database_queries=0,network_requests=0,
        money_cost='unknown; unknown token usage and unavailable unit price not set to zero',
        scope_limit='Retry/model/feedback checks apply to formal core only; history is accounted separately. Attempt accounting only; not full T4 or a method efficiency advantage. Same-plan controls reuse model calls. No full2348 inference before seven graphs.')
    target=ROOT/'reports/t1_all_attempt_ledger_reconciliation_20261025.json'
    target.write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
    (ROOT/'reports/t1_all_attempt_hash_manifest_20261025.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:out[k] for k in ('cumulative_reserved','remaining','scope_counts','formal_physical_attempts',
          'formal_prediction_receipts','logical_generation_receipts','feedback_generation_receipts',
          'extra_physical_retry_attempts','all_formal_attempts_attributed_once')}))


if __name__=='__main__':reconcile()
