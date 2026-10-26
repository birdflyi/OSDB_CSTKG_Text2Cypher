"""Offline identity/failure replay over cached references, never model outputs.

Reference cloning is a software sanity check, not an experiment or EX score.
No query, model, author pipeline, transport, credential or remote worker runs.
"""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path('D:/ch7')
REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
CANON=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
RUNTIME=REPO/'experiment-harness/public_benchmark_v1'
OUT=ROOT/'reports/score_pairing_replay_v1_20261026'
sys.path.insert(0,str(RUNTIME))
from tugraph_results import parse_rest_result, validate_rows, strict_auxiliary_equal, MalformedNativeResult
from offline_score_pairing import evaluate, fingerprint, PairingError, FAILURES


def read(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def write(path,value):
    with path.open('x',encoding='utf-8',newline='\n') as stream:
        json.dump(value,stream,ensure_ascii=False,indent=2);stream.write('\n')
def now():return datetime.now(timezone(timedelta(hours=8))).isoformat()


def validate(answer):
    if type(answer) is not dict or answer.get('status')!='EXECUTED' or answer.get('complete') is not True:
        raise ValueError('No complete successful final answer')
    validate_rows(answer['columns'],answer['rows'])


def main():
    assert not OUT.exists(),'Preserve previous output; do not overwrite or rerun.'
    OUT.mkdir()
    runtime_before={str(p.relative_to(RUNTIME)):sha(p) for p in RUNTIME.rglob('*.py')}
    freeze=read(ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json')
    for name,digest in freeze['code_files'].items():assert sha(CANON/name)==digest
    sys.path.insert(0,str(CANON/'experiment-harness/public_benchmark_v1'))
    from attempt_budget import AttemptBudget
    budget=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
        ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',path_rewrites=[
            (r'C:\Users\10651\Documents\trae_projects\ch7_private',ROOT/'experiments/ch7_private')])
    balance=budget.inspect();assert balance['reserved']==5193 and not balance['pending']
    code={p.name:sha(p) for p in [Path(__file__),Path(__file__).with_name('offline_score_pairing.py'),
        Path(__file__).with_name('test_offline_score_pairing.py')]}
    write(OUT/'protocol.json',{'kind':'CACHED_REFERENCE_CLONE_AND_ID_FAULT_INJECTION_NOT_PREDICTIONS',
        'sealed_local':now(),'source_sha256':code,'planned_positions':3826,'expected_known':3815,
        'expected_unknown_references':11,'graph_order':'previous six_graph_input_plan graph order',
        'comparator':'Existing strict auxiliary comparator, ordered=True for clone sanity only; not task ordering freeze.',
        'scenarios':['reference-clone','reverse-observations','drop-one-known','duplicate-final',
            'wrong-task-version','wrong-model-context','unknown-ID','failure-statuses',
            'unknown-reference-with-synthetic-empty','malformed-answer','comparator-error'],
        'selection':'All cached reference positions. Fault targets first known position(s) in graph/id order, never method outcomes.',
        'new_predictions':0,'no_query_or_question_or_prediction_interpretation':True,
        'author_metric_adopted':False,'REST_Bolt_equivalence_established':False})
    plan=read(ROOT/'reports/text2gql_native_engine_20261026/six_graph_input_plan.json')
    manifest=[];receipts=[];graphs=[]
    for index,graph in enumerate(plan['graph_order']):
        folder=('text2gql_native_reference_graph_00_v3_20261026' if index==0 else
            'text2gql_native_reference_graph_'+str(index).zfill(2)+'_20261026')
        base=ROOT/'reports'/folder/'evidence'
        counts=Counter()
        for path in sorted((base/'reference_records').glob('*.json')):
            record=read(path);response=base/'responses'/('reference_'+path.stem+'.json')
            assert sha(response)==record['response_sha256']
            receipts.append({'record_path':str(path.relative_to(ROOT)), 'record_sha256':sha(path),
                'response_path':str(response.relative_to(ROOT)), 'response_sha256':sha(response)})
            reference=None
            if record['status'].startswith('EXECUTED'):
                reference=parse_rest_result(response.read_bytes(),transport_complete=True,execution_complete=True)
                counts['known']+=1
            else:
                assert record['status']=='GOLD_EXECUTION_ERROR';counts['unknown']+=1
                try:parse_rest_result(response.read_bytes(),transport_complete=True,execution_complete=True)
                except MalformedNativeResult:pass
                else:raise AssertionError('Failed reference may not become successful empty answer')
            identity={'graph':graph,'id':record['id'],'reference_query_sha256':record['query_sha256'],
                'reference_response_sha256':record['response_sha256'],'reference_status':record['status']}
            manifest.append(dict(graph=graph,id=record['id'],task_sha256=fingerprint(identity),reference=reference))
        graphs.append({'graph':graph,**counts})
    assert len(manifest)==3826 and sum(m['reference'] is not None for m in manifest)==3815
    registry=[{k:v for k,v in m.items() if k!='reference'}|{'reference_known':m['reference'] is not None} for m in manifest]
    # This manifest identity pins cached evaluation artifacts, not the full NL,
    # schema and permissions needed for a future formal task/model freeze.
    context={'cohort':fingerprint(registry),'method':'SYNTHETIC_REFERENCE_CLONE_NOT_MODEL',
        'model':'NO_MODEL_CALLED','input_contract':'NO_MODEL_INPUT_CONSTRUCTED',
        'score_contract':'STRICT_AUX_ORDERED_TRUE_CLONE_ONLY_NOT_OFFICIAL_EA'}
    observations=[dict(graph=m['graph'],id=m['id'],task_sha256=m['task_sha256'],
        context=dict(context),status='EXECUTED',answer=m['reference']) for m in manifest if m['reference'] is not None]
    def run(obs,compare=None):
        return evaluate(manifest,obs,context=context,validate_answer=validate,
            compare=compare or (lambda a,b:strict_auxiliary_equal(a,b,ordered=True)))
    baseline=run(observations);s=baseline['summary']
    assert (s['planned'],s['correct_complete_answers'],s['unknown_references'],s['unsent'])==(3826,3815,11,11)
    assert run(list(reversed(observations)))==baseline
    checks=[{'scenario':'reference-clone','all_3815_known_positions_match_self':True},
        {'scenario':'reverse-observations','journal_exactly_unchanged':True}]
    dropped=run(observations[1:])['summary']
    assert dropped['planned']==3826 and dropped['unsent']==12 and dropped['known_reference_unresolved_scores']==1
    assert dropped['diagnostic_known_reference_fraction'] is None
    checks.append({'scenario':'drop-one-known','summary':dropped})
    for name in ('duplicate-final','wrong-task-version','wrong-model-context','unknown-ID'):
        changed=deepcopy(observations)
        if name=='duplicate-final':changed.append(deepcopy(changed[0]))
        elif name=='wrong-task-version':changed[0]['task_sha256']='0'*64
        elif name=='wrong-model-context':changed[0]['context']['model']='CHANGED_SYNTHETIC_MODEL'
        else:changed[0]['id']='SYNTHETIC_ID_NOT_IN_MANIFEST'
        try:run(changed)
        except PairingError:checks.append({'scenario':name,'whole_batch_rejected_before_scoring':True})
        else:raise AssertionError(name+' must abort')
    changed=deepcopy(observations)
    for item,status in zip(changed,sorted(FAILURES)):item.update(status=status,answer=None)
    failure=run(changed)['summary']
    assert failure['planned']==3826 and failure['correct_complete_answers']==3809 and failure['complete_answers']==3809
    assert all(failure['status_counts'][k]==1 for k in FAILURES)
    checks.append({'scenario':'failure-statuses','summary':failure})
    changed=list(observations)
    for m in manifest:
        if m['reference'] is None:
            changed.append(dict(graph=m['graph'],id=m['id'],task_sha256=m['task_sha256'],context=dict(context),
                status='EXECUTED',answer={'status':'EXECUTED','complete':True,'columns':['synthetic'],'rows':[]}))
    unknown=run(changed);assert unknown['summary']['complete_answers']==3826
    assert sum(r['score'] is None for r in unknown['records'])==11
    assert unknown['summary']['correct_complete_answers']==3815
    checks.append({'scenario':'unknown-reference-with-synthetic-empty','unknown_references_still_unscored':11})
    changed=deepcopy(observations);changed[0]['answer']['rows']=[[None]*(len(changed[0]['answer']['columns'])+1)]
    malformed=run(changed)['summary'];assert malformed['status_counts']['INVALID_ANSWER']==1
    assert malformed['complete_answers']==3814
    checks.append({'scenario':'malformed-answer','summary':malformed})
    calls=[0]
    def broken_once(a,b):
        calls[0]+=1
        if calls[0]==1:raise RuntimeError('Constructed comparator error')
        return strict_auxiliary_equal(a,b,ordered=True)
    broken=run(observations,broken_once)['summary']
    assert broken['scoring_errors']==1 and broken['wrong_complete_answers']==0
    assert broken['known_reference_unresolved_scores']==1 and broken['diagnostic_known_reference_fraction'] is None
    checks.append({'scenario':'comparator-error','summary':broken})
    assert len(checks)==11
    for r in receipts:
        assert sha(ROOT/r['record_path'])==r['record_sha256'] and sha(ROOT/r['response_path'])==r['response_sha256']
    assert runtime_before=={str(p.relative_to(RUNTIME)):sha(p) for p in RUNTIME.rglob('*.py')}
    for name,digest in freeze['code_files'].items():assert sha(CANON/name)==digest
    assert balance==budget.inspect()
    write(OUT/'source_receipts.json',receipts);write(OUT/'position_registry.json',registry)
    write(OUT/'reference_clone_journal.json',baseline);write(OUT/'failure_injection_checks.json',checks)
    summary={'kind':'SCORE_PAIRING_DEVELOPMENT_NOT_MODEL_OR_BENCHMARK_EX','time_local':now(),
        'cached_reference_positions':3826,'reference_shape_known':3815,'unknown_reference_positions_retained':11,
        'graphs':graphs,'reference_clone_matches':3815,'reference_clone_is_model_result':False,
        'independent_synthetic_unit_checks_passed':17,'full_cache_scenarios_planned':11,'full_cache_scenarios_passed':11,
        'ID_version_or_context_failures_reject_whole_batch':True,'missing_positions_preserve_denominator':True,
        'known_reference_missing_or_scoring_error_withholds_fraction':True,
        'unknown_reference_never_assigned_score':True,'failure_refusal_or_truncation_not_correct':True,
        'all_cached_source_hashes_unchanged':True,'canonical_frozen_code_unchanged':len(freeze['code_files']),
        'method_runtime_files_unchanged':len(runtime_before),'cumulative_model_attempts':balance['reserved'],
        'cap':balance['cap'],'remaining_original_CB_scope':balance['remaining'],'pending':balance['pending'],
        'this_offline_audit_new_model_calls':0,'this_offline_audit_new_database_queries':0,
        'this_offline_audit_network_requests':0,'source_sha256':code,'protocol_sha256':sha(OUT/'protocol.json'),
        'registry_sha256':sha(OUT/'position_registry.json'),'formal_metric_frozen':False,
        'author_EA_adopted':False,'REST_Bolt_equivalence_established':False,'model_input_or_method_changed':False,
        'formal_prediction_N':None,'formal_predictions_dispatched':0,'formal_predictions_completed':0,
        'formal_predictions_failed':0,'formal_predictions_undispatched':None,'entire_T1_complete':False,
        'limitations':['Self-cloning is an identity/shape sanity check, not 3815 successful model predictions.',
            'No real predicted wrong-answer rate, method EX or effect measured.',
            'No new source-query correctness or REST/Bolt equivalence established.',
            'Formal NL/schema/input/permissions manifest and author scoring/order protocol still pending.',
            'Model attempt5193 belongs to the separately authorized endpoint probe, not this audit.',
            'Text2GQL conditional model budget remains unactivated.']}
    write(OUT/'summary.json',summary)
    write(ROOT/'reports/t1_offline_score_pairing_summary_20261026.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ('graphs','source_sha256','limitations')}))


if __name__=='__main__':main()
