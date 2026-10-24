"""Finalize three exhausted transport attempts as a visible infrastructure failure.

No model response is invented, no gold is read, no fourth request is sent.
Uses the pre-frozen all-task denominator and INFRASTRUCTURE_ERROR status.
"""
from pathlib import Path
import datetime
import hashlib
import json

root=Path('D:/ch7');run=root/'experiments/public_benchmark_v1/core_main_v1_20261024'
qid='713e538e-8c46-4907-8cce-f2022389e195'
folder=run/'geography/tasks'/qid/'ours'
assert not (folder/'record.json').exists()
assert not list(folder.glob('reply_*.json')) and not list(folder.glob('view_*.json'))
assert not list((folder/'query_trace').glob('**/intent.json'))
intent=json.loads((folder/'intent.json').read_text(encoding='utf-8'))
freeze=root/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
sha=hashlib.sha256(freeze.read_bytes()).hexdigest()
assert intent['freeze_sha256']==sha and intent['qid']==qid and intent['arm']=='ours'
calls=run/'geography/segments/journal_company_v1/model_calls'
attempts=[calls/f'physical_{n:04d}' for n in (327,328,329)]
wire=[(p/'request.json').read_bytes() for p in attempts]
assert wire[0]==wire[1]==wire[2]
assert json.loads(wire[0])['messages']==intent['messages']
terminals=[json.loads((p/'terminal.json').read_text(encoding='utf-8')) for p in attempts]
assert [t['physical_attempt_in_request'] for t in terminals]==[1,2,3]
assert [t['budget_slot'] for t in terminals]==[3171,3172,3173]
assert all(t.get('transport_error') and t.get('http_status') is None and 'body' not in t for t in terminals)
assert all(t['retry_wait_seconds']>=10 for t in terminals[1:])
stop=json.loads((calls/'batch_stop.json').read_text(encoding='utf-8'))
assert stop['consecutive_failures']==3
receipt=root/'reports/T1_EXHAUSTED_REQUEST_FINALIZATION_20261025.json'
evidence={'kind':'EXHAUSTED_TRANSPORT_FINALIZATION','time_local':datetime.datetime.now().astimezone().isoformat(),
    'qid':qid,'arm':'ours','graph':'geography','freeze_sha256':sha,
    'physical_budget_slots':[3171,3172,3173],'request_sha256':hashlib.sha256(wire[0]).hexdigest(),
    'no_model_response_available':True,'no_query_dispatched':True,'fourth_attempt_sent':False,
    'status':'INFRASTRUCTURE_ERROR','new_model_calls':0,'gold_read':False,
    'protocol_basis':'Pre-frozen metrics: all tasks retained, infrastructure failures reported separately, not correct; same request max3 physical attempts.',
    'original_wrapper_clock':'unknown','all_original_attempts_preserved':True}
with receipt.open('x',encoding='utf-8') as f:json.dump(evidence,f,ensure_ascii=False,indent=2)
reply={'content':'','usable':False,'finish_reason':None,'usage':None,
       'attempt_folders':[str(p) for p in attempts],
       'response_available':False,'receipt_kind':'NO_RESPONSE_EXHAUSTED_TRANSPORT_NOT_MODEL_OUTPUT'}
outcome={'status':'INFRASTRUCTURE_ERROR','rows':None,'columns':None,'complete':False,
         'error':'Three physical transport attempts exhausted; no model response; not regenerated.'}
view={'step':0,'reply':reply,'outcome':outcome,'generation_wall_seconds':None,
      'transport_finalization':str(receipt),
      'known_physical_attempt_seconds':sum(t['seconds'] for t in terminals),
      'known_retry_wait_seconds':sum(t['retry_wait_seconds'] for t in terminals)}
record={'qid':qid,'graph':'geography','arm':'ours','freeze_sha256':sha,'views':[view],
        'model_and_execution_wall_seconds':None,'transport_finalization':str(receipt),
        'no_model_response_available':True}
with (folder/'record.json').open('x',encoding='utf-8') as f:json.dump(record,f,ensure_ascii=False,indent=2)
print(json.dumps(evidence))
