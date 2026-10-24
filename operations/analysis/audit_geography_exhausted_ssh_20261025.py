"""Preserve cached model output, prove no native dispatch, close idle old daemon.

No SQL execution or model call. No automatic fourth RPC or queue restart.
"""
from pathlib import Path
import datetime
import hashlib
import json
import sys

root=Path('D:/ch7');ops=root/'code/OSDB_CSTKG_Text2Cypher_t1_recovery/operations'
sys.path.insert(0,str(ops))
from journal_public_worker_v2 import JournalPublicWorker,ROOT

run=root/'experiments/public_benchmark_v1/core_main_v1_20261024'
qid='8c848e89-3f3f-4198-9953-dccbe326fa4a'
task=run/'geography/tasks'/qid/'direct'
assert not (task/'record.json').exists()
assert [p.name for p in task.glob('reply_*.json')]==['reply_0.json']
reply=json.loads((task/'reply_0.json').read_text(encoding='utf-8'))
intent=json.loads((task/'intent.json').read_text(encoding='utf-8'))
attempt=Path(reply['attempt_folders'][-1])
terminal=json.loads((attempt/'terminal.json').read_text(encoding='utf-8'))
assert terminal.get('http_status')==200 and terminal['body']['choices'][0]['message']['content']==reply['content']
assert json.loads((attempt/'request.json').read_text(encoding='utf-8'))['messages']==intent['messages']
assert reply['usable'] and not list(task.glob('view_*.json'))
trace=task/'query_trace/step_0/000'
assert (trace/'intent.json').exists() and (trace/'transport_error.json').exists() and not (trace/'result.json').exists()
logs=run/'geography/segments/journal_channel_status_v3/worker_stderr.log'
rows=[json.loads(line) for line in logs.read_text(encoding='utf-8').splitlines()]
errors=[v for v in rows if v.get('journal_id')==12 and v.get('status')=='TRANSPORT_ERROR']
assert [v['physical_rpc_attempt'] for v in errors]==[1,2,3]
folder=run/'infra_audit_geography_ssh_exhausted_v1_20261025'
folder.mkdir(exist_ok=False)
w=JournalPublicWorker.__new__(JournalPublicWorker)
w.key=Path('C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key')
w.session='geography_086123e15373463b8a347da4c6fb040a';w.folder=ROOT+'/transport_journal_v1/'+w.session
w.log=(folder/'worker_close.log').open('x',encoding='utf-8');w.closed=False
client=w.connect()
try:
    with client.open_sftp() as sftp:
        def read(p):
            with sftp.open(p,'rb') as f:return f.read()
        assert json.loads(read(w.folder+'/owner.json'))['pid']==229517
        assert json.loads(read(ROOT+'/.public-runtime.lock'))=={'pid':229533,'graph':'geography','state':'PUBLIC_QUERY_WORKER'}
        argv=read('/proc/229533/cmdline').split(b'\0')
        assert (ROOT+'/public_query_core_v1_code/public_query_worker.py').encode() in argv
        assert argv[argv.index(b'--graph')+1]==b'geography'
        assert 'pipe' in read('/proc/229533/wchan').decode()
        names=set(sftp.listdir(w.folder))
        requests=sorted(n for n in names if n.endswith('.request.json'))
        assert len(requests)==12 and all(n.replace('.request.json','.receipt.json') in names for n in requests)
        assert not any(n.startswith('000012.') for n in names)
        for name in ['spec.json','owner.json','000011.receipt.json']:
            (folder/name).write_bytes(read(w.folder+'/'+name))
finally:client.close()
evidence={'time_local':datetime.datetime.now().astimezone().isoformat(),
          'kind':'EXHAUSTED_SSH_CONNECT_AUDIT','qid':qid,'arm':'direct','graph':'geography',
          'cached_model_output_usable':True,'successful_model_budget_slot':terminal['budget_slot'],
          'physical_rpc_attempts':3,'failed_at':'SSH connect before authentication/request submission',
          'journal_id':12,'native_request_file_absent':True,'prior12_requests_have_receipts':True,
          'native_query_dispatched':False,'recovery_model_calls':0,'recovery_native_queries':0,
          'fourth_rpc_sent':False,'raw_trace_preserved':True,'freeze_sha256':intent['freeze_sha256']}
(folder/'audit.json').write_text(json.dumps(evidence,indent=2)+'\n',encoding='utf-8')
w.close()
assert '"closed"' in (folder/'worker_close.log').read_text(encoding='utf-8')
with (folder/'complete.json').open('x',encoding='utf-8') as f:
    json.dump({**evidence,'old_worker_closed_by_own_finally':True,'manual_lock_unlink':False},f,indent=2)
# Preserve the actually generated response, with an explicit failed execution.
# Pre-frozen metric assigns zero to all non-EXECUTED statuses and retains N.
view={'step':0,'reply':reply,'outcome':{'status':'INFRASTRUCTURE_ERROR','rows':None,
      'columns':None,'complete':False,'error':'SSH connect failed three times; native query not dispatched; no fourth RPC.'},
      'generation_wall_seconds':None,'original_model_transport_seconds':terminal['seconds'],
      'transport_finalization':str(folder/'complete.json')}
record={'qid':qid,'graph':'geography','arm':'direct','freeze_sha256':intent['freeze_sha256'],
        'views':[view],'model_and_execution_wall_seconds':None,
        'transport_finalization':str(folder/'complete.json'),'native_query_dispatched':False,
        'infrastructure_failure_not_model_error':True}
with (task/'record.json').open('x',encoding='utf-8') as f:json.dump(record,f,ensure_ascii=False,indent=2)
print(json.dumps(evidence))
