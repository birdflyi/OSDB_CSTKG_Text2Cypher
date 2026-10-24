"""Actual original-worker checks and lost-response injection, zero model calls."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shlex
import sys
import time

B=Path('D:/ch7');OPS=B/'code/OSDB_CSTKG_Text2Cypher_t1_recovery/operations'
sys.path.insert(0,str(OPS))
from journal_public_worker_v2 import JournalPublicWorker, ROOT

folder=B/'reports/durable_journal_conformance_v3_20261025'
folder.mkdir(exist_ok=False)
freeze=json.loads((B/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json').read_text(encoding='utf-8'))
key=Path('C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key')
bootstrap=JournalPublicWorker.__new__(JournalPublicWorker);bootstrap.key=key
client=bootstrap.connect()
try:
    with client.open_sftp() as sftp:
        try:sftp.stat(ROOT+'/.public-runtime.lock')
        except FileNotFoundError:pass
        else:raise RuntimeError('Public runtime owned; do not deploy/start another worker')
        base=ROOT+'/transport_journal_v1'
        try:sftp.mkdir(base)
        except OSError:sftp.stat(base)
        target=base+'/durable_native_journal_v1.py'
        local=(OPS/'durable_native_journal_v1.py').read_bytes()
        try:
            with sftp.open(target,'rb') as f:assert f.read()==local
        except FileNotFoundError:
            temp=target+'.deploy2'
            with sftp.open(temp,'wx') as f:f.write(local)
            sftp.rename(temp,target)
        with sftp.open(target,'rb') as f:assert hashlib.sha256(f.read()).hexdigest()==hashlib.sha256(local).hexdigest()
finally:client.close()

worker=None;started=time.monotonic()
try:
    worker=JournalPublicWorker(key,'company',folder/'worker_stderr.log',freeze['remote_worker'])
    # Inject the precise SSH condition missed by v1: response is durable but
    # no reliable remote exit status reaches the client. Retry SAME id only.
    original_connect=worker.connect
    fault={'injected':False}
    def connect_with_missing_status_once():
        client=original_connect()
        if not fault['injected']:
            original_exec=client.exec_command
            def exec_with_missing_status(*args,**kwargs):
                stdin,out,err=original_exec(*args,**kwargs)
                out.channel.recv_exit_status=lambda: -1
                fault['injected']=True
                return stdin,out,err
            client.exec_command=exec_with_missing_status
        return client
    worker.connect=connect_with_missing_status_once
    run=B/'experiments/public_benchmark_v1/core_main_v1_20261024'
    qid='885ff11b-d708-451e-80e4-493fdff56895'
    p=run/'company/tasks'/qid/'direct'
    expected=json.loads((p/'record.json').read_text(encoding='utf-8'))['views'][-1]['outcome']
    intent=json.loads((p/'query_trace/recovered_step_0/000/intent.json').read_text(encoding='utf-8'))
    actual=worker.execute(intent['query'],intent['parameters'])
    fields=['status','complete','columns','rows','result_bytes']
    assert all(actual[f]==expected[f] for f in fields)
    tasks=[json.loads(s) for s in (B/'experiments/public_benchmark_v1/cypherbench/runtime/test.jsonl').read_text(encoding='utf-8').splitlines() if s]
    question=next(t['nl_question'] for t in tasks if t['qid']==qid)
    saved=json.loads((run/'company/shared_inputs'/(qid+'.json')).read_text(encoding='utf-8'))
    assert worker.request({'kind':'candidates','question':question})['candidates']==saved['lookup']['candidates']
    before=worker.execute('MATCH (n) RETURN count(n) AS n',{})
    denied=worker.execute('CREATE (n:TransportConformanceMustBeDenied)',{})
    after=worker.execute('MATCH (n) RETURN count(n) AS n',{})
    assert denied['status']=='EXECUTION_ERROR' and before['rows']==after['rows']==[[581306]]
    # Submit one harmless read, let durable completion occur, then DISCARD the
    # response channel. Retrying this same id must return original receipt and
    # result without a second write to native stdin.
    index=worker.sequence
    payload={'query':'MATCH (n) RETURN count(n) AS n','parameters':{}}
    client=worker.connect()
    try:
        cmd=shlex.join([ROOT+'/.venv/bin/python',worker.remote_code,'call',
                         '--session',worker.session,'--index',str(index)])
        stdin,out,err=client.exec_command(cmd,timeout=30)
        stdin.write(json.dumps(payload));stdin.flush();out.channel.shutdown_write()
        with client.open_sftp() as sftp:
            deadline=time.monotonic()+30
            while True:
                try:
                    with sftp.open(worker.folder+f'/{index:06d}.receipt.json','rb') as f:receipt=f.read()
                    break
                except FileNotFoundError:
                    if time.monotonic()>deadline:raise TimeoutError('Injection did not finish')
                    time.sleep(.1)
        out.channel.close()  # deliberately no response consumed by local request
    finally:client.close()
    recovered=worker.request(payload)
    assert recovered['rows']==[[581306]]
    client=worker.connect()
    try:
        with client.open_sftp() as sftp:
            with sftp.open(worker.folder+f'/{index:06d}.receipt.json','rb') as f:assert f.read()==receipt
            with sftp.open(worker.folder+f'/{index:06d}.dispatch.json','rb') as f:dispatch=json.loads(f.read())
    finally:client.close()
    assert json.loads(receipt)['native_writes']==1
    # Reuse with changed bytes must fail closed before native execution.
    try:worker.rpc({'query':'RETURN 999','parameters':{}},index)
    except ValueError:pass
    else:raise AssertionError('Different payload accepted for existing id')
    assert worker.execute('MATCH (n) RETURN count(n) AS n',{})['rows']==[[581306]]
    logrows=[json.loads(line) for line in (folder/'worker_stderr.log').read_text(encoding='utf-8').splitlines()]
    assert any(v.get('journal_id')==0 and v.get('status')=='TRANSPORT_ERROR' for v in logrows)
    assert any(v.get('journal_id')==0 and v.get('physical_rpc_attempt')==2 and v.get('status')=='RECEIVED' and v['journal']['native_writes']==1 for v in logrows)
    result=dict(kind='DURABLE_NATIVE_JOURNAL_ACTUAL_CONFORMANCE',
        timestamp_utc=datetime.now(timezone.utc).isoformat(),session=worker.session,
        graph='company',graph_sha256=worker.ready['graph_sha256'],
        exact_saved_answer_and_candidates=True,write_denied=True,
        original_native_dependency_hashes_checked=True,node_count_unchanged=True,
        missing_ssh_exit_status_injected=fault['injected'],
        missing_exit_status_retried_same_id=True,
        lost_response_injected=True,replayed_same_journal_receipt_byte_identical=True,
        native_writes_for_replayed_id=1,payload_conflict_rejected=True,
        new_model_calls=0,elapsed_seconds=time.monotonic()-started,
        files={n:hashlib.sha256((OPS/n).read_bytes()).hexdigest() for n in
            ['durable_native_journal_v1.py','journal_public_worker_v2.py','core_journal_resume_v3.py']},
        limits='Original native budgets/guards/limits unchanged; remote daemon death remains a stop/unknown, never auto-replayed. Transport overhead retained, not a speed claim.')
    (folder/'result.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result))
finally:
    if worker:worker.close()
