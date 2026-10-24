"""No-model actual company checks against saved original worker observations."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

b=Path(r'D:\ch7');ops=b/'code/OSDB_CSTKG_Text2Cypher_t1_recovery/operations'
sys.path.insert(0,str(ops))
from paramiko_public_worker import ParamikoPublicWorker
run=b/'experiments/public_benchmark_v1/core_main_v1_20261024'
freeze=json.loads((b/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json').read_text())
folder=b/'reports/paramiko_transport_conformance_v1_20261025';folder.mkdir(exist_ok=False)
worker=None;started=time.monotonic()
try:
    worker=ParamikoPublicWorker(Path(r'C:\Users\10651\.codex\private\ch7_ssh_20261023\interactive_2491.key'),
        'company',folder/'worker_stderr.log',freeze['remote_worker'])
    qid='885ff11b-d708-451e-80e4-493fdff56895'
    original=json.loads((run/'company/tasks'/qid/'direct/record.json').read_text(encoding='utf-8'))
    expected=original['views'][-1]['outcome']
    intent=json.loads((run/'company/tasks'/qid/'direct/query_trace/recovered_step_0/000/intent.json').read_text(encoding='utf-8'))
    actual=worker.execute(intent['query'],intent['parameters'])
    fields=['status','complete','columns','rows','result_bytes']
    assert all(actual[field]==expected[field] for field in fields)
    runtime=[json.loads(line) for line in (b/'experiments/public_benchmark_v1/cypherbench/runtime/test.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    question=next(t['nl_question'] for t in runtime if t['qid']==qid)
    saved=json.loads((run/'company/shared_inputs'/(qid+'.json')).read_text(encoding='utf-8'))
    candidates=worker.request({'kind':'candidates','question':question})
    assert candidates['candidates']==saved['lookup']['candidates']
    before=worker.execute('MATCH (n) RETURN count(n) AS n',{})
    denied=worker.execute('CREATE (n:TransportConformanceMustBeDenied)',{})
    after=worker.execute('MATCH (n) RETURN count(n) AS n',{})
    assert before['rows']==after['rows']==[[581306]]
    assert denied['status']=='EXECUTION_ERROR'
    result=dict(kind='ACTUAL_ORIGINAL_WORKER_TRANSPORT_CONFORMANCE',timestamp_utc=datetime.now(timezone.utc).isoformat(),
        graph='company',graph_sha256=worker.ready['graph_sha256'],
        saved_direct_answer_exact_match=True,saved_candidate_context_exact_match=True,
        write_denied=True,node_count_before_after_unchanged=True,
        original_frozen_remote_dependencies_verified=True,new_model_calls=0,
        elapsed_seconds=time.monotonic()-started,
        transport_source_sha256=hashlib.sha256((ops/'paramiko_public_worker.py').read_bytes()).hexdigest(),
        limitations='One bounded transport conformance run; not proof of future network reliability or a method improvement.')
    (folder/'result.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result),flush=True)
finally:
    if worker:worker.close()
