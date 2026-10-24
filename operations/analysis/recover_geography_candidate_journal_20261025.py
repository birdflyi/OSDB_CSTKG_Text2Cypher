"""Recover already-durable entity candidates, then close that audited idle worker."""
from pathlib import Path
import datetime
import hashlib
import json
import sys
import time

root=Path('D:/ch7');ops=root/'code/OSDB_CSTKG_Text2Cypher_t1_recovery/operations'
sys.path.insert(0,str(ops))
from journal_public_worker_v2 import JournalPublicWorker, ROOT
sys.path.insert(0,str(root/'code/OSDB_CSTKG_Text2Cypher_publication_t1/experiment-harness/public_benchmark_v1'))
from entity_candidates import CANDIDATE_NOTICE

folder=root/'experiments/public_benchmark_v1/core_main_v1_20261024/infra_recovery_geography_candidates_v1_20261025'
folder.mkdir(exist_ok=False)
w=JournalPublicWorker.__new__(JournalPublicWorker)
w.key=Path('C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key')
w.session='geography_e812665703d44fb9baa67046ba24cbf1'
w.folder=ROOT+'/transport_journal_v1/'+w.session
w.log=(folder/'worker_close.log').open('x',encoding='utf-8');w.closed=False
client=w.connect();started=time.monotonic()
try:
    with client.open_sftp() as sftp:
        def read(path):
            with sftp.open(path,'rb') as f:return f.read()
        owner=json.loads(read(w.folder+'/owner.json'))
        assert owner['pid']==225662 and owner['graph']=='geography'
        assert json.loads(read(ROOT+'/.public-runtime.lock'))=={'pid':225663,'graph':'geography','state':'PUBLIC_QUERY_WORKER'}
        assert 'pipe' in read('/proc/225663/wchan').decode()
        argv=read('/proc/225663/cmdline').split(b'\0')
        assert (ROOT+'/public_query_core_v1_code/public_query_worker.py').encode() in argv
        assert argv[argv.index(b'--graph')+1]==b'geography'
        names=set(sftp.listdir(w.folder))
        requests=[n for n in names if n.endswith('.request.json')]
        assert len(requests)==176 and all(n.replace('.request.json','.receipt.json') in names for n in requests)
        raw=read(w.folder+'/000175.request.json');req=json.loads(raw)
        response_raw=read(w.folder+'/000175.result.json');response=json.loads(json.dumps(json.loads(response_raw),ensure_ascii=False,sort_keys=True,separators=(',',':')))
        receipt_raw=read(w.folder+'/000175.receipt.json');receipt=json.loads(receipt_raw)
        assert receipt['sha256']==hashlib.sha256(raw).hexdigest() and receipt['native_writes']==1
        assert req['kind']=='candidates' and response['status']=='CANDIDATES'
        for name,value in [('request.json',raw),('result.json',response_raw),('receipt.json',receipt_raw)]:
            (folder/name).write_bytes(value)
finally:client.close()
data=root/'experiments/public_benchmark_v1/cypherbench'
tasks=[json.loads(s) for s in (data/'runtime/test.jsonl').read_text(encoding='utf-8').splitlines() if s.strip()]
matching=[t for t in tasks if t['graph']=='geography' and t['nl_question']==req['question']]
assert len(matching)==1
task=matching[0]
run=root/'experiments/public_benchmark_v1/core_main_v1_20261024'
target=run/'geography/shared_inputs'/(task['qid']+'.json')
assert not target.exists()
schema=json.loads((data/'upstream/graphs/schemas/geography_schema.json').read_text(encoding='utf-8'))
context=('Graph schema:\n'+json.dumps(schema,ensure_ascii=False)+'\nQuestion:\n'+task['nl_question']+'\n'+CANDIDATE_NOTICE+'\nCandidates:\n'+json.dumps(response['candidates'],ensure_ascii=False))
response['worker_roundtrip_seconds']=None
response['infrastructure_recovery_wall_seconds']=time.monotonic()-started
shared={'context':context,'lookup':response,'qid':task['qid'],'graph':'geography',
        'freeze_sha256':hashlib.sha256((root/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json').read_bytes()).hexdigest()}
with target.open('x',encoding='utf-8') as f:json.dump(shared,f,ensure_ascii=False,indent=2)
evidence={'time_local':datetime.datetime.now().astimezone().isoformat(),'state':'DURABLE_CANDIDATES_RESTORED','qid':task['qid'],'session':w.session,'journal_id':175,'native_writes':1,'new_model_calls':0,'new_native_queries':0,'original_roundtrip_clock':'unknown','gold_read':False,'all176_requests_have_receipts':True,'exact_context_builder':'original frozen run_frozen_main.py formula','shared_path':str(target)}
(folder/'recovery.json').write_text(json.dumps(evidence,indent=2)+'\n',encoding='utf-8')
w.close()
assert '"closed"' in (folder/'worker_close.log').read_text(encoding='utf-8')
(folder/'complete.json').write_text(json.dumps({**evidence,'old_worker_closed_by_own_finally':True,'manual_lock_unlink':False},indent=2)+'\n',encoding='utf-8')
print(json.dumps(evidence))
