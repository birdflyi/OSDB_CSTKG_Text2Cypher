"""Single finite actual-host preflight deployment, no formal/model runner."""
from datetime import datetime
import base64
import hashlib
import io
import json
from pathlib import Path
import shlex
import sys
import unittest
import paramiko

ROOT=Path('D:/ch7')
CODE=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OPS=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_recovery/operations'
REMOTE='/mnt/data/louzehua/ch7_t1_public_benchmark_20261023'
HOST='interactive-xuyl0q80gc01-6d6f5ddb9b-xtpfj'
BASE=REMOTE+'/remote_cutover_preflight_v1_20261025'
sys.path.insert(0,str(OPS));sys.path.insert(0,str(CODE/'experiment-harness/public_benchmark_v1'))
from attempt_budget import AttemptBudget
from entity_candidates import CANDIDATE_NOTICE
from test_local_native_cutover import LocalCutoverChecks
from local_native_worker import encode

def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def raw(value):return json.dumps(value,ensure_ascii=False,indent=2).encode('utf-8')

def command(script):
    encoded=base64.b64encode(script.encode()).decode()
    return shlex.join(['python3','-c',"import base64;exec(base64.b64decode('"+encoded+"'))"])

def main():
    local=ROOT/'reports/native_local_cutover_preflight_v2_20261025'
    local.mkdir(exist_ok=False)
    budget=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
        ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',path_rewrites=[
            (r'C:\Users\10651\Documents\trae_projects\ch7_private',str(ROOT/'experiments/ch7_private'))])
    before=budget.inspect();assert not before['pending']
    assert not (budget.ledger/'.t1-reservation.lock').exists()
    log=io.StringIO()
    result=unittest.TextTestRunner(stream=log,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(LocalCutoverChecks))
    (local/'synthetic_checks.log').write_text(log.getvalue(),encoding='utf-8')
    assert result.wasSuccessful(),log.getvalue()
    freeze_path=ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
    freeze=read(freeze_path);admission=read(freeze_path.with_name('admission.json'))
    for relative,expected in freeze['code_files'].items():assert sha(CODE/relative)==expected
    data=ROOT/'experiments/public_benchmark_v1/cypherbench'
    shared=ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024/geography/shared_inputs'
    tasks={r['qid']:r for r in map(json.loads,(data/'runtime/test.jsonl').read_text(encoding='utf-8').splitlines())}
    schema=read(data/'upstream/graphs/schemas/geography_schema.json')
    shared_hashes={p.name:sha(p) for p in shared.glob('*.json')}
    # Use all SAVED geography contexts as byte-preservation checks, not new tasks.
    for p in sorted(shared.glob('*.json')):
        saved=read(p);question=tasks[p.stem]['nl_question']
        # Historical recovered lookup metadata can retain native insertion order;
        # MODEL context used the admitted journal's canonical wire boundary.
        candidates=json.loads(encode(saved['lookup']['candidates']))
        context=('Graph schema:\n'+json.dumps(schema,ensure_ascii=False)+'\nQuestion:\n'+question+
            '\n'+CANDIDATE_NOTICE+'\nCandidates:\n'+json.dumps(candidates,ensure_ascii=False))
        assert context.encode()==saved['context'].encode(),'Saved context differs before deployment'
    p=sorted(shared.glob('*.json'))[0];saved=read(p)
    fixture={'scope':'ALREADY_SAVED_CONTEXT_TRANSPORT_ONLY','graph':'geography','qid':p.stem,
        'question':tasks[p.stem]['nl_question'],'schema':schema,'saved_lookup':saved['lookup'],
        'saved_context':saved['context'],'graph_sha256':admission['admitted_graphs']['geography']['graph_sha256']}
    (local/'fixture.json').write_bytes(raw(fixture))
    old=ROOT/'experiments/public_benchmark_v1/remote_cutover_preparation_v1_20261025'
    history=ROOT/'experiments/public_benchmark_v1/remote_history_preparation_v1_20261025'
    assets={'packet.zip':old/'packet.zip','full_history.zip':history/'full_history.zip',
            'source.bundle':history/'frozen_source_c614367.bundle'}
    for name,p in assets.items():assert p.is_file()
    assert sha(assets['packet.zip'])=='d7fb5643ea33ced2343e726e49700b7c6014c08de952fdbc227aeb917b052adb'
    assert sha(assets['full_history.zip'])=='e30884a58f09de8613d3327a2603fd91b936c7174af2240a82326806e7b687f1'
    assert sha(assets['source.bundle'])=='1494b9aab295b7689ffc9e4e63c83ffe5139e6c3bcf6732e243c5db22a6466e0'
    names=['local_native_worker.py','frozen_path_resolver.py','portable_attempt_history.py','native_cutover_preflight_job.py']
    spec={'frozen_commit':'c6143670a9949ec3b366ef513291740d1b8c4134',
        'packet_sha256':sha(assets['packet.zip']),'history_sha256':sha(assets['full_history.zip']),
        'freeze_sha256':sha(freeze_path),'source_bundle_sha256':sha(assets['source.bundle']),
        'fixture_sha256':sha(local/'fixture.json'),
        'history_manifest_sha256':'fe58fdbb385d2b99be299582d7e0a8ad51e163d375396a07aded1167ddc4d30e',
        'operations_sha256':{name:sha(OPS/name) for name in names}}
    (local/'spec.json').write_bytes(raw(spec))
    (local/'deployment_intent.json').write_bytes(raw({'base':BASE,'spec':spec,
        'no_model_or_formal_runner':True,'unknown_launch_requires_audit':True}))
    client=paramiko.SSHClient();client.load_host_keys(str(Path.home()/'.ssh/known_hosts'))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect('117.50.195.94',port=2491,username='root',
            key_filename='C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key',
            timeout=12,banner_timeout=12,auth_timeout=12,allow_agent=False,look_for_keys=False)
        client.get_transport().set_keepalive(15)
        gate=f'''from pathlib import Path
import os,json,socket
root=Path({REMOTE!r})
assert socket.gethostname()=={HOST!r}
assert json.loads((root/'execution_owner.json').read_text())['hostname']=={HOST!r}
assert not (root/'.public-runtime.lock').exists()
assert not (root/'.t1-cutover-preflight.lock').exists()
v=os.statvfs(root);assert v.f_bavail*v.f_frsize>5*1024**3
for p in Path('/proc').iterdir():
 if not p.name.isdigit() or int(p.name)==os.getpid():continue
 try:args=(p/'cmdline').read_bytes().replace(b'\\0',b' ').decode(errors='replace')
 except OSError:continue
 assert not (str(root) in args and any(n in args for n in ['public_query_worker.py','run_frozen_main.py','durable_native_journal_v1.py'])),'Existing worker'
print(json.dumps({{'hostname':socket.gethostname(),'owner_pass':True,'worker_absent':True,'lock_absent':True}}))
'''
        _,out,err=client.exec_command(command(gate),timeout=25)
        observed=out.read();assert out.channel.recv_exit_status()==0,'Fresh server gate failed'
        observation=json.loads(observed)
        with client.open_sftp() as sftp:
            sftp.mkdir(BASE);sftp.chmod(BASE,0o700)
            for name,path in assets.items():sftp.put(str(path),BASE+'/'+name);sftp.chmod(BASE+'/'+name,0o600)
            for name in names:sftp.put(str(OPS/name),BASE+'/'+name);sftp.chmod(BASE+'/'+name,0o600)
            for name in ['fixture.json','spec.json']:sftp.put(str(local/name),BASE+'/'+name);sftp.chmod(BASE+'/'+name,0o600)
        (local/'upload_complete.json').write_bytes(raw({'base':BASE,'all_named_files_uploaded':True}))
        launch=f'''import subprocess,json,os
from pathlib import Path
base=Path({BASE!r});os.umask(0o077)
log=(base/'job.log').open('xb')
p=subprocess.Popen([{REMOTE+'/.venv/bin/python'!r},str(base/'native_cutover_preflight_job.py'),'--base',str(base)],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
print(json.dumps({{'pid':p.pid,'base':str(base)}}))
'''
        _,out,err=client.exec_command(command(launch),timeout=20)
        launched=json.loads(out.read());assert out.channel.recv_exit_status()==0
    finally:client.close()
    assert budget.inspect()==before
    assert shared_hashes=={p.name:sha(p) for p in shared.glob('*.json')}
    report={'kind':'NATIVE_LOCAL_IPC_ACTUAL_PREFLIGHT_DISPATCH','time_local':datetime.now().astimezone().isoformat(),
        'synthetic_checks_pass':True,'synthetic_checks':result.testsRun,'saved_geography_contexts_byte_checked':len(shared_hashes),
        'saved_inputs_unchanged':True,'fresh_remote_gate':observation,'launched':launched,
        'actual_result_pending':True,'new_model_calls':0,'formal_predictions':0,
        'budget_reserved':before['reserved'],'budget_pending':before['pending'],'budget_unchanged':True,
        'budget_authority_handover':False,'private_model_configuration_uploaded':False,'SSH_key_uploaded':False,
        'spec':spec}
    (local/'dispatch.json').write_bytes(raw(report))
    print(json.dumps({k:report[k] for k in ['synthetic_checks','saved_geography_contexts_byte_checked','launched','new_model_calls','actual_result_pending']}))

if __name__=='__main__':main()
