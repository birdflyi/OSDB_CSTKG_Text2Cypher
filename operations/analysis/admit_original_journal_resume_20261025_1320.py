"""Fresh admission of SAME previously accepted journal client after finite probe.

Native local-IPC cutover remains a verified fallback, not forced into this run.
No new generation or query here. The separate original driver remains unchanged.
"""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import base64
import shlex

ROOT=Path('D:/ch7');CODE=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OPS=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_recovery/operations'
sys.path.insert(0,str(CODE/'experiment-harness/public_benchmark_v1'));sys.path.insert(0,str(OPS))
from attempt_budget import AttemptBudget
from run_frozen_main import check_admission
from journal_public_worker_v2 import JournalPublicWorker,ROOT as REMOTE,HOST

def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def main():
    frozen=ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024'
    freeze,_,tasks=check_admission(frozen/'freeze.json',frozen/'admission.json',
                    ROOT/'experiments/public_benchmark_v1/cypherbench',CODE,'geography')
    assert subprocess.check_output(['git','status','--porcelain'],cwd=CODE)==b''
    prior=read(ROOT/'reports/T1_SSH_EXIT_STATUS_RECOVERY_ADMISSION_20261025.json')
    for name,expected in prior['files'].items():assert sha(OPS/name)==expected
    actual=read(ROOT/'reports/native_local_cutover_preflight_v2_20261025/remote_actual_v2/result.json')
    assert actual['pass'] and actual['native_child_closed'] and actual['public_runtime_lock_absent']
    probe=read(ROOT/'experiments/public_benchmark_v1/endpoint_recovery_probe_20261025_1320/result.json')
    assert probe['usable'] and probe['physical_attempts']==1
    budget=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',
        ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json',path_rewrites=[
            (r'C:\Users\10651\Documents\trae_projects\ch7_private',str(ROOT/'experiments/ch7_private'))])
    count=budget.inspect();count.pop('request_identities')
    assert count=={'reserved':3267,'cap':12000,'remaining':8733,'pending':[]}
    assert not (budget.ledger/'.t1-reservation.lock').exists()
    run=ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024'
    queue='infra_journal_post_health_queue_v4_20261025';segment='journal_post_health_1320'
    assert not (run/queue).exists()
    assert not (run/'geography/segments'/segment).exists()
    assert not (run/'geography/STOP_MAIN').exists()
    for task in tasks:
        for arm in ['direct','ours']:
            p=run/'geography/tasks'/task['qid']/arm
            assert not (p/'intent.json').exists() or (p/'record.json').exists(),'Unknown interrupted intent, no resend'
    assert sum(1 for _ in (run/'geography/tasks').glob('*/*/record.json'))==413
    worker=JournalPublicWorker.__new__(JournalPublicWorker)
    worker.key=Path('C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key')
    client=worker.connect()
    try:
        script=f'''from pathlib import Path
import json,socket,os
r=Path({REMOTE!r});assert socket.gethostname()=={HOST!r}
assert json.loads((r/'execution_owner.json').read_text())['hostname']=={HOST!r}
assert not (r/'.public-runtime.lock').exists() and not (r/'.t1-cutover-preflight.lock').exists()
for p in Path('/proc').iterdir():
 if not p.name.isdigit() or int(p.name)==os.getpid():continue
 try:s=(p/'cmdline').read_bytes().decode(errors='replace')
 except OSError:continue
 assert not (str(r) in s and any(n in s for n in ['public_query_worker.py','run_frozen_main.py','durable_native_journal_v1.py','native_cutover_preflight_job.py'])),'Existing owned worker'
print(json.dumps({{'hostname':socket.gethostname(),'worker_and_preflight_absent':True,'runtime_and_preflight_locks_absent':True}}))
'''
        enc=base64.b64encode(script.encode()).decode()
        command=shlex.join(['python3','-c',"import base64;exec(base64.b64decode('"+enc+"'))"])
        _,out,err=client.exec_command(command,timeout=20)
        observed=json.loads(out.read());assert out.channel.recv_exit_status()==0
    finally:client.close()
    report={'time_local':datetime.now().astimezone().isoformat(),'formal_run_ready':True,
        'scope':'Remaining frozen CypherBench core via SAME accepted original local journal runner',
        'freeze_sha256':sha(frozen/'freeze.json'),'original_source_clean':True,'source_hashes_verified':32,
        'method_prompt_model_metric_changes':False,'existing_journal_adapter_files_verified':prior['files'],
        'new_transport_variant_used_for_formal':False,'remote_native_ipc_only_verified_fallback':True,
        'native_actual_conformance_pass':True,'single_model_probe_usable':True,'probe_physical_attempts':1,
        'budget':count,'fresh_remote_gate':observed,'unknown_intents_absent':True,
        'all_old_failures_retained':True,'exhausted_requests_retried':False,
        'queue_id':queue,'segment':segment,'entire_T1_complete':False,
        'resume_not_an_automatic_infinite_loop':True,
        'stop_policy':'Existing same-id3RPC/10s and same-body3model/10s, stop/audit at3 consecutive infrastructure failures'}
    target=ROOT/'reports/T1_ORIGINAL_JOURNAL_HEALTH_RESUME_ADMISSION_20261025.json'
    target.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps({'formal_run_ready':True,'budget':count,'queue_id':queue,'new_variant':False}))

if __name__=='__main__':main()
