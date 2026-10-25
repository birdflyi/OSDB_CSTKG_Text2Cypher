"""One explicit human-approved lock archive and fresh original-runner admission.

No model calls or database queries. No active executable changes or replay.
Never repeat this helper after an ambiguous archive result: inspect receipts.
"""
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys

ROOT = Path('D:/ch7')
CODE = ROOT / 'code/OSDB_CSTKG_Text2Cypher_publication_t1'
OPS = ROOT / 'code/OSDB_CSTKG_Text2Cypher_t1_recovery/operations'
sys.path.insert(0, str(CODE / 'experiment-harness/public_benchmark_v1'))
sys.path.insert(0, str(OPS))
from attempt_budget import AttemptBudget
from run_frozen_main import check_admission
from answer_scoring import load_official_comparator
from journal_public_worker_v2 import JournalPublicWorker, ROOT as REMOTE, HOST


def read(path):
    return json.loads(Path(path).read_bytes())


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def now():
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


def local_gate():
    frozen = ROOT / 'experiments/public_benchmark_v1/core_freeze_v1_20261024'
    data = ROOT / 'experiments/public_benchmark_v1/cypherbench'
    run = ROOT / 'experiments/public_benchmark_v1/core_main_v1_20261024'
    freeze, admission, tasks = check_admission(frozen / 'freeze.json', frozen / 'admission.json', data, CODE, 'geography')
    assert subprocess.check_output(['git', 'status', '--porcelain'], cwd=CODE) == b''
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=CODE).decode().strip() == freeze['code_commit']
    load_official_comparator(ROOT / 'literature/CypherBench/official_source_20261023')
    prior = read(ROOT / 'reports/T1_SSH_EXIT_STATUS_RECOVERY_ADMISSION_20261025.json')
    for name, expected in prior['files'].items():
        assert sha(OPS / name) == expected
    config = read('C:/Users/10651/.codex/private/ch7_llm/connection_deepseek_n24_131_20261024.json')
    assert hashlib.sha256(config['endpoint'].encode()).hexdigest() == freeze['endpoint_sha256']
    assert all(config[k] == freeze['model_settings'][k] for k in ['model', 'temperature', 'top_p', 'reasoning_effort'])
    budget = AttemptBudget(ROOT / 'experiments/ch7_private/continuation_20261021/budget',
                           ROOT / 'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json', path_rewrites=[
        (r'C:\Users\10651\Documents\trae_projects\ch7_private', str(ROOT / 'experiments/ch7_private'))])
    count = budget.inspect()
    count.pop('request_identities')
    assert count == {'reserved': 3566, 'cap': 12000, 'remaining': 8434, 'pending': []}
    assert not (budget.ledger / '.t1-reservation.lock').exists()
    assert not any(p for p in run.glob('*/tasks/*/*/intent.json') if not p.with_name('record.json').exists())
    assert sum(1 for _ in run.glob('*/tasks/*/*/record.json')) == 3092
    assert sum(1 for _ in (run / 'geography/tasks').glob('*/*/record.json')) == 710
    assert read(run / 'infra_journal_post_health_queue_v4_20261025/queue_stopped.json')['graph'] == 'geography'
    assert read(ROOT / 'reports/t1_disk_quota_audit_20261025/journal_evidence_acceptance.json')['failed_id634_published_request_dispatch_result_receipt_absent']
    queue = 'infra_journal_quota_recovery_queue_v5_20261025'
    segment = 'journal_quota_recovery_20261025'
    assert not (run / queue).exists()
    assert not any((run / g / 'segments' / segment).exists() for g in freeze['graphs'])
    assert not any((run / g / 'STOP_MAIN').exists() for g in freeze['graphs'])
    # Check actual processes, not old ACTIVE text or historical queue PIDs.
    script = "Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|ssh' -and $_.CommandLine -match 'core_journal_resume|run_frozen_main|public_query_worker|durable_native_journal' } | Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
    processes = subprocess.check_output(['powershell', '-NoProfile', '-Command', script], text=True).strip()
    assert not processes, 'Existing local formal runner: ' + processes
    return freeze, prior, count, queue, segment


def main():
    report_dir = ROOT / 'reports/t1_approved_quota_recovery_20261025'
    report_dir.mkdir(exist_ok=False)
    authorization_path = ROOT / 'state/T1_STALE_LOCK_ARCHIVE_APPROVAL_20261025.json'
    authorization = read(authorization_path)
    assert authorization['kind'] == 'EXPLICIT_HUMAN_ONE_TIME_STALE_LOCK_ARCHIVE_APPROVAL'
    freeze, prior, count, queue, segment = local_gate()
    (report_dir / 'archive_intent.json').write_text(json.dumps({
        'time_local': now(), 'human_approval_sha256': sha(authorization_path),
        'authorization': authorization, 'budget_before': count,
        'local_frozen_admission_passed': True, 'queue_id': queue, 'segment': segment,
        'new_model_calls': 0, 'new_database_queries': 0}, indent=2), encoding='utf-8')
    worker = JournalPublicWorker.__new__(JournalPublicWorker)
    worker.key = Path('C:/Users/10651/.codex/private/ch7_ssh_20261023/interactive_2491.key')
    client = worker.connect()
    try:
        remote_script = '''import json,socket,hashlib,os,stat,time
from pathlib import Path
r=Path(REMOTE);a=AUTHORIZATION;expected_files=FILES
assert r.resolve()==r and socket.gethostname()==HOST
assert json.loads((r/'execution_owner.json').read_text())['hostname']==HOST
assert a['authorized_host']==HOST
for pid in [296235,296237,296255]:assert not Path('/proc/'+str(pid)).exists(),'Old PID exists'
for p in Path('/proc').iterdir():
 if not p.name.isdigit() or int(p.name)==os.getpid():continue
 try:parts=(p/'cmdline').read_bytes().split(b'\\0')
 except OSError:continue
 if len(parts)<2:continue
 script=Path(parts[1].decode(errors='replace')).name
 assert script not in ['public_query_worker.py','durable_native_journal_v1.py','run_frozen_main.py','native_cutover_preflight_job.py'],'Existing experiment worker'
s=socket.socket();s.settimeout(2);assert s.connect_ex(('127.0.0.1',17688))!=0;s.close()
assert not (r/'.t1-cutover-preflight.lock').exists()
for n,digest in expected_files.items():
 assert hashlib.sha256((r/'public_query_core_v1_code'/n).read_bytes()).hexdigest()==digest
assert hashlib.sha256((r/'transport_journal_v1/durable_native_journal_v1.py').read_bytes()).hexdigest()==DAEMON_SHA
lock=Path(a['source_path']);dest=Path(a['archive_path'])
assert lock.parent.resolve()==r and dest.parent.resolve()==r
assert stat.S_ISREG(lock.lstat().st_mode) and not lock.is_symlink()
assert not os.path.lexists(dest)
raw=lock.read_bytes();assert hashlib.sha256(raw).hexdigest()==a['source_sha256']
value=json.loads(raw);assert value=={'pid':296237,'graph':'geography','state':'PUBLIC_QUERY_WORKER'}
old=r/'transport_journal_v1/geography_aeb759388f5b42a08a9e76aa0109bd06'
for suffix in ['request','dispatch','result','receipt']:assert not (old/('000634.'+suffix+'.json')).exists()
probe=r/('storage_resume_probe_20261025_'+str(time.time_ns())+'.bin');data=b'0'*65536
with probe.open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
assert probe.read_bytes()==data;probe.unlink()
# Exclusive hard-link + removal of the verified original preserves bytes and
# inode, and will not overwrite an independently created archive destination.
os.link(lock,dest,follow_symlinks=False)
assert lock.stat().st_ino==dest.stat().st_ino and lock.read_bytes()==raw==dest.read_bytes()
fd=os.open(str(r),os.O_DIRECTORY)
try:os.fsync(fd);lock.unlink();os.fsync(fd)
finally:os.close(fd)
assert not lock.exists() and dest.read_bytes()==raw
print(json.dumps({'hostname':HOST,'old_processes_absent':True,'port_idle':True,
 'source':str(lock),'archive':str(dest),'source_sha256':a['source_sha256'],
 'archive_sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),
 'archive_bytes':len(raw),'storage_fsync_readback_bytes':65536,
 'runtime_lock_absent':True,'archive_was_exclusive':True,'raw_journal_preserved':True,
 'remote_source_dependencies_unchanged':True,'new_model_calls':0,'new_database_queries':0}))
'''
        prefix = '\n'.join(['REMOTE=' + repr(REMOTE), 'HOST=' + repr(HOST),
                            'AUTHORIZATION=' + repr(authorization), 'FILES=' + repr(freeze['remote_worker']['files']),
                            'DAEMON_SHA=' + repr(prior['files']['durable_native_journal_v1.py'])]) + '\n'
        encoded = base64.b64encode((prefix + remote_script).encode()).decode()
        command = shlex.join(['python3', '-c', "import base64;exec(base64.b64decode('" + encoded + "'))"])
        _, out, err = client.exec_command(command, timeout=30)
        raw, error = out.read(), err.read()
        status = out.channel.recv_exit_status()
        if status:
            (report_dir / 'remote_error.json').write_text(json.dumps({
                'status': status, 'stdout': raw.decode('utf-8', 'replace'),
                'stderr': error.decode('utf-8', 'replace'),
                'automatic_retry': False, 'fresh_remote_audit_required': True}, indent=2), encoding='utf-8')
            raise RuntimeError('Archive gate failed; inspect preserved remote_error.json, no automatic retry')
        observed = json.loads(raw)
        (report_dir / 'archive_receipt.json').write_text(json.dumps(observed, indent=2), encoding='utf-8')
    finally:
        client.close()
    # Recheck all local admission requirements after the one authorized mutation.
    local_gate()
    report = {'time_local': now(), 'formal_run_ready': True, 'queue_id': queue, 'segment': segment,
              'freeze_sha256': sha(ROOT / 'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'),
              'budget': count, 'human_approval_sha256': sha(authorization_path),
              'source_hashes_verified': len(freeze['code_files']),
              'method_prompt_model_metric_changes': False, 'original_journal_files': prior['files'],
              'old_failures_and_quota_stop_preserved': True, 'no_exhausted_request_retried': True,
              'new_model_probes': 0, 'remote_gate': observed, 'entire_T1_complete': False,
              'stop_policy': 'Original bounded 3 attempts and 10-second wait; stop/audit on renewed infrastructure failures.'}
    (report_dir / 'admission.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({'formal_run_ready': True, 'queue_id': queue, 'segment': segment,
                      'budget': count, 'archive_sha256': observed['archive_sha256']}))


if __name__ == '__main__':
    main()
