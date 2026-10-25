"""Finite authorized-host preflight, NO model calls or formal prediction loop."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import zipfile

from local_native_worker import LocalNativePublicWorker, ROOT, HOST
from frozen_path_resolver import FrozenEvidenceResolver
from portable_attempt_history import open_clone


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_text(encoding='utf-8'))


def extract(archive,destination,expected):
    assert sha(archive)==expected,'Transferred archive differs'
    destination.mkdir(exist_ok=False)
    with zipfile.ZipFile(archive) as source:
        for item in source.infolist():
            target=(destination/item.filename).resolve()
            assert destination.resolve() in target.parents and not item.filename.startswith('/'),'Unsafe archive path'
            assert (item.external_attr>>16)&0o170000 != 0o120000,'No symlinks in artifact'
        assert source.testzip() is None
        source.extractall(destination)


def run(base):
    assert base.parent==ROOT and socket.gethostname()==HOST
    assert read(ROOT/'execution_owner.json')['hostname']==HOST
    assert not (ROOT/'.public-runtime.lock').exists(),'Native worker already occupied'
    spec=read(base/'spec.json')
    fence=ROOT/'.t1-cutover-preflight.lock'
    fence_record={'pid':os.getpid(),'hostname':HOST,'base':str(base)}
    with fence.open('x') as stream:json.dump(fence_record,stream)
    worker=None;started=time.monotonic();error=None;result={}
    try:
        for name,expected in spec['operations_sha256'].items():
            assert name.endswith('.py') and Path(name).name==name
            assert sha(base/name)==expected,'Operational executable differs'
        assert sha(base/'source.bundle')==spec['source_bundle_sha256']
        subprocess.run(['git','-c','core.autocrlf=false','clone','--branch','codex/ch7-t1-public-benchmark',
                        str(base/'source.bundle'),str(base/'source')],check=True)
        source=base/'source'
        assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==spec['frozen_commit']
        assert subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
        extract(base/'packet.zip',base/'packet',spec['packet_sha256'])
        extract(base/'full_history.zip',base/'history',spec['history_sha256'])
        sys.path.insert(0,str(source/'experiment-harness/public_benchmark_v1'))
        import run_frozen_main as original
        from attempt_budget import AttemptBudget
        from entity_candidates import CANDIDATE_NOTICE
        freeze_path=base/'packet/control/freeze.json'
        assert sha(freeze_path)==spec['freeze_sha256']
        resolver=FrozenEvidenceResolver(base/'packet',freeze_path)
        original.sha=resolver.sha  # File access only; original check logic unchanged.
        freeze,admission,tasks=original.check_admission(freeze_path,base/'packet/control/admission.json',
                                                base/'packet/data',source,'geography')
        history=open_clone(AttemptBudget,base/'history',spec['history_manifest_sha256'])
        count=history.inspect();count.pop('request_identities')
        assert count=={'reserved':3266,'cap':12000,'remaining':8734,'pending':[]}
        fixture=read(base/'fixture.json')
        assert sha(base/'fixture.json')==spec['fixture_sha256']
        assert fixture['graph']=='geography' and fixture['scope']=='ALREADY_SAVED_CONTEXT_TRANSPORT_ONLY'
        worker=LocalNativePublicWorker(None,'geography',base/'native_stderr.log',freeze['remote_worker'])
        expected_graph=admission['admitted_graphs']['geography']
        # Exact graph identity compared with the original frozen admission evidence.
        assert worker.ready['graph_sha256']==fixture['graph_sha256']==expected_graph['graph_sha256']
        candidates=worker.request({'kind':'candidates','question':fixture['question']})
        assert candidates['candidates']==fixture['saved_lookup']['candidates']
        context=('Graph schema:\n'+json.dumps(fixture['schema'],ensure_ascii=False)+
            '\nQuestion:\n'+fixture['question']+'\n'+CANDIDATE_NOTICE+
            '\nCandidates:\n'+json.dumps(candidates['candidates'],ensure_ascii=False))
        assert context.encode()==fixture['saved_context'].encode(),'Candidate context bytes differ'
        before=worker.execute('MATCH (n) RETURN count(n) AS n',{})
        denied=worker.execute('CREATE (n:NativePreflightMustBeDenied)',{})
        after=worker.execute('MATCH (n) RETURN count(n) AS n',{})
        assert denied['status']=='EXECUTION_ERROR'
        assert before['rows']==after['rows']==[[worker.ready['graph_validation']['nodes']]]
        result={'pass':True,'frozen_admission_logic_pass':True,'tasks_in_graph':len(tasks),
            'all_frozen_code_data_and_evidence_hashes_pass':True,'source_git_clean':True,
            'original_budget_counter_snapshot':count,'native_dependencies_verified':len(freeze['remote_worker']['files']),
            'graph':'geography','graph_sha256':worker.ready['graph_sha256'],
            'same_seen_context_qid':fixture['qid'],'candidate_context_exact_bytes':True,
            'candidate_context_bytes':len(context.encode()),'candidate_count':len(candidates['candidates']),
            'write_guard_denied':True,'node_count_before_after':before['rows'],
            'native_ipc_requests':worker.sequence,'private_model_configuration_uploaded':False}
    except BaseException as exc:
        error=exc
        result={'pass':False,'error_type':type(exc).__name__,'error':str(exc)[:700]}
    finally:
        if worker:worker.close()
        ipc=base/'native_ipc'
        closed=(not ipc.exists() or ((ipc/'closed.json').exists() and
                read(ipc/'closed.json')['native_pid']==read(ipc/'owner.json')['native_pid']))
        if worker is not None:closed=closed and worker.process.poll() is not None
        lock_absent=not (ROOT/'.public-runtime.lock').exists()
        result.update(kind='ACTUAL_AUTHORIZED_NATIVE_LOCAL_IPC_PREFLIGHT',
            hostname=HOST,seconds=time.monotonic()-started,new_model_calls=0,
            formal_predictions=0,execution_authority=False,
            native_child_closed=closed,public_runtime_lock_absent=lock_absent,
            original_native_finally_cleanup=bool(closed and lock_absent),manual_runtime_lock_unlink=False,
            dataset_or_method_tuning=False,operations_sha256=spec['operations_sha256'])
        result['pass']=bool(result.get('pass') and closed and lock_absent)
        (base/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        if closed and lock_absent and read(fence)==fence_record:fence.unlink()
    if error:raise error


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--base',type=Path,required=True)
    run(parser.parse_args().base)
