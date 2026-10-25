"""Prepare a local portability packet. NOT a deployment or execution admission.

No remote connections, no model/DB calls, no lease or quota allocation.
"""
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import zipfile

ROOT=Path('D:/ch7')
sys.path.insert(0,str(ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1/experiment-harness/public_benchmark_v1'))
from attempt_budget import AttemptBudget


def sha(data):return hashlib.sha256(data).hexdigest()
def read(path):return json.loads(path.read_text(encoding='utf-8'))


def prepare():
    freeze_path=ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
    admission=freeze_path.with_name('admission.json');freeze=read(freeze_path)
    approval=ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json'
    budget=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',approval,
        path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',str(ROOT/'experiments/ch7_private'))])
    before=budget.inspect();assert not before['pending']
    assert read(admission)['freeze_sha256']==sha(freeze_path.read_bytes())
    folder=ROOT/'experiments/public_benchmark_v1/remote_cutover_preparation_v1_20261025'
    folder.mkdir(exist_ok=False)
    sources=[];mapping={}
    def add(path,target,group,expected=None):
        path=Path(path).resolve()
        assert ROOT.resolve() in path.parents,'Only allowlisted D:/ch7 artifacts'
        assert path.suffix in ('.json','.jsonl','.py','.md')
        assert '.codex' not in path.parts and 'private' not in path.parts
        sources.append((path,target,group,expected))
    for relative,expected in freeze['code_files'].items():
        add(ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'/relative,'frozen_code/'+relative,'frozen_code',expected)
    for relative,expected in freeze['data_files'].items():
        add(ROOT/'experiments/public_benchmark_v1/cypherbench'/relative,'data/'+relative,'frozen_data',expected)
    for number,(original,expected) in enumerate(sorted(freeze['evidence_files'].items())):
        suffix=Path(original).suffix
        target=f'evidence/{number:04d}_{expected}{suffix}'
        add(original,target,'frozen_evidence',expected);mapping[original]=target
    for path,name in [(freeze_path,'freeze.json'),(admission,'admission.json'),(approval,'budget_approval.json')]:
        add(path,'control/'+name,'control')
    add(ROOT/'literature/CypherBench/official_source_20261023/cypherbench/metrics/execution_accuracy.py',
        'official_source/cypherbench/metrics/execution_accuracy.py','official_evaluator')
    run=ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024'
    records=sorted((run/'geography/tasks').glob('*/*/record.json'))
    assert len(records)==413
    for p in records:add(p,'resume/'+p.relative_to(run).as_posix(),'existing_geography_record')
    for p in sorted((run/'geography/shared_inputs').glob('*.json')):
        add(p,'resume/'+p.relative_to(run).as_posix(),'existing_shared_input')
    assert len({target for _,target,_,_ in sources})==len(sources)
    # Read current key ONLY to detect accidental credential inclusion. Do not copy,
    # print, serialize or hash the key. SSH private key is not read or copied.
    secret=read(Path('C:/Users/10651/.codex/private/ch7_llm/connection_deepseek_n24_131_20261024.json'))['api_key'].encode()
    assert secret
    manifest=[];counts=Counter();sizes=Counter();zip_path=folder/'packet.zip'
    with zipfile.ZipFile(zip_path,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as archive:
        for path,target,group,expected in sources:
            data=path.read_bytes();digest=sha(data)
            assert expected is None or expected==digest,'Original frozen artifact changed'
            if secret in data:raise ValueError('Credential material found; packet not admissible')
            archive.writestr(target,data)
            manifest.append({'target':target,'group':group,'bytes':len(data),'sha256':digest})
            counts[group]+=1;sizes[group]+=len(data)
        archive.writestr('control/evidence_path_map.json',json.dumps(mapping,indent=2).encode())
    with zipfile.ZipFile(zip_path) as archive:
        assert archive.testzip() is None
        for item in manifest:assert sha(archive.read(item['target']))==item['sha256']
    assert before==budget.inspect(),'Ledger changed while preparing packet'
    result={'kind':'LOCAL_REMOTE_CUTOVER_PACKET_PREPARATION_ONLY',
        'time_local':datetime.now().astimezone().isoformat(),'freeze_sha256':sha(freeze_path.read_bytes()),
        'packet_path':str(zip_path),'packet_sha256':sha(zip_path.read_bytes()),'packet_bytes':zip_path.stat().st_size,
        'file_counts':dict(counts),'uncompressed_bytes_by_group':dict(sizes),
        'source_file_count':len(manifest),'all_frozen_source_data_evidence_verified':True,
        'archive_crc_and_each_uncompressed_sha_verified':True,
        'baseline_budget_reserved':before['reserved'],'cumulative_cap':before['cap'],
        'remaining_cap':before['remaining'],'pending':before['pending'],
        'ledger_unchanged':True,'budget_execution_lease_created':False,
        'existing_terminal_paths_copied':413,'existing_geography_failures_preserved':True,
        'remaining_graph_order':['geography','movie','politics'],
        'model_config_or_API_key_copied':False,'current_API_key_absence_checked':True,
        'SSH_private_key_read_or_copied':False,'uploaded':False,'remote_process_started':False,
        'new_model_calls':0,'new_database_queries':0,'experiment_network_requests':0,
        'formal_prediction_admitted':False,
        'required_before_cutover':['Exclusive Windows+remote execution lease with stale/unknown owner fail-closed handling',
          'Cumulative budget authority and immutable remote-attempt mirror/reconciliation, never reset baseline',
          'Explicit evidence path resolver and offline attempt path resolver; original frozen files unchanged',
          'Native local IPC transport conformance against saved outputs; original guard/limits/source',
          'Exact authorized hostname/owner/process/lock and already-deployed graph identity',
          'Safe private config placement outside this packet; original endpoint/settings verification',
          'Interrupted-generation/unknown-result no-resend recovery and final record/cost synchronization',
          'Independent cutover admission before any generation; packet alone authorizes no experiment']}
    (folder/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    (folder/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:result[k] for k in ('source_file_count','file_counts','packet_bytes',
        'baseline_budget_reserved','remaining_cap','uploaded','formal_prediction_admitted')}))


if __name__=='__main__':prepare()
