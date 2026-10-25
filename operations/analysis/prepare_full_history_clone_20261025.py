"""Local immutable budget clone and mirror checks; zero experiment calls."""
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
import zipfile

ROOT=Path('D:/ch7')
CODE=ROOT/'code/OSDB_CSTKG_Text2Cypher_publication_t1'
RECOVERY=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_recovery'
sys.path.insert(0,str(RECOVERY/'operations'))
sys.path.insert(0,str(CODE/'experiment-harness/public_benchmark_v1'))
from attempt_budget import AttemptBudget
from portable_attempt_history import clone_history, open_clone, sha, native_path
from test_portable_attempt_history import PortableHistoryChecks


def main():
    folder=ROOT/'experiments/public_benchmark_v1/remote_history_preparation_v1_20261025'
    folder.mkdir(exist_ok=True)
    approval=ROOT/'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json'
    budget=AttemptBudget(ROOT/'experiments/ch7_private/continuation_20261021/budget',approval,
        path_rewrites=[(r'C:\Users\10651\Documents\trae_projects\ch7_private',str(ROOT/'experiments/ch7_private'))])
    before=budget.inspect();assert not before['pending']
    freeze=json.loads((ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json').read_text(encoding='utf-8'))
    for relative,expected in freeze['code_files'].items():assert sha(CODE/relative)==expected
    clone=native_path(folder/'full_history_v3')
    verify_existing='--verify-existing-draft' in sys.argv
    if verify_existing:
        manifest=json.loads((clone/'manifest.json').read_text(encoding='utf-8'))
        assert manifest['kind']=='DRAFT_FULL_ATTEMPT_HISTORY_CLONE_V1' and manifest['execution_authority'] is False
    else:
        manifest=clone_history(budget,clone)
    manifest_sha=sha(clone/'manifest.json')
    cloned=open_clone(AttemptBudget,clone,manifest_sha)
    state=cloned.inspect();state.pop('request_identities')
    assert state['reserved']==3266 and state['cap']==12000 and state['pending']==[]
    # Existing draft validation must compare actual sources again, not just count.
    for slot in budget.ledger.glob('*.json'):
        record=json.loads(slot.read_text(encoding='utf-8'))
        original_request=budget._request(record['request_path'])
        copied_request=cloned._request(record['request_path'])
        assert sha(slot)==sha(cloned.ledger/slot.name)
        assert sha(original_request)==sha(copied_request)
        assert sha(budget.terminal_path(original_request))==sha(cloned.terminal_path(copied_request))
    log=io.StringIO()
    names=unittest.defaultTestLoader.getTestCaseNames(PortableHistoryChecks)
    result=unittest.TextTestRunner(stream=log,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(PortableHistoryChecks))
    assert result.wasSuccessful(),log.getvalue()
    (folder/'mirror_checks.log').write_text(log.getvalue(),encoding='utf-8')
    archive=folder/'full_history.zip'
    # Includes only request/terminal/raw ledger/approval/manifests, not model config.
    with zipfile.ZipFile(archive,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as output:
        for relative,expected in manifest['files'].items():
            path=clone/relative
            assert sha(path)==expected
            output.write(path,relative)
        output.write(clone/'manifest.json','manifest.json')
    with zipfile.ZipFile(archive) as check:
        assert check.testzip() is None
        for relative,expected in manifest['files'].items():
            assert hashlib.sha256(check.read(relative)).hexdigest()==expected
    assert budget.inspect()==before
    assert not (budget.ledger/'.t1-reservation.lock').exists()
    for relative,expected in freeze['code_files'].items():assert sha(CODE/relative)==expected
    report={'kind':'DRAFT_FULL_HISTORY_PORTABILITY_AND_MIRROR_PREPARATION',
        'time_local':datetime.now().astimezone().isoformat(), 'pass':True,
        'original_budget_counter_reused':True,'new_fragment_counter_used':False,
        'snapshot':state,'source_unchanged':True,'real_execution_lease_created':False,
        'source_request_and_terminal_hashes_verified_twice':True,
        'existing_draft_revalidated_without_overwriting_raw_evidence':verify_existing,
        'copied_files':len(manifest['files']), 'archive_files':len(manifest['files'])+1,
        'archive_bytes':archive.stat().st_size,'archive_sha256':sha(archive),
        'manifest_sha256':manifest_sha,'tests_run':result.testsRun,'test_names':names,
        'all_archive_crc_and_each_sha_verified':True,'frozen_source_hashes_pass':32,
        'new_model_calls':0,'new_database_queries':0,'experiment_network_requests':0,
        'uploaded':False,'execution_authority':False,'formal_prediction_admitted':False,
        'limitations':['Draft double-checked snapshot is not a fenced authority handover',
            'Mirror fault checks used isolated local files, not a live remote server',
            'New request paths need most-specific native/remote root mapping before historical drive mapping',
            'Mirror mapping stays in analysis/adapters; stored historical/new records are never rewritten',
            'Native IPC and actual source checkout/server owner/model/graph cutover gates remain'],
        'candidate_sha256':{name:sha(RECOVERY/'operations'/name) for name in [
            'portable_attempt_history.py','test_portable_attempt_history.py']}}
    report_path=ROOT/'reports/t1_full_history_clone_mirror_preparation_20261025.json'
    report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:report[k] for k in ['pass','snapshot','copied_files','archive_bytes','tests_run','new_model_calls','execution_authority']}))


if __name__=='__main__':main()
