"""Offline fault checks plus read-only real-ledger/frozen-source invariants."""
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest

ROOT = Path('D:/ch7')
CANONICAL = ROOT / 'code/OSDB_CSTKG_Text2Cypher_publication_t1'
RECOVERY = ROOT / 'code/OSDB_CSTKG_Text2Cypher_t1_recovery'
sys.path.insert(0, str(CANONICAL / 'experiment-harness/public_benchmark_v1'))
sys.path.insert(0, str(RECOVERY / 'operations'))
from attempt_budget import AttemptBudget
from test_leased_attempt_fragment import CutoverBudgetChecks


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def snapshot():
    ledger = ROOT / 'experiments/ch7_private/continuation_20261021/budget'
    approval = ROOT / 'state/MODEL_CALL_BUDGET_APPROVAL_20261024.json'
    budget = AttemptBudget(ledger, approval, path_rewrites=[
        (r'C:\Users\10651\Documents\trae_projects\ch7_private',
         str(ROOT / 'experiments/ch7_private'))])
    state = budget.inspect()
    state.pop('request_identities')
    slots = [[int(p.stem), sha(p)] for p in sorted(ledger.glob('*.json'), key=lambda p: int(p.stem))]
    state['ledger_seal'] = hashlib.sha256(json.dumps(slots).encode()).hexdigest()
    state['real_budget_lock_present'] = (ledger / '.t1-reservation.lock').exists()
    freeze_path = ROOT / 'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
    freeze = json.loads(freeze_path.read_text(encoding='utf-8'))
    for relative, expected in freeze['code_files'].items():
        assert sha(CANONICAL / relative) == expected, 'Frozen source changed'
    state.update(freeze_sha256=sha(freeze_path), verified_frozen_code_files=len(freeze['code_files']))
    return state


def main():
    before = snapshot()
    assert not before['pending'] and not before['real_budget_lock_present']
    names = unittest.defaultTestLoader.getTestCaseNames(CutoverBudgetChecks)
    log = io.StringIO()
    result = unittest.TextTestRunner(stream=log, verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(CutoverBudgetChecks))
    after = snapshot()
    assert before == after, 'Real ledger, lease or frozen source changed during offline checks'
    assert result.wasSuccessful(), log.getvalue()
    report = {
        'kind': 'OFFLINE_EXECUTION_LEASE_AND_CUMULATIVE_FRAGMENT_CHECKS',
        'time_local': datetime.now().astimezone().isoformat(),
        'pass': result.wasSuccessful(), 'tests_run': result.testsRun, 'test_names': names,
        'actual_before': before, 'actual_after': after,
        'actual_ledger_unchanged': True, 'real_execution_lease_created': False,
        'synthetic_tests_only': True, 'model_and_sleep_functions_stubbed_in_transport_tests': True,
        'new_model_calls': 0, 'new_database_queries': 0, 'experiment_network_requests': 0,
        'uploaded': False, 'formal_prediction_admitted': False,
        'source_sha256': {name: sha(RECOVERY / 'operations' / name) for name in [
            'leased_attempt_fragment.py', 'test_leased_attempt_fragment.py']},
        'remaining_gates': [
            'Actual Windows fence and remote owner admission, with sealed live history',
            'Result/request/terminal mirror plus cumulative ledger synchronization and release proof',
            'Original clean Git checkout on server: 32-file packet alone is insufficient',
            'Allowlisted evidence paths and preserved frozen hashes',
            'Native local IPC conformance and candidate context byte fidelity',
            'Server model/graph/owner/process/lock checks and bounded connectivity acceptance',
            'Interrupted generation and output synchronization, no unknown-request resend',
            'Independent operational cutover admission before any prediction'],
        'limitations': [
            'Synthetic baseline uses 3266 entries, not a remote allocation of the real ledger',
            'No multi-host deployment, native worker, filesystem failure or distributed mirror test',
            'Lease release evidence is an external admission/reconciliation requirement',
            'Candidate operational classes are not imported by the current formal runner']}
    report_path = ROOT / 'reports/t1_execution_lease_fragment_checks_20261025.json'
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    (ROOT / 'reports/t1_execution_lease_fragment_checks_20261025.log').write_text(log.getvalue(), encoding='utf-8')
    print(json.dumps({'pass': True, 'tests': result.testsRun,
                      'real_reserved': after['reserved'], 'remaining': after['remaining'],
                      'pending': after['pending'], 'formal_prediction_admitted': False,
                      'new_model_calls': 0, 'report': str(report_path)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
