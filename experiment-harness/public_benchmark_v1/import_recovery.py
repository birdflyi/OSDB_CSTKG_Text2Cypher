"""Admission for read-only verification of a previously timed-out import."""
import hashlib
import json


def recovery_evidence(folder, prior_output, graph, reason):
    """Admit a read-only re-verification, never a partial-import continuation."""
    folder, prior_output = folder.resolve(), prior_output.resolve()
    if not reason.strip() or folder not in prior_output.parents:
        raise ValueError('Documented recovery must refer to this owned graph folder')
    started = json.loads((folder / 'import_started.json').read_text())
    if started != {'nodes': len(graph['entities']), 'edges': len(graph['relations'])}:
        raise ValueError('Prior import identity counts differ')
    if (folder / 'graph_validation.json').exists():
        raise ValueError('Validated store does not require partial-import recovery')
    failure_path = prior_output / 'failure.json'
    failure = json.loads(failure_path.read_text())
    stopped = json.loads((prior_output / 'process_stopped.json').read_text())
    if stopped.get('owned_process_stopped') is not True:
        raise ValueError('Prior database shutdown not certified')
    if 'TransactionTimedOut' not in failure.get('error', ''):
        raise ValueError('Prior failure requires a separate recovery design')
    return {'kind': 'READ_ONLY_EXISTING_IMPORT_VERIFICATION', 'reason': reason,
            'prior_output': str(prior_output),
            'prior_failure_sha256': hashlib.sha256(failure_path.read_bytes()).hexdigest(),
            'prior_counts': started, 'no_import_or_repair_writes': True,
            'verification_timeout_seconds': 600, 'formal_query_timeout_seconds': 60}

