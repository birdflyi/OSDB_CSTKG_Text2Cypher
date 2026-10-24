"""Build the reviewed core freeze; never submit model calls or infer readiness.

An explicit evidence review must already certify all seven graph references.
Source and evidence hashes are checked again by the execution runner.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from dev_preparation import DIRECT_INSTRUCTIONS
from schema_services import PLAN_INSTRUCTIONS, VERSION
from run_frozen_main import GRAPHS, SOURCE_FOLDER, REQUIRED_DATA, sha
from model_transport import immutable_json


def build(args):
    root = Path(__file__).resolve().parents[2]
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip():
        raise ValueError('Commit reviewed source before freezing')
    review = json.loads(args.review.read_text(encoding='utf-8'))
    if (review.get('core_ready') is not True or review.get('formal_predictions_before_freeze') != 0
        or set(review.get('graph_reviews', {})) != set(GRAPHS)):
        raise ValueError('Full graph and core evidence review is not ready')
    evidence = {str(args.review.resolve()): sha(args.review)}
    admitted = {}
    for graph, count in GRAPHS.items():
        item = review['graph_reviews'][graph]
        summary_path = Path(item['summary'])
        summary = json.loads(summary_path.read_text(encoding='utf-8'))
        checks = list((summary_path.parent / 'references').glob('*.json'))
        failures = [json.loads(p.read_text(encoding='utf-8'))['qid'] for p in checks
                    if json.loads(p.read_text(encoding='utf-8'))['reference_answer_match'] != 1]
        certified = item.get('certified_reference_issues', [])
        if (summary['graph'] != graph or summary['checked'] != count or len(checks) != count
            or summary.get('diagnostic_subset') is not False
            or not summary['graph_validation']['all_properties_labels_and_endpoints_match']
            or set(failures) != set(certified) or len(certified) != count - summary['passed']):
            raise ValueError('Incomplete reference evidence for ' + graph)
        if sha(args.data / ('upstream/graphs/simplekg/' + graph + '_simplekg.json')) != summary['graph_sha256']:
            raise ValueError('Reviewed graph does not match the local frozen asset')
        evidence[str(summary_path.resolve())] = sha(summary_path)
        for path in checks:
            evidence[str(path.resolve())] = sha(path)
        if failures:
            path = Path(item['issue_audit'])
            audit = json.loads(path.read_text(encoding='utf-8'))
            for witness in audit['witnesses']:
                if witness['qid'] not in failures:
                    raise ValueError('Audit contains an unexpected task')
                if not (witness.get('certified') is True or witness.get('witness', {}).get('certified') is True):
                    raise ValueError('Reference issue lacks an independent witness')
            if {r['qid'] for r in audit['witnesses']} != set(failures):
                raise ValueError('Reference issue audit incomplete')
            evidence[str(path.resolve())] = sha(path)
        admitted[graph] = {'graph_sha256': summary['graph_sha256'], 'reference_tasks': count,
                           'strict_agreement': summary['passed'], 'certified_reference_issues': certified}
    for value in review['required_evidence_files']:
        path = Path(value).resolve()
        evidence[str(path)] = sha(path)
    settings = json.loads(args.model_config.read_text(encoding='utf-8'))
    names = ['public_query_worker.py', 'public_read_guard.py', 'public_graph_runtime.py',
             'entity_candidates.py', 'answer_scoring.py', 'smoke_train_database.py',
             'schema_identity.py', 'import_recovery.py', 'model_transport.py', 'attempt_budget.py']
    code = {str(p.relative_to(root)).replace('\\', '/'): sha(p)
            for p in (root / SOURCE_FOLDER).glob('*.py')}
    data_paths = set(REQUIRED_DATA) | {'upstream/graphs/simplekg/' + g + '_simplekg.json' for g in GRAPHS}
    freeze = {'kind': 'CYPHERBENCH_CORE_FORMAL_FREEZE_V1', 'code_commit': subprocess.check_output(
               ['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(), 'service_version': VERSION,
              'graphs': GRAPHS, 'graph_order': list(GRAPHS), 'code_files': code,
              'data_files': {p: sha(args.data / p) for p in sorted(data_paths)}, 'evidence_files': evidence,
              'remote_worker': {'code_folder': 'public_query_core_v1_code',
                                'files': {n: code[SOURCE_FOLDER+n] for n in names}},
              'endpoint_sha256': hashlib.sha256(settings['endpoint'].encode()).hexdigest(),
              'model_settings': {k: settings[k] for k in ['model', 'temperature', 'top_p', 'reasoning_effort',
                                                         'max_tokens', 'read_timeout_seconds']},
              'prompt_sha256': {'direct': hashlib.sha256(DIRECT_INSTRUCTIONS.encode()).hexdigest(),
                                'ours': hashlib.sha256(PLAN_INSTRUCTIONS.encode()).hexdigest()},
              'protocol': {'feedback': 'at most one format/compile/execution repair per arm; no gold feedback',
                           'primary': 'full2348 official EX micro; Ours final minus Direct final',
                           'direct_first': 'reuse first view; not a second sample',
                           'controls': 'fixed-plan full-detail and always-sufficient-record policy share planner',
                           'coverage': 'all2348; refusal/error/truncation not correct; never silently drop tasks',
                           'resource_limits': {'query_seconds': 60, 'explain_seconds': 30,
                                               'max_rows': 1000000, 'max_result_bytes': 67108864},
                           'bootstrap': {'replicates': 10000, 'seed': 20261024,
                                         'unit': 'within-graph official template family; fixed graph weights'},
                           'reference_issues': 'official primary unchanged; frozen issue flags auxiliary only'},
              'budget': {'cumulative_cap': 12000, 'historical_attempts_before_formal': review['reserved_attempts'],
                         'scope': 'CypherBench core including development, feedback, failures and transport retries',
                         'retry_wait_seconds': 10, 'max_same_body_attempts': 3,
                         'consecutive_infrastructure_failure_stop': 3, 'monetary_price': 'unknown'}}
    args.output.mkdir(parents=True, exist_ok=False)
    immutable_json(args.output / 'freeze.json', freeze)
    immutable_json(args.output / 'admission.json', {'kind': 'CYPHERBENCH_CORE_ADMISSION',
                   'formal_run_ready': True, 'freeze_sha256': sha(args.output/'freeze.json'),
                   'admitted_graphs': admitted, 'entire_T1_complete': False})
    print(json.dumps({'state': 'CORE_FREEZE_CREATED', 'graphs': len(admitted), 'tasks': sum(GRAPHS.values()),
                      'source': freeze['code_commit'], 'model_calls': 0}), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    for field in ['data', 'review', 'model-config', 'output']:
        p.add_argument('--'+field, type=Path, required=True)
    build(p.parse_args())
