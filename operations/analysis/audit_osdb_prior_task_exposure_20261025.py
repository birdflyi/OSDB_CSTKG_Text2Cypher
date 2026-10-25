"""Read-only inventory of explicitly scoped pre-existing OSDB task strata.

No model, database, network, source edits, or independent-evaluation admission.
Record presence is historical evaluation exposure, not successful generation.
"""
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path

ROOT = Path('D:/ch7')
BASE = ROOT / 'experiments/ch7_private/continuation_20261021'
STRATA = ['e5_tasks', 'e6_tasks', 'e7_blind_intake_v1/tasks',
          'e7_grounded_tasks_v1', 'e7_source_tasks_v1', 'e8_known_goal_transfer_v1']
OUT = ROOT / 'reports/osdb_prior_task_exposure_20261025.json'


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':')).encode()).hexdigest()


def main():
    runs = defaultdict(list)
    # Only immediate historical continuation run folders. Never read private
    # configuration files or import any historical runner/author code.
    for folder in sorted(BASE.iterdir()):
        manifest = folder / 'manifest.json'
        records = folder / 'records'
        if not folder.is_dir() or not manifest.is_file() or not records.is_dir():
            continue
        meta = read(manifest)
        digest = meta.get('task_sha256')
        if not digest:
            continue
        ids = set()
        arm_counts = Counter()
        receipts = []
        for record_path in sorted(records.rglob('*.json')):
            record = read(record_path)
            ids.add(str(record['id']))
            arm_counts[str(record['arm'])] += 1
            receipts.append({'path':str(record_path.relative_to(BASE)),
                             'sha256':sha(record_path)})
        runs[digest].append({'run':folder.name, 'manifest_sha256':sha(manifest),
                            'ids':ids, 'record_files':receipts,
                            'record_count_by_arm':dict(arm_counts)})

    inventory = []
    projects = defaultdict(lambda: {'task_positions':0, 'strata':[],
                                   'positions_with_historical_records':0})
    all_receipts = []
    question_owners = defaultdict(list)
    for stratum in STRATA:
        manifests = sorted((BASE / stratum).rglob('manifest.json'))
        assert manifests, stratum
        for manifest in manifests:
            spec = manifest.parent / 'specifications.json'
            meta = read(manifest)
            tasks = read(spec)
            assert isinstance(tasks, list)
            digest = sha(spec)
            assert digest == meta['task_sha256'], str(spec)
            ids = [str(t['id']) for t in tasks]
            assert len(set(ids)) == len(ids), str(spec)
            project = meta['project']
            assert all(t.get('project') == project for t in tasks)
            matched = runs.get(digest, [])
            evaluated = set().union(*(x['ids'] for x in matched)) if matched else set()
            assert evaluated.issubset(set(ids)), str(spec)
            # Hashes support exact identity checks; they are not semantic
            # equivalence or independent-goal certification.
            task_receipts = []
            for task in tasks:
                qhash = fingerprint(task.get('question'))
                if task.get('question'):
                    question_owners[(project, qhash)].append(
                        (stratum, fingerprint(task['id'])))
                task_receipts.append({'id_sha256':fingerprint(task['id']),
                                      'question_sha256':qhash,
                                      'semantic_goal_label_sha256':fingerprint(task.get('semantic_goal')),
                                      'historical_record_present':str(task['id']) in evaluated})
            inventory.append({'stratum':stratum, 'project':project,
                              'mode':meta['mode'], 'task_count':len(tasks),
                              'manifest_path':str(manifest.relative_to(BASE)),
                              'manifest_sha256':sha(manifest),
                              'specification_sha256':digest,
                              'schema_sha256':sha(manifest.parent/'shared_schema.json'),
                              'source_hashes_declared':meta.get('source_hashes'),
                              'source_files_reverified':False,
                              'positions_with_historical_records':len(evaluated),
                              'matched_historical_runs':[
                                  {k:v for k,v in x.items() if k not in ('ids','record_files')}
                                  for x in matched],
                              'original_limit_statement':meta.get('limits',meta.get('selection')),
                              'task_receipts':task_receipts})
            for run in matched:
                all_receipts.extend(run['record_files'])
            projects[project]['task_positions'] += len(tasks)
            projects[project]['strata'].append(stratum)
            projects[project]['positions_with_historical_records'] += len(evaluated)
    repeated = [{'project':project,'question_sha256':digest,'occurrences':len(owners),
                 'strata':sorted(set(x[0] for x in owners))}
                for (project,digest),owners in sorted(question_owners.items())
                if len(owners)>1]
    # Old queues are not resumed. Refuse a report if a historical input changed
    # during this read-only inventory rather than silently accepting a race.
    for item in inventory:
        manifest = BASE / item['manifest_path']
        assert sha(manifest) == item['manifest_sha256']
        assert sha(manifest.parent/'specifications.json') == item['specification_sha256']
        assert sha(manifest.parent/'shared_schema.json') == item['schema_sha256']
        for run in item['matched_historical_runs']:
            assert sha(BASE/run['run']/'manifest.json') == run['manifest_sha256']
    for receipt in all_receipts:
        assert sha(BASE/receipt['path']) == receipt['sha256']
    result = {'kind':'SCOPED_HISTORICAL_OSDB_TASK_EXPOSURE_INVENTORY',
              'time_local':datetime.now().astimezone().isoformat(),
              'scope':STRATA, 'task_packages':len(inventory),
              'task_positions':sum(x['task_count'] for x in inventory),
              'positions_with_historical_records':sum(x['positions_with_historical_records'] for x in inventory),
              'projects':dict(projects), 'inventory':inventory,
              'same_project_exact_question_repeats':repeated,
              'matched_record_file_receipts':all_receipts,
              'historical_inputs_stable_during_audit':True,
              'prediction_admitted':False, 'new_model_calls':0,
              'new_database_queries':0,
              'limits':[
                  'Scoped six historical strata, not exhaustive project/session exposure history.',
                  'Task positions and repeated variants are not independent task counts.',
                  'Record presence does not establish successful model inference or answer correctness.',
                  'Missing records do not establish unseen task, model non-exposure, or valid independence.',
                  'Exact question duplicates do not establish semantic equivalence or leakage.',
                  'Source hashes are historical declarations; graph/source bytes not reverified here.',
                  'No prior task is newly admitted to the independent T1 denominator by this inventory.',
                  'No scoring, raw question/answer publication, old queue restart, or budget scope change.']}
    OUT.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k not in
                      ('inventory','matched_record_file_receipts','same_project_exact_question_repeats')},
                     ensure_ascii=False))


if __name__ == '__main__':
    main()
