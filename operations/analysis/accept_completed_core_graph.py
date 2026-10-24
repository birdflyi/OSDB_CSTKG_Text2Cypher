"""Read-only acceptance of a completed graph; no model, DB or method changes."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def accept(run, data, freeze, graph, queue_state='infra_resume_queue_v1'):
    digest = sha(freeze)
    tasks = [json.loads(s) for s in (data/'runtime/test.jsonl').read_text(encoding='utf-8').splitlines() if s.strip()]
    ids = {t['qid'] for t in tasks if t['graph'] == graph}
    score_path = run/graph/'scores'/f'{graph}.json'
    rows = json.loads(score_path.read_text(encoding='utf-8'))
    expected = {(qid, arm) for qid in ids for arm in ('direct', 'ours')}
    actual = [(r['qid'], r['arm']) for r in rows]
    assert len(actual) == len(expected) and set(actual) == expected
    assert all(r['freeze_sha256'] == digest for r in rows)
    summary = json.loads((run/queue_state/f'{graph}_completed.json').read_text())
    assert summary['state'] == 'COMPLETE' and summary['remaining_paths'] == 0
    for row in rows:
        original = json.loads((run/graph/'tasks'/row['qid']/row['arm']/'record.json').read_text(encoding='utf-8'))
        scored = json.loads(json.dumps(row))
        for view in scored['views']:
            del view['score']
        for key in ('fixed_plan_full_detail_score', 'simple_safe_policy'):
            scored.pop(key, None)
        assert original == scored, 'Scoring changed the saved prediction'
    statuses = Counter(r['arm']+':'+r['views'][-1]['outcome']['status'] for r in rows)
    pair = {qid:{} for qid in ids}
    methods = {}
    for arm in ('direct', 'ours'):
        subset = [r for r in rows if r['arm'] == arm]
        for label, index in ([('Direct',0),('Direct+feedback',-1)] if arm == 'direct' else [('Ours',-1)]):
            correct = sum(r['views'][index]['score']['ex'] for r in subset)
            returned = sum(r['views'][index]['score']['returned_answer'] for r in subset)
            methods[label] = dict(n=len(ids), correct=correct, returned=returned,
                                  ex_percent=100*correct/len(ids))
        for row in subset:
            pair[row['qid']][arm] = row['views'][-1]['score']['ex']
    ours = [r for r in rows if r['arm'] == 'ours']
    executed = [r for r in ours if r['views'][-1]['outcome']['status'] == 'EXECUTED']
    raw_disagreement = [r['qid'] for r in executed
        if r['views'][-1]['outcome']['rows'] != r['fixed_plan_full_detail']['rows']]
    control_status = Counter(r.get('fixed_plan_full_detail', r['views'][-1]['outcome'])['status'] for r in ours)
    methods['Full-detail(shared-plan)'] = dict(n=len(ids),
        correct=sum(r['fixed_plan_full_detail_score']['ex'] for r in ours),
        returned=sum(r['fixed_plan_full_detail_score']['returned_answer'] for r in ours))
    terminals = [json.loads(p.read_text(encoding='utf-8'))
                 for p in (run/graph/'segments').glob('*/model_calls/physical_*/terminal.json')]
    assert len(terminals) == len({t['budget_slot'] for t in terminals})
    token_totals = Counter()
    for t in terminals:
        usage = (t.get('body') or {}).get('usage') or {}
        for key in ('prompt_tokens','completion_tokens','total_tokens'):
            if type(usage.get(key)) is int:
                token_totals[key] += usage[key]
    return dict(kind='COMPLETED_GRAPH_ACCEPTANCE', timestamp_utc=datetime.now(timezone.utc).isoformat(),
        graph=graph, n=len(ids), terminal_primary_paths=len(rows), freeze_sha256=digest,
        score_path=str(score_path), score_sha256=sha(score_path),
        all_frozen_task_arm_ids_present_once=True, saved_predictions_unchanged_by_scoring=True,
        final_status_counts=dict(statuses), methods=methods,
        descriptive_pairs=dict(ours_only=sum(p['ours']==1 and p['direct']==0 for p in pair.values()),
            direct_only=sum(p['direct']==1 and p['ours']==0 for p in pair.values()),
            both_correct=sum(p['ours']==1 and p['direct']==1 for p in pair.values()),
            neither_correct=sum(p['ours']==0 and p['direct']==0 for p in pair.values())),
        fixed_plan_control_status_counts=dict(control_status),
        executed_plan_raw_row_disagreement_qids=raw_disagreement,
        physical_model_attempts=len(terminals), known_token_sums=dict(token_totals),
        attempts_with_unknown_usage=sum(not isinstance((t.get('body') or {}).get('usage'),dict) for t in terminals),
        monetary_cost='unknown; unit price unavailable',
        unknown_original_wrapper_clocks=sum(r.get('model_and_execution_wall_seconds') is None for r in rows),
        full2348_conclusion_ready=False, entire_T1_complete=False,
        interpretation='One completed graph only. Do not tune the frozen method from these results.')


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('run','data','freeze','output'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--graph',required=True)
    p.add_argument('--queue-state',default='infra_resume_queue_v1')
    a=p.parse_args()
    result=accept(a.run,a.data,a.freeze,a.graph,a.queue_state)
    a.output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False))
