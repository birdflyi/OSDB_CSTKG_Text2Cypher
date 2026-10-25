"""Verify and aggregate all frozen CypherBench core results offline.

Use the official saved scores. Do not re-score with relaxed criteria, call a
model/database, change any task/method, or claim the whole T1 is complete.
"""
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path

ROOT = Path('D:/ch7')
RUN = ROOT/'experiments/public_benchmark_v1/core_main_v1_20261024'
DATA = ROOT/'experiments/public_benchmark_v1/cypherbench'
GRAPHS = {'flight_accident':189,'nba':270,'fictional_character':385,
          'company':347,'geography':366,'movie':401,'politics':390}


def read(p):
    return json.loads(p.read_text(encoding='utf-8'))


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    freeze = ROOT/'experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json'
    audit_path = ROOT/'reports/t1_core_integrity_cost_live_20261024.json'
    audit = read(audit_path)
    ledger = read(ROOT/'reports/t1_all_attempt_ledger_reconciliation_20261025.json')
    complete = RUN/'infra_journal_quota_recovery_queue_v5_20261025/queue_complete.json'
    assert read(complete)['state'] == 'CORE_COMPLETE'
    assert audit['full2348_score_ready'] and audit['resumed_queue_complete']
    assert audit['terminal_primary_paths'] == audit['dispatched_primary_paths'] == 4696
    assert ledger['pending'] == [] and ledger['ledger_unchanged_during_audit']
    assert ledger['formal_physical_attempts'] == audit['physical_model_attempts'] == 4756
    assert ledger['all_formal_attempts_attributed_once']
    tasks = [json.loads(x) for x in (DATA/'runtime/test.jsonl').read_text(encoding='utf-8').splitlines()]
    expected = {t['qid']:t['graph'] for t in tasks}
    assert len(tasks) == len(expected) == 2348
    flags_path = DATA/'evaluator/reference_flags_v1.jsonl'
    flags = [json.loads(x) for x in flags_path.read_text(encoding='utf-8').splitlines()]
    excluded = {x['qid'] for x in flags}
    assert len(flags) == len(excluded) == 29 and excluded <= expected.keys()
    assert all(x['formal_predictions_at_flagging'] == 0 and x['primary_score_unchanged'] for x in flags)
    groups_path = DATA/'evaluator/task_groups_v1.jsonl'
    groups = [json.loads(x) for x in groups_path.read_text(encoding='utf-8').splitlines()]
    assert {x['qid'] for x in groups} == expected.keys()
    cells = {k:[] for k in ('Direct','Direct+feedback','Ours','Full-detail(shared-plan)')}
    pairs = Counter()
    costs = {}
    receipts = []
    for graph,n in GRAPHS.items():
        tag = 'flight' if graph == 'flight_accident' else graph
        acceptance_path = ROOT/f'reports/t1_{tag}_formal_graph_acceptance_20261024.json'
        acceptance = read(acceptance_path)
        score = RUN/graph/'scores'/f'{graph}.json'
        before = sha(score)
        assert acceptance['score_sha256'] == before
        assert acceptance['all_frozen_task_arm_ids_present_once'] and acceptance['saved_predictions_unchanged_by_scoring']
        assert acceptance['freeze_sha256'] == sha(freeze)
        rows = read(score)
        assert len(rows) == 2*n
        assert {(x['qid'],x['arm']) for x in rows} == {
            (qid,arm) for qid,g in expected.items() if g == graph for arm in ('direct','ours')}
        paired = {}
        for row in rows:
            assert row['graph'] == graph and row['freeze_sha256'] == sha(freeze)
            qid = row['qid']
            paired.setdefault(qid,{})[row['arm']] = row['views'][-1]['score']['ex']
            labels = [('Direct',row['views'][0]['score']),('Direct+feedback',row['views'][-1]['score'])] if row['arm']=='direct' else [
                ('Ours',row['views'][-1]['score']),('Full-detail(shared-plan)',row['fixed_plan_full_detail_score'])]
            for label,s in labels:
                assert s['ex'] in (0,1) and (s['ex']==0 or s['returned_answer'])
                cells[label].append({'qid':qid,'graph':graph,'ex':s['ex'],'returned':bool(s['returned_answer'])})
        for p in paired.values():
            pairs['both_correct' if p['ours'] and p['direct'] else 'ours_only' if p['ours'] else 'direct_only' if p['direct'] else 'neither_correct'] += 1
        cost_path = ROOT/f'reports/t1_{tag}_route_cost_components_20261024.json'
        costs[graph] = read(cost_path)
        assert costs[graph]['n'] == n
        receipts.extend({'path':str(p),'bytes':p.stat().st_size,'sha256':sha(p)} for p in (score,acceptance_path,cost_path))
        assert sha(score) == before

    def metrics(rows):
        n = len(rows); correct = sum(x['ex'] for x in rows); returned = sum(x['returned'] for x in rows)
        return {'n':n,'correct':correct,'returned':returned,'wrong_returned':returned-correct,
                'ex_percent':100*correct/n,'coverage_percent':100*returned/n,
                'wrong_answer_percent':100*(returned-correct)/n}

    full = {name:metrics(rows) for name,rows in cells.items()}
    auxiliary = {name:metrics([x for x in rows if x['qid'] not in excluded]) for name,rows in cells.items()}
    assert all(m['n']==2348 for m in full.values())
    assert all(m['n']==2319 for m in auxiliary.values())
    assert all(abs(m['ex_percent']-audit['micro_ex_percent'][name])<1e-10 for name,m in full.items())
    assert full['Ours'] == full['Full-detail(shared-plan)']
    delta = full['Ours']['ex_percent']-full['Direct+feedback']['ex_percent']
    assert abs(delta-audit['primary_delta_percent_points']) < 1e-10
    interval = audit['primary_delta_interval']
    assert interval['replicates']==10000 and interval['seed']==20261024
    method_costs = {}
    for arm in ('direct','ours'):
        method_costs[arm] = {
            'physical_model_attempts':sum(c['per_arm'][arm]['physical_model_attempts'] for c in costs.values()),
            'known_tokens':{k:sum(c['per_arm'][arm]['known_usage'][k] for c in costs.values())
                            for k in ('prompt_tokens','completion_tokens','total_tokens')},
            'unknown_usage_attempts':sum(c['per_arm'][arm]['unknown_usage_attempts'] for c in costs.values())}
    assert sum(x['physical_model_attempts'] for x in method_costs.values()) == 4756
    assert sum(x['known_tokens']['total_tokens'] for x in method_costs.values()) == audit['known_token_sums']['total_tokens']
    result = {'kind':'COMPLETE_CYPHERBENCH_CORE_NOT_ENTIRE_T1','time_local':datetime.now().astimezone().isoformat(),
              'tasks':2348,'primary_paths':4696,'graphs':GRAPHS,'methods':full,
              'macro_ex_percent':audit['macro_ex_percent'],
              'primary_paired_delta_pp':delta,
              'primary_95_interval_pp':[100*interval['lower_95'],100*interval['upper_95']],
              'bootstrap':interval,'template_families':len({(x['graph'],x['family']) for x in groups}),
              'descriptive_pairs_ours_vs_direct_feedback':dict(pairs),
              'prefrozen_reference_auxiliary':{'n':2319,'excluded':29,
                  'flag_categories':dict(Counter(x['category'] for x in flags)),
                  'methods':auxiliary,'replaces_full_official_primary':False},
              'model_costs':method_costs,
              'summary_route_executions':sum(c['certified_disjoint_summary_executions'] for c in costs.values()),
              'executed_plan_control_raw_agreements':sum(c['executed_plan_control_comparison'].get('identical_intermediate_columns_rows',0) for c in costs.values()),
              'attempt_ledger':{k:ledger[k] for k in ('cumulative_reserved','approved_cap','remaining','pending','scope_counts','formal_physical_attempts','feedback_generation_receipts','extra_physical_retry_attempts','all_formal_attempts_attributed_once','physical_evidence_seal','prediction_evidence_seal')},
              'freeze_sha256':sha(freeze),'source_commit':read(freeze)['code_commit'],
              'all_saved_scores_unchanged_during_audit':True,'new_model_calls':0,'new_database_queries':0,
              'monetary_cost':'unknown','entire_T1_complete':False,
              'limits':['The prespecified interval includes zero; no confirmed EX advantage or equivalence conclusion.',
                        'No observed summary execution; no empirical summary savings evidence.',
                        'Same-plan controls reuse planner/model generations; not independent model baselines.',
                        'Known token totals exclude unknown usage; fees are unknown.',
                        'Control replay order/cache and unknown clocks prevent a full causal latency/cost claim.',
                        'Author baselines, complementary public blocks and independent OSDB tasks remain incomplete.']}
    for p in (freeze,audit_path,flags_path,groups_path,complete,ROOT/'reports/t1_all_attempt_ledger_reconciliation_20261025.json'):
        receipts.append({'path':str(p),'bytes':p.stat().st_size,'sha256':sha(p)})
    (ROOT/'reports/t1_completed_core_evidence_index_20261026.json').write_text(json.dumps(receipts,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (ROOT/'reports/t1_complete_cypherbench_core_20261026.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k not in ('limits','graphs')},ensure_ascii=True))


if __name__=='__main__':
    main()
