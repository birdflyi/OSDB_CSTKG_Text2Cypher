"""Read-only, cross-version developer delivery. No model/DB/network calls.

Requires all three detached executions to have exited; never changes predictions,
old posthoc, budget, approval or frozen code. One audited SSH failure stays failed.
"""
from collections import Counter
from pathlib import Path
import csv,hashlib,json,sys

ROOT=Path('D:/ch7')
RUNS=[ROOT/('experiments/osdb_exposed_live_v%d_20261027'%i) for i in (1,2,3)]
PLAN=ROOT/'reports/osdb_exposed_regression_plan_v1_20261027'
OUT=ROOT/'reports/osdb_exposed_development_delivery_v1_20261027'
AUDIT=ROOT/'reports/osdb_persistent_ssh_v3_20261027'
sys.path.insert(0,str(RUNS[-1]/'operations/analysis'))
from osdb_response_boundary_dev import score_numeric
from osdb_budgeted_model_client_live_v2 import normalized_reply


def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,v):
    p=Path(p)
    with p.open('x',encoding='utf-8') as f:json.dump(v,f,ensure_ascii=False,indent=2,allow_nan=False)
def summary(values):
    known=sorted(v for v in values if type(v) in (int,float))
    def quant(p):
        if not known:return None
        z=(len(known)-1)*p;i=int(z)
        return known[i]+(known[min(i+1,len(known)-1)]-known[i])*(z-i)
    return {'known_n':len(known),'unknown_n':len(values)-len(known),
        'sum_known':sum(known) if known else None,'mean':sum(known)/len(known) if known else None,
        'p50':quant(.5),'p90':quant(.9)}


def build():
    assert all((r/'runner_exit.json').exists() for r in RUNS),'Background batch still active; do not finalize'
    assert all(read(r/'runner_exit.json')['failure'] is None for r in RUNS)
    assert read(RUNS[-1]/'runner_exit.json')['paths_completed']==48,'Incomplete required paths'
    assert read(RUNS[-1]/'campaign/scope_close.json')['pending']==[],'Unresolved physical attempt'
    provenance={};paths={};attempts={};versions={};rows=[]
    for version,r in enumerate(RUNS,1):
        f=read(r/'freeze.json');provenance[str(r/'freeze.json')]=sha(r/'freeze.json')
        assert sha(PLAN/'model_path_manifest.json')==f['manifest_sha256']
        assert sha(PLAN/'evaluator_only.json')==f['evaluator_sha256']
        assert all(sha(r/'operations/analysis'/n)==h for n,h in f['source_sha256'].items())
        assert all(sha(r/'experiment-harness/public_benchmark_v1'/n)==h for n,h in f['adapter46'].items())
        for p in (r/'campaign/paths').glob('*.json'):
            assert p.stem not in paths,'Path rerun across versions'
            paths[p.stem]=read(p);versions[p.stem]=version;provenance[str(p)]=sha(p)
        for p in (r/'campaign/attempts').glob('*/terminal.json'):
            t=read(p);slot=t['budget_slot'];assert slot not in attempts,'Duplicate physical billing'
            req=p.with_name('request.json');b=read(req)
            ledger=ROOT/'experiments/ch7_private/continuation_20261021/budget'/('%04d.json'%slot)
            l=read(ledger);assert Path(l['request_path']).resolve()==req.resolve() and sha(req)==l['request_sha256']
            assert l['request_id']==p.parent.name
            reply=normalized_reply(t,'ling-3.0-flash-n72-75')
            # New generic classification applies only to the analysis sidecar.
            status=('INFRASTRUCTURE_ERROR' if reply['transport_error'] else
                'MODEL_TRUNCATED' if reply['finish_reason']=='length' else 'MODEL_COMPLETED')
            attempts[slot]={'slot':slot,'version':version,'credential_owner':'louzehua' if version==3 else 'previous_private_configuration',
                'request_id':p.parent.name,'http_status':t.get('http_status'),
                'transport_error':t.get('transport_error'),'classification':status,
                'provider_wall_seconds':t.get('provider_wall_seconds'),'started_epoch':t.get('started_epoch'),
                'ended_epoch':t.get('ended_epoch'),'input_characters':sum(len(m['content']) for m in b['messages']),
                'output_characters':reply['response_characters'],**reply['usage'],
                'terminal_path':str(p),'terminal_sha256':sha(p),'request_sha256':sha(req),
                'money':None,'retry':False}
            provenance[str(ledger)]=sha(ledger);provenance[str(p)]=sha(p)
    failed=read(AUDIT/'audited_failed_path.json');key=failed['path_key']
    assert key not in paths;paths[key]=failed;versions[key]='2-audited-transport-failure'
    provenance[str(AUDIT/'reconciliation.json')]=sha(AUDIT/'reconciliation.json')
    manifest=read(PLAN/'model_path_manifest.json');refs={v['task_id']:v for v in read(PLAN/'evaluator_only.json')}
    assert len(paths)==48 and len(attempts)<=96
    assert set(paths)=={v['case_id']+'__'+v['arm'] for v in manifest}
    assert Counter(v['case_id'] for v in manifest)==Counter({k:2 for k in refs})
    for m in manifest:
        k=m['case_id']+'__'+m['arm'];p=paths[k];ref=refs[m['case_id']];expected=ref['numeric_reference_rows']
        final=p['final'];first=p.get('first_result',final)
        status=final['status'];raw_status=status
        if status=='INFRASTRUCTURE_ERROR' and p.get('records'):
            last=p['records'][-1];a=attempts[last['budget_slot']]
            if a['classification']=='MODEL_TRUNCATED':status='MODEL_TRUNCATED'
        native=p.get('native_control') or {};controls=p.get('shared_plan_controls') or {}
        detail=(controls.get('full_detail') or {}).get('result')
        row={'path_key':k,'case_id':m['case_id'],'graph':m['graph'],'arm':m['arm'],
            'execution_version':versions[k],'reference_kind':'numeric' if expected is not None else 'boundary_unadjudicated',
            'final_status':status,'original_final_status':raw_status,'numeric_EX':score_numeric(final,expected)['numeric_EX'],
            'first_numeric_EX':score_numeric(first,expected)['numeric_EX'],
            'complete_answer':final.get('status')=='EXECUTED' and final.get('complete') is True,
            'feedback_attempts':sum(a['request_id']==k+'__feedback' for a in attempts.values()),
            'feedback_withheld_by_stop':p.get('feedback_withheld_due_to_batch_stop'),
            'method_wall_seconds':p['costs'].get('method_path_wall_seconds'),
            'native_control_status':native.get('status'),'native_matches_ours':native.get('matches_ours'),
            'native_numeric_EX':score_numeric(native['result'],expected)['numeric_EX'] if native.get('result') else None,
            'full_detail_numeric_EX':score_numeric(detail,expected)['numeric_EX'] if detail else None,
            'simple_correct_same_as_ours':m['arm']=='ours' and 'simple_correct' in controls}
        rows.append(row)
    by_arm={}
    for arm in ('direct','ours'):
        group=[v for v in rows if v['arm']==arm];numeric=[v for v in group if v['reference_kind']=='numeric'];boundary=[v for v in group if v['reference_kind']!='numeric']
        assert len(group)==24 and len(numeric)==16 and len(boundary)==8
        by_arm[arm]={'planned_paths':24,'terminated_paths':24,'undispatched_paths':0,
            'complete_answers':sum(v['complete_answer'] for v in group),'numeric_N':16,
            'numeric_final_correct':sum(v['numeric_EX'] is True for v in numeric),
            'numeric_first_correct':sum(v['first_numeric_EX'] is True for v in numeric),
            'boundary_N':8,'boundary_status_counts':dict(Counter(v['final_status'] for v in boundary)),
            'boundary_correct_rejection_not_adjudicated':True,
            'status_counts':dict(Counter(v['final_status'] for v in group)),
            'method_wall_seconds':summary([v['method_wall_seconds'] for v in group]),
            'observed_cost_groups':{}}
        for key in sorted(set(g for k,p in paths.items() if k.endswith('__'+arm) for g in p['costs'].get('groups',{}))):
            groups=[p['costs'].get('groups',{}).get(key) for k,p in paths.items() if k.endswith('__'+arm)]
            columns=sorted(set(c for g in groups if g for c in g if c.endswith('_seconds') or c in ('physical_statements','result_bytes','returned_rows')))
            by_arm[arm]['observed_cost_groups'][key]={c:summary([g.get(c) if g else None for g in groups]) for c in columns}
    billing=list(attempts.values());billing.sort(key=lambda v:v['slot'])
    windows={}
    for version in (1,2,3):
        a=[v for v in billing if v['version']==version and type(v['ended_epoch']) in (int,float)]
        start=min(v['started_epoch'] for v in a);end=max(v['ended_epoch'] for v in a)
        windows[str(version)]={}
        for seconds in (60,300):
            samples=[];t=start+seconds
            while t<end:
                samples.append({'window_end_epoch':t,'physical_attempts_completed':sum(t-seconds<v['ended_epoch']<=t for v in a),
                    'method_paths_completed':sum(versions[k]==version and t-seconds<p['method_ended_epoch']<=t for k,p in paths.items())});t+=seconds
            exposure=min(seconds,end-start)
            samples.append({'window_end_epoch':end,'physical_attempts_completed':sum(end-exposure<v['ended_epoch']<=end for v in a),
                'method_paths_completed':sum(versions[k]==version and end-exposure<p['method_ended_epoch']<=end for k,p in paths.items()),
                'last_window_exposure_seconds':exposure})
            windows[str(version)][str(seconds)]=samples
    numeric={k:{v['arm']:v for v in rows if v['case_id']==k} for k,ref in refs.items() if ref['numeric_reference_rows'] is not None}
    paired=Counter(('both_correct' if v['direct']['numeric_EX'] and v['ours']['numeric_EX'] else 'ours_only' if v['ours']['numeric_EX'] else 'direct_only' if v['direct']['numeric_EX'] else 'both_failed_or_wrong') for v in numeric.values())
    report={'kind':'EXPOSED_DEVELOPMENT_CROSS_VERSION_DELIVERY_NOT_FORMAL','formal_N':None,'T1_complete':False,
        'planned_paths':48,'terminated_paths':48,'undispatched_paths':0,'by_arm':by_arm,'paired_numeric_counts':dict(paired),
        'by_graph':{g:{arm:{'numeric_correct':sum(v['numeric_EX'] is True for v in rows if v['graph']==g and v['arm']==arm),
          'numeric_N':4,'status_counts':dict(Counter(v['final_status'] for v in rows if v['graph']==g and v['arm']==arm))} for arm in by_arm} for g in ('redis','duckdb','iotdb','elasticsearch')},
        'physical_reserved_attempts':len(billing),'initial_attempts':sum(v['request_id'].endswith('__initial') for v in billing),
        'feedback_attempts':sum(v['request_id'].endswith('__feedback') for v in billing),
        'HTTP_status_counts':dict(Counter(str(v['http_status']) for v in billing)),
        'attempt_classifications':dict(Counter(v['classification'] for v in billing)),
        'attempt_metrics':{k:summary([v[k] for v in billing]) for k in ('provider_wall_seconds','prompt_tokens','completion_tokens','total_tokens','reasoning_tokens','cached_tokens','input_characters','output_characters')},
        'throughput_windows':windows,'monetary_cost':None,'server_queue':None,'pure_decode_TPS':None,
        'controls_shared_planner_no_extra_model_calls':True,'simple_correct_equals_current_ours':True,
        'independent_method_increment_established':False,'cache_order_controlled':False,
        'confidence_interval':None,'confidence_interval_reason':'Exposed correlated developer positions, no independent confirmation claim; report paired counts only',
        'resource_notes':['64 global cap; actual12 paths per graph','graph admission separate from method cost',
            'One audited SSH failure retains reservation and unknown usage; no resend',
            '3 v1 length/null labels corrected only in analysis sidecar; raw results immutable',
            'feedback withheld by bounded stops preserved; not a complete uninterrupted feedback study'],
        'provenance_sha256':provenance}
    OUT.mkdir(parents=True,exist_ok=False);write(OUT/'summary.json',report);write(OUT/'attempts.json',billing)
    write(OUT/'paths.json',rows)
    with (OUT/'paths.csv').open('x',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    lines=['# OSDB暴露开发完整交付','', '24个暴露题位、两方法共48条首轮路径已终结；不是独立正式成绩。','',
        '| 方法 | 数值最终正确/16 | 首轮正确/16 | 返回完整答案/24 | 边界位 |','|---|---:|---:|---:|---:|']
    for arm,label in [('direct','Direct及既定一次操作反馈'),('ours','Ours及既定一次操作反馈')]:
        a=by_arm[arm];lines.append('| %s | %d | %d | %d | 8，未裁定拒答正确率 |'%(label,a['numeric_final_correct'],a['numeric_first_correct'],a['complete_answers']))
    lines+=['','物理预约%d次：48首轮、%d次反馈。未知usage不补零，费用未知。'%(len(billing),report['feedback_attempts']),
        '首次/最终答案与所有失败均保留；停批抑制的反馈不补跑。跨三个冻结版本和归类审计分别追溯，不作完整不中断反馈实验或正式泛化。','',
        '详见summary.json、paths.csv、paths.json、attempts.json及其原始路径/SHA。置信区间不对这些相关暴露开发位作独立确认解释。',
        'simple_correct等价引用；固定计划控制不是独立模型方法。缓存与执行顺序未平衡，独立完整成本仍未建立；不据本报告宣称算法/效率优势。','',
        'T1尚缺独立OSDB任务/结构与参考、必要公开块与作者方法资源及各自准入/预算。T2–T5和第二底模要求保留。']
    (OUT/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(json.dumps({k:report[k] for k in ('planned_paths','terminated_paths','physical_reserved_attempts','feedback_attempts','paired_numeric_counts')},ensure_ascii=True))


if __name__=='__main__':build()
