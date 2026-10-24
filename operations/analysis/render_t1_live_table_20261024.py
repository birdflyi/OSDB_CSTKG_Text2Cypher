"""Render traceable main-table progress. Unfinished cells never become zeros."""
import argparse
from datetime import datetime
import json
from pathlib import Path

ROOT=Path(r'D:\ch7')
GRAPHS={'flight_accident':189,'nba':270,'fictional_character':385,'company':347,
        'geography':366,'movie':401,'politics':390}


def render(active_graph,stopped_graph=None):
    audit=json.loads((ROOT/'reports/t1_core_integrity_cost_live_20261024.json').read_text(encoding='utf-8'))
    scores=audit['graph_scores'];full=audit['full2348_score_ready']
    lines=['# T1 主表：本项目实测与缺项', '',
        f"快照：{datetime.now().astimezone().isoformat(timespec='seconds')}。最新总体设计为 v3，执行计划为 v4，接续任务为 v5。",'',
        '本页只填本项目正式实测。文献 reported 数字保留在原讨论表，不能代替同条件复现。CypherBench 全量未完成前，不将部分图成绩填成全量成绩。','',
        '## 公共主表', '',
        '| 方法 | CypherBench，全量 2,348 | MindTheQuery | Text2GQL-Bench | ZOGRASCOPE |',
        '|---|---|---|---|---|']
    for name in ['Cypher-RI','CypherSmith','CYQUARK','LAST-CQ，历史候选']:
        lines.append(f'| {name} | 作者资源／推理协议待核验；未复现 | 作者资源与适用性待核验；未复现 | 作者资源与适用性待核验；未复现 | 作者资源与适用性待核验；未复现 |')
    for name,key in [('Direct，同底模','Direct'),('Direct + 执行反馈，同底模','Direct+feedback'),
                     ('正确全明细／简单安全策略，共享计划','Full-detail(shared-plan)'),('Ours，同底模','Ours')]:
        cypher=(f"{audit['micro_ex_percent'][key]:.2f}%" if full else f'进行中：{len(scores)}/7 整图；全量 EX 待填')
        lines.append(f'| {name} | {cypher} | 待轨道与参考验收；未预测 | 待引擎与参考验收；未预测 | 待划分与输入协议验收；未预测 |')
    lines += ['',
        '当前共同底模为 deepseek-v4.1-flash-n24-131，冻结源码 c614367；这是首轮底模，不是第二底模稳健性结果。CypherBench 主指标为按题 EX micro。宏平均另报。Direct 首轮与反馈版本共享首轮生成；正确全明细与简单安全策略在当前有限版本下等价，共享 Ours 计划，不能算两项独立模型成绩。具名方法的候选行仍待作者身份、资源和适用性验收，不能以同名工具代替。', '',
        '## CypherBench 整图分项：已完成图可用的实测', '',
        '| 图 | N | Direct EX | Direct+反馈 EX | Ours EX | 共享计划全明细 EX | Ours 完整答案覆盖 | 状态 |',
        '|---|---:|---:|---:|---:|---:|---:|---|']
    for graph,n in GRAPHS.items():
        if graph in scores:
            cells=scores[graph]
            vals=[f"{cells[k]['correct']:g}/{n}（{cells[k]['ex_percent']:.2f}%）" for k in ['Direct','Direct+feedback','Ours','Full-detail(shared-plan)']]
            lines.append(f"| {graph} | {n} | {' | '.join(vals)} | {cells['Ours']['coverage_percent']:.2f}% | 已完成并评分 |")
        else:
            state='已停止，待审计接续' if graph==stopped_graph else '运行中' if graph==active_graph else '未派发'
            lines.append(f'| {graph} | {n} | — | — | — | — | — | {state} |')
    lines += ['',
        f"路径快照：计划 4,696；已终结 {audit['terminal_primary_paths']}；已派发 {audit['dispatched_primary_paths']}；未派发 {4696-audit['dispatched_primary_paths']}。各统计在活动队列中顺序读取，可能有秒级差异。拒答、不支持和错误保留在固定分母；未派发不伪装成失败。",'',
        '## 已完成图的路线与模型成本', '',
        '| 图 | Ours 摘要路线触发数 | Direct 总 token | Ours 总 token | 物理模型调用总数 | 与全明细中间结果一致的已执行计划数 |',
        '|---|---:|---:|---:|---:|---:|']
    for graph in GRAPHS:
        report=ROOT/'reports'/f't1_{"flight" if graph=="flight_accident" else graph}_route_cost_components_20261024.json'
        if graph not in scores or not report.exists():continue
        r=json.loads(report.read_text(encoding='utf-8'))
        lines.append(f"| {graph} | {r['certified_disjoint_summary_executions']} | {r['per_arm']['direct']['known_usage']['total_tokens']:,} | {r['per_arm']['ours']['known_usage']['total_tokens']:,} | {r['physical_billed_model_attempts']} | {r['executed_plan_control_comparison'].get('identical_intermediate_columns_rows',0)} |")
    lines += ['',
        '路线来自模型实际执行，不是独立任务适用标签。摘要没有触发时，不能据此宣称摘要选择有收益。全明细固定在 Ours 后回放，时间有缓存／顺序影响；不能将单次回放比值当成无偏速度提升。全部实际调用计账一次，共享计划控制不重复计费。费用未知；原 SSH 中断的未知计时和未知数据库完成状态仍保留。完整 T4 尚未验收。','',
        '## 其余结果块的准入状态', '',
        '| 结果块 | 数据盘点；不是已接受的正式分母 | 计划／派发／完成状态 | 尚缺的验收 |',
        '|---|---|---|---|',
        '| MindTheQuery | Automated TEST 8,284；Manual TEST 1,829 | 范围待定／0／0 | 论文轨道、标签筛选、图部署及参考执行；额外预算 |',
        '| Text2GQL Cypher | 当前作者链接包：6 图、3,826 条唯一图-ID记录 | 范围待准入／0／0 | 原生引擎、空值／日期／多重边语义、参考执行、划分；额外预算 |',
        '| ZOGRASCOPE | 固定仓库 IID 768、COMP 1,349、LEN 1,253；并集 2,672 个唯一 ID | 范围待准入／0／0 | 版本分区差异、实体链接、结构族与参考执行；额外预算 |',
        '| 具名方法 | 作者资源与版本尚未全核清 | 适用矩阵待定／0／0 | 权重身份、推理模板／接口、训练暴露与对应输出；所需资源 |',
        '| OSDB-STKG | Redis、DuckDB、IoTDB、Elasticsearch；独立任务 N 待冻结 | N 待定／0／0 | 独立任务与参考、图服务适配、访问暴露记录、准入与预算 |', '',
        'OSDB-STKG 必须按项目另列 Direct+反馈、共享计划全明细、Ours 的 EX、覆盖与错误答案率。现有四项目已有历史可见性，不能写成未见项目。旧开发题不填独立任务结果。', '',
        '## 证据入口', '',
        '- [实时完整性与成本报告](D:/ch7/reports/t1_core_integrity_cost_live_20261024.json)',
        '- [冻结清单](D:/ch7/experiments/public_benchmark_v1/core_freeze_v1_20261024/freeze.json)',
        '- [其他候选协议审计](D:/ch7/discussion/T1_OTHER_PUBLIC_CANDIDATES_AUDIT_20261024.md)',
        '- [原讨论表，含有来源的 reported 数值](D:/ch7/discussion/DASFAA_EXPERIMENT_TABLES_FILLED_20261023.md)']
    for graph in scores:
        tag='flight' if graph=='flight_accident' else graph
        lines.append(f'- [{graph} 验收](D:/ch7/reports/t1_{tag}_formal_graph_acceptance_20261024.json)')
    lines+=['','整张 T1 尚未完成。任何必要缺项继续保留为未完成或阻塞，不能将本页已有数字解释为整表交付。','']
    return '\n'.join(lines)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--active-graph',required=True)
    p.add_argument('--stopped-graph')
    a=p.parse_args()
    output=ROOT/'discussion/T1_MAIN_TABLE_LIVE_20261024.md'
    output.write_text(render(a.active_graph,a.stopped_graph),encoding='utf-8')
    print(str(output))
