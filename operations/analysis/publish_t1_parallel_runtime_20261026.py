# -*- coding: utf-8 -*-
"""Publish aggregate capacity and Chinese-monitor acceptance after integrity audit.

No endpoint, model, database, SSH, or ledger calls. Raw prompts and replies stay
in the local evidence tree; all versions, failures and unknown costs are retained.
"""
from pathlib import Path
import json

ROOT=Path('D:/ch7')
REPO=ROOT/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'


def read(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def main():
    evidence=read(ROOT/'reports/t1_parallel_capacity_and_monitor_delivery_20261026.json')
    monitor=read(ROOT/'reports/t1_parallel_capacity_sshroute_v6_20261026/monitor_review/final_monitor_check.json')
    checks=('old5195_physical_evidence_unchanged','old4696_formal_records_unchanged','canonical32_and_adapter46_unchanged')
    assert all(evidence[k] for k in checks) and not evidence['budget']['pending']
    stage_rows=[]
    for s in evidence['capacity_stages']:
        stage_rows.append('| {c} | {peak} | {done}/{plan} | {good} | {steady} | {full:.2f} | {p50:.2f}/{p90:.2f} | {note} |'.format(
            c=s['concurrency'],peak=s.get('actual_peak_requests','未知'),done=s['completed'],plan=s['planned'],
            good=s.get('complete_responses',0),steady=f"{s['steady_complete_cases_per_minute']:.2f}" if s.get('steady_sample_sufficient') else '样本不足',
            full=s.get('complete_cases_per_minute',0),p50=s.get('latency',{}).get('p50',0),p90=s.get('latency',{}).get('p90',0),
            note=('连续基础设施失败，停止' if s['concurrency']==128 else '历史调度停止，原数保留')
                if s.get('stop_reason') else ('全部派发终结' if not s['undispatched'] else '派发时限，未派发保留')))
    prior=evidence['first128_confirmation_required']
    budget=evidence['budget'];cost=evidence['new_attempt_cost_fields']
    costs=[]
    for name,fields in evidence['new_cost_by_version'].items():
        costs.append(f"| {name} | {fields['attempts']} | {fields['usage_object_unknown']} |")
    cpu=(monitor['pipeline']['cpu_seconds']+monitor['job']['cpu_seconds'])/30/3*100
    recommendation=evidence['provisional_operational_cap']
    latest=next(s for s in evidence['capacity_stages'] if s['concurrency']==128)
    gain=latest.get('gain_over_previous')
    conclusion=(f"当前暂定保守请求上限 **{recommendation}**。" if recommendation else '尚无满足质量/样本门槛的推荐点。')
    if latest.get('stop_reason'):
        conclusion+='128确认轮因连续基础设施失败停止，未通过稳定性验收；当前没有证明服务饱和甜点，停止进一步加压。'
    if gain is not None and gain>=.05:
        conclusion+='128档对64档仍有超过5%的中段收益，因此这是已测范围内的推荐上限，尚未证实饱和甜点；不自动扩大到256。'
    elif latest.get('plateau'):
        conclusion+='128档已出现吞吐平台；推荐按预定最高速率95%范围内的最小合格档选择。'
    text=f'''# T1 并发与中文看板验收

{conclusion}

这是一轮固定TRAIN负载的客户端容量开发，不是新增T1准确率或第二底模质量结果。整张T1仍未完成。

## 实测并发

| 请求上限 | 实际峰值 | 终结/计划 | 完整响应 | 中段完整响应/分钟 | 全批完整响应/分钟 | P50/P90耗时，秒 | 说明 |
|---:|---:|---:|---:|---:|---:|---:|---|
{chr(10).join(stage_rows)}

中段规则在测量前固定：按完成时刻排序，首尾各去一并发波，再用中段完成时刻间隔计算；至少四波才可估计。全批率包含启动和排空长尾。所有失败/截断仍保留在全批分母、成本及原始日志中，不将“完整模型响应”叫作答对。

128档首轮另有{prior['completed']}/{prior['planned']}终结、{prior['complete_responses']}完整响应，全批{prior['complete_cases_per_minute']:.2f}条/分钟，4次连接重置和5次length截断；到240秒派发期限只发出499条，低于原定512条稳态门槛，保留269条未派发。确认轮单独固定512个新trial、360秒派发窗，没有覆盖或免费复用首轮请求。16/32/64档引用原SHA，成本只计一次。

128确认轮299次尝试中，277次HTTP200、276完整响应、1次length、22次连接失败，213条未派发；连续基础设施失败触发停止，没有自动重试或再开新模型批次。SSH转发日志同时有22次连接模型网关失败，首轮有4次；这证明访问链路存在故障，不能独自证明GPU或模型并发容量不足。原自动推荐{evidence['recommended_concurrency']}按“所有fatal标记都排除”的规则产生，排除了带旧客户端误判标记的64档；原数不改。审计后将64档作为暂定保守上限，因为该档363次全部HTTP200、完整率98.35%、稳态样本充分，停止标记来自已证实的客户端时限排空误判。此建议不是确认性论文指标，也未声称当前链路已恢复。

负载为原先暴露的12条CypherBench TRAIN任务、Direct/Ours两类首轮提示，共24种输入。百灵服务模型回显ling-3.0-flash-n72-75；temperature0.2、top_p0.95、reasoning_effort=high、max_tokens8192。无gold、测试题、图查询、训练和准确率挑样。串行预检15/15完整，但未覆盖原24类的匹配完整轮次，不据此声称精确的串行加速倍数。

3500输出token/秒是用户提供的资源估计。客户端用已返回用量纠正预计输出信用，不能保证后端瞬时QoS；服务端其他任务、排队、GPU占用、缓存和TTFT未知。这里只测我方请求上限，不能反推出机器总并发或纯解码速度。同题随机推理和共享服务波动仍可能影响档间差异；当前结果不能外推所有benchmark或完整多步case。

## 中文双窗口

授权节点3107，任务专属tmux socket `ch7-t1-monitor`，session `ch7-t1`：

```sh
tmux -L ch7-t1-monitor attach -t ch7-t1
```

- `Ctrl+b`后按`0`：全局流水线，列出benchmark×方法的完成/准备/资源阻塞状态、容量阶梯、预算及停止原因。
- `Ctrl+b`后按`1`：逐题监控，含字面ASCII进度条、完成/在途/未派发、近1/5分钟速率、ETA、题目摘要、耗时、近完成和用量。
- `Ctrl+b`后按`d`：断开查看，任务不受影响。两个独立查看会话`ch7-t1-global`、`ch7-t1-detail`共享这两个窗口。

两屏中文标签、原始英文题目和模型/benchmark名保留原文。端点总负载/排队无遥测就显示未知，不能把我方在途请求当全服务负载。最近窗口存在未知用量时输出速率也显示未知；不以0补齐。阶段ETA是滚动估计，预热中不估、长尾时不保证准确。

看板每3秒仅读小快照、分位数每30秒计算，零额外模型调用。节点实测各30帧：全局CPU {monitor['pipeline']['cpu_seconds']:.4f}秒、逐题CPU {monitor['job']['cpu_seconds']:.4f}秒，折算两屏每3秒刷新约单核{cpu:.2f}%；这是渲染CPU微测，包含终端IO的实际整机开销未单独隔离。看板不占模型GPU推理额度。

看板仅读状态，部署失败单列并允许实验继续；调度派发窗、网络超时、预算硬上限、连续3次基础设施失败停止和整批总期限防无界等待。未知请求先审计，不清其他人的锁，不盲目重发。已修正早期信用容量不足、阶段排空误判和ASCII进度条被当富文本解析的问题，各旧版本字节/错误/成本保留。

最终组件16项本机检查通过，包含并发预算幂等/冲突/未知终结保护、三次基础设施失败停止的模拟整批、中文进度条、统计和看板异常隔离。另补“先处理已返回响应再补新请求、按返回时刻处理完成集合”，避免失败积压在补满并发之后；服务停止后未发模型请求重验，属于本机模拟通过的客户端修正。

当前真实接入是TRAIN容量执行器与原DeepSeek核心完成状态。其他benchmark/作者方法显示准备或资源缺项，尚非可自动运行全部T1的通用编排器，必须逐块验收协议/数据/评分/预算/冻结。

模型请求在授权Linux节点的worker发出，经SSH反向通道访问本机可达的模型网关，本机D盘仍是预算权威。**当前需要本机及SSH通道持续在线**，不是完全脱离本机的远端独立执行。仅操作自有隔离目录和专属tmux；未改其他服务、GPU/模型/图进程、默认tmux和共享图owner。

## 全部成本与证据

本阶段新增{evidence['new_capacity_physical_attempts']}次物理尝试，原累计从5195到{budget['reserved']}/{budget['cap']}，剩余{budget['remaining']}，pending=[]。所有失败/重试预留/未知usage均计入原连续账本，未申请或动用Text2GQL额外预算。

| 执行版本 | 物理尝试 | 未知usage对象 |
|---|---:|---:|
{chr(10).join(costs)}

本阶段已知输入token小计{cost['tokens']['prompt_tokens']['known_sum']:,}，已知输出{cost['tokens']['completion_tokens']['known_sum']:,}；未知usage对象{cost['usage_object_unknown']}次，费用未知。旧冻结32文件、适配46文件、4696正式记录及5195条历史物理证据逐一SHA不变。未终结请求不按失败随意清除；原结果不混入本轮容量测量。

本地完整证据在`D:/ch7/reports/t1_parallel_capacity_integrity_20261026`及各版本容量目录；原始prompt、输出、逐trial证据、一次性批准、私有节点/连接配置留本地。聚合JSON、统计脚本和运行组件可公开复核。

本阶段只验收并发/监控组件。T1的其他必要单元格仍需逐项推进，T2–T5未因此自动启动。
'''
    destination=REPO/'operations/analysis/T1_PARALLEL_AND_CHINESE_MONITOR_ACCEPTANCE_20261026.md'
    destination.write_bytes(text.encode('utf-8'))
    evidence['monitor_cpu_30frames']={v:monitor[v] for v in ('pipeline','job')}
    evidence['capacity_acceptance']={'cap64':'provisional_conservative_only','cap128':'blocked_by_gateway_connection_failures',
        'saturation_sweet_spot_established':False,'new_model_requests_after_stop':0,'final_local_tests_passed':16}
    public=REPO/'operations/analysis/t1_parallel_capacity_and_monitor_delivery_20261026.json'
    public.write_bytes((json.dumps(evidence,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))
    print(json.dumps({'report':str(destination),'aggregate':str(public),'recommended':recommendation},ensure_ascii=True))


if __name__=='__main__':main()
