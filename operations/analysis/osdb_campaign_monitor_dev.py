# coding: utf-8
"""Pure Chinese ASCII Rich views for OSDB development paths, not capacity EX.

No socket, provider, graph or ledger imports. Rendering never controls work.
"""
from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

LABELS={'EXECUTED':'已完整执行','UNSUPPORTED':'不支持','INSUFFICIENT_EVIDENCE':'证据不足',
 'INFRASTRUCTURE_ERROR':'基础设施错误','MODEL_TRUNCATED':'模型输出截断','MODEL_PROTOCOL_ERROR':'回答格式错误',
 'PLAN_VALIDATION_ERROR':'计划验证错误','READ_POLICY_ERROR':'读权限错误','EXECUTION_ERROR':'查询执行错误',
 'EXECUTION_TIMEOUT':'查询超时','TRUNCATED_RESULT':'结果超限'}

def value(v,suffix=''):
 return '未知' if v is None else f'{v:,.1f}'+suffix
def seconds(v):
 if v is None:return '待稳定'
 v=max(0,int(v));return f'{v//3600:02d}:{v%3600//60:02d}:{v%60:02d}'
def panel(body,title):return Panel(Text(body) if type(body) is str else body,title=title,box=box.ASCII,border_style='cyan')
def table(columns):
 t=Table(box=box.ASCII,expand=True)
 for c in columns:t.add_column(c,overflow='ellipsis')
 return t


def render(state,view='job',now=None):
 now=state['updated_epoch'] if now is None else now
 synthetic=state.get('evidence_kind')=='synthetic'
 mode='离线合成回放；不是模型结果' if synthetic else 'OSDB 暴露开发回归；不计正式EX'
 title=panel(f"{state.get('state','等待')} | {mode}\n当前图：{state.get('current_graph') or '待准备'} | 更新距今{value(now-state['updated_epoch'],'秒')}",
  'T1 全局流水线 · OSDB开发阶段' if view=='pipeline' else '当前任务 · 逐题监控')
 done=state['paths_completed'];total=state['planned_paths'];fraction=done/total if total else 0
 progress=(f"[{'#'*int(44*fraction)}{'-'*(44-int(44*fraction))}] {fraction*100:.1f}%\n"
  f"方法路径终结 {done}/{total} | 首轮已派发 {state['initial_paths_dispatched']} | 未派发 {state['undispatched_paths']}\n"
  f"已派发但未收口 {state['unresolved_dispatched_paths']} | 调度异常 {state['runner_error_paths']} | 终结错误 {state['terminal_failure_paths']}\n"
  f"双方均终结的题位 {state['paired_cases_completed']}/24 | 完整执行答案 {state['complete_answers']}；不是答对数\n"
  f"剩余时间：{seconds(state.get('eta_seconds'))} | 停止原因：{state.get('stop_reason') or '无'}")
 rates=state['rates'];speed='\n'.join(
  f"最近{1 if span=='60' else 5}分钟：终结{rates[span]['path_completed']}方法路径，{value(rates[span]['paths_per_minute'])}路径/分钟；双方终结题位{rates[span]['paired_cases_completed']}"
  for span in ('60','300'))+'\n运行不足完整窗口时按已运行时长折算，预热或停批时不估剩余时间。'
 endpoint=(f"模型：{state['model']}\n"
  f"我方正在请求模型 {state['model_requests_inflight']} | 活动方法路径 {len(state['active_paths'])}/{state['max_inflight_paths']}\n"
  f"最近1分钟已知输出 {value(rates['60'].get('output_tokens_per_second'),' token/秒')}；含等待/输入处理，不是服务端纯生成速度\n"
  '服务端总负载：未知 | 服务端排队：未知\n'
  f"累计预留 {state['budget']['reserved']}/{state['budget']['cap']} | 待终结尝试 {state['budget']['pending']}\n"
  '看板只读，显示故障不停止实验，也不发额外模型请求。')
 if view=='pipeline':
  t=table(['图','直接查询+反馈（开发）','我们的方法/共享控制（开发）','全局状态'])
  for g,arms in state['by_graph'].items():
   n=sum(arms.values());status='已终结' if n==12 else '当前' if g==state['current_graph'] else '等待'
   t.add_row(g,f"{arms['direct']}/6",f"{arms['ours']}/6",status)
  return Group(title,panel(progress,'开发批次进度'),panel(t,'逐图流水线 · 正式分母仍待冻结'),panel(endpoint,'端点与预算'),
   panel('CypherBench原冻结核心已完成；其他T1单元格沿用主表状态。\n整张T1未完成，当前开发路径不增加正式完成数。','证据范围'))
 t=table(['题位','方法','当前阶段','已耗时'])
 for row in sorted(state['active_paths'],key=lambda r:r['started_epoch'])[:8]:
  t.add_row(row['case_id'],'直接查询' if row['arm']=='direct' else '我们的方法',row['stage'],seconds(now-row['started_epoch']))
 model=state['model_latency'];path=state['path_latency']
 stats=(f"模型请求耗时：平均{value(model['mean'],'秒')} | 中位数{value(model['p50'],'秒')} | 90%不超过{value(model['p90'],'秒')}\n"
  f"方法路径耗时：平均{value(path['mean'],'秒')} | 中位数{value(path['p50'],'秒')} | 90%不超过{value(path['p90'],'秒')}\n"
  f"终结状态分布：{ {LABELS.get(k,'未知状态'):v for k,v in state['status_counts'].items()} }\n监控写入错误 {state['observer_errors']}；原始结果/账本是验收依据。")
 recent='\n'.join(f"{r['path_key']} {LABELS.get(r['status'],'未知状态')} {r['seconds']:.2f}秒" for r in state['recent']) or '暂无终结路径'
 return Group(title,panel(progress,'本次执行进度'),panel(speed,'完成速度与剩余时间'),panel(endpoint,'端点状态'),
  panel(t,'正在处理 · 模型与图读取分开'),panel(recent,'最近完成'),panel(stats,'轻量耗时统计 · 非准确率'))
