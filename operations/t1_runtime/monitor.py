"""Chinese read-only Rich views. No model, endpoint or ledger imports."""
import argparse
from datetime import datetime, timezone, timedelta
import io
import json
from pathlib import Path
import time

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


def num(value, suffix='', digits=1):
    if value is None:return '未知'
    return f'{value:,.{digits}f}'+suffix


def duration(seconds):
    if seconds is None:return '待稳定'
    seconds=max(0,int(seconds));h,rem=divmod(seconds,3600);m,s=divmod(rem,60)
    return f'{h:02d}:{m:02d}:{s:02d}'


def table(columns):
    t=Table(box=box.ASCII,expand=True,show_lines=False,header_style='bold cyan')
    for name in columns:t.add_column(name,overflow='ellipsis',no_wrap=False)
    return t


def panel(value,title):return Panel(Text(value) if isinstance(value,str) else value,
    title=title,box=box.ASCII,border_style='cyan')


def stage_label(value):
    if value=='baseline1':return '1并发基线'
    if value=='refine48':return '48并发细测'
    if value and value.startswith('c') and value[1:].isdigit():return value[1:]+'并发'
    return value or '等待准备'


def render(state,view,now=None):
    now=time.time() if now is None else now
    updated=state.get('updated_epoch');age=now-updated if updated else None
    clock=datetime.fromtimestamp(now,timezone(timedelta(hours=8))).strftime('%m-%d %H:%M:%S')
    stale=' | 数据已过期，请检查执行器' if age is not None and age>20 and state.get('state') not in ('已完成','已停止') else ''
    head=f"{state.get('state','等待执行器')} | 北京时间 {clock} | 更新距今 {num(age,'秒')}"+stale
    pool=state.get('active',[]);cap=state.get('concurrency_target',0);rate=state.get('rates',{})
    r60=rate.get('60',{});r300=rate.get('300',{})
    budget=state.get('budget',{})
    endpoint=(f"模型：{state.get('model','待连接')}\n"
        f"我方模型请求：{len(pool)}/{cap} | 目标并发：{cap}\n"
        f"我方输出：{num(r60.get('output_tokens_per_second'),' token/秒')}（最近1分钟已知用量）\n"
        f"输出额度：{num(state.get('output_budget'),' token/秒',0)}（用户提供，估计调速）\n"
        f"服务端总负载：未知 | 服务端排队：未知\n"
        f"累计预留：{budget.get('reserved','未知')}/{budget.get('cap','未知')} | 待终结：{budget.get('pending','未知')}")
    title=panel(head,'T1 全局流水线' if view=='pipeline' else '当前任务 · 逐题监控')
    if view=='pipeline':
        t=table(['任务 / 基准','直接查询+反馈','我们的方法/共享控制','计划分母','状态 / 原因'])
        t.add_row('当前容量验收',stage_label(state.get('stage_name')),
                  f"{state.get('completed',0)}/{state.get('total','待定')}",'开发测量','不计正式EX')
        job_rows=state.get('jobs',[])
        for benchmark in ('CypherBench','Text2GQL','MindTheQuery','ZOGRASCOPE','OSDB-STKG'):
            selected=[j for j in job_rows if j['benchmark']==benchmark]
            if len(selected)==2:
                def cell(j):return f"{j['completed']}/{j['total']}" if j['state']=='已完成' else j['state']
                t.add_row(benchmark,cell(selected[0]),cell(selected[1]),str(selected[0]['total']),selected[0].get('reason',''))
        t.add_row('Cypher-RI / CypherSmith / CYQUARK','资源待核','适用范围待核','待定','作者权重/推理/输出待验')
        stages=table(['并发阶段','实际峰值','完成 / 计划','完整响应/分钟','状态 / 原因'])
        for s in state.get('stages',[]):
            stages.add_row(str(s['concurrency']),str(s.get('actual_peak_requests','--')),
                f"{s.get('completed',0)}/{s.get('planned',0)}",num(s.get('steady_complete_cases_per_minute',s.get('complete_cases_per_minute'))),
                s.get('reason') or s.get('stop_reason') or ('吞吐平台' if s.get('plateau') else '阶段完成'))
        guidance=(f"整张T1：未完成 | 当前任务：{state.get('job_id','等待准备')}\n"
            f"停止/阻塞原因：{state.get('stop_reason') or '无'}\n"
            f"观测推荐并发：{state.get('recommendation') or '待本轮测量'}\n"
            f"建议说明：{state.get('recommendation_note') or '按完整样本验收，未证明端点饱和'}\n"
            '调度：已准入任务持续推进；阶段超时/失败有界停止；未准入任务保留原因。\n'
            '看板只读，关闭或显示故障不停止实验。未终结请求先审计，不盲目重发。')
        return Group(title,panel(guidance,'执行与防无界等待'),panel(t,'全局工作清单'),
                     panel(endpoint,'端点与全局并发池'),panel(stages,'并发阶梯 · 原分母保留'))
    total=state.get('total',0);done=state.get('completed',0);n=44
    fraction=min(1,done/total) if total else 0
    bar='#'*int(n*fraction)+'-'*(n-int(n*fraction))
    pending=state.get('undispatched',max(0,total-done-len(pool)))
    progress=(f"{state.get('job_id','等待准备')} | 阶段：{stage_label(state.get('stage_name'))}\n"
        f"[{bar}] {fraction*100:.1f}%\n"
        f"已结束：{done}/{total} | 正在请求：{len(pool)} | 尚未派发：{pending}\n"
        f"完整模型响应：{state.get('complete_responses',0)} | 当前为容量测试，不执行图查询/不计算EX")
    per_min=r300.get('per_minute',0);stable=state.get('state')=='运行中' and state.get('stage_started',now)<=now-30 and r300.get('completed',0)>=16
    eta=(total-done)*60/per_min if stable and per_min else None
    speed=(f"最近1分钟：完成{r60.get('completed',0)}题，平均{num(r60.get('per_minute',0))}题/分钟\n"
        f"最近5分钟：完成{r300.get('completed',0)}题，平均{num(r300.get('per_minute',0))}题/分钟\n"
        f"本阶段预计剩余：{duration(eta)} | 不足完整窗口时按已运行时长折算，预热中不估ETA")
    t=table(['题号','问题摘要','方法','当前阶段','已耗时','请求超时剩余'])
    for row in sorted(pool,key=lambda r:r['started_epoch'])[:6]:
        elapsed=now-row['started_epoch']
        t.add_row(row['trial_id'],row['question'][:90],'直接查询' if row['arm']=='direct' else '我们的方法',
                  '等待模型',duration(elapsed),duration(240-elapsed))
    recent='\n'.join(f"{r['trial_id']}  {r['state']}  {r['seconds']:.1f}秒" for r in state.get('recent',[])[:4]) or '尚无完成记录'
    statistics=state.get('statistics',{});lat=statistics.get('latency',{});tokens=statistics.get('tokens',{})
    foot=(f"近期最多1000次：平均耗时{num(lat.get('mean'),'秒')} | 中位数{num(lat.get('p50'),'秒')} | "
          f"90%不超过{num(lat.get('p90'),'秒')}\n"
          f"平均输入{num(tokens.get('prompt_tokens',{}).get('mean'),' token',0)} | "
          f"平均输出{num(tokens.get('completion_tokens',{}).get('mean'),' token',0)} | "
          f"其中推理{num(tokens.get('reasoning_tokens',{}).get('mean'),' token',0)}\n"
          f"HTTP状态分布：{state.get('http_counts',{})} | 停止原因：{state.get('stop_reason') or '无'}")
    return Group(title,panel(progress,'本次执行进度'),panel(speed,'完成速度与预计剩余'),
                 panel(endpoint,'端点状态'),panel(t,'正在处理 · 最多显示6题'),
                 panel(recent,'最近完成'),panel(foot,'轻量统计 · 每30秒更新'))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--state',required=True)
    parser.add_argument('--view',choices=['pipeline','job'],default='pipeline')
    parser.add_argument('--once',action='store_true');parser.add_argument('--html')
    parser.add_argument('--width',type=int);parser.add_argument('--benchmark',action='store_true')
    args=parser.parse_args();console=Console(record=bool(args.html),width=args.width)
    path=Path(args.state);last={};refreshes=[]
    def frame():
        nonlocal last
        try:
            if path.stat().st_size>256*1024:raise ValueError('Snapshot too large')
            last=json.loads(path.read_text(encoding='utf-8'))
        except (OSError,ValueError):pass
        return render(last,args.view)
    if args.benchmark:
        state=json.loads(path.read_text(encoding='utf-8'));sink=io.StringIO();c=Console(file=sink,width=140)
        start=time.process_time()
        for _ in range(30):
            before=time.perf_counter();c.print(render(state,args.view));refreshes.append(time.perf_counter()-before)
            sink.seek(0);sink.truncate()
        print(json.dumps({'view':args.view,'frames':30,'cpu_seconds':time.process_time()-start,
                          'mean_frame_seconds':sum(refreshes)/30,'max_frame_seconds':max(refreshes)}));return
    if args.once or args.html:
        console.print(frame())
        if args.html:console.save_html(args.html,clear=False)
        return
    with Live(frame(),console=console,screen=True,auto_refresh=False) as live:
        while True:
            try:live.update(frame(),refresh=True)
            except (OSError,ValueError):pass
            time.sleep(3)


if __name__=='__main__':main()
