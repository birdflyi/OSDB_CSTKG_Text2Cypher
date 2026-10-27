"""Read-only Chinese Rich panes. Displaying data never invokes a model."""
from pathlib import Path
import argparse,json,time
from rich.console import Console,Group
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich import box

def render(root,view):
    try:s=json.loads((root/'state.json').read_text(encoding='utf-8'))
    except (OSError,ValueError):return Panel('等待实验状态文件；看板不会启动或恢复实验',title='百灵实验监控',box=box.ASCII)
    done=s['terminal'];total=s['planned'];rate=s['completed_last_60s'];rate5=s['completed_last_300s']/5
    progress='['+'#'*int(done/total*36)+'-'*(36-int(done/total*36))+']'
    eta=(total-done)/rate5 if rate5 else None
    header=Panel(f"状态：{'已终结' if s['state']=='TERMINATED' else '运行中'} | 更新：{time.strftime('%H:%M:%S',time.localtime(s['updated_epoch']))}\n"
        f"{progress} {done}/{total} ({done/total:.1%})\n"
        f"已派发 {s['dispatched']} | 已终结 {done} | 未派发 {total-s['dispatched']}\n"
        f"实际在途 {s['inflight']} | 目标 {s['current_dispatch_target']} | 总上限 {s['global_cap']} | 实测峰值 {s['peak_client_inflight']}\n"
        f"最近1分钟 {rate}条 | 最近5分钟 {s['completed_last_300s']}条 ({rate5:.1f}条/分)\n"
        f"按5分钟速率预计剩余：{eta:.1f}分钟" if eta is not None else '等待完成样本',title='百灵开发预取 · 全局流水线' if view=='global' else '百灵开发预取 · 当前请求',box=box.ASCII)
    endpoint=Panel(f"模型：ling-3.0-flash-n72-75 | 凭据归属：louzehua\n"
        f"完整响应输出吞吐：{s['completed_output_tokens_per_wall_second_60s']:.1f} token/秒（60秒窗口）\n"
        f"服务端排队 / GPU负载 / 纯解码速度：未知\n"
        f"基础设施失败 {s['infrastructure_failures']} | 未知 {s['unknown']} | 停批原因：{s['stop_reason'] or '无'}\n"
        f"累计预算 {s['budget']['reserved']}/{s['budget']['cap']} | 余额 {s['budget']['remaining']}\n"
        '监控只读状态；不发送模型请求。开发输出不计正式成绩。',title='端点与预算',box=box.ASCII)
    if view=='global':
        t=Table('结果块','当前事实',box=box.ASCII,expand=True)
        for row in [('原CypherBench正式核心','2348题已冻结完成，不重跑'),('OSDB暴露开发','48路径已终结，独立正式任务仍缺'),('当前TRAIN开发预取',f'原4096条：v2终结207；v3 {done}/3889'),('Text2GQL','7652首轮输入已准备；新正式块尚未准入'),('其他必要基线与T2-T5','缺项保留，T1尚未完成')]:t.add_row(*row)
    else:
        t=Table('题号','方法','图','已耗时','当前',box=box.ASCII,expand=True)
        for row in sorted(s['active'].values(),key=lambda v:v['started_epoch'])[:12]:
            t.add_row(row['qid'], '直接生成' if row['arm']=='direct' else '本方法',row['graph'],f"{time.time()-row['started_epoch']:.0f}秒",'HTTP请求在途')
        if not s['active']:t.add_row('-','-','-','-','无在途请求')
    return Group(header,endpoint,Panel(t,title='结果块清单' if view=='global' else f"在途请求（显示最多12条 / 共{s['inflight']}条）",box=box.ASCII))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('view',choices=['global','cases']);a=p.parse_args()
    with Live(console=Console(),screen=True,auto_refresh=False) as live:
        while True:
            live.update(render(Path(a.root),a.view),refresh=True);time.sleep(3)
