"""Read-only Chinese Rich panes. Displaying data never invokes a model."""
from pathlib import Path
import argparse,json,time
from rich.console import Console,Group
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich import box
from rich.text import Text

def render(root,view):
    try:s=json.loads((root/'state.json').read_text(encoding='utf-8'))
    except (OSError,ValueError):return Panel('等待实验状态文件；看板不会启动或恢复实验',title='百灵实验监控',box=box.ASCII)
    done=s['terminal'];total=s['planned'];rate=s['completed_last_60s'];rate5=s['completed_last_300s']/5
    progress='['+'#'*int(done/total*36)+'-'*(36-int(done/total*36))+']'
    eta=(total-done)/rate5 if rate5 and s['state']!='TERMINATED' and not s['stop_reason'] else None
    stop_text='\u65e0' if not s['stop_reason'] else '\u8fde\u7eed\u4e09\u6b21\u57fa\u7840\u8bbe\u65bd\u5931\u8d25\uff0c\u5df2\u505c\u53d1\u5e76\u6392\u7a7a'
    status_text='\u5df2\u7ec8\u7ed3' if s['state']=='TERMINATED' else '\u8fd0\u884c\u4e2d'
    header_lines=(f"\u72b6\u6001\uff1a{status_text} | \u66f4\u65b0\uff1a{time.strftime('%H:%M:%S',time.localtime(s['updated_epoch']))}\n"
        f"{progress} {done}/{total} ({done/total:.1%})\n"
        f"\u5df2\u6d3e\u53d1 {s['dispatched']} | \u5df2\u7ec8\u7ed3 {done} | \u672a\u6d3e\u53d1 {total-s['dispatched']}\n"
        f"\u5b9e\u9645\u5728\u9014 {s['inflight']} | \u76ee\u6807 {s['current_dispatch_target']} | \u603b\u4e0a\u9650 {s['global_cap']} | \u5b9e\u6d4b\u5cf0\u503c {s['peak_client_inflight']}\n"
        f"\u6700\u540e\u7a97\u53e3\uff1a1\u5206\u949f {rate}\u6761 | 5\u5206\u949f {s['completed_last_300s']}\u6761 ({rate5:.1f}\u6761/\u5206)\n")
    header_lines += f"\u9884\u8ba1\u5269\u4f59 {eta:.1f}\u5206\u949f" if eta is not None else '\u5df2\u6682\u505c\u6216\u7ec8\u7ed3\uff0c\u6682\u65e0\u5269\u4f59\u65f6\u95f4\u4f30\u8ba1'
    header=Panel(Text(header_lines),title='\u767e\u7075\u5f00\u53d1\u9884\u53d6 / '+('\u5168\u5c40\u6d41\u6c34\u7ebf' if view=='global' else '\u5f53\u524d\u8bf7\u6c42'),box=box.ASCII)
    endpoint=Panel(f"模型：ling-3.0-flash-n72-75 | 凭据归属：louzehua\n"
        f"完整响应输出吞吐：{s['completed_output_tokens_per_wall_second_60s']:.1f} token/秒（60秒窗口）\n"
        f"服务端排队 / GPU负载 / 纯解码速度：未知\n"
        f"基础设施失败 {s['infrastructure_failures']} | 未知 {s['unknown']} | 停批原因：{stop_text}\n"
        f"累计预算 {s['budget']['reserved']}/{s['budget']['cap']} | 余额 {s['budget']['remaining']}\n"
        '监控只读状态；不发送模型请求。开发输出不计正式成绩。',title='端点与预算',box=box.ASCII)
    if view=='global':
        t=Table('结果块','当前事实',box=box.ASCII,expand=True)
        for row in [('原CypherBench正式核心','2348题已冻结完成，不重跑'),('OSDB暴露开发','48路径已终结，独立正式任务仍缺'),('当前TRAIN开发预取',f'原4096条：v2/v3终结2309；v4 {done}/1787'),('Text2GQL','7652首轮输入已准备；新正式块尚未准入'),('其他必要基线与T2-T5','缺项保留，T1尚未完成')]:t.add_row(*row)
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
