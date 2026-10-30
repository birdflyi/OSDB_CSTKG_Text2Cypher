"""Read-only cohort monitor. Never imports or controls the experiment runner."""
import argparse
from collections import Counter, deque
import json
from pathlib import Path
import subprocess
import time


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


class Journal:
    def __init__(self, path):
        self.path = Path(path)
        self.offset = 0
        self.active = {}
        self.finals = set()
        self.cells = Counter()
        self.statuses = Counter()
        self.recent = deque(maxlen=20000)

    def update(self):
        if not self.path.exists():
            return
        with self.path.open("rb") as stream:
            stream.seek(self.offset)
            while True:
                start = stream.tell()
                line = stream.readline()
                if not line or not line.endswith(b"\n"):
                    self.offset = start
                    break
                event = json.loads(line)
                kind = event.get("kind")
                key = event.get("logical_key", "")
                if kind == "send_started":
                    self.active[event["request_id"]] = event
                elif kind == "attempt_terminal":
                    self.active.pop(event["request_id"], None)
                    self.statuses[event.get("status", "unknown")] += 1
                    self.recent.append(event)
                elif kind == "path_final_saved" and key not in self.finals:
                    self.finals.add(key)
                    parts = key.split("__")
                    if len(parts) >= 5:
                        self.cells[parts[1] + " / " + parts[-2]] += 1


def publish(config):
    root = Path(config["run"])
    journals = {}
    local = Path(config["snapshot"])
    local.parent.mkdir(parents=True, exist_ok=True)
    ssh = ["ssh", "-i", config["key"], "-p", str(config["port"]),
           "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", config["host"]]
    while True:
        try:
            stages = []
            for name, relative in (("首轮生成", "generation"), ("操作反馈", "feedback/requests")):
                directory = root / relative
                if not (directory / "progress.json").exists():
                    continue
                journal = journals.setdefault(relative, Journal(directory / "journal.jsonl"))
                journal.update()
                progress = read_json(directory / "progress.json")
                now = time.time()
                recent = [e for e in journal.recent if now - e.get("t", 0) <= 300]
                stages.append({"name": name, "progress": progress,
                               "active": list(journal.active.values()),
                               "cells": dict(journal.cells), "statuses": dict(journal.statuses),
                               "recent": [{k: e.get(k) for k in
                                           ("t", "status", "endpoint", "elapsed_seconds", "task_id")}
                                          for e in recent]})
            phase = "模型请求"
            if (root / "generation_receipt.json").exists():
                phase = "离线消费 / 评分（首轮已结束）"
            if len(stages) > 1:
                phase = "操作反馈 / 后处理"
            if (root / "completion.json").exists():
                phase = "流水线已收口，请查看结果审计"
            snapshot = {"published": time.time(), "run": str(root), "phase": phase, "stages": stages,
                        "populations": config["populations"]}
            local.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
            # A separate destination prevents interference with any older publisher.
            remote = config["remote_snapshot"]
            subprocess.run(["scp", "-q", "-i", config["key"], "-P", str(config["port"]),
                            "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", str(local),
                            config["host"] + ":" + remote + ".new"], check=True, timeout=20)
            subprocess.run(ssh + ["mv " + remote + ".new " + remote], check=True, timeout=20,
                           stdout=subprocess.DEVNULL)
        except Exception as exc:
            print(time.strftime("%F %T"), type(exc).__name__, str(exc), flush=True)
        time.sleep(5)


def render(snapshot):
    from rich.console import Group
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    now = time.time()
    stage = snapshot["stages"][-1]
    p = stage["progress"]
    stale = now - snapshot["published"] > 30 or now - p.get("updated_epoch", p.get("t", 0)) > 30
    live = lambda value: "未知（数据过期）" if stale else str(value)
    total, done = p.get("planned", 0), p.get("completed", 0)
    recent = [e for e in stage["recent"] if now - e["t"] <= 300]
    last_minute = [e for e in recent if now - e["t"] <= 60]
    rate = len(recent) / 300
    eta = f"约 {(total-done)/rate/60:.0f} 分钟" if rate and not stale else "暂不可估"
    filled = min(30, int(30 * done / max(total, 1)))
    header = (f"当前全量任务  |  {snapshot['phase']}  |  {time.strftime('%H:%M:%S')}\n"
              f"实际 HTTP 在途：{live(p.get('network_inflight', '未提供'))} / {p.get('global_cap')}"
              f"    许可占用：{live(p.get('inflight'))}    历史 HTTP 峰值：{p.get('network_peak')}\n"
              f"{stage['name']}  [{'█'*filled}{'░'*(30-filled)}] {done:,}/{total:,} 已终结路径（含失败）\n"
              f"近1/5分钟终结尝试：{len(last_minute)}/{len(recent)}    近5分钟完成速率：{rate:.2f}/秒"
              f"    本阶段剩余估计：{eta}\n"
              f"成功/其他物理尝试：{stage['statuses']}    数据年龄：{now-p.get('t',0):.0f}秒")
    endpoints = Table(title="模型端点：许可占用不是服务端解码并发", expand=True)
    for column in ("端点", "占用 / 有效 / 配置上限", "已发送未终结", "状态"):
        endpoints.add_column(column)
    pending = Counter(e.get("endpoint") for e in stage["active"])
    for e in p.get("endpoints", []):
        if e.get("enabled") or e.get("inflight"):
            endpoints.add_row(e["name"], f"{live(e['inflight'])} / {e['effective_cap']} / {e['cap']}",
                              live(pending[e["name"]]), e.get("state", "未知"))
    cells = Table(title="任务总览：共享控制与反馈不独立发首轮请求", expand=True)
    for column in ("基准", "Direct 首轮保存", "Ours 首轮保存", "其余4方法"):
        cells.add_column(column)
    for benchmark, size in snapshot["populations"].items():
        cells.add_row(benchmark, f"{stage['cells'].get(benchmark+' / direct',0)}/{size}",
                      f"{stage['cells'].get(benchmark+' / ours',0)}/{size}", "复用首轮/计划，离线单列")
    cells.add_row("OSDB", "未准入", "未准入", "补充结构诊断，等待参考验收")
    active = Table(title=f"进行中的请求（轮播，每页8条，共{len(stage['active'])}条；过期则仅历史记录）", expand=True)
    for column in ("题位 / 方法", "端点", "等待响应秒"):
        active.add_column(column)
    items = sorted(stage["active"], key=lambda e: e["t"])
    page = (int(now / 8) % max(1, (len(items)+7)//8))*8
    for e in items[page:page+8]:
        active.add_row(e["logical_key"].split("__",1)[-1], e["endpoint"], f"{now-e['t']:.0f}")
    events = "\n".join(f"{time.strftime('%H:%M:%S',time.localtime(e['t']))}  {e['endpoint']}  {e['status']}  {e['elapsed_seconds']} 秒" for e in recent[-3:])
    return Group(Panel(Text(header), border_style="red" if stale else "green"), endpoints, cells, active,
                 Panel(Text(events or "近5分钟无终结记录"), title="最近事件"),
                 Text("只读看板 · 终结/保存不等于答对 · ETA仅请求阶段 · 不启动/停止实验 · Ctrl+B 松开后按 D 退出"))


def view(path):
    from rich.live import Live
    from rich.text import Text
    with Live(refresh_per_second=1, screen=True) as live:
        while True:
            try:
                live.update(render(read_json(path)))
            except Exception as exc:
                live.update(Text("监控数据暂不可读：" + str(exc)))
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("publish", "view"))
    parser.add_argument("path")
    args = parser.parse_args()
    publish(read_json(args.path)) if args.mode == "publish" else view(args.path)
