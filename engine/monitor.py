from __future__ import annotations

import json
from pathlib import Path
import time


def snapshot(root):
    root = Path(root)

    def read(path):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    pipeline = read(root / "progress.json")
    rows = []
    for path in sorted((root / "runs").glob("**/progress.json")):
        row = read(path)
        if not row:
            continue
        rows.append(
            dict(
                名称=str(path.parent.relative_to(root / "runs")),
                阶段=row.get("phase", "未知"),
                完成=row.get("completed", 0),
                计划=row.get("planned", 0),
                许可占用=row.get("inflight", 0),
                实际HTTP=row.get("network_inflight", 0),
                HTTP峰值=row.get("network_peak", 0),
                排队=row.get("waiting", 0),
            )
        )
    return dict(
        流水线=pipeline.get("phase", "尚未启动"),
        当前块=pipeline.get("current_block", ""),
        已完成块=pipeline.get("completed", 0),
        计划块=pipeline.get("planned", 0),
        任务=rows,
    )


def watch(root, interval=2):
    """Read derived counters only; no model calls, DB reads or tokenization."""
    try:
        from rich.live import Live
        from rich.table import Table
    except ImportError:
        last = None
        while True:
            value = snapshot(root)
            if value != last:
                print(json.dumps(value, ensure_ascii=False), flush=True)
                last = value
            time.sleep(interval)
    else:

        def render():
            value = snapshot(root)
            table = Table(title="实验流水线：" + value["流水线"])
            for name in ["任务", "阶段", "完成/计划", "实际HTTP", "许可占用", "排队", "HTTP峰值"]:
                table.add_column(name)
            for row in value["任务"]:
                table.add_row(
                    row["名称"],
                    row["阶段"],
                    str(row["完成"]) + "/" + str(row["计划"]),
                    str(row["实际HTTP"]),
                    str(row["许可占用"]),
                    str(row["排队"]),
                    str(row["HTTP峰值"]),
                )
            return table

        with Live(render(), auto_refresh=False) as live:
            while True:
                time.sleep(interval)
                live.update(render(), refresh=True)
