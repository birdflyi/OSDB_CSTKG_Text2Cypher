"""Read-only Rich task wall. A live wall is not a live model dispatcher."""

from __future__ import annotations

import json
from pathlib import Path
import time


def read(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {}


def snapshot(plan_path, root):
    plan, root = read(plan_path), Path(root)
    progress = read(root / "progress.json")
    cells = []
    for cell in plan.get("cells", []):
        block = root / "blocks" / cell["name"]
        receipt = read(block / "completed.json") or read(block / "incomplete.json")
        missing = cell.get("missing", [])
        status = receipt.get("status") or ("缺项" if missing else "待运行")
        if not receipt and (block / "started.json").exists():
            status = "运行中" if progress.get("phase") == "运行中" else "中断待审计"
        result = receipt.get("result", {})
        runtime = read(cell["progress_path"]) if cell.get("progress_path") else {}
        cells.append(
            dict(
                method=cell["method"],
                benchmark=cell["benchmark"],
                state=status,
                smoke=result.get("smoke_passed", False),
                role=cell.get("role", "independent"),
                missing="; ".join(missing),
                planned=cell.get("planned_paths"),
                completed=runtime.get("completed"),
                network=runtime.get("network_inflight"),
            )
        )
    return dict(mode=plan.get("mode"), progress=progress, cells=cells)


def watch(plan_path, root, interval=2):
    from rich.console import Group
    from rich.live import Live
    from rich.table import Table
    from rich.text import Text

    def render():
        value = read(Path(root) / "wall_snapshot.json") or snapshot(plan_path, root)
        table = Table(title="T1 method × benchmark 任务墙", expand=True)
        for title in (
            "Benchmark",
            "Method / control",
            "状态",
            "完成/计划",
            "HTTP在途",
            "Smoke通过",
            "缺项 / 范围",
        ):
            table.add_column(title, overflow="fold")
        for row in value["cells"]:
            table.add_row(
                row["benchmark"],
                row["method"],
                row["state"],
                str(row["completed"] if row["completed"] is not None else "?")
                + "/"
                + str(row["planned"] if row["planned"] is not None else "?"),
                str(row["network"] if row["network"] is not None else "?"),
                "是" if row["smoke"] else "否",
                row["missing"] or row["role"],
            )
        progress = value["progress"]
        updated = progress.get("updated_epoch")
        age = "无运行心跳" if not updated else "状态距今 %.0f 秒" % (time.time() - updated)
        heading = Text(
            "模式：%s | %s | %s\n看板只读；看板存活不代表模型请求在运行。"
            % (value["mode"], progress.get("phase", "未启动"), age)
        )
        return Group(heading, table)

    with Live(render(), auto_refresh=False, screen=True) as live:
        while True:
            time.sleep(interval)
            live.update(render(), refresh=True)
