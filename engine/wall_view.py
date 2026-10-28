"""Chinese Rich presentation and local selection; no experiment control."""

from dataclasses import dataclass
from pathlib import Path
import math
import sys
import time
from urllib.parse import urlsplit

from .wall_data import SnapshotReader


def duration(seconds):
    if seconds is None:
        return "暂无法估计"
    seconds = max(0, int(seconds))
    return f"{seconds//60}分{seconds%60:02d}秒"


def bar(done, total, width=12):
    if total is None:
        return "题数未确定"
    filled = min(width, int(width * done / total)) if total else 0
    return "[" + "#" * filled + "-" * (width - filled) + f"] {done}/{total}"


@dataclass
class Selection:
    task: str = ""
    case: str = ""
    page: int = 0
    matrix_page: int = 0
    follow: bool = True
    show_disabled: bool = False

    def choose(self, value):
        cells = value.get("cells", [])
        if not cells:
            return {}
        if self.follow:
            self.task = value.get("progress", {}).get("current_block", "")
        selected = next((c for c in cells if c["name"] == self.task), None)
        if selected is None:
            selected = next(
                (c for c in cells if "处理" in c["state"] or "派发" in c["state"]), cells[0]
            )
            self.task = selected["name"]
        cases = selected.get("cases", [])
        if self.follow or self.case not in {c["task_id"] for c in cases}:
            active = [c for c in cases if c["state"] not in ("已完成", "等待发送")]
            self.case = (active or cases or [{}])[0].get("task_id", "")
        return selected

    def key(self, key, value, page_size=5):
        cells = value.get("cells", [])
        if key == "a":
            self.follow = not self.follow
        elif key == "e":
            self.show_disabled = not self.show_disabled
        elif key == "m":
            self.matrix_page += 1
        elif key in ("j", "k") and cells:
            index = next((i for i, c in enumerate(cells) if c["name"] == self.task), 0)
            self.task = cells[(index + (1 if key == "j" else -1)) % len(cells)]["name"]
            self.follow, self.case, self.page = False, "", 0
        elif key in ("n", "p"):
            self.follow = False
            self.page = max(0, self.page + (1 if key == "n" else -1))
        elif key in ("h", "l"):
            cases = self.choose(value).get("cases", [])
            if cases:
                index = next((i for i, c in enumerate(cases) if c["task_id"] == self.case), 0)
                index = (index + (1 if key == "l" else -1)) % len(cases)
                self.follow, self.case, self.page = (
                    False,
                    cases[index]["task_id"],
                    index // page_size,
                )


def render(value, selection, height=50, now=None):
    from rich.console import Group
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    now = time.time() if now is None else now
    chosen = selection.choose(value)
    cells = value.get("cells", [])
    blocked = sum(c["state"] == "缺适配或资源" for c in cells)
    finished = sum(c["state"] == "已完成" for c in cells)
    phase = value.get("progress", {}).get("phase", "未知")
    status = "监控数据过期，实际并发未知" if value.get("stale") else phase
    headline = f"组合共{len(cells)}项 | 已完成{finished}项 | 缺适配或资源{blocked}项 | {status}"
    headline += "\n总体预计剩余：" + (
        "有未准入任务，暂无法估计" if blocked else duration(chosen.get("eta_seconds"))
    )
    headline += f" | 更新于{time.strftime('%H:%M:%S',time.localtime(value.get('published_epoch',value.get('generated_epoch',now))))}"
    matrix = Table(title="全部方法 × 基准总览", expand=True)
    benchmarks = list(dict.fromkeys(c["benchmark"] for c in cells))
    methods = list(dict.fromkeys(c["method"] for c in cells))
    matrix.add_column("方法")
    for benchmark in benchmarks:
        matrix.add_column(benchmark, overflow="fold")
    matrix_size = max(3, min(10, height // 5))
    pages = max(1, math.ceil(len(methods) / matrix_size))
    matrix_start = selection.matrix_page % pages * matrix_size
    for method in methods[matrix_start : matrix_start + matrix_size]:
        values = []
        for benchmark in benchmarks:
            c = next(
                (c for c in cells if c["method"] == method and c["benchmark"] == benchmark), None
            )
            values.append(
                "未列入"
                if c is None
                else c["state"]
                + f" {c.get('completed',0)}/{c.get('planned') or '--'}"
                + ("（共享）" if c.get("role") == "共享首轮结果" else "")
            )
        matrix.add_row(method, *values, style="bold cyan" if method == chosen.get("method") else "")
    matrix.caption = f"矩阵第{selection.matrix_page%pages+1}/{pages}页；进度按最终评分计算"
    endpoints = Table(title="模型端点：实际HTTP与容量", expand=True)
    for title in (
        "端点 / 模型",
        "状态",
        "配置 / 生效上限",
        "实际HTTP占用",
        "最近1分钟 发送/完成",
        "近5分钟耗时 均值/P50/P90",
    ):
        endpoints.add_column(title, overflow="fold")
    for endpoint in value.get("endpoints", []):
        if not endpoint["enabled"] and not selection.show_disabled:
            continue
        actual = endpoint.get("inflight")
        effective = endpoint["effective_cap"] if endpoint["enabled"] else 0
        occupancy = "未知" if actual is None else bar(actual, effective, 10)
        latency = endpoint.get("latency", {})
        timing = " / ".join(
            "--" if latency.get(k) is None else f"{latency[k]:.1f}秒"
            for k in ("mean", "p50", "p90")
        )
        endpoints.add_row(
            urlsplit(endpoint["url"]).netloc + "\n" + endpoint["model"],
            "启用" if endpoint["enabled"] else "已禁用",
            f"{endpoint['cap']} / {effective}",
            occupancy,
            f"{endpoint['sent_60s']} / {endpoint['completed_60s']}",
            timing,
        )
    actual = value.get("network_inflight")
    endpoints.caption = f"全部实际在途 {actual if actual is not None else '未知'} / 总上限{value.get('global_cap') or '--'}；不把许可占用当HTTP并发"
    disabled = [e for e in value.get("endpoints", []) if not e["enabled"]]
    if disabled and not selection.show_disabled:
        endpoints.caption += f"；另有{len(disabled)}条停用路由，E键展开"
    cases = chosen.get("cases", [])
    task = f"模型阶段 {bar(chosen.get('model_completed',0),chosen.get('planned'))}\n"
    task += f"最终答案 {bar(chosen.get('final_completed',0),chosen.get('planned'))}\n"
    task += f"评分完成 {bar(chosen.get('completed',0),chosen.get('planned'))}\n"
    task += f"已运行 {duration(chosen.get('elapsed_seconds'))} | 预计剩余 {duration(chosen.get('eta_seconds'))}（近期完成率估计）"
    if chosen.get("recovered"):
        task += "\n包含已披露的恢复版本，不是一次无中断正式结果"
    if chosen.get("missing"):
        task += "\n缺项：" + chosen["missing"]
    selected = next((c for c in cases if c["task_id"] == selection.case), {})
    detail = f"编号 {selected.get('number','--')} | 图 {selected.get('graph','--')} | {selected.get('state','暂无案例')}\n"
    end = selected.get("scored_at") or now
    detail += f"本步骤 {duration(now-selected['stage_started']) if selected.get('stage_started') and not selected.get('scored') else '--'} | 本轮用时 {duration(end-selected['started']) if selected.get('started') else '--'}"
    detail += f" | 模型尝试 {selected.get('attempts',0)}{'（复用已有回复）' if selected.get('cached') else ''} | 端点 {selected.get('endpoint') or '--'}"
    page_size = max(2, min(5, height // 12))
    case_pages = max(1, math.ceil(len(cases) / page_size))
    selection.page %= case_pages
    if selection.follow and selected:
        selection.page = (selected["number"] - 1) // page_size
    rows = Table(
        title=f"当前任务案例 第{selection.page+1}/{case_pages}页（编号不跳动）", expand=True
    )
    for title in ("编号", "案例ID", "图", "当前步骤", "步骤耗时", "尝试", "答案"):
        rows.add_column(title, overflow="fold")
    for c in cases[selection.page * page_size : (selection.page + 1) * page_size]:
        grade = "未评分" if c.get("correct") is None else ("正确" if c["correct"] else "错误")
        rows.add_row(
            str(c["number"]),
            c["task_id"][:12],
            c["graph"],
            c["state"],
            duration(now - c["stage_started"]) if c.get("stage_started") and not c.get("scored") else "--",
            str(c["attempts"]),
            grade,
            style="bold cyan" if c["task_id"] == selection.case else "",
        )
    event_names = {
        "send_started": "开始模型请求",
        "attempt_terminal": "模型请求终结",
        "path_final_saved": "结果已保存",
        "graph_scored": "本图评分完成",
    }
    events = []
    for event in value.get("recent_events", [])[-2:]:
        label = event.get("stage") or event_names.get(event["kind"], "阶段记录")
        events.append(
            time.strftime("%H:%M:%S", time.localtime(event["t"]))
            + " "
            + event.get("cell", "")
            + " "
            + label
        )
    footer = "J/K选任务 H/L选案例 N/P案例翻页 M矩阵翻页 E停用端点 A跟随/固定 Q退出（不停止实验）"
    task_panel = Panel(
        Text(task), title=f"当前任务：{chosen.get('method','--')} × {chosen.get('benchmark','--')}"
    )
    detail_panel = Panel(Text(detail), title="当前关注案例")
    if height < 40:
        return Group(
            Panel(Text(headline), title="实验流水线总览"),
            endpoints,
            task_panel,
            detail_panel,
            Text("终端不足40行，暂收起完整矩阵和案例表；仍可用J/K、H/L选择。"),
            Text(footer),
        )
    return Group(
        Panel(Text(headline), title="实验流水线总览"),
        matrix,
        endpoints,
        task_panel,
        detail_panel,
        rows,
        Text("最近事件：\n" + "\n".join(events)),
        Text(footer),
    )


class TerminalKeys:
    """Single-letter navigation; restore terminal settings on every exit."""

    def __enter__(self):
        self.saved = None
        if sys.stdin.isatty() and sys.platform != "win32":
            import termios, tty

            self.saved = termios.tcgetattr(sys.stdin)
            tty.setcbreak(sys.stdin.fileno())
        return self

    def poll(self):
        if not sys.stdin.isatty():
            return ""
        if sys.platform == "win32":
            import msvcrt

            return msvcrt.getwch().lower() if msvcrt.kbhit() else ""
        import select

        return sys.stdin.read(1).lower() if select.select([sys.stdin], [], [], 0)[0] else ""

    def __exit__(self, *unused):
        if self.saved is not None:
            import termios

            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self.saved)


def watch(plan_path, root, interval=2):
    from rich.console import Console
    from rich.live import Live
    from .matrix_wall import read

    console = Console()
    reader, selection = SnapshotReader(), Selection()
    value = {}
    updated = 0
    with TerminalKeys() as keys, Live(console=console, auto_refresh=False, screen=True) as live:
        while True:
            if time.monotonic() - updated >= interval:
                value = read(Path(root) / "wall_snapshot.json") or reader.snapshot(plan_path, root)
                updated = time.monotonic()
                if (
                    value.get("progress", {}).get("phase") == "运行中"
                    and time.time() - value.get("published_epoch", value.get("generated_epoch", 0))
                    > 15
                ):
                    value = dict(
                        value,
                        stale=True,
                        network_inflight=None,
                        endpoints=[dict(e, inflight=None) for e in value.get("endpoints", [])],
                    )
            key = keys.poll()
            if key == "q":
                return
            if key:
                selection.key(key, value, max(2, min(5, console.height // 12)))
            live.update(render(value, selection, console.height), refresh=True)
            time.sleep(0.2)
