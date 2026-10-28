"""Publish read-only task snapshots over SSH, independently of the agent.

Failure to publish never controls model dispatch. This process has no model key
or ledger access. The experiment itself persists all authoritative logs locally.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shlex
import subprocess
import time

from .wall_data import SnapshotReader


def publish(plan, run, ssh_command, remote_path, interval=5):
    code = (
        "from pathlib import Path; import sys; "
        "p=Path(" + repr(remote_path) + "); "
        "t=p.with_suffix('.incoming'); t.write_bytes(sys.stdin.buffer.read()); t.replace(p)"
    )
    command = list(ssh_command) + ["python3 -c " + shlex.quote(code)]
    reader = SnapshotReader()
    while True:
        value = reader.snapshot(plan, run)
        value["published_epoch"] = time.time()
        try:
            result = subprocess.run(
                command,
                input=json.dumps(value, ensure_ascii=False).encode(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=20,
            )
            if result.returncode:
                print(
                    json.dumps(
                        dict(
                            error="snapshot_publish_failed",
                            returncode=result.returncode,
                            epoch=time.time(),
                        )
                    ),
                    flush=True,
                )
        except (OSError, subprocess.TimeoutExpired) as exc:
            print(json.dumps(dict(error=type(exc).__name__, epoch=time.time())), flush=True)
        # A terminal pipeline needs no permanent polling process.
        if value["progress"].get("phase") in ("完成", "存在未完成项", "失败"):
            return
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8-sig"))
    publish(config["plan"], config["run"], config["ssh_command"], config["remote_path"])


if __name__ == "__main__":
    main()
