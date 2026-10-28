"""Publish read-only task snapshots over SSH, independently of the agent.

Failure to publish never controls model dispatch. This process has no model key
or ledger access. The experiment itself persists all authoritative logs locally.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shlex
import time

from .wall_data import SnapshotReader
from .execution import JsonRpcClient


def publish(
    plan,
    run,
    ssh_command,
    remote_path,
    interval=5,
    *,
    expected_hostname=None,
    owner_path=None,
    client_factory=JsonRpcClient,
):
    code = "REMOTE_PATH = " + repr(remote_path) + "\nOWNER_PATH = " + repr(owner_path) + "\n"
    code += """import json,socket,sys
from pathlib import Path
p=Path(REMOTE_PATH)
if OWNER_PATH:
    assert json.loads(Path(OWNER_PATH).read_text())['hostname']==socket.gethostname()
print(json.dumps({'kind':'READY','role':'dashboard-publisher','hostname':socket.gethostname()}),flush=True)
for line in sys.stdin:
    request=json.loads(line)
    p.parent.mkdir(parents=True,exist_ok=True)
    temporary=p.with_name(p.name+'.'+request['id']+'.tmp')
    temporary.write_text(json.dumps(request['snapshot'],ensure_ascii=False),encoding='utf-8')
    temporary.replace(p)
    print(json.dumps({'id':request['id'],'status':'saved'}),flush=True)
"""
    ssh = list(ssh_command)
    ssh[1:1] = ["-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4"]
    command = ssh + ["python3 -u -c " + shlex.quote(code)]
    reader = SnapshotReader()
    ready = dict(role="dashboard-publisher")
    if expected_hostname:
        ready["hostname"] = expected_hostname
    client = None
    final_failures = 0
    log = Path(run).parent / "mirror-channel.stderr.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a", encoding="utf-8") as stderr:
        try:
            while True:
                value = reader.snapshot(plan, run)
                value["published_epoch"] = time.time()
                delivered = False
                try:
                    if client is None:
                        client = client_factory(
                            command, expected_ready=ready, stderr=stderr, ready_timeout=20
                        )
                    reply = client.request(dict(kind="snapshot", snapshot=value), timeout=20)
                    if reply.get("status") != "saved":
                        raise ValueError("Snapshot acknowledgement missing")
                    delivered = True
                except Exception as exc:
                    print(json.dumps(dict(error=type(exc).__name__, epoch=time.time())), flush=True)
                    if client is not None:
                        client.close()
                        client = None
                terminal = value["progress"].get("phase") in ("完成", "存在未完成项", "失败")
                if terminal:
                    final_failures += not delivered
                    if delivered or final_failures >= 3:
                        return
                time.sleep(interval)
        finally:
            if client is not None:
                client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8-sig"))
    publish(
        config["plan"],
        config["run"],
        config["ssh_command"],
        config["remote_path"],
        expected_hostname=config.get("expected_hostname"),
        owner_path=config.get("owner_path"),
    )


if __name__ == "__main__":
    main()
