"""Detach the ordinary pipeline process; no agent or scheduler service needed."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import time

from .config import canonical, digest
from .journal import write_new


def launch_process(command, directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    options = dict(
        stdin=subprocess.DEVNULL, close_fds=True, cwd=str(Path(__file__).resolve().parent.parent)
    )
    if os.name == "nt":
        options["creationflags"] = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options["start_new_session"] = True
    with (directory / "stdout.log").open("xb") as stdout, (directory / "stderr.log").open(
        "xb"
    ) as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, **options)
    receipt = dict(
        pid=process.pid,
        launched_epoch=time.time(),
        command=command,
        command_sha256=digest(canonical(command)),
        detached=True,
        stdout=str(directory / "stdout.log"),
        stderr=str(directory / "stderr.log"),
    )
    write_new(directory / "launch.json", canonical(receipt))
    return receipt


def launch_pipeline(config, directory):
    config = Path(config).resolve(strict=True)
    return launch_process([sys.executable, "-m", "engine.cli", "pipeline", str(config)], directory)
