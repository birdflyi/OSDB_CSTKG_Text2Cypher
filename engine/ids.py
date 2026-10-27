from __future__ import annotations

import hashlib
import re


def component(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Nonempty identifier required")
    if (
        re.fullmatch(r"[A-Za-z0-9_.-]{1,72}", value)
        and "__" not in value
        and value not in (".", "..")
    ):
        return value
    return "id-" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def logical_key(benchmark: str, task: str, method: str, phase: str) -> str:
    if phase not in ("initial", "feedback"):
        raise ValueError("Unknown generation phase")
    return "__".join(component(v) for v in (benchmark, task, method, phase))


def request_id(key: str, attempt_no: int) -> str:
    if type(attempt_no) is not int or attempt_no < 1 or not re.fullmatch(r"[A-Za-z0-9_.-]+", key):
        raise ValueError("Invalid request identity")
    return key + "__a" + str(attempt_no)
