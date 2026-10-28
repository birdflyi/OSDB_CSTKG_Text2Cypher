from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit


def canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Sampling:
    model: str
    temperature: float = 0.2
    top_p: float = 0.95
    reasoning_effort: str = "high"
    max_tokens: int = 8192
    read_timeout_seconds: float = 240
    reasoning_format: str = "standard"
    reasoning_budget: int = 50

    def __post_init__(self):
        if not self.model or self.max_tokens < 1 or self.read_timeout_seconds <= 0:
            raise ValueError("Explicit model and positive limits required")
        if self.reasoning_format not in ("standard", "chat_template") or self.reasoning_budget < 1:
            raise ValueError("Explicit reasoning wire format required")

    def body(self, messages: list) -> dict:
        body = dict(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            top_p=self.top_p,
        )
        if self.reasoning_format == "chat_template":
            body["chat_template_kwargs"] = dict(
                thinking=True, reasoning_effort=self.reasoning_budget
            )
        else:
            body["reasoning_effort"] = self.reasoning_effort
        body["max_tokens"] = self.max_tokens
        return body


@dataclass(frozen=True)
class Endpoint:
    name: str
    url: str
    model: str
    key_ref: str
    cap: int
    enabled: bool = True
    wire_params: tuple = (
        "model",
        "messages",
        "temperature",
        "top_p",
        "reasoning_effort",
        "max_tokens",
    )

    def __post_init__(self):
        url = urlsplit(self.url)
        if (
            not self.name
            or not self.model
            or type(self.cap) is not int
            or self.cap < 0
            or url.scheme not in ("http", "https")
            or not url.hostname
            or url.username
            or url.password
            or url.fragment
            or url.query
        ):
            raise ValueError("Invalid endpoint; URL credentials/query are forbidden")
        if Path(self.key_ref).name != self.key_ref or not self.key_ref:
            raise ValueError("key_ref must be a plain local key name")

    @property
    def identity(self) -> tuple:
        # Routing identity, NOT proof of a separate physical backend.
        return self.name, self.url, self.model


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    wait_seconds: float = 10
    before_write_only: bool = True
    retry_unknown: bool = False
    recovery_cap: int = 16
    recovery_min_cap: int = 4

    def __post_init__(self):
        if not 1 <= self.max_attempts <= 3 or self.wait_seconds < 10:
            raise ValueError("At most3 attempts; retry interval at least10 seconds")
        if not 1 <= self.recovery_min_cap <= self.recovery_cap:
            raise ValueError("Invalid recovery concurrency bounds")
        if self.retry_unknown and self.before_write_only:
            raise ValueError("Unknown-outcome retry requires explicit after-write authorization")


@dataclass(frozen=True)
class CampaignSpec:
    name: str
    model: str
    run_dir: Path
    ledger_dir: Path
    approval_path: Path
    budget_scope: str
    global_cap: int = 128
    admission_timeout_seconds: float = 600
    quarantine_seconds: float = 60
    consecutive_failures: int = 3
    db_cap: int = 4
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    compare_with_legacy: bool = False
    models: tuple = ()
    batch_size: int = 8
    batch_interval_seconds: float = 0
    queue_wait_until_available: bool = False

    def __post_init__(self):
        if (
            not self.name
            or not self.model
            or not self.budget_scope
            or self.global_cap < 1
            or self.db_cap < 1
            or self.admission_timeout_seconds <= 0
            or type(self.batch_size) is not int
            or self.batch_size < 2
            or self.batch_interval_seconds < 0
            or self.quarantine_seconds < 0
            or self.consecutive_failures < 1
        ):
            raise ValueError("Incomplete campaign or invalid concurrency limits")


def endpoints_from_file(path: Path) -> list:
    value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    rows = []
    for row in value["endpoints"]:
        data = dict(row)
        if "wire_params" in data:
            data["wire_params"] = tuple(data["wire_params"])
        rows.append(Endpoint(**data))
    if len({row.name for row in rows}) != len(rows):
        raise ValueError("Endpoint names must be unique in each registry revision")
    return rows
