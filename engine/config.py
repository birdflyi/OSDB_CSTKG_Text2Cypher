from __future__ import annotations

from dataclasses import dataclass, field
from dataclasses import fields, replace
import hashlib
import json
import math
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
class Timeouts:
    """Runtime deadlines. Model input manifests retain their explicit read limit."""

    model: float = 240
    worker_ready: float = 600
    explain: float = 30
    query: float = 60
    rpc: float = 125
    http_connect: float = 10
    ssh_connect: float = 10
    ssh_keepalive_interval: float = 15
    ssh_keepalive_count: int = 4
    admission: float = 600
    queue_total: float = 900
    channel_close: float = 60
    process_terminate: float = 10
    reader_join: float = 2

    def __post_init__(self):
        for name, value in vars(self).items():
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError("Timeout must be positive and finite: " + name)
        if type(self.ssh_keepalive_count) is not int:
            raise ValueError("ssh_keepalive_count must be an integer")
        if self.rpc <= self.explain + self.query:
            raise ValueError("RPC deadline must exceed EXPLAIN plus query deadlines")


DEFAULT_TIMEOUTS = Timeouts()


def timeouts_from(values=None):
    if isinstance(values, Timeouts):
        return values
    values = {} if values is None else dict(values)
    unknown = set(values) - {f.name for f in fields(Timeouts)}
    if unknown:
        raise ValueError("Unknown timeout fields: " + ", ".join(sorted(unknown)))
    return Timeouts(**values)


@dataclass(frozen=True)
class Sampling:
    model: str
    temperature: float = 0.2
    top_p: float = 0.95
    reasoning_effort: str = "high"
    max_tokens: int = 8192
    read_timeout_seconds: float = DEFAULT_TIMEOUTS.model
    reasoning_format: str = "standard"
    reasoning_budget: int = 50

    def __post_init__(self):
        if not self.model or self.max_tokens < 1 or self.read_timeout_seconds <= 0:
            raise ValueError("Explicit model and positive limits required")
        if (
            self.reasoning_format not in ("standard", "chat_template")
            or self.reasoning_budget < 1
        ):
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
            or type(self.enabled) is not bool
            or (self.enabled and self.cap == 0)
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

    def __post_init__(self):
        if not 1 <= self.max_attempts <= 3 or self.wait_seconds < 10:
            raise ValueError("At most3 attempts; retry interval at least10 seconds")
        if self.retry_unknown and self.before_write_only:
            raise ValueError(
                "Unknown-outcome retry requires explicit after-write authorization"
            )


def retry_policy(values):
    """Migrate only named obsolete knobs; never silently ignore an unknown policy."""
    import warnings
    from dataclasses import fields

    values = dict(values)
    removed = sorted(set(values) & {"recovery_cap", "recovery_min_cap"})
    if removed:
        warnings.warn(
            "Removed retry settings ignored: "
            + ", ".join(removed)
            + "; endpoint-local recovery now owns capacity",
            UserWarning,
        )
        for key in removed:
            values.pop(key)
    unknown = set(values) - {f.name for f in fields(RetryPolicy)}
    if unknown:
        raise ValueError("Unknown retry policy fields: " + ", ".join(sorted(unknown)))
    return RetryPolicy(**values)


@dataclass(frozen=True)
class CampaignSpec:
    name: str
    model: str
    run_dir: Path
    ledger_dir: Path
    approval_path: Path
    budget_scope: str
    global_cap: int
    admission_timeout_seconds: float = None  # Read-only compatibility with old configs.
    db_cap: int = 4
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    compare_with_legacy: bool = False
    models: tuple = ()
    batch_size: int = 8
    batch_interval_seconds: float = 0
    queue_wait_until_available: bool = False
    max_queue_wait_seconds: float = None
    timeouts: Timeouts = field(default_factory=Timeouts)

    def __post_init__(self):
        timeouts = timeouts_from(self.timeouts)
        aliases = {
            name: value
            for name, value in (
                ("admission", self.admission_timeout_seconds),
                ("queue_total", self.max_queue_wait_seconds),
            )
            if value is not None
        }
        object.__setattr__(self, "timeouts", replace(timeouts, **aliases))
        if (
            not self.name
            or not self.model
            or not self.budget_scope
            or self.global_cap < 1
            or self.db_cap < 1
            or type(self.batch_size) is not int
            or self.batch_size < 2
            or self.batch_interval_seconds < 0
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
