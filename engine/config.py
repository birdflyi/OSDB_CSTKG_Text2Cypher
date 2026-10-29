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


@dataclass(frozen=True)
class EndpointPolicy:
    failures_to_degrade: int = 3
    successes_to_restore: int = 3
    degraded_ttl_seconds: float = 600
    quarantine_seconds: float = 60

    def __post_init__(self):
        for name in ("failures_to_degrade", "successes_to_restore"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError("Positive integer required: " + name)
        for name in ("degraded_ttl_seconds", "quarantine_seconds"):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError("Positive finite duration required: " + name)


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
    serves: tuple = ()
    family: str = ""
    alias_evidence_sha256: str = ""

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
        if not isinstance(self.family, str):
            raise ValueError("family must be a string")
        if self.alias_evidence_sha256 and (
            len(self.alias_evidence_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.alias_evidence_sha256)
        ):
            raise ValueError("alias_evidence_sha256 must identify vetted equivalence evidence")
        if not all(isinstance(name, str) and name for name in self.serves):
            raise ValueError("serves must list non-empty model names")
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
        if (
            type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 3
            or type(self.wait_seconds) not in (int, float)
            or not math.isfinite(self.wait_seconds)
            or self.wait_seconds < 10
            or type(self.before_write_only) is not bool
            or type(self.retry_unknown) is not bool
        ):
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
    endpoint_policy: EndpointPolicy = field(default_factory=EndpointPolicy)

    def __post_init__(self):
        timeouts = timeouts_from(self.timeouts)
        if not isinstance(self.endpoint_policy, EndpointPolicy):
            object.__setattr__(
                self, "endpoint_policy", EndpointPolicy(**self.endpoint_policy)
            )
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
            or type(self.global_cap) is not int
            or self.global_cap < 1
            or type(self.db_cap) is not int
            or self.db_cap < 1
            or type(self.queue_wait_until_available) is not bool
            or type(self.batch_size) is not int
            or self.batch_size < 2
            or self.batch_interval_seconds < 0
        ):
            raise ValueError("Incomplete campaign or invalid concurrency limits")


def registry_from_file(path: Path) -> dict:
    value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    allowed = {"endpoints", "global_cap", "reload_seconds", "per_model_cap"}
    if not isinstance(value, dict) or set(value) - allowed:
        raise ValueError("Registry allows only: " + ", ".join(sorted(allowed)))
    cap = value.get("global_cap")
    if cap is not None and (type(cap) is not int or cap < 1):
        raise ValueError("global_cap must be a positive integer")
    interval = value.get("reload_seconds", 10)
    if (
        type(interval) not in (int, float)
        or not math.isfinite(interval)
        or interval <= 0
    ):
        raise ValueError("reload_seconds must be positive and finite")
    if not isinstance(value.get("endpoints"), list):
        raise ValueError("endpoints must be a list")
    rows = []
    for row in value["endpoints"]:
        data = dict(row)
        if "wire_params" in data:
            data["wire_params"] = tuple(data["wire_params"])
        rows.append(Endpoint(**data))
    if len({row.name for row in rows}) != len(rows):
        raise ValueError("Endpoint names must be unique in each registry revision")
    model_caps = value.get("per_model_cap", {})
    if not isinstance(model_caps, dict):
        raise ValueError("per_model_cap must be a mapping")
    models = {row.model for row in rows}
    models.update(alias for row in rows if row.alias_evidence_sha256 for alias in row.serves)
    for model, limit in model_caps.items():
        if model not in models:
            raise ValueError("per_model_cap names an undeclared model: " + model)
        if type(limit) is not int or limit < 1:
            raise ValueError("per_model_cap must be a positive integer: " + model)
    return dict(
        endpoints=rows,
        global_cap=cap,
        reload_seconds=interval,
        per_model_cap=dict(model_caps),
    )


def endpoints_from_file(path: Path) -> list:
    return registry_from_file(path)["endpoints"]


def validate_run_fields(value):
    """Reject misspelled runtime settings instead of silently ignoring them."""
    allowed = {f.name for f in fields(CampaignSpec)} | {
        "admission_path",
        "manifest_path",
        "registry_path",
        "key_dir",
        "legacy_ledger",
        "protected_roots",
        "path_rewrites",
        "stages",
        "analysis",
        "recovery",
        "continue_on_path_failures",
        "performance_report",
        "prefetch_initial",
        "overlap_initial_generation",
        "config_schema_version",
    }
    unknown = set(value) - allowed
    if unknown:
        raise ValueError(
            "Unknown run settings: "
            + ", ".join(sorted(unknown))
            + "; allowed: "
            + ", ".join(sorted(allowed))
        )
