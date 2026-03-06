from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ValidationResult:
    valid: bool
    schema_valid: bool
    errors: list[str] = field(default_factory=list)

