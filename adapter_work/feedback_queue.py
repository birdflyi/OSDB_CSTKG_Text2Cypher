"""Operational feedback preparation. No reference, database or HTTP access."""

from dataclasses import dataclass


@dataclass
class OperationalError:
    type: str
    message: str

    def __str__(self):
        return self.message


def operational_error(answer):
    """Only a known query/plan rejection can request model repair."""
    code = answer.get("database_code") or ""
    known = (
        code.startswith("Neo.ClientError.Statement.")
        or answer.get("status") in ("EXECUTION_ERROR", "PROTOCOL_ERROR")
        or answer.get("error_type") == "ReadOnlyViolation"
    )
    if not known:
        return None
    text = (
        answer.get("error")
        or answer.get("operational_error")
        or "Database rejected the query"
    )
    return OperationalError("ClientError", text[:1000])
