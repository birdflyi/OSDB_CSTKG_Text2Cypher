"""Account for physical source attempts once, including failures and unknowns."""

from collections import defaultdict
import math

from .config import canonical


def summarize_attempts(records):
    """Records are {source_id, terminal}; source_id comes from the ledger audit.

    A cached view references that same source_id. New attempts have new IDs,
    even when request bytes match. Unknown usage and fees never become zero.
    """
    unique = {}
    for record in records:
        identity = record["source_id"]
        if not isinstance(identity, str) or not identity:
            raise ValueError("An audited physical source identity is required")
        terminal = record["terminal"]
        if identity in unique and canonical(unique[identity]) != canonical(terminal):
            raise ValueError("Conflicting physical cost evidence: " + identity)
        unique[identity] = terminal
    grouped = defaultdict(list)
    for identity, terminal in unique.items():
        grouped[(terminal.get("requested_model", terminal.get("model")),
                 terminal.get("method"), terminal.get("graph"))].append(terminal)

    def total(rows):
        value = dict(physical_records=len(rows), charged=sum(r.get("charged") is True for r in rows),
                     charge_unknown=sum(r.get("charged") is None for r in rows),
                     failures=sum(r.get("status") != "ok" for r in rows))
        for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
            known = [(r.get("usage") or {}).get(field) for r in rows]
            if any(v is not None and (type(v) is not int or v < 0) for v in known):
                raise ValueError("Invalid usage value")
            value[field] = dict(known_sum=sum(v for v in known if v is not None),
                                unknown_records=sum(v is None for v in known))
        fees = [r.get("monetary_cost") for r in rows]
        if any(v is not None and (type(v) not in (int, float)
               or not math.isfinite(v) or v < 0) for v in fees):
            raise ValueError("Invalid monetary cost")
        currencies = {r.get("currency") for r in rows if r.get("monetary_cost") is not None}
        currency = next(iter(currencies)) if len(currencies) == 1 else None
        if not isinstance(currency, str) or not currency:
            currency = None
        value["monetary_cost"] = sum(fees) if rows and currency and all(v is not None for v in fees) else None
        value["currency"] = currency
        value["fee_unknown_records"] = sum(v is None for v in fees)
        return value

    return dict(total=total(list(unique.values())), groups=[
        dict(model=key[0], method=key[1], graph=key[2], **total(rows))
        for key, rows in grouped.items()],
        billing_deduplicated_by_source=True,
        source_ledger_audit_required=True)
