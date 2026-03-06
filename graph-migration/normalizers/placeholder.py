from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from config.defaults import PLACEHOLDER_VALUES
from normalizers.parsers import find_first_url


@dataclass(frozen=True)
class PlaceholderResolution:
    should_skip_record: bool
    resolved_entity_id: str | None
    forced_label: str | None


class PlaceholderPolicyResolver:
    def __init__(self, policy: str) -> None:
        normalized = policy.strip().lower()
        if normalized not in {"skip", "unknown", "external_if_url"}:
            raise ValueError("Unsupported placeholder policy. Use skip/unknown/external_if_url.")
        self.policy = normalized

    def resolve(
        self,
        raw_entity_id: str | None,
        side: str,
        record_index: int,
        context_values: list[Any],
    ) -> PlaceholderResolution:
        if not self._is_placeholder(raw_entity_id):
            return PlaceholderResolution(
                should_skip_record=False,
                resolved_entity_id=str(raw_entity_id),
                forced_label=None,
            )
        if self.policy == "skip":
            return PlaceholderResolution(
                should_skip_record=True,
                resolved_entity_id=None,
                forced_label=None,
            )
        if self.policy == "external_if_url":
            url = find_first_url(*(context_values or []))
            if url:
                return PlaceholderResolution(
                    should_skip_record=False,
                    resolved_entity_id=url,
                    forced_label="ExternalResource",
                )
        unknown_id = f"unknown:{side}:{record_index}"
        return PlaceholderResolution(
            should_skip_record=False,
            resolved_entity_id=unknown_id,
            forced_label="UnknownObject",
        )

    def _is_placeholder(self, value: str | None) -> bool:
        if value is None:
            return True
        norm = str(value).strip().lower()
        return norm in PLACEHOLDER_VALUES

