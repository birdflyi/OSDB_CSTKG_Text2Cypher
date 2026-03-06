from __future__ import annotations

from typing import Any

from config.defaults import (
    DEFAULT_ALLOWED_REL_VOCAB,
    DEFAULT_RELATION_FALLBACK,
    DEFAULT_RELATION_RULES,
)


class RelationMapper:
    def __init__(
        self,
        rules: list[dict[str, Any]] | None = None,
        allowed_rel_vocab: set[str] | None = None,
        fallback: str = DEFAULT_RELATION_FALLBACK,
    ) -> None:
        self.rules = list(DEFAULT_RELATION_RULES if rules is None else rules)
        self.allowed_rel_vocab = (
            set(DEFAULT_ALLOWED_REL_VOCAB)
            if allowed_rel_vocab is None
            else set(allowed_rel_vocab)
        )
        self.fallback = fallback

    def map(self, relation_label_repr: str | None, relation_type: str | None, event_type: str | None, event_trigger: str | None) -> str:
        blob = " ".join(
            [
                relation_label_repr or "",
                relation_type or "",
                event_type or "",
                event_trigger or "",
            ]
        ).lower()
        for rule in self.rules:
            keywords = [str(k).lower() for k in rule.get("contains_any", [])]
            if keywords and any(word in blob for word in keywords):
                candidate = str(rule.get("maps_to", self.fallback)).upper().strip()
                if candidate in self.allowed_rel_vocab:
                    return candidate
        candidate = (relation_type or "").upper().strip()
        if candidate in self.allowed_rel_vocab:
            return candidate
        return self.fallback

