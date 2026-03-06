from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DiagnosisItem:
    error_type: str  # relation / direction / property / label / path / aggregation
    error_location: str
    violated_constraint: str
    candidate_fix_types: list[str]


@dataclass(frozen=True)
class StructuredDiagnosis:
    items: list[DiagnosisItem]

    def to_dict(self) -> dict[str, object]:
        return {
            "items": [
                {
                    "error_type": item.error_type,
                    "error_location": item.error_location,
                    "violated_constraint": item.violated_constraint,
                    "candidate_fix_types": list(item.candidate_fix_types),
                }
                for item in self.items
            ]
        }

