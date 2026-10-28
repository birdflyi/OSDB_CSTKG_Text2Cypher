"""Small benchmark boundary; adapters own semantics, never HTTP or billing."""

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Split:
    name: str
    tasks: tuple

    @property
    def n(self):
        return len(self.tasks)


class BenchmarkAdapter(Protocol):
    kind: str

    def validate(self, stage, items, models, purpose): ...
    def splits(self, items): ...
    async def run(self, stage, items, config, make_spec, pool, ledger, **services): ...
