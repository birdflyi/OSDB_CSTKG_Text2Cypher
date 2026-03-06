from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from models import NormalizedGraph


class BaseExporter(ABC):
    @abstractmethod
    def export(self, graph: NormalizedGraph, output_path: str | Path | None) -> Any:
        raise NotImplementedError

