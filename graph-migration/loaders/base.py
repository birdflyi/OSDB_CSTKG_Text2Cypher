from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class BaseLoader(ABC):
    @abstractmethod
    def load(self, input_path: str | Path) -> list[dict[str, Any]]:
        raise NotImplementedError

