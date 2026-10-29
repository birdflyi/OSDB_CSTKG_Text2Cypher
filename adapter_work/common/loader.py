"""Load one cell's adapter under a unique module name.

Every cell directory has its own ``adapter.py``; importing it as plain
``adapter`` collides when the whole tree runs in one pytest process. Tests use
this loader (through their ``conftest.py``) so each cell gets its own module.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def load(adapter_path) -> object:
    path = Path(adapter_path).resolve()
    name = "cell_" + "_".join(path.parts[-3:-1])
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
