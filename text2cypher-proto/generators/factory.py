from __future__ import annotations

from generators.base import BaseGenerator
from generators.controlled import ControlledGenerator
from generators.free_form import FreeFormGenerator
from generators.template_first import TemplateFirstGenerator


def build_generator(name: str) -> BaseGenerator:
    normalized = name.strip().lower()
    if normalized == "free_form":
        return FreeFormGenerator()
    if normalized == "template_first":
        return TemplateFirstGenerator()
    if normalized == "controlled":
        return ControlledGenerator()
    raise ValueError(
        "Unknown generator: "
        f"{name}. Supported: free_form, template_first, controlled."
    )

