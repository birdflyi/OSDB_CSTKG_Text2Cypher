"""Shared adapter exceptions. Translate backend exceptions at the runtime boundary."""
from engine.errors import IntegrityError, PersistenceError


class IdentityScopeConflict(IntegrityError):
    """Task, model, graph or source receipt has changed."""


class EvidenceConflict(IntegrityError):
    """Immutable evidence conflicts with this result."""


class UnsupportedPlan(ValueError):
    """A valid plan uses an unsupported backend capability."""


class UnsupportedSelection(UnsupportedPlan):
    """A selection cannot be compiled for this backend."""


HARD_ERRORS = (OSError, IntegrityError)
