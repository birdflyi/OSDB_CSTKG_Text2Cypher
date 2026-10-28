"""Errors that affect trusted execution rather than one benchmark case."""


class IntegrityError(ValueError):
    """Frozen input, routing or runtime identity differs; do not dispatch."""


class PersistenceError(OSError):
    """The durable evidence store cannot accept an intended write."""
