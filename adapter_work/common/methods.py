"""Execution shape is independent from the recorded method identity."""

AUTHOR_METHODS = ("cypher-ri", "cyquark")
CONTROL_METHODS = ("correct-detail", "simple-correct", "native-set-count")
CYPHER_METHODS = ("direct", "direct-feedback") + AUTHOR_METHODS
METHODS = ("direct", "ours", "direct-feedback") + CONTROL_METHODS + AUTHOR_METHODS
