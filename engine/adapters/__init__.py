"""One explicit registry: a new benchmark changes its adapter and this table."""

from .cypherbench import CypherBenchAdapter

ADAPTERS = {"cypherbench": CypherBenchAdapter}


def get(kind):
    if kind not in ADAPTERS:
        raise ValueError("Unknown benchmark adapter: " + str(kind))
    return ADAPTERS[kind]()
