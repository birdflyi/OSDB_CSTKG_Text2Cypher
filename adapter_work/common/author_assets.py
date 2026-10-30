"""Shared author asset declarations for request and consumer admission.

Model names here are logical campaign identities. Endpoint `serves` aliases
remain the pool's responsibility; an actual wire deployment is not compared.
"""

from engine.errors import IntegrityError


def method_binding(bindings, method, benchmark, graph):
    base = bindings.get(method)
    if not base:
        raise ValueError("Missing explicit method binding: " + method)
    if method == "cyquark":
        key = benchmark + "/" + graph
        if key not in base.get("graphs", {}):
            raise ValueError(
                "CYQUARK needs a graph-specific checkpoint binding: " + key
            )
        return base["graphs"][key]
    return base


def validate_asset(binding, method, benchmark, graph, *, read_pin):
    asset = read_pin(binding["asset_manifest"])
    if asset.get("method") != method or asset.get("model") != binding["model"]:
        raise IntegrityError("Author asset identity differs from method binding")
    if not asset.get("checkpoint_sha256") or not asset.get("tokenizer_sha256"):
        raise IntegrityError(
            "Author service requires checkpoint and tokenizer evidence"
        )
    if method == "cyquark" and (asset.get("benchmark"), asset.get("graph")) != (
        benchmark,
        graph,
    ):
        raise IntegrityError("CYQUARK checkpoint belongs to another target graph")
    return asset
