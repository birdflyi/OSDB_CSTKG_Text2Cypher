# Author-method consumer bridge

The offline consumer accepts `cypher-ri` and `cyquark` for all five existing
backends. Both produce final Cypher. They use the same read-only execution and
reference-scoring code as Direct, while retaining their own method, task, graph
and source-reply identity. They never enter the Ours IR compiler or replay path.

## Two separate readiness questions

1. **The interface works:** deterministic responses and injected database doubles
   test preparation, parsing, query dispatch, final-answer persistence and score
   ordering. This is `test_only`, not an author-model result.
2. **The trained method is available:** a real service, its checkpoint/tokenizer,
   the frozen protocol and the benchmark's database still need verification.
   Passing interface tests does not change any `resource_pending` delivery.

RI retains `completion_prefix`: select schema, insert expanded schema, continue
the same prefix. The consumer receives only the terminal Cypher prediction.
CYQUARK still needs a separate checkpoint binding for each benchmark/graph.

## Configuring a real consumer run

The existing offline config gains one pin when author outputs are present:

```json
{
  "author_request_config": {
    "path": "ABSOLUTE_PATH_TO_THE_FROZEN_REQUEST_CONFIG",
    "sha256": "SHA256_OF_THAT_FILE"
  }
}
```

That request config uses the existing `bindings` contract. RI binds one logical
model; CYQUARK uses `bindings.cyquark.graphs[benchmark + "/" + graph]`. A runnable
binding has `ready: true`, `model`, and a pinned `asset_manifest`. The asset names
the method and logical model and records `checkpoint_sha256` and
`tokenizer_sha256`; CYQUARK also records its benchmark and graph. These checks
are shared with request admission in `common/author_assets.py`, not reimplemented
per backend. Missing declarations remain unready.

The selected request config, prepared request templates and source code manifest
must be frozen together. They contain the actual prompt and protocol, rather
than allowing a late template substitution. Service bootstrap validation must
still verify checkpoint files, tokenizer/config, prompt rendering and RI prefix
continuation; this bridge does not claim to perform that service validation.

`source.model` is the original logical `Item.model`. It must match that frozen
binding. An endpoint's actual wire model can differ under the authorized family
`serves` policy; this bridge does not compare it or change pool policy.

## Tests and future base-model engineering runs

`test_only: true` is allowed only with an explicitly injected test backend.
The CLI cannot launch a real database under this flag. Tests use scripted text
and local temporary records, without model or database connections.

A future untuned-base-model run must use a separate frozen engineering run:
record `model_role: untrained_base` and `engineering_only: true`, the real model
and prompt provenance, and its own admission/usage records. Do not change an
author binding to `ready` merely because an ordinary base service responds.
This version deliberately does not add a production bypass for that case.
It leaves the shared consumer routing reusable once the separately reviewed
engineering-run identity/config is implemented.

All existing query guards, graph ownership, scoring and denominator rules still
apply. Ordinary invalid queries are recorded and the next case proceeds.
Identity, pin and persistence errors remain hard errors. No old freeze,
prediction, method profile or result is changed by this bridge.
