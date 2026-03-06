# text2cypher-proto

Research prototype for Text-to-Cypher generation over the CSTKG schema. This module contains the method-side implementation used in the paper, including controlled generation, schema-aware validation, structured diagnosis, and lightweight repair.

In the paper pipeline, this module corresponds to the Text-to-Cypher method layer: it sits between graph construction and unified evaluation, and provides the prototype implementations used by the pilot experiments.

## What is in this module

- `generators/`: free-form, template-first, and controlled generation variants
- `validators/`: Cypher and schema-oriented validation logic used by the prototype
- `repair/`: structured diagnosis objects and bounded repair actions
- `eval/`: prototype-side evaluation helpers
- `data/`: lightweight internal data structures and schemas
- `run_experiment.py`: prototype entry script for local method experiments

## How this module is used

This module is primarily invoked by the surrounding pipeline rather than as a standalone product package. In the current repository layout, the main pilot runner in `graph-migration/` locates this directory from the repository root and injects it into `sys.path` before loading the generation components.

## Scope

This is a research prototype, not a production API. The code is organized for paper reproduction, controlled experiments, and method comparison, rather than long-term service packaging or external API stability.
