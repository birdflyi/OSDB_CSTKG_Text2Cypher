# OSDB_CSTKG_Text2Cypher

Research prototype for OSDB-to-CSTKG graph construction, controlled Text-to-Cypher generation, and reproducible evaluation over software traceability queries.

Controlled success: 13/13 executable queries  
Pilot query set: 15 queries  
Template pack: 8 templates

## Overview

This repository implements an end-to-end research pipeline for constrained Text-to-Cypher generation over a software engineering knowledge graph. The pipeline starts from OSDB-derived records, constructs a CSTKG-aligned graph representation, generates Cypher queries under structural constraints, and evaluates them with reproducible experiment runners.

## Why Controlled Generation?

Text-to-Cypher generation over software knowledge graphs is difficult because graph schemas impose hard structural constraints, while natural-language inputs often underspecify relation types, directions, and property scopes. Pure free-form generation is flexible but fragile; template-only generation is more stable but often too rigid.

This repository focuses on a controlled strategy that combines three elements:

1. Template-first candidate generation
2. Schema-aware structural constraints
3. Structured diagnosis and minimal repair

The goal is to keep generated Cypher consistent with the CSTKG schema, relation semantics, and evidence scopes while preserving a deterministic, auditable pipeline.

## What this repository contains

- OSDB-to-CSTKG graph construction and normalization
- Controlled Text-to-Cypher prototype implementations
- Schema-aware static validation
- Structured diagnosis and lightweight repair
- Reproducible pilot evaluation artifacts and experiment entry points

## Key contributions

- Two-track query semantics with native relation boundaries limited to `EVENT_ACTION` and `REFERENCE`, while service-facing semantics are expressed through `service_rel_type`.
- Controlled generation with explicit schema checks, slot grounding, and bounded failure handling.
- Group-3 minimal template pack v3, where 8 executable templates cover 13 executable queries and 2 additional queries remain injection-pending.

## Repository structure

```text
OSDB_CSTKG_Text2Cypher/
|- graph-migration/
|- text2cypher-proto/
|- experiment-harness/
|- data_real/
|- data_scripts/
|- materials/
\- README.md
```

- `graph-migration/`: OSDB to CSTKG graph construction, preprocessing, mappings, pilot runners, and fixtures
- `text2cypher-proto/`: research prototype for free-form, template-first, and controlled Text-to-Cypher methods
- `experiment-harness/`: unified evaluation harness and reporting utilities
- `data_real/`: pilot queries, schema metadata, and public real-data inputs
- `data_scripts/`: auxiliary preparation scripts and local support files

## System pipeline

```text
OSDB data
   -> graph-migration
   -> CSTKG graph artifacts
   -> text2cypher-proto
   -> experiment-harness
```

## Quick reproduction

All commands below are executed from the repository root.

### 1. Build CSTKG mappings

```bash
python graph-migration/cli.py build-real-mappings \
  --input data_real/redis_redis_2023_aug_exidfix.csv \
  --outdir graph-migration/fixtures/real_pilot_redis
```

### 2. Run pilot queries

```bash
python graph-migration/cli.py run-real-pilot-queries \
  --queries data_real/pilot_queries/queries_pilot.jsonl \
  --taxonomy data_real/pilot_queries/query_taxonomy.yaml \
  --schema data_real/pilot_queries/schema_metadata.yaml \
  --mappings graph-migration/fixtures/real_pilot_redis \
  --outdir graph-migration/fixtures/real_pilot_redis
```

### 3. Run Group-3 template experiments

```bash
python graph-migration/cli.py run-group3-templates \
  --queries data_real/pilot_queries/queries_pilot.jsonl \
  --templates data_real/pilot_queries/minimal_template_pack_group3_v3.yaml \
  --schema data_real/pilot_queries/schema_metadata.yaml \
  --outdir graph-migration/fixtures/real_pilot_redis
```

## Results snapshot

| Mode | Success | Fail | Skip |
|---|---:|---:|---:|
| template_first | 12 | 1 | 2 |
| controlled | 13 | 0 | 2 |

`COUPLES_WITH` and `RESOLVES` remain injection-pending and are therefore counted in the skipped set rather than the executable success set.

## Notes on injection-pending relations

This repository keeps `COUPLES_WITH` and `RESOLVES` as explicit placeholders during the current stage. They are not materialized by default and are intended to be injected later by Chapter 5 and Chapter 6 outputs, respectively.

## License

This repository includes an existing `LICENSE` file at the repository root.
