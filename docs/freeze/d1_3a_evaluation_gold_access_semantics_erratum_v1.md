# D1.3a Evaluation Gold-Access Semantics Erratum v1

Date: 2026-10-09
Scope: historical D1.3a development-regression evidence metadata

## Correction

An audit of the checked-in artifacts found that evaluator summaries `v13` through
`v19` contain `evaluation_annotations_loaded=false` and
`gold_or_reference_cypher_loaded=false`. Those values are inaccurate for the
evaluation stage: the post-generation evaluator loaded the frozen annotations
and reference behavior/Cypher through `_classify_row`. The values describe the
gold-blind generation boundary but were copied into evaluation summaries.

The evaluator summaries `v1` through `v12` do not contain these fields, so they
are not asserted to have the same metadata defect. Generation receipts across
the historical range use the false values for the generation stage; those
generation-stage declarations remain correct.

## Historical preservation

No historical rows, metrics, summaries, checkpoints, receipts, chronology, or
SHA-256 values were rewritten. This erratum corrects the interpretation of
existing method metadata; it does not retroactively change loaded reference
data or reported metrics.

The historical artifacts did not include the new exact per-ID expected-query
binding check. The v20 binding result therefore improves evidence validity from
v20 onward and must not be read as retrospective proof for v1–v19.

## New phase-qualified convention

From v20 onward:

- generation receipts declare generation as gold-blind;
- evaluator summaries declare evaluation as gold-consuming;
- the evaluator records the canonical expected-query authority and exact
  `(heldout_id, nl_query)` binding result separately.

This is a provenance/method correction, not a claim of runtime Neo4j
correctness or independent held-out generalization.
