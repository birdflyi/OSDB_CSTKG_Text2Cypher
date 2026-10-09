# D1.3a Controlled-Language Boundary and Closure

## Architectural boundary

The deterministic NL parser is a bounded controlled-language reference
front-end. It is not intended to provide arbitrary-English semantic parsing.
Its role is to interpret a declared subset of controlled expressions into a
typed semantic representation while refusing to turn known unsupported
constraints into executable approximations.

```text
Open / variable natural language
        |
        v
Language Interpretation Layer
  current: bounded deterministic reference parser
  future: replaceable LLM / SFT / RL semantic front-end
        |
        v
ControlledQueryIR
        |
        v
Controlled Execution Layer
  deterministic alignment
  derivation
  contract selection
  Cypher rendering
  validation
  bounded repair
        |
        v
Neo4j runtime / result validation
```

The current paper contribution is primarily the `ControlledQueryIR` and
controlled execution boundary, not open-domain NLU coverage. The current
development harness does not establish Neo4j runtime or result correctness.

## Supported and unsupported behavior

A supported controlled-language construct must preserve its explicit
semantics and pass the selected template contract audit. An unsupported or
out-of-grammar expression is allowed to abstain. Unsupported language must
never silently drop an explicit constraint, invert it, weaken it into an
executable default, or fabricate an unsupported schema, relation, or operator.

In particular, the following are outside the frozen D1.3a controlled grammar
and are represented only as unsupported explicit-constraint metadata, causing
fail-closed selection:

- exclusion surfaces such as `other than`, `apart from`, `unless`, and `but
  those` when they govern a typed-ID prefix condition;
- postfix result-wide de-duplication such as `without duplicates`, `with no
  duplicates`, and `no duplicate results`;
- entity-noun cardinality caps such as `at most 10 people` when not consumed by
  the supported limit grammar.

These detectors are safety boundaries, not new executable grammar. The
existing bounded positive/negative prefix, item/tuple/aggregate DISTINCT, and
explicit `top N` / `limit N` constructs remain separately governed by their
declared contracts.

## D1.3a closure criterion

D1.3a is complete when:

1. Canonical semantic families have deterministic contract semantics.
2. The declared controlled grammar is documented.
3. Supported constraints map deterministically into IR.
4. No known supported constraint is silently dropped.
5. No known unsupported explicit constraint is converted into opposite or
   weaker executable semantics.
6. Out-of-grammar expressions may abstain.
7. Canonical development regression has zero undetected semantic errors and
   zero previous-success regressions.
8. Evidence and provenance are append-only and commit-bound.

## Claim boundary

The v1 through v17 artifacts are controlled-language conformance and
development-regression evidence. They are not evidence of arbitrary-English
understanding, arbitrary-NL generalization, Neo4j runtime correctness,
result-set correctness, or full-schema coverage. The v1-v16 artifacts remain
historically immutable; v17 is a new development diagnostic and does not
change the role of the evaluation set.

## Deferred replaceable LLM front-end

Future architecture may use:

```text
NL -> LLM semantic interpretation -> ControlledQueryIR
```

An LLM would be a replaceable semantic front-end, not an execution authority.
Potential future training directions are:

- SFT: NL/paraphrase to `ControlledQueryIR`;
- RL or preference optimization: reward constraint preservation, boundary
  compliance, runtime/result correctness; penalize hallucinated schema and
  unsafe execution.

No LLM front-end, SFT, or RL is implemented as part of D1.3a.

## Post-freeze TODO

- `NEXT_1`: upgrade the existing 13 executable plus 2 boundary families into
  Canonical Query Specification / Semantic Contract cards.
- `NEXT_2`: D1.3b bounded deterministic semantics: relation phrase
  normalization, temporal predicates, limit normalization, and
  projection/aggregation/sort/composition audit.
- `NEXT_3`: independently authored held-out v2 after parser freeze.
- `NEXT_4`: small normalized Neo4j runtime fixture and result-equivalence
  evaluation.
- `DEFERRED`: LLM prompt front-end, SFT, and RL.

## Evidence and post-freeze review policy

Every existing result artifact under a versioned `d1_3a_v1_dev_regression*`
directory is append-only. New evidence is written to a new versioned directory
and bound to its exact source commit and implementation bytes.

After Fix16, a supported-grammar semantic drop or inversion and any provenance
or evidence-integrity defect are blockers. An out-of-grammar expression that
safely abstains is a non-blocking coverage suggestion for D1.3a. An
out-of-grammar expression that still executes with weakened or inverted
semantics is a blocker for the firewall, not a mandate to expand paraphrase
support. General arbitrary-English coverage is outside D1.3a.
