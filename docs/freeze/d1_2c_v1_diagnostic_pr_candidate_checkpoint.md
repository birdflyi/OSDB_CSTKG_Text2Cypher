# D1.2c-v1 Diagnostic PR Candidate Checkpoint

## Frozen lineage

```text
D1_2B_FREEZE_TAG = ch7-d1-2b-heldout-freeze
D1_2B_FREEZE_SHA = 432227d9b0bc418fd5a44b5fb4f616a66ac09b50

D1_2C_HARNESS_COMMIT_A = 69eb9b818ccab7b4965c1b6fdc5f45ff50885c65
COMMIT_A_RETAINED_AS_ANCESTOR = YES
```

COMMIT_A is the exact pre-run harness commit and remains unchanged in history.
The D1.2b freeze commit is an ancestor of COMMIT_A.

## Authoritative artifact hashes

```text
RAW_TRACE_SHA256 = 9df9e576460f77d4c8d6fa9bd9a6b65b9eece83cd6837ec1e828027ba2604334
RECONCILIATION_SHA256 = 30e7e8d0d965d8d645b50b78ce8429275479d3a2ac8380ac330f444c2d3a01a8

AUTHORITATIVE_EVAL_ROWS_SHA256 = cc15c59d9a35b1902acc4366b7aead1ca845b39168eb9effe06178a5faee9519
AUTHORITATIVE_EVAL_SUMMARY_SHA256 = e4b7f27e2a00fb26eeff3124c19cc54a14ed532a18808bb81c3f4663e8c52695

ROOT_CAUSE_AUDIT_SHA256 = 7aaff65865cf04117ae52d80d653a3eea27975df13cdec5fa8cb5676111f34e5
BOUNDED_PARSER_DEVELOPMENT_SPEC_SHA256 = c0f17e5c4a519a853cbc582e0e93187dcef84b7d3675a0acc30139ba77967c2d
```

The original recovered v1 reporting remains preserved as historical
provenance. The recovered v2 rows and summary are authoritative because they
correct only evaluation-reference provenance reporting; generation and all
correctness classifications remain unchanged.

```text
REFERENCE_CORRECTION_COUNT = 3
HISTORICAL_REFERENCE = 36
CORRECTED_EVALUATION_REFERENCE = 3
KNOWN_PLACEHOLDER_BOUNDARY = 6
```

## Recovered first-run diagnostic

```text
N = 45
N_EXECUTABLE = 39
N_ABSTENTION = 6

EXECUTABLE_SUCCESS = 4 / 39
KNOWN_BOUNDARY_ABSTENTION = 6 / 6
FALSE_ABSTENTION = 35 / 39
UNDETECTED_SEMANTIC_ERROR = 0
NATURAL_REPAIR_TRIGGER = 0

EXECUTABLE_FAMILIES_3_OF_3 = 0 / 13
ABSTENTION_FAMILIES_3_OF_3 = 2 / 2
EMPIRICAL_PATTERN = CONSERVATIVE_UNDERCOVERAGE
```

All 35 executable failures occurred before render. This is an empirical
first-run pattern, not a safety theorem, and 4/4 rendered semantic correctness
must not be restated as overall accuracy.

## Protocol and metadata incident status

```text
RAW_TRACE_IMMUTABLE = YES
TRACE_SALVAGEABLE = YES
EXACT_NL_BIJECTION = YES
REQUEST_ID_BEHAVIORAL_DEPENDENCY = NO
BLANK_ID_GENERATION_SEMANTICS_IMPACT = NO

NO_PREVIEW_RULE_VIOLATED = YES
PREVIEW_INFLUENCE_CLASSIFICATION = NO_EVIDENCE_OF_CHANGE
PRISTINE_BLIND_HELDOUT_PROTOCOL = NO
```

The immutable protocol-deviation artifact uses the semantically equivalent
field `PREVIEW_INFLUENCED_SYSTEM_BEHAVIOR = NO_EVIDENCE_OF_CHANGE`; the incident
adjudication checkpoint preserves the exact
`PREVIEW_INFLUENCE_CLASSIFICATION = NO_EVIDENCE_OF_CHANGE` field. Both original
artifacts and their specified hashes are retained without rewriting history.

## Scientific role and execution freeze

```text
HELDOUT_V1_ROLE = DEVELOPMENT_DIAGNOSTIC
V1_MUST_NOT_BE_USED_AS_POST_TUNING_HELDOUT = YES
NEW_INDEPENDENT_HELDOUT_V2_REQUIRED_AFTER_TUNING = YES

GENERATION_RERUN = NO
CORE_IMPLEMENTATION_CHANGED_IN_DIAGNOSTIC_TASK = NO
DATASET_CHANGED = NO
NEO4J_RUN = NO
```

This checkpoint freezes evidence only. It does not implement parser, IR,
template-selection, repair, or evaluator changes and does not authorize a v1
generation rerun.
