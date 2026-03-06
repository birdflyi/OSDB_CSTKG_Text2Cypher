# Pilot Level Summary

## By Level and Generator

| Level | Generator | Success | Fail | Top Failure Categories | Top Validator Error Codes |
|---|---|---:|---:|---|---|
| L1 | free_form | 0 | 3 | schema_mismatch:3 | UNKNOWN_LABEL:3, UNKNOWN_REL:3, DIRECTION_MISMATCH:3 |
| L1 | template_first | 3 | 0 | - | - |
| L1 | controlled | 3 | 0 | - | - |
| L2 | free_form | 0 | 3 | schema_mismatch:3 | UNKNOWN_LABEL:3, UNKNOWN_REL:3, DIRECTION_MISMATCH:3 |
| L2 | template_first | 3 | 0 | - | - |
| L2 | controlled | 3 | 0 | - | - |
| L3 | free_form | 0 | 3 | schema_mismatch:3 | UNKNOWN_LABEL:3, UNKNOWN_REL:3, DIRECTION_MISMATCH:3 |
| L3 | template_first | 3 | 0 | - | - |
| L3 | controlled | 3 | 0 | - | - |
| L4 | free_form | 0 | 5 | schema_mismatch:5 | UNKNOWN_LABEL:5, UNKNOWN_REL:5, DIRECTION_MISMATCH:5 |
| L4 | template_first | 5 | 0 | - | - |
| L4 | controlled | 5 | 0 | - | - |
| Comprehensive | free_form | 0 | 1 | schema_mismatch:1 | UNKNOWN_LABEL:1, UNKNOWN_REL:1, DIRECTION_MISMATCH:1 |
| Comprehensive | template_first | 1 | 0 | - | - |
| Comprehensive | controlled | 1 | 0 | - | - |

## Injection-Pending Queries

| ID | Level | QueryType | Placeholder Relation | Status |
|---|---|---|---|---|
| q_ch5_01 | L4 | ch5_repo_couples_with_placeholder | COUPLES_WITH | injection-pending |
| q_ch6_01 | L4 | ch6_pr_resolves_issue_placeholder | RESOLVES | injection-pending |

## Interpretation

free_form fails consistently because it emits labels/relations outside the configured schema boundary, producing schema_mismatch with UNKNOWN_LABEL/UNKNOWN_REL.
template_first and controlled succeed in this pilot because their outputs stay within the allowed label/relation sets; controlled additionally provides deterministic constraint-trace fields.
The two Chapter 5/6 placeholder queries are explicitly marked injection-pending and kept for reporting completeness; they require external injection before they can be treated as materialized-ground-truth queries.
