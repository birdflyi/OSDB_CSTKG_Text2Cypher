# D1.2c-v1 false-abstention root-cause summary v1

## Frozen empirical outcome

| Measure | Result |
|---|---:|
| Executable semantic success | 4 / 39 |
| Known-boundary correct abstention | 6 / 6 |
| Rendered executable outputs | 4 |
| Semantically correct among rendered executable outputs | 4 / 4 |
| Executable false abstentions | 35 / 39 |
| Undetected semantic errors | 0 |
| Natural repair triggers | 0 |

This is `CONSERVATIVE_UNDERCOVERAGE` as an empirical first-run behavioral pattern, not a safety theorem. The result must not be restated as 4/4 overall accuracy.

## Independently verified failure partition

| Trace state | Count | Failure layer |
|---|---:|---|
| `ABSTAIN_UNALIGNED_ENTITY` | 12 | parser/alignment, before contract selection |
| `BOUNDED_PARSED` + selection `unconsumed IR constraint` | 23 | parser/IR-to-contract reachability, before render |
| Render failure | 0 | none observed |
| Validator failure | 0 | none observed |
| Semantic-signature failure after render | 0 | none observed |

All 35 executable failures occur before render. The repair module is not triggered because there is no rendered, validator-diagnosable Cypher to repair.

## Primary root-cause counts

| Primary label | Count |
|---|---:|
| `RC1_SCOPE_PREFIX_ALIGNMENT` | 12 |
| `RC3_TARGET_ROLE_INFERENCE` | 8 |
| `RC10_OPTIONAL_BRANCH_OR_COMPOSITION_SEMANTICS` | 7 |
| `RC2_RELATION_SEMANTIC_LEXICAL_COVERAGE` | 6 |
| `RC4_TIME_OPERATOR_OR_BOUNDARY_PARSING` | 1 |
| `RC7_PROJECTION_EXTRACTION` | 1 |

Primary labels identify the earliest/dominant row-level gap. Secondary labels in the JSONL preserve additional missing constraints. Limit misses are explicitly marked `SEMANTICALLY_MASKED_BY_TEMPLATE_DEFAULT` when the historical template default equals the requested bound; they remain parser-coverage defects but are not promoted to the primary failure cause.

## Hypothesis adjudication

| Hypothesis | Finding | Evidence |
|---|---|---|
| H1 prefix scope | Confirmed | Exactly 12/35 rows use prefix-only `PR_156018` or `I_156018`; all 12 have empty aligned entities and `ABSTAIN_UNALIGNED_ENTITY`. |
| H2 source-biased target inference | Confirmed contributor | Multiple bounded rows recover the anchor/relation but set target to Issue, PullRequest, or IssueComment rather than Actor/UnknownObject/ExternalResource. |
| H3 phrase-specific relation lexicon | Confirmed contributor | Paraphrases lose COMMENTED_ON_ISSUE, OPENED_BY, LINKS_TO, or MENTIONS in several variants even when sibling variants recover them. |
| H4 lower-only vs year window | Confirmed | All q_l2_01 variants incorrectly add an upper 2024-01-01 bound; q_l4_01 June-1 lower-only variants become a Jan-1-to-Jan-1 year window. |
| H5 limit paraphrase coverage | Confirmed but often masked | Only 4/35 failed rows have an explicit parsed limit. Missing bounds usually equal a template default and therefore are not by themselves evidence of wrong output. |
| H6 flat composition/optional semantics | Confirmed | q_l3_02, q_l4_01, q_l4_03, and q_comp_01 require shared sources, multiple branches, optionality/ANY_OF, relation-owned time, and typed aggregates not captured by flat cues. |

## Family-level diagnosis

| Intent family | Successes / 3 | Dominant failure layer | Dominant root cause | Secondary causes | Parser capability | Template contract | Recommended bounded improvement |
|---|---:|---|---|---|---|---|---|
| q_l1_01 | 2 / 3 | parser/IR before selection | RC3 target role | RC7, RC6 masked | PARTIAL | YES | Role-aware Actor target and ID projection. |
| q_l1_02 | 0 / 3 | parser/alignment | RC1 prefix scope | RC7, RC6 masked | NO | YES | Typed PullRequest prefix constraint plus ID projection. |
| q_l1_03 | 1 / 3 | parser/IR before selection | RC3 target role | RC7, RC6 masked | PARTIAL | YES | Infer UnknownObject output independently of PR anchor. |
| q_l2_01 | 0 / 3 | parser/IR before selection | RC2 relation chain | RC3, RC4, RC5, RC8, RC10 | PARTIAL | YES | Required IssueComment chain, lower-only owner-bound time, distinct Actor output. |
| q_l2_02 | 0 / 3 | parser/IR before selection | RC7 projection | RC2, RC3, RC6, RC11 | PARTIAL | YES | Typed two-column domain/resource-ID projection. |
| q_l2_03 | 1 / 3 | parser/IR before selection | RC3 target role | RC2, RC7, RC6 masked | PARTIAL | YES | Mention phrase normalization and Actor target/projection. |
| q_l3_01 | 0 / 3 | parser/alignment | RC1 prefix scope | RC3, RC4, RC5, RC7, RC9 | PARTIAL | YES | Prefix-scoped reference event with interval, pair projection, sort. |
| q_l3_02 | 0 / 3 | parser/IR before selection | RC10 composition | RC2, RC3, RC7, RC8, RC11 | PARTIAL | YES | Shared-source required/optional branch graph and nullable distinct pairs. |
| q_l3_03 | 0 / 3 | parser/IR before selection | RC2 relation coverage | RC4, RC5, RC7, RC8, RC9 | PARTIAL | YES | Complete typed link/group/count/sort/time contract. |
| q_l4_01 | 0 / 3 | parser/IR before selection | RC10 composition | RC1, RC3, RC4, RC5, RC7, RC9 | PARTIAL | YES | Two required PR branches with typed prefix and LINKS_TO-owned lower bound. |
| q_l4_02 | 0 / 3 | parser/alignment | RC1 prefix scope | RC2, RC7, RC9, RC10 | PARTIAL | YES | Issue prefix plus IssueComment bridge, pair projection, recency sort. |
| q_l4_03 | 0 / 3 | parser/IR before selection | RC10 chain/ANY_OF | RC3, RC4, RC5, RC8, RC11 | PARTIAL | YES | Typed review chain, ANY_OF actor relation, owner-bound interval. |
| q_comp_01 | 0 / 3 | parser/alignment | RC1 prefix scope | RC4, RC7, RC8, RC9, RC10, RC11 | PARTIAL | YES | Prefix scope plus complete required/optional aggregate branch contract. |

No family failed at render, validator, or semantic-signature comparison. `Template contract = YES` means a frozen family contract exists; it does not mean the current flat IR can express or reach it safely.

## Experimental consequence

`HELDOUT_V1_ROLE_AFTER_THIS_TASK = DEVELOPMENT_DIAGNOSTIC`  
`V1_MUST_NOT_BE_USED_AS_POST_TUNING_HELDOUT = YES`  
`NEW_INDEPENDENT_HELDOUT_V2_REQUIRED_AFTER_TUNING = YES`
