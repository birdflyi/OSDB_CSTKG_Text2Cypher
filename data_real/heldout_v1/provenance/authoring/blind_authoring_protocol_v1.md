# D1.2b Blind NL Authoring Protocol v1

## Purpose and permitted inputs

Create the D1.2b held-out natural-language requests from the semantic cards
only. This authoring session receives:

- `semantic_cards_executable_v1.jsonl`;
- `semantic_cards_abstention_v1.jsonl`;
- this protocol.

Do not request, search for, or receive original pilot requests, historical
pilot wording, references/raw Cypher, parser or generator implementation,
template-selection logic, parser trigger vocabulary, evaluator regressions,
review/adversarial examples, prior parser outcomes, or held-out results.
The card IDs are bookkeeping identifiers, not prompts to retrieve any source
material. Do not infer or target implementation behavior.

## Required output count and structure

Author exactly three distinct English, single-turn request variants per card:

- 13 executable intent cards × 3 = **39** executable requests;
- 2 abstention intent cards × 3 = **6** abstention requests;
- total = **45** requests.

Use variant IDs `V1`, `V2`, and `V3` under each intent. The three variants
should be materially different while remaining natural requests:

- V1: vary syntax or information ordering;
- V2: use a different ordinary lexical/relational formulation;
- V3: use a different request form or compositional framing.

These are broad diversity goals, not parser-adversarial categories. Do not
make variants by mechanically swapping the same handful of words, and do not
force every variant to share one lexical trigger.

## Semantic fidelity

For every executable variant, preserve every `must_preserve` item and every
constraint in `semantic_contract`. In particular, preserve exact anchors and
prefixes, entity/relation types and direction, optional versus required
branches, relation-specific time bounds and endpoint inclusivity, distinct
versus non-distinct aggregation, grouping and projection, ordering, and limit.

Do not add or remove a filter, change an interval boundary, shift a time
constraint to another relation, change aggregation/counting semantics, alter
which target is optional, change a projection/sort key, or introduce a new
entity/relation constraint. Respect `must_not_invent`.

For abstention cards, express the requested graph meaning clearly enough to
test the stated boundary. Do not silently replace the unsupported relation
with a supported relation or otherwise represent the intent as executable.

If a card appears internally inconsistent or under-specified, flag it for the
data steward instead of guessing.

## Natural-language style

Requests must read like realistic user requests in natural English, not schema
dumps. Do not include Cypher, explicit template IDs, IR field names, query
IDs, split labels in the request text, gold markers, or implementation
instructions. Domain terms may be used naturally when needed to express the
card meaning; avoid mechanically restating internal field names.

## Independence and confidentiality

The author must not see the original 15 pilot NL strings, known review or
adversarial examples, evaluator regression texts, parser/generator code, or
any outputs from pilot/held-out evaluation. A separate steward will perform
contamination checks after candidate authoring. The steward must not provide
the exclusion corpus or match details to the author. The only return to the
author should be a candidate-level accept/revise decision based on fidelity
or contamination, without parser/evaluator/generator performance information.

Do not target known parser triggers, expected failures, or prior review
findings. Do not ask another model to inspect the implementation or generate
from pilot NL.

## Post-authoring semantic-fidelity review

A separate reviewer may receive only the semantic card and its authored NL
candidate. The reviewer must not receive parser/generator output,
semantic-signature output, execution results, original pilot NL, or excluded
examples. For each candidate, record:

- `PASS` or `FAIL` semantic fidelity;
- preserved constraints;
- missing constraints;
- invented constraints;
- ambiguity note.

A failed candidate may be replaced before the first held-out evaluation only
for semantic-fidelity or contamination reasons. Replacement must never be
selected using parser, generator, evaluator, or runtime performance.

## Contamination audit plan (performed after authoring)

An independent steward—not the author—will compare candidates against the
original 15 pilot NL strings, known review/adversarial examples, linguistically
relevant evaluator regression texts, and any rejected candidate retained from
this construction round. Keep that exclusion corpus outside the authoring
package and do not show it to the author or semantic-fidelity reviewer.

Audit rules:

- normalized exact duplicate: **hard fail**;
- near-duplicate lexical similarity: **manual-review flag**, not an automatic
  scientific failure;
- unavoidable shared domain/entity words: allowed unless surrounding wording
  is substantively copied;
- record the heuristic/version and manual adjudication, but do not treat the
  heuristic as a performance metric.

Suggested construction-quality heuristic: Unicode NFKC normalization,
case-folding, punctuation-to-space, whitespace collapse, followed by token
Jaccard and character 5-gram Jaccard comparison. Flag a pair for manual review
if token Jaccard is at least 0.65 or character 5-gram Jaccard is at least
0.55. These thresholds are review aids only; a human steward must decide
whether a candidate is copied or merely shares unavoidable terminology.
Retain rejected candidates and audit decisions in the internal construction
record, never in the author-facing package.

## Future dataset freeze (not created in this task)

The eventual frozen D1.2b package is expected to contain:

- `heldout_queries_v1.jsonl` — `heldout_id`, `intent_id`, `variant_id`,
  `split`, `nl_query`; no gold/reference Cypher;
- `heldout_gold_v1.jsonl` — held-out ID, expected intent, effective reference
  provenance and Cypher/signature or expected abstention, semantic-card hash;
- `heldout_manifest_v1.json` — dataset version, base evaluator freeze SHA/tag,
  authoring protocol and card hashes, query/gold/review/audit hashes, row
  counts, and provenance;
- `heldout_semantic_review_v1.jsonl`;
- `heldout_contamination_audit_v1.json`;
- `heldout_dataset_card_v1.md`.

None of these final held-out data files is created by this task.

## D1.2c firewall and freeze rule

Only `heldout_queries_v1.jsonl` may be passed to the frozen D1.1/D1.2
generation pipeline. `heldout_gold_v1.jsonl` is evaluation-only and must be
inaccessible to generation. The first D1.2c run must use generator/parser and
template state frozen before any held-out outcomes, evaluator
`ch7-d1-2a-evaluator-freeze` at
`ae75b8f0b4c076e17dfaa5cd18ac2480c5452e35`, and the future independently
reviewed/frozen D1.2b dataset.

Do not tune between inspecting held-out results and reporting the primary
held-out outcome. If implementation changes are made after viewing held-out
failures, D1.2b-v1 becomes development data and a newly authored independent
held-out v2 is required.

## Output format for the future author

Return exactly 45 JSONL records, one per candidate, with fields:
`intent_id`, `variant_id`, `split`, `language`, `nl_query`. Do not include
gold, reference Cypher, evaluator output, or explanations of parser behavior.
