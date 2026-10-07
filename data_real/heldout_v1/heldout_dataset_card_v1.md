# D1.2b Held-out v1 Dataset Card

## Purpose

This package is a frozen-candidate dataset for held-out controlled paraphrase
robustness within the frozen F1-F5 semantic intent families, plus abstention on
the two known pending placeholder families.

The package contains 45 requests: 39 executable paraphrases across 13 intent
families and 6 abstention requests across two placeholder families. Each intent
has exactly three final variant slots.

## Construction and review

- D1.2a evaluator state was frozen before held-out construction.
- Natural-language requests were authored in blind external sessions from
  semantic cards.
- Semantic fidelity was reviewed externally before any held-out system outcome.
- Text-only contamination audits used normalized exact matching, token Jaccard
  threshold 0.65, and character 5-gram Jaccard threshold 0.55.
- The q_ch6 V3 slot was replaced once before evaluation because the original
  wording could be read as adding per-PR cardinality semantics. The replacement
  V3R1 was independently authored, audited, and reviewed PASS.
- Generation input and evaluation gold are separate files. Generation may read
  `heldout_queries_v1.jsonl` only.

## Supported claim boundary

This dataset supports a bounded claim about controlled paraphrase robustness
within the frozen intent families and explicit abstention on the two known
placeholder boundaries (`COUPLES_WITH` and `RESOLVES`).

This dataset alone does not support claims about arbitrary-NL generalization,
unseen semantic-intent generalization, general Cypher equivalence, full-schema
support, Neo4j runtime correctness, result-set equivalence, or per-operator
statistical robustness.

## Execution boundary

No parser, generator, evaluator, repair, or Neo4j execution has been run on the
final held-out package. No held-out outcome was used to author or select a
candidate. D1.2c remains blocked until a later merge/freeze task verifies the
accepted PR and creates the final freeze tag.