# D1.2b Held-out v1 Dataset Candidate

This PR prepares the D1.2b held-out dataset candidate from the frozen
`ch7-d1-2a-evaluator-freeze` base.

- 45 final held-out requests
- 39 executable paraphrases across 13 intent families
- 6 abstention requests across 2 known placeholder families
- D1.2a evaluator frozen before held-out construction
- blind external authoring and pre-evaluation semantic-fidelity review
- contamination hard fails: 0
- unresolved contamination flags: 0
- final semantic-fidelity acceptance: 45/45
- q_ch6 V3 replaced pre-evaluation for semantic fidelity; V3R1 occupies the
  existing V3 slot and is not a fourth sample
- q_comp_01 uses the corrected evaluation reference provenance
- generation/gold firewall established: generation reads queries only; gold is
  evaluation-only
- no held-out parser, generator, evaluator, repair, or Neo4j execution has
  occurred

This PR does not merge the branch and does not create the final freeze tag.
Those actions are reserved for the later merge/freeze task.
