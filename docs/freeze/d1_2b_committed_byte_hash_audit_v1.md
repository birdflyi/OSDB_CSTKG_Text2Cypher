# D1.2b Committed-Byte Hash Audit

This audit uses the exact Git index blob bytes for the follow-up candidate,
which are the bytes that will be committed and received by a normal checkout.
It does not use Windows working-tree text bytes. The canonical rule is
`SHA256_OF_COMMITTED_GIT_BLOB_BYTES`.

| Path | Semantic role | Recorded SHA-256 | Computed staged Git-blob SHA-256 | Result |
| --- | --- | --- | --- | --- |
| `data_real/heldout_v1/heldout_queries_v1.jsonl` | final held-out NL query inputs | `433b55308edf7806775e855d5c3bfd4c40c4d29602a62291d18e523b8f05fe91` | `433b55308edf7806775e855d5c3bfd4c40c4d29602a62291d18e523b8f05fe91` | PASS |
| `data_real/heldout_v1/heldout_gold_v1.jsonl` | final held-out gold package | `d04f2ce061327d25357d3b9f6479d54a76608071aba65e80a29aa760ac351be7` | `d04f2ce061327d25357d3b9f6479d54a76608071aba65e80a29aa760ac351be7` | PASS |
| `data_real/heldout_v1/heldout_semantic_review_v1.jsonl` | semantic-fidelity review package | `335e6342238acf6d334f406834c8273cc6580749609d1e125a17b86055b5d3b4` | `335e6342238acf6d334f406834c8273cc6580749609d1e125a17b86055b5d3b4` | PASS |
| `data_real/heldout_v1/heldout_contamination_audit_v1.json` | contamination audit | `142f04f76b4c9b77165491d8acd25f5165d6884a05b052dbbedd39c229c73d77` | `142f04f76b4c9b77165491d8acd25f5165d6884a05b052dbbedd39c229c73d77` | PASS |
| `data_real/heldout_v1/heldout_dataset_card_v1.md` | dataset card | `a1f9ee59ab1a49f65179c8816c5199ae8d1867184cc4642443cfe52307860ba6` | `a1f9ee59ab1a49f65179c8816c5199ae8d1867184cc4642443cfe52307860ba6` | PASS |
| `data_real/heldout_v1/provenance/heldout_candidates_raw_v1.jsonl` | immutable raw authoring baseline | `021198dba2b35c059a5b985e05c3d099d96a81cf0c9aee0dd2e7617e250a4749` | `021198dba2b35c059a5b985e05c3d099d96a81cf0c9aee0dd2e7617e250a4749` | PASS |
| `data_real/heldout_v1/provenance/authoring/blind_authoring_protocol_v1.md` | blind-authoring protocol | `1caf043e73e799eba8bfe807297a3dba0d8160c1ff42a8b9754e12f51d9ee3b3` | `1caf043e73e799eba8bfe807297a3dba0d8160c1ff42a8b9754e12f51d9ee3b3` | PASS |
| `data_real/heldout_v1/provenance/authoring/semantic_cards_executable_v1.jsonl` | executable semantic cards | `3bdb0b721c446f0c97442b302a4fcf5583be6cfc501808a2177aa9e32cbbe366` | `3bdb0b721c446f0c97442b302a4fcf5583be6cfc501808a2177aa9e32cbbe366` | PASS |
| `data_real/heldout_v1/provenance/authoring/semantic_cards_abstention_v2.jsonl` | abstention semantic cards | `69c7b263523410c2cd69f5769828b2013794a4a25b5f89b55bc0cdb512839ffd` | `69c7b263523410c2cd69f5769828b2013794a4a25b5f89b55bc0cdb512839ffd` | PASS |
| `data_real/heldout_v1/provenance/authoring/authoring_provenance_ledger_v2.json` | authoring provenance ledger | `b2d6c8e011f0da53a89c8a9ce8a2486e7ee97ec12fbf3d3562273421fe56c3db` | `b2d6c8e011f0da53a89c8a9ce8a2486e7ee97ec12fbf3d3562273421fe56c3db` | PASS |
| `data_real/heldout_v1/provenance/reference/queries_pilot.jsonl` | pilot reference queries | `569bd8f364e5943c85a84966da47de69c1da7356ae603ef46aa7bb9e7a6233cb` | `569bd8f364e5943c85a84966da47de69c1da7356ae603ef46aa7bb9e7a6233cb` | PASS |
| `data_real/heldout_v1/provenance/reference/independent_eval_reference_corrections_v1.yaml` | corrected evaluation references | `db5dab2e7017fda54019483b78b17b05374bd9fae731cfac608ffe204f95319f` | `db5dab2e7017fda54019483b78b17b05374bd9fae731cfac608ffe204f95319f` | PASS |
| `data_real/heldout_v1/provenance/review/semantic_fidelity_review_results_v1.jsonl` | external semantic-fidelity review | `bd4a206967dc48c60589d906eb9fd07f6973f212e46b3b4c2c7064102d9882f8` | `bd4a206967dc48c60589d906eb9fd07f6973f212e46b3b4c2c7064102d9882f8` | PASS |
| `data_real/heldout_v1/provenance/review/semantic_fidelity_adjudication_v1.json` | controller adjudication | `cad9b3d3724ed025b947d397587a8f7b7c92e3519624e3fa86e856201ecd9855` | `cad9b3d3724ed025b947d397587a8f7b7c92e3519624e3fa86e856201ecd9855` | PASS |
| `data_real/heldout_v1/provenance/review/semantic_fidelity_replacement_review_result_v1.jsonl` | replacement semantic review | `cd1a25e69c2ca36937164f63574b33c3d112263fdaa906785af19d327f4b4b37` | `cd1a25e69c2ca36937164f63574b33c3d112263fdaa906785af19d327f4b4b37` | PASS |
| `data_real/heldout_v1/provenance/replacement/claude_q_ch6_v3_replacement_raw_v1.jsonl` | accepted q_ch6 V3R1 raw replacement | `3bd705d221bae878462769f77699f0f8099b5e1135616724a1410ac27779d5c4` | `3bd705d221bae878462769f77699f0f8099b5e1135616724a1410ac27779d5c4` | PASS |
| `data_real/heldout_v1/provenance/replacement/semantic_card_q_ch6_01_replacement_v1.json` | q_ch6 V3R1 semantic card | `c078152770ba1e7f61e66de04846431496b7f4dfa22db503b8d99e32bf783084` | `c078152770ba1e7f61e66de04846431496b7f4dfa22db503b8d99e32bf783084` | PASS |
| `data_real/heldout_v1/provenance/replacement/replacement_contamination_audit_v1.json` | q_ch6 replacement contamination audit | `ac1ee4c795e22ce94a3fa00d8de2a983af31389bfafb255e65baf2eb6cd04350` | `ac1ee4c795e22ce94a3fa00d8de2a983af31389bfafb255e65baf2eb6cd04350` | PASS |
| `data_real/heldout_v1/heldout_gold_v1.jsonl` (`semantic_card_hash`, 39 executable rows) | executable-card row provenance | `3bdb0b721c446f0c97442b302a4fcf5583be6cfc501808a2177aa9e32cbbe366` | `3bdb0b721c446f0c97442b302a4fcf5583be6cfc501808a2177aa9e32cbbe366` | PASS |
| `data_real/heldout_v1/heldout_gold_v1.jsonl` (`semantic_card_hash`, 5 abstention rows) | abstention-card row provenance | `69c7b263523410c2cd69f5769828b2013794a4a25b5f89b55bc0cdb512839ffd` | `69c7b263523410c2cd69f5769828b2013794a4a25b5f89b55bc0cdb512839ffd` | PASS |
| `data_real/heldout_v1/heldout_gold_v1.jsonl` (`semantic_card_hash`, q_ch6 replacement row) | replacement-card row provenance | `c078152770ba1e7f61e66de04846431496b7f4dfa22db503b8d99e32bf783084` | `c078152770ba1e7f61e66de04846431496b7f4dfa22db503b8d99e32bf783084` | PASS |

The following hashes remain historical or external-process references and are
not repository-blob authentication claims: raw authoring-session artifacts,
review-item/prompt handoff files, and other files under
`temp_solution_discussion/` or the untracked authoring/review workspaces.
They were not mechanically replaced.

No semantic card content, NL query text, effective reference Cypher,
expected behavior, held-out ID, replacement provenance, source code, or
evaluation outcome was changed by this repair.
