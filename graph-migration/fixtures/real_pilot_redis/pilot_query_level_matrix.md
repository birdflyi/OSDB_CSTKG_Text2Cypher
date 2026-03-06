# Pilot Query Level Matrix

| ID | Level | QueryType | Core Relation(s) | Key Evidence Fields | Why not pure RAG | Why not pure graph DB UX | Expected hardest baseline |
|---|---|---|---|---|---|---|---|
| q_l1_01 | L1 | l1_issue_opened_by_actor | - | source_event_time | Needs schema-safe relation grounding beyond lexical retrieval. | Users still need NL->Cypher translation with schema-safe defaults. | free_form |
| q_l1_02 | L1 | l1_pr_in_repo_scope | - | source_event_time | Needs schema-safe relation grounding beyond lexical retrieval. | Users still need NL->Cypher translation with schema-safe defaults. | free_form |
| q_l1_03 | L1 | l1_pr_references_object | - | source_event_time | Needs schema-safe relation grounding beyond lexical retrieval. | Users still need NL->Cypher translation with schema-safe defaults. | free_form |
| q_l2_01 | L2 | l2_issue_commented_after_time | - | source_event_time | Time/evidence fields require structured constraints and canonical field usage. | Users still need NL->Cypher translation with schema-safe defaults. | free_form |
| q_l2_02 | L2 | l2_pr_links_to_external | - | source_event_time | Time/evidence fields require structured constraints and canonical field usage. | Users still need NL->Cypher translation with schema-safe defaults. | free_form |
| q_l2_03 | L2 | l2_issuecomment_mentions_actor | - | source_event_time | Time/evidence fields require structured constraints and canonical field usage. | Users still need NL->Cypher translation with schema-safe defaults. | free_form |
| q_l3_01 | L3 | l3_repo_pr_reference_time | - | source_event_time | Multi-hop composition and aggregation are unstable under ungrounded generation. | Manual Cypher for multi-hop and time constraints has high UX burden. | template_first |
| q_l3_02 | L3 | l3_actor_mentions_and_links | - | source_event_time | Multi-hop composition and aggregation are unstable under ungrounded generation. | Manual Cypher for multi-hop and time constraints has high UX burden. | template_first |
| q_l3_03 | L3 | l3_pr_reference_domain_agg | - | source_event_time | Multi-hop composition and aggregation are unstable under ungrounded generation. | Manual Cypher for multi-hop and time constraints has high UX burden. | template_first |
| q_l4_01 | L4 | l4_repo_pr_actor_external_combo | - | source_event_time | Cross-constraint joins (entity+relation+time) need deterministic slot control. | Manual Cypher for multi-hop and time constraints has high UX burden. | template_first |
| q_l4_02 | L4 | l4_issue_comment_reference_commit | - | source_event_time | Cross-constraint joins (entity+relation+time) need deterministic slot control. | Manual Cypher for multi-hop and time constraints has high UX burden. | template_first |
| q_l4_03 | L4 | l4_pr_actor_review_reference | - | source_event_time | Cross-constraint joins (entity+relation+time) need deterministic slot control. | Manual Cypher for multi-hop and time constraints has high UX burden. | template_first |
| q_comp_01 | COMPREHENSIVE | comprehensive_repo_pr_external_actor_time | - | source_event_time | Comprehensive query needs stable multi-hop+aggregation+evidence alignment. | Comprehensive analytic queries are costly to hand-author and debug. | template_first |
| q_ch5_01 | L4 | ch5_repo_couples_with_placeholder | - | source_event_time | Cross-constraint joins (entity+relation+time) need deterministic slot control. | Manual Cypher for multi-hop and time constraints has high UX burden. | template_first |
| q_ch6_01 | L4 | ch6_pr_resolves_issue_placeholder | - | source_event_time | Cross-constraint joins (entity+relation+time) need deterministic slot control. | Manual Cypher for multi-hop and time constraints has high UX burden. | template_first |
