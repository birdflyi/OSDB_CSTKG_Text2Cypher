# D1.2c v1 Recovered Failure Analysis

Metrics are recovered from the immutable first-run trace after exact NL reconciliation. The raw trace was not edited and generation was not rerun.

Controlled outcome correct: **10/45**.

| heldout_id | intent | variant | classification | template | failure_stage | repair |
|---|---|---|---|---|---|---|
| ho_q_comp_01_V1 | intent_q_comp_01 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_comp_01_V2 | intent_q_comp_01 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_comp_01_V3 | intent_q_comp_01 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l1_01_V3 | intent_q_l1_01 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l1_02_V1 | intent_q_l1_02 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l1_02_V2 | intent_q_l1_02 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l1_02_V3 | intent_q_l1_02 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l1_03_V1 | intent_q_l1_03 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l1_03_V3 | intent_q_l1_03 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l2_01_V1 | intent_q_l2_01 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l2_01_V2 | intent_q_l2_01 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l2_01_V3 | intent_q_l2_01 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l2_02_V1 | intent_q_l2_02 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l2_02_V2 | intent_q_l2_02 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l2_02_V3 | intent_q_l2_02 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l2_03_V2 | intent_q_l2_03 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l2_03_V3 | intent_q_l2_03 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_01_V1 | intent_q_l3_01 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_01_V2 | intent_q_l3_01 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_01_V3 | intent_q_l3_01 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_02_V1 | intent_q_l3_02 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_02_V2 | intent_q_l3_02 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_02_V3 | intent_q_l3_02 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_03_V1 | intent_q_l3_03 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_03_V2 | intent_q_l3_03 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l3_03_V3 | intent_q_l3_03 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_01_V1 | intent_q_l4_01 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_01_V2 | intent_q_l4_01 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_01_V3 | intent_q_l4_01 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_02_V1 | intent_q_l4_02 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_02_V2 | intent_q_l4_02 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_02_V3 | intent_q_l4_02 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_03_V1 | intent_q_l4_03 | V1 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_03_V2 | intent_q_l4_03 | V2 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |
| ho_q_l4_03_V3 | intent_q_l4_03 | V3 | FALSE_ABSTENTION | ABSTAIN | template_selection_or_abstention | NOT_TRIGGERED |

The strict pristine-blind claim is disallowed because preparation inspected a few held-out NL rows before COMMIT_A. This does not alter the immutable trace or its diagnostic status.

Neo4j was not run; these are static semantic-signature metrics only.
