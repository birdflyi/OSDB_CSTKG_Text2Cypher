# V47 scoring integration

This change adds a formal-cohort scoring path without rewriting old scorers or old results. It contains no model calls, graph startup or query execution outside the existing worker's explicit scoring request.

## Consumer configuration

Use one pinned scoring manifest per benchmark, shared by every method:

```json
{
  "positions": [
    {
      "source_position_id": "the frozen source identity",
      "graph": "the frozen graph",
      "task_id": "original reference task ID",
      "policy": {"...": "build_task_policy output"},
      "reference_binding": {"...": "existing pinned reference binding"}
    }
  ]
}
```

The runtime owns loading the manifest, checking its SHA, resolving reference bindings and validating exact selected identities. `position_id` in native RPC is the same source_position_id; `task_id` remains the original dataset reference ID. They must not be conflated.

## Pure preparation

```python
from adapter_work.task_scoring_policy import build_task_policy, policy_digest

policy = build_task_policy(
    benchmark, source_position_id, reference_query,
    reference_status="UNVERIFIED",  # use VALID only with actual evidence
    comparator_pin={"path": comparator_source, "sha256": comparator_sha},
    cohort_sha256=cohort_sha,
)
```

`comparator_pin` is needed only for CypherBench and Text2GQL. It points to the actual `execution_accuracy.py` or `ea_cypher.py`, not the directory. The existing author comparator is loaded through the project's pure AST extraction boundary.

The builder retains the CypherBench author substring policy, and Text2GQL's author-derived unordered row-set rule. MTQ/ZOG's new project metric uses conservative final-output ordering classification. Quoted strings/comments and subquery-only ORDER BY are excluded. UNION, unrecognized forms and malformed lexical structure remain UNADJUDICATED. The detector is not a full Cypher parser. LIMIT/tie and nondeterministic-function concerns remain visible for reference review; this does not silently delete tasks.

### Qualifying references without freezing every task as unknown

Root can call `prepare_score_policies(positions, reference_loader, comparator_pins=..., cohort_sha256=...)`. Positions contain benchmark, source_position_id, graph and original task_id. The loader returns `{record, reference_binding, query?}` from already verified pinned local files. It is called once for **every** position and must not inspect predictions. The output is the shared positions manifest above plus reference-status counts and pending_reference_positions.

- CB published complete answer_json can qualify VALID; retain its anomaly labels in `reference_anomalies`, not auto-drop it.
- Text2GQL existing sealed actual reference records qualify VALID or retained failure. `reference_known=false` stays invalid, never an empty successful answer.
- MTQ source runtime/nonempty flags do not qualify a denotation. Query-only records remain UNVERIFIED. ZOG return_count likewise is not a denotation.
- MTQ/ZOG's actual same-capture successful reference execution produces a new reference receipt and VALID in the score record. The **policy and comparator remain frozen**; qualification is new evidence, not a semantic-policy edit. The native worker already saves that receipt.
- Before final reporting, reference qualification must cover the selected cohort independently of model success. Reuse valid reference-status receipts across methods on the same snapshot/query, but run a separate bounded reference-qualification task for positions where no successful reference receipt exists. Do not let only successful model predictions determine the valid-reference subset. This prerequisite requires DB execution in the admitted phase; this preparation helper does not execute it.
- A failed method with an UNVERIFIED reference remains R until such qualification exists. Re-score that saved failure after an audited reference-status receipt arrives; do not resend the model. Preserve the earlier score version and append the new evaluation receipt.

If frozen policy reference_status is AMBIGUOUS/MALFORMED or another declared invalid state, a later executable answer does not silently clear it; that needs an explicit new reference-policy version. UNVERIFIED is the only unresolved preparation state intended to be resolved by new evidence without changing semantic rules.

## Local backend scoring

```python
from adapter_work.score_position import (
    load_comparator, verify_answer_receipt, score_position,
)

# Before loading any reference data:
verify_answer_receipt(answer, receipt)
reference = load_pinned_reference_after_seal(...)
# Cache this callable once per pinned backend; no DB/model execution occurs.
comparator = load_comparator(policy)  # None for project metrics
score = score_position(
    policy, answer, reference, method=row["method"],
    comparator=comparator,
    answer_receipt=receipt,
    reference_receipt=reference_pin,
)
```

Actual existing shapes are accepted:

- CypherBench: `{answer_json: "[[...]]", gold_cypher: "..."}`.
- Text2GQL SealedReferenceStore: `{reference_known, query_sha256, answer: native_json_answer}`.
- OSDB: `{numeric_reference_rows: ...}`; null remains missing reference.
- MTQ/ZOG: the complete typed Neo4j answer from the same capture.
- Canonical reference: `{status: "VALID", answer: ...}` or an explicit reference failure status.

Call scoring for **every** selected position, not only `complete=True` answers. For terminal method failures, the consumer need not execute the reference again: pass `{status: <frozen reference status>, answer: None}`. VALID permits a false result for a terminal method failure; UNVERIFIED remains R and cannot be called wrong. A successful prediction with no reference answer still remains R. Existing successful Direct initial scores reused by Direct-feedback must have their `method` field changed to the actual method; never mutate the shared original record.

`ScorePolicyError` means policy, receipt, or reference-query identity mismatch. The central runtime must map it to its IntegrityError stop class. Ordinary reference failures are data records, not such exceptions.

## Native same-capture scoring

`NativeWorker.score()` recognizes two additional RPC fields:

```python
request.update(
    task_scoring_policy=policy,
    task_scoring_policy_sha256=policy_digest(policy),
)
```

The new branch verifies policy hash, benchmark, source position and saved method answer. It verifies the reference query hash **before** calling capture.read. The existing same-capture read executes the reference, stores its answer or failure, then calls score_position and stores score.json. Reference read timeout/error produces R; method answer remains intact. Capture cleanup remains in the existing finally block. Requests without the new fields keep the old auxiliary path.

Terminal execution failures are scored locally from the frozen reference-status manifest. They do not open a new native transaction to manufacture comparable entity IDs. The central consumer must keep references without validated status unresolved until that prerequisite is supplied.

## Record and summary

The returned record remains backend compatible (`status`, `value`) and adds `source_position_id`, `method`, `metric_id`, `metric_authority`, policy/cohort SHA, reference/prediction status, diagnostics and one partition:

| Partition | Meaning |
|---|---|
| C | valid reference, complete matching final answer |
| W | valid reference, complete mismatch or terminal method failure |
| R | unknown/invalid/missing reference; reference takes precedence |
| U | valid reference but prediction/comparison unresolved |
| NA | explicitly reported control not applicable |

`summarize_scores(policies, records, method=...)` retains the declared denominator, rejects extra/duplicate identities and mismatched policy hashes, and reports missing records. R and U are disjoint, including a position with both a missing prediction and invalid reference. Controls report N_selected, N_applicable and NA; applicable bounds are not described as all-question NL accuracy.

The new MTQ/ZOG metric is `project_numeric_exact_output_v1`, not an author score. It treats finite integer/float mathematical equality and signed zero consistently without lossy large-integer conversion; booleans remain separate. Old strict typed output equality remains a diagnostic. Nonfinite/malformed reference is R; malformed prediction encoding is U. No implicit numerical tolerance is introduced.

OSDB accepts the existing numeric reference shape for integration testing. It does not make the exposed development population a formal independent cohort. Formal OSDB tasks and reference N are still required.

## Validation boundary

Tests use fabricated records and a fake capture while calling the actual worker.score method. They do not start a DB, model or SSH. Author-result comparator tests inject pure callables; production source pins still require a separate frozen manifest. `formal_admitted` remains false in the score helper: actual cohort/runtime/resource admission belongs to the central run manifest, not to a helper's name.
