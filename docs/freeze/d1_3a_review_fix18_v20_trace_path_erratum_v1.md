# D1.3a Fix18 v20 Trace-Path Provenance Erratum v1

Date: 2026-10-09
Reviewed head: `2ee5d095ae110cefb705cfd811d9e2fb1fd7beee`
Fresh review: `5470938420`
Finding: `4230815227`

The accepted v20 evaluator summary reported the canonical query dataset in
`generation_trace_path`, while `generation_trace_sha256` correctly referred to
the generation trace bytes. The original v20 generation receipt correctly
recorded the true trace path and SHA-256:

`experiment-harness/results/d1_3a_v1_dev_regression_fix18_v20/d1_3a_v1_dev_generation_traces_v20.jsonl`

with SHA-256
`4b026b0feffc546870e7b35b59ee2070c0ca8584b52a88733cc239e137a9509b`.

The cause was a metadata-only variable shadowing defect in the receipt verifier:
the query-path canonical variable overwrote the trace-path variable before the
return metadata was assembled. Receipt authentication itself still checked the
trace path and trace bytes before returning, and the v20 classifications and
45/45 expected-query binding are preserved. The existing
`generation_trace_receipt_verification=PASS` did not assert correctness of the
evaluator summary's returned path metadata.

Fix19/v21 supersedes only this erroneous provenance-field claim. It does not
retroactively rewrite v20 traces, receipt, rows, summary, delta, checkpoint,
or any v1–v19 artifact. v21 emits independent trace and query path variables
and cross-artifact path/SHA checks pass.
