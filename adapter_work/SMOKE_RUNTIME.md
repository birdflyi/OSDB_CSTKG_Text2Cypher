# Independent smoke runtime

The request producer and database consumer are separate programs. Neither needs
an agent to dispatch individual cases. All model traffic uses the existing
`Scheduler`, endpoint pool and budget ledger.

1. `request_pipeline.py run CONFIG` generates and seals the A replies. Its
   inventory pins positions, public inputs, method profiles and source code.
   Shared controls bind the Ours plan; they do not make extra model calls.
2. `smoke_chain.py CONFIG` runs B using the sealed A replies. Its `offline`
   configuration selects one or five fixed positions per cell. Backends own
   their graph lifetime. Each actual answer is saved before reading references.
3. A known query rejection creates an immutable feedback request. The chain
   sends one feedback stage through the same model scheduler, closes the graph
   between phases, then executes and scores that reply. Successful initial
   Direct answers are shared with Direct-feedback. Transport failures do not
   manufacture a model repair. Retries retain all charged attempts.

Launch through `engine.launch.launch_process` for a detached process and PID,
command, stdout and stderr receipts. Freeze both child configurations before
launch. `source` must match the deployed package. Do not edit an active package.

## Artifacts and interpretation

- Each cell/position has `input.json`, `final.json` and `result.json`.
- Known operational failures additionally have `feedback_request.json`.
- The chain retains initial and feedback results separately and writes a final
  view without changing the original evidence.
- `EXECUTED` means a complete DB answer, not a correct answer.
- `not_applicable` denotes a documented control domain restriction.
- `backend_pending`, missing references and unsupported plans remain visible.
  A completed run is not proof that every cell passed.
- MTQ/ZOGRASCOPE currently use an explicitly auxiliary typed comparator.
  Text2GQL uses the pinned author comparison routine on archived REST answers;
  this does not certify REST/Bolt equivalence. OSDB uses exposed developer
  references. These smoke scores are not formal Table 1 results.

Current limits: arbitrary crash reconstruction is not automatic. Native
set-count is compiled only for two compatible identity sets with count output;
engine dialect support remains subject to real execution. The runtime does not
silently drop these positions or substitute another method.
