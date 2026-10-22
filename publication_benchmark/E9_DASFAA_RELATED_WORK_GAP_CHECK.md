# Contribution gap check for the DASFAA working draft

22 October 2026. This is a focused primary-source check, not a systematic literature survey or a reproduced baseline. It changes contribution judgment, not frozen predictions, interfaces or scoring. Relevant sections were read in the original papers; descriptions below concern those methods, not measured outcomes on our data.

| Primary source | Established design relevant to our claim | Consequence for this paper |
|---|---|---|
| [StructGPT, EMNLP 2023](https://aclanthology.org/2023.emnlp-main.574/), introduction and approach | Specialized interfaces retrieve structured evidence; a model iterates reading and reasoning. The study covers knowledge graphs, tables and databases. | Separating a service layer from model reasoning is not by itself our new contribution. Our current one-call controller is narrower than its iterative design. |
| [QueryAgent, ACL 2024](https://aclanthology.org/2024.acl-long.274/), introduction and method | Tools build formal queries stepwise; environmental feedback supports selective correction of intermediate failures. | Typed/tool-mediated query construction and environment-guided repair are prior ideas. A single authored two-call composition does not establish a competing planner or repair method. |

The inference is ours: a paper centered only on “LLM plus query services plus repair” is insufficiently distinguished from these works. Their published benchmark results are not comparable numbers for our property-graph snapshots, model or goals. We have not reproduced either method and cannot claim to outperform them. This check also does not establish that our proposed boundary diagnosis is novel; a broader comparison would still be needed before making that claim.

## Preserve the current evidence and narrow the candidate contribution

| Candidate claim | Current support | Decision |
|---|---|---|
| Layered access is a new architecture | Prior interface/reasoning systems exist | Retain as design motivation, not novelty |
| Our repair method improves independent conversion | Old gold-guided checks; uniform single structural repair in current comparisons | No independent repair-effect claim |
| The extended interface has missing information for all new goals | One source goal is recoverable with two existing calls | Reject that broad claim; distinguish information from controller restrictions |
| A compiler ensures request semantics | Literal-prefix mapping can be wrong even with a valid query/call | Restrict the guarantee to admitted structure and implemented contracts |
| Our mechanism is more accurate or cheaper | Incomplete independent outcomes, clustered provider failures and unknown costs | Unresolved; no superiority claim |
| Boundary diagnosis yields reusable design guidance | Specific information, composition and snapshot witnesses exist | Candidate only; needs systematic support beyond a small catalog's limited coverage |

## Execution implication

Continue the already frozen comparisons and retain every eligible task. Do not immediately build another planner, add a RAG arm, generate service-supported questions or increase project counts to repair the novelty argument. First determine whether the completed evidence yields an actionable finding about retained query information, executable composition or semantic failure. If the eventual main claim becomes planner superiority, an iterative tool-query baseline and matched budgets would then be necessary in a separate declared experiment; the present direct-versus-single-call comparison cannot support that claim.

These papers strengthen the working draft's evidence gate. They do not supply missing experimental evidence or guarantee DASFAA suitability. The primary sources are the [StructGPT PDF](https://aclanthology.org/2023.emnlp-main.574.pdf) and [QueryAgent PDF](https://aclanthology.org/2024.acl-long.274.pdf). No experiment or inference configuration changed during this check.
