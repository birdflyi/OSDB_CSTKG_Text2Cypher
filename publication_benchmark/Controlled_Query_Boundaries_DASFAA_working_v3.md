# Semantic Boundaries of Typed Query Interfaces for Natural-Language Access to Collaboration Graphs

**DASFAA 2027 Research Track working version, 22 October 2026.** Target: a database-query boundary study for DASFAA. This version updates the research questions, evidence structure and observed results; it does not change any frozen experiment. Versions 1, 2 and the historical SPE manuscript are preserved. Results are through the 337-attempt checkpoint. Independent predictions remain incomplete. This is a coauthor working draft, not a submission-ready or anonymized paper. Author details and release approval remain pending.

## Abstract

Typed query interfaces constrain generated graph queries, but executable calls can still misrepresent a request or exclude useful queries. We study these boundaries against guarded direct Cypher generation on four OSS collaboration graphs. The comparison separates output protocol, retained event information and a single-call controller. Development results do not establish a stable completion advantage: direct and service paths complete 143 and 144 of 144 constructed requests, while direct and extended services each complete 22 of 24 answerable cases in a separate comparison. Independently sourced tasks expose coverage restrictions, but provider failures leave their prediction results incomplete. A fixed-observation graph pair demonstrates an information-loss case for the older interface. In contrast, one source goal is reconstructed on all four projects by composing two unchanged extended-service calls. A separate post-hoc counterexample shows that snapshot answer agreement can conceal a literal-prefix error. Known-goal project-transfer runs also expose contract omissions and unequal infrastructure failures. These observations motivate separating information sufficiency, controller expressivity and realized semantic correctness. The present evidence supports bounded diagnostics, not overall accuracy or cost superiority. Completing independent evaluation and establishing a reusable contribution remain necessary before submission.

**Keywords:** natural-language graph queries; typed query interfaces; query expressivity; semantic evaluation; property graphs.

## 1. Introduction

OSS collaboration data connect issues, pull requests, actors, comments, commits and external references. A graph makes these connections queryable, but a graph schema does not make natural-language requests unambiguous. A request can omit a scope, confuse a recorded event with current state, or ask for information that the graph never captured. An executable Cypher query can still answer the wrong question.

Typed services offer one access path. A language model selects an operation and supplies arguments. A deterministic compiler validates the call and emits Cypher. This restricts executable structures and makes supported semantics explicit. It also limits which questions the interface can answer. Good accuracy on tasks chosen from the service catalog can conceal this coverage cost.

Our practical goal is a query service that can support a later conversational layer. This paper evaluates only the lower query/result layer. It does not evaluate a complete conversation, a production RAG pipeline or human interaction. The proposed conversational layer would receive service results and user context; its ability to compensate for missing evidence is not assumed here.

We study the following database questions. Each question is tied to an observable result rather than an assumed service advantage.

| Question | Controlled comparison or diagnostic | Required outcome |
|---|---|---|
| RQ1: Does output/refusal control improve contract use without changing answerability? | S versus P; same operations and compiler | Correct completion, wrong returned answers and safe catalog refusal reported separately |
| RQ2: Which answer losses concern retained information, and which concern the allowed controller? | Old-observation graph pair; generic event interface; legal two-call composition | A specific distinguishing witness or an executable reconstruction, with explicit scope |
| RQ3: What survives independent goals and project transfer against strong direct Cypher? | Frozen source/grounded/boundary tasks; two preselected holdout projects | Full task flow, graph answers, errors, failures and undispatched positions; no pooled development headline |
| RQ4: Does an apparent snapshot or cost benefit survive semantic and accounting checks? | Reference cross-checks, discriminating instances and all-attempt ledger | Snapshot agreement distinguished from semantic fidelity; known and missing costs disclosed |

The candidate contribution is an empirical diagnosis of failures at these boundaries, supported by executable comparisons and independently implemented references. The present artifacts include one information-loss witness, one successful authored composition and one post-hoc semantic counterexample. They instantiate established ideas rather than establishing a new determinacy theorem or a general planner. A reusable diagnostic method or transferable finding beyond the known limitation of a small catalog has not yet been established. That distinction is a submission gate, not a claim deferred to prose revision.

### 1.1 Separating three boundaries

Let D be a graph and q the agreed task contract. The target answer is q(D). Let O(D) denote the particular lower-interface observations available in a declared setting, and let L denote the permitted controller language and execution budget.

* **Information:** a pair D1, D2 with O(D1) = O(D2) but q(D1) != q(D2) shows that those observations cannot determine this answer. Our witness concerns its specified old observations; it does not quantify over all possible calls or the full historical engine.
* **Expression and execution:** a legal plan in L that reconstructs q(D) demonstrates feasibility for its tested graph and limits. A two-call reconstruction does not establish one-call feasibility, a universal rewriting or successful model planning. The 500-row gate is an execution constraint and must remain part of this claim.
* **Realized correctness:** the model must choose the right task interpretation, call/query and arguments, and execution must complete. Passing a compiler or matching one graph answer does not establish all of these conditions.

These are diagnostic questions, not an already validated exhaustive classifier. A case can involve more than one fault, and some composition cases remain unclassified. We do not infer missing information merely from a safe catalog refusal.

## 2. Background and related work

CypherBench studies Text-to-Cypher retrieval on large property graphs and provides a substantially larger benchmark than the present domain-specific workload [1]. Its existence means that adding graph size or synthetic question count alone is not a sufficient novelty argument. Our focus is the separation between catalog coverage and correctness under a controlled service interface. We have not reproduced CypherBench systems on this OSS schema, so the direct baseline here is not a claim of superiority over published systems.

Execution on one database is useful but can accept semantically different queries when their outputs happen to agree. Test-suite evaluation for Text-to-SQL addresses this problem through multiple database instances [2]. Our earlier multi-instance diagnostics follow the general motivation of checking discriminating instances, but they do not reproduce that method or prove semantic equivalence for the new workload.

Whether view outputs determine a query answer is an established database question [3]. We use a concrete pair of graphs to show that the restricted old service observations can remain equal while an event-window answer differs. This is a witness for the particular interface and observation setting, not a new general theorem. It does not show that arbitrary richer service catalogs, the complete historical application or an interface with additional data access have the same limitation.

The source census uses externally motivated repository-analysis concepts. Several can only be operationalized as partial observed-snapshot measures. A count of observed events is not complete GitHub activity; an observed opening-to-close interval is not necessarily the true historical resolution time. These distinctions are part of the task contract rather than post-hoc excuses for prediction errors.

## 3. Systems and scope

### 3.1 Direct and service paths

The direct path G emits a structured status and a parameterized read-only Cypher query. Its context includes the graph schema, business meanings and relevant examples. The harness checks the output, applies the read guard and executes an admitted query. At most one structural, protocol or execution repair is allowed. Repair feedback contains no reference answers or correctness signal. A valid executable but wrong query is not repaired using gold.

The service path emits a structured status and one typed operation call. A deterministic compiler checks the operation name, argument set, identifier form, date interval and allowed reductions. The initial six operations cover Issue opening actors, PR external links, PR link-domain counts, Issue prefix counts, Issue-comment-to-commit path counts and PR link counts. The service compiler does not read the raw request or prove that every natural-language condition was mapped correctly. Its guarantee is narrower: admitted calls follow the implemented contract.

S uses the original experimental service instructions. P uses the same six operations and compiler with explicit output/refusal rules. Comparing S and P isolates this protocol change within the development setting. These six operations form a new experimental prototype for the comparison; they are not an inventory of every capability in the historical paper engine.

I retains the six operations and adds one generic `observed_events` operation. It reads Issue/PullRequest-to-Actor event records. It applies literal identifier prefix, half-open UTC time interval, action and explicit decline filters before projecting records or reducing to distinct entity lists, actor counts, entity counts or a two-filter ratio. Raw projections can retain event time and edge identity. A PR decline requires an explicit recorded false-merge trigger; absence of a merge record alone is insufficient. The extension is not an arbitrary graph-query language.

### 3.2 Evidence and answer status

A correct answer must match the frozen output columns and complete expected rows under the declared comparison rules. An executable wrong answer is counted separately. A graph-answerable question that exceeds a service catalog can be safely refused, but that refusal does not count as useful task completion. Questions that require absent titles, authorship, current state or causal evidence belong to a separate boundary stratum. Missing required input and infrastructure failures also remain separate.

Snapshot event time means the canonical normalized `source_event_time` on the edge when specified. It is not assumed to be a commit authored date. Literal entity prefixes are not silently interpreted as graph repository-membership relations. An empty result is a valid answer only when the declared query semantics imply it.

### 3.3 Context and cost controls

The initial equal-information diagnostic contexts include material relevant to both representations. They are useful for development but can burden direct generation with service descriptions it cannot execute. A separately declared compact renderer retains all direct schema labels, properties, typed signatures, trigger meanings and direct examples for G. It retains complete executable contracts and service examples for P/I. All arms keep common snapshot and event meanings. No compiler, scoring rule, model setting or repair budget changes with this renderer.

We record actual API payloads, terminal responses, repairs and failures in a private archive. An exclusive shared ledger caps the continuation at 1,200 new attempts, including generation and probes. A batch stops after three consecutive infrastructure failures. Later transport recovery must be separately recorded. Reusing a byte-identical cached response after a bookkeeping interruption does not create a second charged call. No valid wrong answer or refusal is rerolled to seek a better outcome.

## 4. Data and study design

### 4.1 Project graphs

All projects use local 2023 OSS collaboration snapshots from the same data family and schema. CSV exports have unique node/edge identifiers and no dangling edge endpoints. Each project is imported into an isolated local database. Source hashes and database identities are recorded before query checks.

| Project | Nodes | Edges | Role |
|---|---:|---:|---|
| Redis | 25,747 | 56,836 | Development |
| DuckDB | 35,270 | 84,925 | Development |
| IoTDB | 29,425 | 140,313 | Preselected project holdout |
| Elasticsearch | 130,413 | 352,591 | Preselected project holdout |

Elasticsearch was chosen for scale and IoTDB for a different relation distribution before prediction. A larger graph is not automatically a harder query. The two holdouts test project transfer within this source/schema setting, not transfer to unseen schemas or domains.

### 4.2 Development evidence

The first comparison contains 144 developer-constructed requests, 24 base groups and six supported task families over Redis and DuckDB. Rephrasings are not treated as independent semantic goals. This is a development/regression set, not a representative user sample.

The output/refusal experiment uses 14 known cases per project: six supported tasks, six graph-answerable tasks outside the initial catalog and two boundary cases. The information-interface comparison uses the same development cases with G, P and I. Keeping these tasks known allows a mechanism diagnosis but prevents them from becoming independent evidence after a method change.

Earlier static validation and gold-guided typed-repair corpora are historical prototype checks. Their static pass counts are not pooled with database execution accuracy. Gold-guided repair construction is not blind repair effectiveness. The present first version uses the new execution comparisons for its quantitative conclusions.

### 4.3 Independent task intake

The full external census has 32 source entries: six already used development operationalizations, eight new partial candidates, seven requiring unresolved definitions/history and eleven requiring missing evidence. This flow is retained. The eight new candidates receive explicit snapshot definitions and independent references: branch create/delete counts, distinct pushed commits, observed PR opening-to-terminal duration, PRs with review records, opening-actor Issue/PR attribution, observed Issue opening-to-close duration, other-actor comment response and explicitly defined Issue event/comment activity.

Each source goal has one canonical request per project. The April 1–8 UTC window and binding policy were fixed before predictions. The source concepts are partial operationalizations, not exact CHAOSS or RepoQA replications. All eight exceed the frozen single-call output language and therefore form a controller-coverage stratum, not a same-capability accuracy contest. This does not establish that no composition of lower-interface calls can answer them; Section5.6 gives one counterexample to that broader reading.

Four catalog-blind role prompts requested three goals each with paired wording. The model received schema/source motivation but no service catalog, compiler, results or gold. Nine complete originals arrived; a truncated response left three missing. Six originals became clear graph-evidence boundary tasks. Three remain unresolved because inactivity, resolution or contribution attribution was not sufficiently defined. Drifted paraphrases were not used as equivalent robustness pairs. Roles are not human participants.

One bounded schema-grounded supplement requested eight goals in four calls. Two calls failed with HTTP500, leaving four received goals. Three are answerable graph goals: outgoing Issue references grouped by type; timed comment-to-Issue and comment-to-Actor pairs; and timed Push-reference-to-PR with Push-to-Branch joins. Their two phrasings are retained with disclosed clarifications. The fourth is a literal outgoing-Actor event query whose direction yields no rows; it is retained once as a separate diagnostic because its paraphrase changed direction.

Adaptations normalize output aliases and canonical edge time, specify earliest qualifying time for one row per requested pair and resolve Push-reference orientation from the schema. They are disclosed as adaptations, not hidden as original developer speech. All received answerable goals remain outside the fixed single-call output language. We do not generate further sets merely to obtain service-supported questions or add new service operations after inspecting these tasks.

| Frozen stratum | Goal definitions | Cases per project | Purpose |
|---|---:|---:|---|
| New source snapshot proxies | 8 | 8 | Useful catalog coverage |
| Adapted grounded answerable goals | 3 | 6 | Graph querying and paired wording |
| Literal direction control | 1 | 1 | Direction sensitivity; separate diagnostic |
| Evidence-boundary originals | 6 | 6 | Refusal behavior |

The 21 cases per project do not become 84 independent goals after four-project binding. The counts are convenience-workload counts and do not estimate a population task distribution.

### 4.4 Reference computation and freezing

Reference answers are computed from CSV without the tested service compiler. Separately written Cypher is executed on each imported graph and compared with these answers. The 32 source goal-project checks all pass. The grounded set has 28 successful wording checks, corresponding to 16 unique goal-project queries. Repeated wording does not add a new independent reference. Local anchor rules depend on graph structure, not model success; empty answers are retained.

The service implementation was frozen at `3d6bd22`, compact renderer at `4ea628e`, evidence-boundary grading at `4a30839`, and the reference builders at `6961a56`. Per-project task hashes are in `E7_FOUR_PROJECT_FREEZE.json`. Model inputs omit gold rows, reference queries, expected statuses and support labels. E5/E6 payload isolation checks pass. The latest audit covers all 337 continuation inputs, including the attempted independent and holdout cohorts. Protected reference and score fields are absent; this field check does not establish absence of all textual or pretraining contamination.

### 4.5 Measures and analysis

We report graph-answerable exact completion, wrong returned answers, correct boundary decisions, safe catalog refusal, infrastructure outcomes and evaluated/planned counts separately. Paired first/repaired views refer to the same initial call and are not independent samples. Source, synthetic, direction and boundary strata are not pooled into one headline accuracy.

Costs include all actual attempts and repair overhead. Unknown provider usage stays unknown. A known-token sum with missing usage is not a complete total, and success-only token averages cannot establish total efficiency. Dispatch duration, database execution and queue wall time are different measurements. This study does not include conversational synthesis, service construction/maintenance or human effort. Verified billing is unavailable, so currency costs are not estimated.

Any later uncertainty analysis must group by semantic goal, retaining paired wording and project bindings together as appropriate. With few convenience goals and four related projects, conditional intervals cannot establish population representativeness. We do not claim equivalence or noninferiority from equal small-sample counts.

## 5. Results available at this checkpoint

### 5.1 Strong baseline and output protocol

On the 144 development requests, repaired direct generation completes 143 and the repaired initial service completes 144. The one-case difference does not support a general superiority claim. Earlier multi-instance diagnostics preserve the outputs of originally correct queries on their tested fixtures; they are finite diagnostics over known families.

In the protocol experiment, both original and clarified instructions preserve all 12 supported cases across the two projects. For the 12 graph-answerable cases outside the six-service catalog, the original instructions produce two safe refusals and the clarified protocol produces twelve. One original-service output executes an incorrect Issue-comment-path count for a time-filtered closed-Issue request. These findings show why protocol compliance, semantic completion and refusal should be separated. They do not add a missing query capability or establish broad prompt superiority.

### 5.2 Information extension against direct generation

| Repaired arm | Redis answerable | DuckDB recovery answerable | Boundary decisions across projects |
|---|---:|---:|---:|
| Direct G | 12/12 | 10/12 | 4/4 |
| Six-service P | 6/12 | 6/12 | 4/4 |
| Extended I | 12/12 | 10/12 | 4/4 |

The extension completes the six known event tasks that the initial catalog cannot express when the model/provider returns a usable result. Direct generation already has access to the needed graph evidence. Across development projects, G and I each complete 22/24 answerable cases. This is no overall operational accuracy advantage.

DuckDB's direct path has one wrong executed answer: it uses equality with an entity-prefix string instead of prefix matching and returns an empty list. It also has one infrastructure outcome. I has two infrastructure outcomes and no wrong returned answer in this run. The typed prefix compiler prevents this particular direct-query error, but one developer-known case does not demonstrate a general mapping guarantee. The original interrupted DuckDB run contains three HTTP500 failures and no usable predictions; all three remain in costs. The recovery did not change tasks or methods.

### 5.3 Information-loss witness

Two synthetic graph instances differ in a relevant Issue opening time. They expose the same recorded observations through the tested old services but differ on the July opening count, one versus zero. Under the stipulated identical observation/context setting, a downstream layer cannot infer the different answer from those observations alone. This supports preserving query-relevant event information before aggregation for this case. It does not show that the complete old application has only those observations, or that every minimal extension is sufficient for all tasks.

### 5.4 Cost and infrastructure

The complete full-context Redis G/I comparison reports88,773 and88,222tokens respectively over14goals. The551-token difference, about0.62%, does not establish stable savings. The full direct context includes representation-inapplicable material. The separately frozen compact pilot yields7/12answerable completions for G and10/12for I, but G has six infrastructure outcomes across all14cases versus three for I. Known-token sums35,207/35,350 have six/three unknown-usage attempts. They are not complete total costs or a clean method-effect comparison.

At the current 337-attempt continuation checkpoint, 78 attempts have unknown usage. Known usage is 1,238,600 tokens: 1,043,295 prompt and 195,305 completion. Recorded duration sums to 8,355.554 seconds. It includes separately logged waiting in the paced cohort and is neither pure network latency nor experiment wall time. Probes, generation, structural repairs and failed attempts are included; first/repaired views and copied cached responses are not charged twice. Earlier E1/E3 calls retain separate logs outside this continuation budget. No verified price or invoice exists. No monetary or full-pipeline efficiency advantage is demonstrated.

### 5.5 Incomplete independent and holdout predictions

The Redis source v1 run stops after8attempts with5infrastructure failures. Its separately named v2 reuses all three prior valid refusals exactly and sends five new requests: one direct answer, one service refusal and three consecutive connection failures. Both versions together contain13unique attempts, eight unknown-usage failures and15,991known tokens. Adding their token summaries would count cached responses twice.

After a later successful recorded probe, the originally next untouched project DuckDB is admitted in a separately declared bounded batch. It also stops after8attempts with5infrastructure failures. No valid output is rerolled. Later cohorts are recorded below with their separate admission and transport conditions. Simple connectivity success is not evidence of reliable full-task transport.

| Project/source repaired view | Evaluated/planned | Exact snapshot answers | Safe catalog refusals | Infrastructure outcomes |
|---|---:|---:|---:|---:|
| Redis v2 G1 | 3/8 | 1 | 0 | 2 |
| Redis v2 P1 | 2/8 | 0 | 2 | 0 |
| Redis v2 I1 | 3/8 | 0 | 2 | 1 |
| DuckDB v1 G1 | 3/8 | 1 | 0 | 2 |
| DuckDB v1 P1 | 2/8 | 0 | 1 | 1 |
| DuckDB v1 I1 | 3/8 | 0 | 1 | 2 |

These are incomplete operational views, not completed0/8accuracy figures or a general comparative effect. Both direct answers are for the branch-count goal. Their original snapshot matches do not establish query equivalence.

A later paced source IoTDB batch stops after 13 actual attempts, including one repair: seven HTTP200 responses, five HTTP500 responses and one timeout. G1, P1 and I1 each evaluate 4/8 planned cases. G1 returns one matching snapshot answer and has three infrastructure outcomes; P1 has three safe catalog refusals and one infrastructure outcome; I1 has two safe refusals and two infrastructure outcomes. The measured pacing wait is 493.845 seconds and network time is 345.921 seconds. This cohort does not establish a rate-limit cause or method latency advantage.

A fresh seven-case grounded Redis cohort subsequently stops after its first G/P/I requests all return HTTP500. Each arm has only 1/7 positions attempted, with no answer. This is not a completed 0/7 accuracy measurement. Ten preceding client/encoding diagnostics succeeded, but they are excluded from effectiveness scores and do not establish stable availability. Source Elasticsearch and the other unattempted independent strata remain pending. The gateway's physical failure cause is unresolved.

### 5.5.1 Known-goal transfer to the two held-out projects

These tasks transfer previously known goals to preselected projects. They test same-schema project transfer, not independent-goal generalization. IoTDB has 12 answerable and two boundary cases; Elasticsearch has 11 answerable and two boundary cases after the unchanged row-limit exclusion of one 557-row task, without replacement.

| Project / repaired arm | Cases attempted / eligible | Correct / attempted answerable | Correct boundary / attempted boundary | Infrastructure outcomes, all cases |
|---|---:|---:|---:|---:|
| IoTDB G1 | 14/14 | 11/12 | 0/2 | 2 |
| IoTDB P1 | 14/14 | 6/12 | 1/2 | 2 |
| IoTDB I1 | 14/14 | 7/12 | 1/2 | 6 |
| Elasticsearch G1 | 10/13 | 6/10 | Not dispatched | 4 |
| Elasticsearch P1 | 11/13 | 2/11 | Not dispatched | 7 |
| Elasticsearch I1 | 11/13 | 9/11 | Not dispatched | 2 |

IoTDB completes all planned positions, not all model requests successfully. Two I first responses omit required `fields: []`; both permitted repairs time out. G uses INSUFFICIENT_EVIDENCE instead of the frozen NEEDS_INPUT status for a missing anchor; the reference is not changed. Five P event cases are safely refused but not completed. No executed returned answer is wrong on its snapshot.

Elasticsearch stops after three consecutive HTTP500 responses. Both boundary cases are undispatched for all arms, and one G answerable position is also undispatched. Two I first responses omit `explicit_decline`; their permitted repairs succeed. The I9/11 and G6/10 totals have unequal attempted positions and infrastructure outcomes, so they are not a complete or causal ranking. No executed returned answer is wrong on its snapshot.

The two batches add 44 and 34 actual attempts respectively, including two repairs each. Their known tokens are 103,824 and 65,068; ten and thirteen attempts lack usage. The outcome and cost receipts are E9_IOTDB_KNOWN_TRANSFER_CHECKPOINT.json and E9_ELASTICSEARCH_KNOWN_TRANSFER_CHECKPOINT.json. Together with the source results, these observations leave independent method advantage unresolved.

### 5.6 Compositional expressivity of the existing lower service

The original design separates a lower query service from an upper interaction layer. The frozen benchmark permits one service call; it does not evaluate a full upper planner. To distinguish that controller restriction from missing lower-level information, we perform one bounded offline composition check after the source goal is known.

For opening_actor_attribution, the unchanged observed_events operation retrieves Issue opening-actor counts and PR opening-actor counts in two calls. A deterministic full outer join on actor_id fills absent counts with0. Both calls pass the actual compiler, read guard, EXPLAIN, execution and500row gate. The combined result matches the frozen independent CSV gold on every project.

| Project | Issue-call rows | PR-call rows | Joined rows | Matches frozen gold |
|---|---:|---:|---:|---|
| Redis | 9 | 7 | 16 | Yes |
| DuckDB | 13 | 13 | 25 | Yes |
| IoTDB | 2 | 21 | 21 | Yes |
| Elasticsearch | 26 | 35 | 54 | Yes |

There is one known goal with four project bindings, not eight independent tasks. No new operation or model call is added. The calls and join are authored explicitly for this witness; no language-model planner is evaluated. This establishes lower-interface composability for this goal under the observed row limits. It does not establish planning accuracy, natural-language generalization, latency or token advantage. Two calls also introduce additional interaction steps relative to one direct query.

Thus, all eight source goals are outside the tested one-call language, but they are not proven information-impossible for the extended lower interface. The frozen single-call support labels and scores remain unchanged. This one check does not classify every other source goal as composable or impossible. The old six-service information-loss witness remains specific to its older observations and is not contradicted by this extension.

### 5.7 Finite reference diagnostics and one post-hoc semantic witness

Two synthetic reference worlds contain48nodes and87edges. Manual rows, unchanged CSV references and separately written Cypher agree on24goal-world checks. All26declared mutant-world executions, representing13mutation definitions, are distinguished from gold. They exercise record/distinct counts, half-open time bounds, earliest/latest events, self-response, comment-only activity, multi-branch joins and literal direction. They were constructed without reading the new model queries, but after partial prediction had already occurred. They are finite evaluator checks, not a prospectively evaluated model test suite or general equivalence proof.

Later inspection of the returned Redis branch query finds that it binds B_156018 rather than the explicitly requested B_156018:. A separately labeled post-hoc temporary witness has branches B_156018:main and B_1560189:foreign, each with an in-window creation event. The exact returned query gives[2,0], while the literal contract gives[1,0]. The transaction is rolled back; the original48/87fixture is restored and all real project graphs are unchanged. The original snapshot grade remains correct.

The returned DuckDB branch query also omits its prefix colon, but we do not turn the constructed witness into a prospective error-rate estimate. The catalogs cannot complete this branch task, so the defect is not a service win. A literal-prefix service argument could suffer the same mapping omission. This example shows why snapshot answer matching and NL-to-argument semantic fidelity require separate evidence.

## 6. Discussion

The supported development tasks expose a small practical distinction between constrained calls and direct Cypher, not a stable superiority result. Clarifying refusal rules improves behavior outside the catalog without expanding useful coverage. Adding observed events repairs a known capability gap, but the strong direct baseline already accesses the same evidence. These observations caution against attributing every improvement to semantic mapping.

The independent intake exposes a restriction of the evaluated one-call controller. At least one source goal can be reconstructed with two unchanged lower-service calls, so its missing one-call answer is not evidence of insufficient underlying information. The source coverage finding must therefore be separated from compositional expressivity and realized upper-planner behavior. The small experimental catalog/controller is not the full historical engine. An interface-internal accuracy figure alone still cannot justify general access claims; neither does this offline witness validate a complete conversational architecture.

A plausible later deployment is a hybrid router: use a typed operation when the request matches a supported contract, route other graph-answerable requests to guarded direct generation, and decline requests that lack evidence. This is an untested design implication, not a measured contribution in this version. It requires evaluation of routing errors and combined cost; safe refusal and direct fallback must not be assumed perfect.

The information witness also constrains the proposed conversational upper layer. User context can disambiguate an intended question, but it cannot reconstruct missing event distinctions from insufficient service outputs without additional evidence. Service results should expose the identifiers, time and provenance required by the intended downstream computations. The necessary interface depends on those computations; this paper has not derived a universal minimal interface.

For publication, the current defensible direction is an empirical study of these boundaries and their practical diagnostics. A claim that the service is more accurate, cheaper than RAG, usable by a broader population or ready for arbitrary OSS analysis is unsupported. A limited negative result still needs actionable evidence beyond the already known fact that a small catalog has limited coverage. Completing the frozen independent tests is necessary; it does not establish originality or guarantee conference acceptance.

## 7. Threats to validity

**Task construction.** Development goals influenced the interface and cannot estimate independent effect. Source candidates are convenience operationalizations; synthetic requests come from one provider model family, with AI-assisted semantic review. No humans were recruited or counted as annotators. Missing and unresolved outputs are retained. Their distribution may differ substantially from developer needs.

**Reference validity.** Separately implemented CSV/Cypher agreement reduces implementation coupling but can share an interpretation error. The source/grounded reference queries have a finite two-world corner-case check, but all actual model predictions have not been replayed on a prospectively fixed discriminating suite. The 24 CSV reference checks also reproduce byte-identically in a separate Linux CPU environment; that check does not reproduce Neo4j execution or model predictions. The literal-prefix witness is explicitly post-hoc. Empty or single-row results can mask some query mistakes. The literal direction control is separated to prevent easy empty answers from inflating practical performance.

**Internal validity.** Representation contexts differ where each path needs different executable information. The compact control reduces unnecessary prompt content but is not a pure architecture-only intervention. Structural repair budgets match; valid wrong results are not gold-repaired. Provider failures are frequent and temporally clustered. Conditional available-response results cannot remove this threat.

**External validity.** Four projects share a year, schema and source pipeline; only two are development-project holdouts. No new schema, production load, security evaluation, user study or complete conversational task is tested. The experimental catalogs are not the full historical engine. We do not extrapolate their coverage to all service interfaces.

**Cost and reproducibility.** Seventy-eight continuation attempts lack usage at this checkpoint, prices are unverified, and deployment/maintenance effort is unmeasured. Private credentials are excluded from artifacts. Exact raw logs may need a separate release review before public distribution. A returned model alias identifies the accessed deployment but does not independently verify its underlying weights.

## 8. Conclusion

The completed development experiments show no stable operational advantage over a strong direct-query baseline. They identify a limited protocol-control effect and an information-loss witness for the older six-service observations. Independent source predictions and the Elasticsearch known-goal transfer remain incomplete because of transport failures; IoTDB known-goal transfer is fully attempted but has substantial infrastructure failures. Their one-call coverage losses must not be generalized to every computation of the lower interface: one existing source goal is reconstructed on all four projects using two unchanged legal calls and deterministic composition. No upper language-model planner has been tested. Finite reference diagnostics and a separate post-hoc prefix counterexample further distinguish snapshot matching from semantic fidelity. The present evidence supports a limited boundary analysis and the plausibility of the layered design for one demonstrated goal, not a general accuracy, cost, usability or publication-readiness claim.

## Artifact and data statement

Internal provenance only: the current development repository is identified in earlier review versions. This working copy is not an anonymous submission release. Before submission, an anonymous artifact must be checked for author names, repository identity, paths and history; it must not falsely claim that prior public material does not exist. The new experimental stages are on separate `codex/ch7-e*` branches rather than a single consolidated release. Data source record: https://zenodo.org/records/18817348. Local extracted data and execution CSV members have recorded hashes; no claim is made here that the entire downloaded archive checksum was independently verified.

Public preparation identifiers: E5 `c9b434a`; E6 `a7fcb2f`; source/generation protocol `083a0fa`; reference preparation `b985e7a`; holdout preparation `2659a11`; latest outcome checkpoint E8 `1a7634a` and E9 `f23467b`; historical cost checkpoint `ec5d6ec`; later source/composition update `307f4a5`; composition script/plan `1ba4dca` (reference branch). Exact private requests, responses, failures, references and budget receipts remain preserved. A consolidated reproducibility release and clean-environment replay are pending. This statement does not imply that all private raw logs or new branches are already a permanent archive.

## References

1. Feng Y, Papicchio S, Rahman S. CypherBench: Towards Precise Retrieval over Full-scale Modern Knowledge Graphs in the LLM Era. ACL 2025:8934–8958. https://aclanthology.org/2025.acl-long.438/ doi:10.18653/v1/2025.acl-long.438.
2. Zhong R, Yu T, Klein D. Semantic Evaluation for Text-to-SQL with Distilled Test Suites. EMNLP 2020:396–411. https://aclanthology.org/2020.emnlp-main.29/ doi:10.18653/v1/2020.emnlp-main.29.
3. Nash A, Segoufin L, Vianu V. Views and Queries: Determinacy and Rewriting. ACM Transactions on Database Systems. 2010;35(3):Article21. https://dbucsd.github.io/paperpdfs/2010_3.pdf doi:10.1145/1806907.1806913.

## Internal DASFAA assembly and evidence gates

This section is coauthor control material and will not be part of the submitted manuscript. Target: DASFAA 2027 Research full paper. The official CFP allows 16 LNCS pages in total, including references and appendices, with an abstract of at most 200 words. The paper is reviewed double-blind. See https://dasfaa2027.github.io/CallforResearchTrack/list.htm . No LNCS compilation, page-count compliance or anonymity compliance is claimed by this Markdown version.

| Planned content | Page allocation, including figures/tables | Evidence focus |
|---|---:|---|
| Abstract and introduction | 1.5 | Database problem, questions and measured contribution |
| Problem, interfaces and scope | 2.5 | Information, controller, interpretation and execution boundaries |
| Design and reproducibility | 2.5 | Strong baseline, task flow, freeze, independent references |
| Results | 4.5 | Independent goals first once complete; transfer, controls, semantic witnesses and costs |
| Related work | 1.5 | Distinguish contribution from benchmark, tool-query and semantic-evaluation work |
| Discussion, threats and conclusion | 1.5 | Useful limits and transfer scope |
| References | 2.0 | Primary sources that directly change the contribution argument |
| **Total** | **16.0** | No additional appendix budget is assumed |

This allocation is a planning constraint, not a rendered page count. Old 13/13 static and 24/24 gold-guided repair checks are implementation history, not main independent-effectiveness tables. Full API diagnostics remain in operational provenance; only their material effect on validity belongs in the paper.

| Necessary gate | Current state | Next authorized action |
|---|---|---|
| Independent effectiveness | Incomplete; new answerable goals all exceed the frozen single-call language | Finish the frozen positions only after recorded transport admission; retain all prior outcomes |
| Matched-capability independent benefit | Not established; existing independent goals primarily probe coverage | Do not sell coverage losses as mapping accuracy. A later matched-capability study, if needed, requires a separately frozen phase and disclosed task selection; never relabel old tasks |
| Mechanism or reusable finding | Specific witnesses exist; reusable contribution remains unproven | Classify observed failures against information, controller and mapping evidence; retain unknown composition cases |
| Semantic correctness | Snapshot references and finite witnesses exist; no general equivalence proof | Separate prospective grading from post-hoc diagnostics and report the actual query defects |
| Cost | 78 unknown-usage attempts; no billing; no measured full RAG pipeline | Keep all-attempt accounting and disclose missing data; no service-versus-RAG savings claim |
| Reproduction and anonymity | Branch artifacts exist; consolidated anonymous release pending | Assemble a clean release only after the scientific result is settled |

Complete execution alone is insufficient if the only conclusion is that a small service catalog has limited coverage. We need a supported result that tells a database-system designer when a constraint helps, when it removes needed information, or when composition can recover a task, beyond the specific demonstration. If that contribution does not emerge, we must narrow or revise the research direction rather than claim an advantage. Further roles, projects or models are not automatic requirements and will not be added to chase a favorable result. Author review is required before any submission; no automatic submission is authorized.
