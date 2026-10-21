# When Does a Typed Query Service Help? An Empirical Boundary Study of Natural-Language Access to OSS Collaboration Graphs

**Second coauthor review version, 22 October 2026.** Evidence update: stopped source batches, finite reference diagnostics and one offline two-call composition witness. Version1 is preserved. The tested model system is unchanged. This is an evidence-based working manuscript. Independent prediction and held-out-project prediction are incomplete because of provider failures. Their cells remain pending. This version is not ready for journal submission. Author names, affiliations and funding require author confirmation.

## Abstract

Natural-language access to collaboration graphs can use direct Cypher generation or typed query services. A service constrains executable structures, but a valid call need not preserve request semantics, and a one-call controller can exclude useful compositions of existing services. We compare a strong direct baseline, a protocol-only service control and an interface extended with one generic observed-event operation. On144 developer-constructed requests, repaired direct and service generation complete143 and144 respectively. In a separate development comparison, direct and extended service each complete22/24answerable cases and4/4boundary cases. These small developer-known results do not establish superiority. Eight new source task operationalizations exceed the frozen single-call language; both attempted source-project batches stop because of infrastructure failures. An offline witness nevertheless reconstructs one of these goals on all four projects by composing two unchanged legal service calls, without adding an operation or testing a language-model planner. Independent CSV/manual-query reference checks and finite synthetic diagnostics support evaluator consistency. A separate post-hoc scope witness shows that one returned query can match the original snapshot while violating a requested literal prefix. The evidence separates controller restrictions, available information, compositional expressivity and observed model success. Independent predictions, project-transfer effects and complete cost evidence remain insufficient for general accuracy, efficiency or usability claims.

**Keywords:** Text-to-Cypher; software repository mining; typed query services; task coverage; semantic evaluation; reproducibility.

## 1. Introduction

OSS collaboration data connect issues, pull requests, actors, comments, commits and external references. A graph makes these connections queryable, but a graph schema does not make natural-language requests unambiguous. A request can omit a scope, confuse a recorded event with current state, or ask for information that the graph never captured. An executable Cypher query can still answer the wrong question.

Typed services offer one access path. A language model selects an operation and supplies arguments. A deterministic compiler validates the call and emits Cypher. This restricts executable structures and makes supported semantics explicit. It also limits which questions the interface can answer. Good accuracy on tasks chosen from the service catalog can conceal this coverage cost.

Our practical goal is a query service that can support a later conversational layer. This paper evaluates only the lower query/result layer. It does not evaluate a complete conversation, a production RAG pipeline or human interaction. The proposed conversational layer would receive service results and user context; its ability to compensate for missing evidence is not assumed here.

We ask three questions. RQ1: How much of the observed development behavior changes when output and refusal rules are clarified without changing service capability? RQ2: What changes when one generic event relation preserves information before aggregation? RQ3: What does a task set constructed outside the service catalog reveal about useful coverage and transfer?

The current contributions are an executable comparison protocol that separates these factors; an independently checked task/reference preparation across four project snapshots; and a concrete information-loss witness for the restricted service observations. These are bounded empirical artifacts. The information argument instantiates established view-determinacy reasoning, and the observed development results do not show a stable advantage over strong direct generation. A competitive journal contribution still depends on the independent evaluation and the actionable value of the resulting boundary analysis.

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

The service implementation was frozen at `3d6bd22`, compact renderer at `4ea628e`, evidence-boundary grading at `4a30839`, and the reference builders at `6961a56`. Per-project task hashes are in `E7_FOUR_PROJECT_FREEZE.json`. Model inputs omit gold rows, reference queries, expected statuses and support labels. Existing completed E5/E6 payload isolation audits pass. Independent/holdout audits will accompany their eventual predictions.

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

At the current229-attempt continuation checkpoint,44attempts have unknown usage. Known usage is1,025,251tokens:867,464prompt and157,787completion. Recorded dispatch duration sums to5,687.689seconds; it is not experiment wall time. Probes, generation, structural repairs and failed attempts are included; first/repaired views and copied cached responses are not charged twice. Earlier E1/E3 calls retain separate logs outside this continuation budget. No verified price or invoice exists. No monetary or full-pipeline efficiency advantage is demonstrated.

### 5.5 Incomplete independent and holdout predictions

The Redis source v1 run stops after8attempts with5infrastructure failures. Its separately named v2 reuses all three prior valid refusals exactly and sends five new requests: one direct answer, one service refusal and three consecutive connection failures. Both versions together contain13unique attempts, eight unknown-usage failures and15,991known tokens. Adding their token summaries would count cached responses twice.

After a later successful recorded probe, the originally next untouched project DuckDB is admitted in a separately declared bounded batch. It also stops after8attempts with5infrastructure failures. No valid output is rerolled, and no subsequent project batch is started. Simple connectivity success is not evidence of reliable full-task transport.

| Project/source repaired view | Evaluated/planned | Exact snapshot answers | Safe catalog refusals | Infrastructure outcomes |
|---|---:|---:|---:|---:|
| Redis v2 G1 | 3/8 | 1 | 0 | 2 |
| Redis v2 P1 | 2/8 | 0 | 2 | 0 |
| Redis v2 I1 | 3/8 | 0 | 2 | 1 |
| DuckDB v1 G1 | 3/8 | 1 | 0 | 2 |
| DuckDB v1 P1 | 2/8 | 0 | 1 | 1 |
| DuckDB v1 I1 | 3/8 | 0 | 1 | 2 |

These are incomplete operational views, not completed0/8accuracy figures or a general comparative effect. Both direct answers are for the branch-count goal. Their original snapshot matches do not establish query equivalence. Other independent strata and all held-out-project model predictions remain pending. Four-project imports and reference checks are preparation, not transfer accuracy.

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

For publication, the current defensible direction is an empirical study of these boundaries and their practical diagnostics. A claim that the service is more accurate, cheaper than RAG, usable by a broader population or ready for arbitrary OSS analysis is unsupported. A limited negative result still needs actionable evidence beyond the already known fact that a small catalog has limited coverage. Completing the frozen independent tests is necessary; it does not guarantee journal acceptance or novelty.

## 7. Threats to validity

**Task construction.** Development goals influenced the interface and cannot estimate independent effect. Source candidates are convenience operationalizations; synthetic requests come from one provider model family, with AI-assisted semantic review. No humans were recruited or counted as annotators. Missing and unresolved outputs are retained. Their distribution may differ substantially from developer needs.

**Reference validity.** Separately implemented CSV/Cypher agreement reduces implementation coupling but can share an interpretation error. The source/grounded reference queries have a finite two-world corner-case check, but all actual model predictions have not been replayed on a prospectively fixed discriminating suite. The literal-prefix witness is explicitly post-hoc. Empty or single-row results can mask some query mistakes. The literal direction control is separated to prevent easy empty answers from inflating practical performance.

**Internal validity.** Representation contexts differ where each path needs different executable information. The compact control reduces unnecessary prompt content but is not a pure architecture-only intervention. Structural repair budgets match; valid wrong results are not gold-repaired. Provider failures are frequent and temporally clustered. Conditional available-response results cannot remove this threat.

**External validity.** Four projects share a year, schema and source pipeline; only two are development-project holdouts. No new schema, production load, security evaluation, user study or complete conversational task is tested. The experimental catalogs are not the full historical engine. We do not extrapolate their coverage to all service interfaces.

**Cost and reproducibility.** Forty-four continuation attempts lack usage at this checkpoint, prices are unverified, and deployment/maintenance effort is unmeasured. Private credentials are excluded from artifacts. Exact raw logs may need a separate release review before public distribution. A returned model alias identifies the accessed deployment but does not independently verify its underlying weights.

## 8. Conclusion

The completed development experiments show no stable operational advantage over a strong direct-query baseline. They identify a limited protocol-control effect and an information-loss witness for the older six-service observations. Independent source predictions remain incomplete because of transport failures. Their one-call coverage losses must not be generalized to every computation of the lower interface: one existing source goal is reconstructed on all four projects using two unchanged legal calls and deterministic composition. No upper language-model planner has been tested. Finite reference diagnostics and a separate post-hoc prefix counterexample further distinguish snapshot matching from semantic fidelity. The present evidence supports a limited boundary analysis and the plausibility of the layered design for one demonstrated goal, not a general accuracy, cost, usability or publication-readiness claim.

## Artifact and data statement

Project repository: https://github.com/birdflyi/OSDB_CSTKG_Text2Cypher. The new experimental stages are on separate `codex/ch7-e*` branches rather than a single consolidated release. Data source record: https://zenodo.org/records/18817348. Local extracted data and execution CSV members have recorded hashes; no claim is made here that the entire downloaded archive checksum was independently verified.

Public preparation identifiers: E5 `c9b434a`; E6 `a7fcb2f`; source/generation protocol `083a0fa`; reference preparation `b985e7a`; holdout preparation `2659a11`; cost checkpoint `ec5d6ec`; later source/composition update `307f4a5`; composition script/plan `1ba4dca` (reference branch). Exact private requests, responses, failures, references and budget receipts remain preserved. A consolidated reproducibility release and clean-environment replay are pending. This statement does not imply that all private raw logs or new branches are already a permanent archive.

## References

1. Feng Y, Papicchio S, Rahman S. CypherBench: Towards Precise Retrieval over Full-scale Modern Knowledge Graphs in the LLM Era. ACL 2025:8934–8958. https://aclanthology.org/2025.acl-long.438/ doi:10.18653/v1/2025.acl-long.438.
2. Zhong R, Yu T, Klein D. Semantic Evaluation for Text-to-SQL with Distilled Test Suites. EMNLP 2020:396–411. https://aclanthology.org/2020.emnlp-main.29/ doi:10.18653/v1/2020.emnlp-main.29.
3. Nash A, Segoufin L, Vianu V. Views and Queries: Determinacy and Rewriting. ACM Transactions on Database Systems. 2010;35(3):Article21. https://dbucsd.github.io/paperpdfs/2010_3.pdf doi:10.1145/1806907.1806913.

## Coauthor review items before submission

Complete the frozen independent and held-out-project predictions with recorded infrastructure recovery. Audit their input isolation and all costs. Keep one-call coverage, lower-interface composability and measured planner success separate. Decide whether the completed boundary results offer an actionable contribution beyond limited catalog/controller coverage; if they do not, state that the current method claim is insufficient rather than relabeling a negative result as superiority. Consolidate runnable branches and data instructions. Verify author information, source attribution and artifact release conditions. Expand the related-work comparison where it changes the contribution judgment. Journal formatting and prose polishing follow these evidence checks.
