# OSDB-STKG任务来源、隔离与公平主比较协议草案 v1

2026-10-26。本轮只完成T1任务准备；不生成正式题、不派发模型，不启动T2–T5。本文不是任务冻结或准入通过。原CypherBench版本、预测和负结果保留。

## 当前可核对的范围

沿用旧暴露盘点的全部18包/167题位，逐包核对manifest/specification SHA、逐题对齐ID/问题/语义目标指纹；不读旧模型输出和评分决定纳入。先保存审计规则，再重建本轮谱系登记。原始问题、参考和公开源文件缓存留本地，只发布自有测量代码及紧凑统计。

旧四项目全部已接触。99题位有历史记录，68题位未匹配到记录；后者仍不能视为未见。56题位没有semantic_goal字段，保持未知，不能按family标签擅补“独立目标”。reference_query/reference_call是否存在仅是元数据，不是参考执行正确性或可回答性的证明。问题哈希、项目名和时间参数变化也不是新结构证明。

740个固定窗口组合是已看数据层盘点，不能直接套NL模板形成740个独立题。若后续将其用于固定计划机制诊断，须另列高度相关的构造条件，不计入自然语言主比较的独立分母，更不能拿交集大小当修复题数。

## 公开需求与现有操作化的关系

本轮重新读取旧E7 source层8种代理需求对应的CHAOSS源码，固定上游commit `ada8c11ee342fdc64e94f5f894676565aa2e6506`。这些来源此前已经用于任务设计，不是新发现的独立需求样本。以下是对公开定义与旧参考表达式的审计；不修改旧题、旧参考或已评分结果。

| 旧代理族 | 来源要求与旧操作化差别 | 新任务必须明确的范围 |
|---|---|---|
| branch_events | 来源讨论创建、删除、持续存在及分支管理；旧查询只数窗口内观测创建/删除分支 | 不把两项计数称完整生命周期或当前存续分支 |
| pushed_commits | 来源按commit本身的作者/提交时间及merge/empty策略定义代码修改；旧查询按Push事件时间取COMMITTED_AS目标 | 明说观测推送关联commit，不能称完整源码提交指标 |
| pr_observed_duration | 来源Question提到接受或关闭，但Overview/Data points限定accepted/merged；旧查询取CLOSED_BY或MERGED_BY的最早观测终点 | 公开源内部口径差别单列；新协议须选定accepted-only或观察关闭，不能静默混用 |
| reviewed_prs | 来源涵盖评审数量、类型、反馈、结果与正式流程；旧查询只数有观测Review关联的PR | 不能外推正式评审质量或通过率 |
| opening_actor_attribution | 来源涉及人员和组织的广泛贡献归属；旧查询只按观测Issue/PR开启者计数 | 不称全部贡献、组织归属或真实人去重；Actor ID与真实人分开 |
| issue_observed_duration | 来源从初次开启到关闭，并要求明确按开启/关闭时点取cohort；旧查询取快照中最早观测开启和其后的关闭，逐实体返回 | 披露快照截断、重开和右删失；不把观测最早时间当真实初始时间，不将未关闭填0 |
| issue_observed_response | 来源要求其他贡献者的首次回应；旧查询已排除观测开启者，缺回应返回NULL | 这是已有约束，不能当新算法增量；观测开启者不保证完备作者归属，NULL不是零延迟 |
| issues_observed_active | 来源GitHub专门口径列评论、标签变化、指派人变化及关闭；旧参考合并开启/关闭/重开和IssueComment，未覆盖标签/指派人变化 | 旧代理本身不是完整源指标；当前740组合又只含Issue/PR到Actor动作，未覆盖评论分支，三者不可等同 |

证据链接：[Branch Lifecycle](https://github.com/chaoss/wg-evolution/blob/ada8c11ee342fdc64e94f5f894676565aa2e6506/focus-areas/code-development-activity/branch-lifecycle.md)、[Code Changes Commits](https://github.com/chaoss/wg-evolution/blob/ada8c11ee342fdc64e94f5f894676565aa2e6506/focus-areas/code-development-activity/code-changes-commits.md)、[Change Requests Duration](https://github.com/chaoss/wg-evolution/blob/ada8c11ee342fdc64e94f5f894676565aa2e6506/focus-areas/code-development-efficiency/change-requests-duration.md)、[Change Request Reviews](https://github.com/chaoss/wg-evolution/blob/ada8c11ee342fdc64e94f5f894676565aa2e6506/focus-areas/code-development-process-quality/change-request-reviews.md)、[Contribution Attribution](https://github.com/chaoss/wg-evolution/blob/ada8c11ee342fdc64e94f5f894676565aa2e6506/focus-areas/community-growth/contribution-attribution.md)、[Issue Resolution Duration](https://github.com/chaoss/wg-evolution/blob/ada8c11ee342fdc64e94f5f894676565aa2e6506/focus-areas/issue-resolution/issue-resolution-duration.md)、[Issue Response Time](https://github.com/chaoss/wg-evolution/blob/ada8c11ee342fdc64e94f5f894676565aa2e6506/focus-areas/issue-resolution/issue-response-time.md)、[Issues Active](https://github.com/chaoss/wg-evolution/blob/ada8c11ee342fdc64e94f5f894676565aa2e6506/focus-areas/issue-resolution/issues-active.md)。

GitHub Pulse也提供按时段查看PR/Issue活动的公开使用场景，但它有仓库范围及自己的状态口径。这只能补需求动机，不能证明我们的快照完整、问题独立或与Pulse定义等价。[GitHub Pulse说明](https://docs.github.com/en/repositories/viewing-activity-and-data-for-your-repository/using-pulse-to-view-a-summary-of-repository-activity)。

## 新任务登记与隔离

每条候选必须有稳定task_id、来源URL/版本/位置/缓存SHA、原需求与改写说明、项目/快照hash/明确仓库范围、实体键和输出粒度、动作/关系方向、时间绑定及半开端点、去重/聚合目标、空值/无结果规则、参考实现及验收证据。来源只支持动机时标“开发者/AI操作化”，不写成真实用户问题或原指标复现。

暴露登记包括来源、图、原问题、语义目标、参数变体、NL改写、结构族、参考、模型输出八类材料的实际接触时间和用途。缺记录填未知，不填未见。family/goal字符串是待审标签，不能直接用于认证结构留出；须提交规范结构描述和与旧族的逐项差别。纯换项目、实体、日期或同义改写不算新组合结构。声明真正新schema外推需要未用于开发的图源，四个历史项目不能满足这一项。

划分先于新任务效果观测：历史材料永久标开发/诊断；新增候选进入intake，按来源与语义/结构族成组，禁止同目标改写跨开发和确认。先固定去重/聚类规则、全部纳入/排除原因和预定统计组，再划分；不能只从已知有交集的位置选确认题。已有数据盘点对任务设计的影响明确披露。AI生成/复核如实标注，不能把当前同一助手的重读称为独立人类标注或盲审。

确认集在适配、提示和工具冻结后才供预测runner读取。评测器单独持有参考、来源标签和结构组；模型输入白名单仅任务ID、问题、公共schema、双方同等实体候选和明确任务范围。不得把任务的gold能力标签、预期策略或答案基数泄漏给任一方法。

## 参考与范围验收

独立CSV集合/记录计算与手写完整查询分别实现，均不能调用被测Ours编译器作为唯一参考。对方向、同名/复合键、空集、重复关系、评论关联、时区/半开边界、重开及缺失事件逐项验收；保留所有原始响应与异常。CSV参考只能证明当前快照定义的答案，不证明完整GitHub历史。必须在授权图数据库宿主上实际核对返回值与完整性，离线准备不能替代执行验收。

仓库过滤必须由图中可验证身份和明确映射给出。固定窗口盘点用全快照范围；旧source代理带项目entity_id前缀。后续不能混用两者分母或按结果选择范围。观察不到关闭/回应、空cohort或缺失元数据不自动判0、不填因果结论；参考歧义进入预先固定的异常清单，主指标分母及未知处理在预测前冻结。

拒答能力单列：不能让全是不可回答的问题成为“有用答案准确率”证据。对于需要实际答案的全题EX，拒答不算正确；参考明确不足的问题另报正确拒答/错误作答及其分母，不能把它们与可回答EX无说明混合。

## 公平方法与完整成本

| 条件 | 同等信息/实现约束 | 在T1中的解释 |
|---|---|---|
| Direct / Direct+一次执行反馈 | 同底模、schema、实体候选、原始图读权限、采样/输出/反馈/重试上限；不将Direct限制在Ours有限IR | 主要独立生成强对照，首轮共享须披露 |
| Ours | 仅在开发材料上做通用schema/身份/时间适配；不根据确认错题逐项加工具 | 评价完整最终答案，含拒答、错误、超时和截断 |
| 正确全明细 | 对同一有效计划，按正确身份粒度取完整distinct ID/必要载荷；允许数据库DISTINCT和谓词下推 | 固定计划共享规划器，不能冒充另一独立模型 |
| 简单正确策略 | 预先固定：有充分的实体不相交证明才安全组合；其余取完整distinct身份后做精确集合组合；复用请求/结果并缓存 | 不能用无条件相加或故意传重复明细弱化基线；具体规则及等价情况须登记 |

两种控制若在当前图/任务上完全相同，明确报告相同路线/结果，不制造两个独立方法收益。事后知道实体交集为0不是可供规划器使用的不相交证明。事件/动作/月份分区不是实体键分区。任何共享计划或缓存均保存依赖图，真实模型账本去重；报告独立部署成本时各自计必要规划、索引、检查、数据库、完整传输、组合、失败/重试和端到端时间。准备扫描与参考验证成本单列，不能当方法运行收益。

数据层真实重复只证明问题有实例，不能证明组合规则新颖或Ours胜过正确数据库DISTINCT。当前主线没有被证明的EX优势/摘要效用，需保留中性和负结果。此协议不扩一般NF理论，也不为研究失败找基础设施借口。

## 当前验收和下一步

本轮计划登记167个历史题位，正式模型派发/完成/失败均0；正式N未定，因此正式未派发数为未定，不能填0伪装整块完成。旧18包的登记完整不等于任何新题通过确认准入。8个源代理族共32题位；逐源缓存成功与否见source_receipts，不把浏览到标题当缓存及语义合同全部通过。

下一步先按上述协议形成**实际候选来源—目标—结构差异**清单，明确哪些只适合开发/机制诊断、哪些能够隔离确认；完成独立参考及公平适配后，再确定正式N、统计组、调用与资源。未满足隔离不能随意挑一个N开跑，也不能因此永久删除OSDB必要块。MTQ/ZOGRASCOPE及其他公开候选继续独立核验，不按方法胜负取舍。

新n24-112接口和授权图数据库CPU宿主仍待可达资源；不复探旧节点、不重复索要已提供令牌。累计上限12000的剩余6808仍限CypherBench；OSDB任务、新公开基准及第二底模另核资源。此来源工作不触发Text2GQL条件20000额度。源审计代码/紧凑报告阶段完成后提交，只有完整阶段才推送。
