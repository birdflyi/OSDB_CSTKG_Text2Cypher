# OSDB历史结构与当前候选暴露核对

2026-10-27。18个历史包、167个题位已逐位关联到原始问题、参考操作及SHA；56个缺失semantic_goal的位置完成结构旁证。**旧题和当前24个ACT候选全部保留为开发材料，独立正式N仍未定。** 没有新增模型、图查询或来源网络请求，没有改变旧方法、答案或评分。

56个缺标签位置是14个任务组各4个位置，分布于Redis/DuckDB的E5/E6开发材料。原semantic_goal=null不补写；派生说明单独存档，不能冒充原始标签或独立语义目标。48个有参考查询的位置与已声明known族有相同参考查询文本；另8个是缺PR标识、真实解决因果关系两个边界。相同文本不证明参考正确，两个null相同也不证明语义等价，旁证同时保留原问题和操作。

| 旧任务组 | 实际结构 | 位置数 |
|---|---|---:|
| issue_openers | 固定Issue到Actor的开启关系投影 | 4 |
| pr_links | 固定PR的外链记录投影，保留重复 | 4 |
| pr_domain_counts | 单时间窗外链记录按域分组计数 | 4 |
| issue_count | 字面前缀下Issue节点数 | 4 |
| issue_commit_path_count | 同一评论连接Issue与Commit的路径计数 | 4 |
| pr_link_count | 单时间窗外链记录数，含零 | 4 |
| issues_new / issues_closed | 单窗开启／关闭Issue的去重计数 | 8 |
| pr_new / pr_accepted / pr_declined | 单窗开启／合并／明确拒绝PR的投影或计数 | 12 |
| pr_acceptance_ratio | 同窗条件去重计数及分母为零保护 | 4 |
| missing / causality | 缺标识与真实解决因果证据边界，无数值参考 | 8 |

另外111个有goal的位置也全部核对；32个历史family标签不等于32个独立目标。旧issue_observed_response包含“回应者不属于开启者集合”和零回应NULL；pr_declined要求明确拒绝事件，均不能仅因有NOT或缺席字样就认作ACT03的同一合同。ACT03按同一账号取“前一窗口开启账号集合减去后一窗口开启账号集合”，方向是artifact到Actor，报告观测开启记录的缺席，不推断人的真实不活跃。在本次167位范围未找到完全相同的合同；这不证明完整历史中未见，也不证明统计独立。

更关键的是，ACT03四个项目绑定已在这次暴露开发回归中派发。已按初轮request文件核对ACT01–06全部24题位、两方法48条初轮路径；不读取答对与否来决定资格。因此不能在事后完成谱系说明后将ACT03升格为独立正式确认题。ACT01是旧开启归属的投影，ACT02是旧正计数的交集筛选，ACT04改变first-ever为有界first-observed，ACT05重复旧先前历史边界，ACT06雇主及分母仍未裁定；它们也全部保持开发隔离。8个边界位不因此获得正式拒答gold。

本次结构核对计划167／完成167／失败0／未核对0；缺goal计划56／完成56。独立新正式任务派发／完成／失败均0，N与未派发数未定，不能写成“0题正式实验已完成”。当前没有模型runner；3107的hostname和自有execution_owner一致、模型worker为0。账本6794/12096、余5302=CypherBench5270+OSDB开发32，pending为空；剩32不授权重跑旧路径。64并发验收已完成，后续获准入的就绪模型队列使用合计64硬上限。

完整逐位旁证仅本地`D:/ch7/reports/osdb_structure_lineage_v1_20261027/`：protocol、historical_review、missing_goal_comparisons、current_candidate_exposure、candidate_contract_comparison、input_hashes与summary。88个输入文件前后SHA一致；原registry和候选登记不覆写。公开只保存自有脚本和紧凑汇总，不复制原题、参考或模型响应。

接下来补充第二公开来源的答案评分合同、独立任务来源与结构隔离设计、具名作者方法资源；独立任务、方法可归因增量、正式新块与T2–T5缺口保留。不为保持API忙碌而重复旧题，也不把这次谱系核对当作T1完成。
