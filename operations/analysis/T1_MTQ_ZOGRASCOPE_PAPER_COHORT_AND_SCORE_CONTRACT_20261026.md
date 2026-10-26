# T1：MindTheQuery论文图域候选与MTQ／ZOGRASCOPE评分来源合同

阶段：2026-10-26。仅离线来源、数据身份与评分准入准备；不是新模型实验，也不是正式分母冻结。

本轮把MindTheQuery论文列出的三个留出图域与固定发布数据逐行对齐，得到7,059个候选题位；其中4,934位在当前仓库被放在TRAIN文件中。因此，直接沿用当前文件名安排训练或示例，会破坏这条论文图域留出候选。7,059的数量和内容对齐仍不能证明论文实际运行ID、参考正确性或正式准入。原CypherBench完整结果、负结果及所有必要比较保持原样，整张T1尚未完成。

## 1. 先固定规则，再做来源对齐

MindTheQuery固定版本为`ec4054a1b7df7e6310efa95c56bb6e7cebc4d161`。本轮protocol在下载和读取本轮原始五类文件前写入，包含自有脚本SHA；已有发布数据和论文暴露如实承认，不声称全历史预注册。只按论文域名单取Bloom、HCA、WWC各自全部五类原始文件，不读取方法预测或成绩作选择。以原样NL／Cypher对及完整解码JSON行两个多重集核对，保留重复，不做语义归一或去题。论文§3.1给出图域留出及18,470／2,000／7,059的训练、验证、测试规模。[论文](https://aclanthology.org/2025.emnlp-industry.133.pdf)；[固定发布版本](https://github.com/endeavorXx/Mind-the-Query/tree/ec4054a1b7df7e6310efa95c56bb6e7cebc4d161)。

| 论文留出域／发布目录 | 原始五类题位 | 当前TRAIN题位 | 当前TEST题位 | 原样问题／查询唯一对 | 完整JSON行多重集一致 |
|---|---:|---:|---:|---:|---|
| Bloom／bloom50 | 1,547 | 1,081 | 466 | 1,547 | 是 |
| HCA／healthcare | 2,566 | 1,794 | 772 | 2,566 | 是 |
| WWC／wwc | 2,946 | 2,059 | 887 | 2,945 | 是，保留1个重复余量 |
| 合计题位 | **7,059** | **4,934** | **2,125** | 不据此定义独立语义目标 | 三域均通过 |

这比“数量恰好相等”更强：15个原始文件与30个现行划分文件在各域完整行多重集上相同。但仍缺论文实际7,059测试ID清单、18,470／2,000训练验证ID分配和运行材料。若采用此图域候选，4,934个现行TRAIN位置必须整体隔离于训练／ICL／方法开发；若采用现行逐图划分，则另列协议，不能称论文图域复现。只取三个域现行TEST的2,125位也不能冒称7,059。其他图、Manual轨道和原发布位置全部保留，不能按结果决定删哪一部分。

候选逐位registry记录发布路径、行号、原unique_id、原样对SHA和完整行SHA；未输出原NL和查询到论文仓库。它是可追溯候选，不是新的语义独立性证明。

## 2. Manual标签语义：0不能统一解释为空答案

论文Appendix B把`logical`定义为查询语义正确性，把`result`定义为是否得到非空输出；`result=0`同时覆盖空结果和执行错误。由此可确认JSON中的logical语义；仓库annotation_protocol使用manual字段名的差异留档，不再把它误写成logical含义完全未知。[论文Appendix B](https://aclanthology.org/2025.emnlp-industry.133.pdf)。

现行Manual TEST共1,829位置：logical=1有1,316，logical=0有85，空标签有428；result缺失859。logical=1且result=0的206位置不能直接标为有效空答案；logical=0且result=1的8位置也说明非空不等于正确。全部缺失、冲突和原始位置保留，不从Automated标签补值。这些是参考元数据，不是我们新模型的EX，不把1,316自动作为已验收分母。历史报告中的“result=0空结果”说法以本补充合同限定为“空或错误，待实际参考执行区分”。

## 3. 作者运行验证器与最终答案评分分开

已取得固定版本9个Python模块并仅AST解析、静态阅读；没有import或运行作者代码，没有安装依赖。`automated_validators.py`的运行检查针对单条候选Cypher：捕获错误文本、判断返回列表是否为空，以及值是否为None、空字符串或空列表。其“non-zero”注释并不等于实现会拒绝数值0，实际条件仍允许0。`utils/neo4j_conn.py`返回记录字典列表，不能由此推导最终评分的顺序／重复／类型合同。[固定运行验证器](https://github.com/endeavorXx/Mind-the-Query/blob/ec4054a1b7df7e6310efa95c56bb6e7cebc4d161/automated_validators.py)。

这是数据构造验证，不是本轮已取得并验收的gold-vs-prediction最终答案比较器。不能把非空筛选当作新模型成功率、把数值0当失败，或把两个都执行错误当答案相等。未声称全网络不存在作者评分代码；目前核验材料尚不足以冻结该合同。

## 4. ZOGRASCOPE：gold-linked输入与作者EX仍需可执行合同

固定版本`b2f91718b1324fdf8c759131d0d7c47c0eb33373`的完整树只有14个blob、没有直接`.py`路径；图导入ZIP内部是否含评分工具尚未据此排除。原固定发布IID768、COMP1,349、LEN1,253，共3,370分区题位／2,672并集唯一ID保留；与论文767／1,350／1,253及训练口径差异继续单列，不能只因总数相同就合并分区。

论文§4与README采用gold-linked实体输入；同一结果块内各方法应获得相同`nl_gold_linked`及实体信息，不能一方带gold、一方自行链接。这一设置不等于部署场景的实体链接或真正泛化成绩。`return_count`只是参考结果基数元数据，应留在评分端，不能进入提示或路由。MTQ与ZOGRASCOPE都含POLE来源，不能把两者自动算两个独立图域；dump版本和具体任务仍需对齐。[论文v2](https://arxiv.org/html/2503.05268v2)；[固定README](https://github.com/interact-erc/ZOGRASCOPE/blob/b2f91718b1324fdf8c759131d0d7c47c0eb33373/README.md)。

论文的EX要求执行结果相同，但这句话不足以确定行顺序、重复、类型、NULL、浮点、实体序列化、超时和截断的实现细节。不能悄悄沿用CypherBench比较器并称其为官方ZOGRASCOPE评分。

## 5. SPOT链接与CYQUARK生成源码不等于可直接推理基线

沿ZOGRASCOPE作者链接核验SPOT仓库，固定到`29123dc1ad26b6d13c0b7d5aee91b4bcb64968b7`；当前README又链接已知的`Text-To-Cypher-knowledge-graph-data-generation`作者生成仓库，本轮固定到`013eab46e71aa1b5cfbd3ed91a44c030b17931ea`。这不是新发现一份CYQUARK模型权重，也不把新生成器当原SPOT实验版本。两仓库完整树分别21／35个blob；按文件名检查尚未获得可对应论文的评分／预测／checkpoint／推理材料，不能由文件名盘点断言全球无资源。[SPOT固定README](https://github.com/interact-erc/SPOT/blob/29123dc1ad26b6d13c0b7d5aee91b4bcb64968b7/README.md)；[生成源码固定README](https://github.com/interact-erc/Text-To-Cypher-knowledge-graph-data-generation/blob/013eab46e71aa1b5cfbd3ed91a44c030b17931ea/README.md)。

SPOT的zero-shot是无人类标注训练数据的设置，仍包含图落地合成数据和解析器微调；论文Table 1明确标为SPOT fine-tuning Qwen3-4B。不能将其等同于当前同底模Direct零示例推理，也不能只拿生成源码就宣称复现该行。需要对应checkpoint／作者原始输出，及训练来源、配置、种子、实体输入、版本和成本；本轮没有运行生成器、训练或新增GPU需求。[SPOT论文](https://aclanthology.org/2025.findings-acl.524.pdf)。

Cypher-RI、CypherSmith、CYQUARK必要候选行继续保留为资源／协议未完成。公开来源核验继续，不向作者发消息，不拿文献reported填本项目实测。资源缺项不阻塞已独立准入的核心块，也不被标为完成。

## 6. 下一步共同答案合同验收清单

以下是待明确并验证的合同项目，不是已实现或已通过的评分器；分别保留作者规则、可证实实现及本项目辅助严格比较，不能混称官方结果。

| 项目 | 必须固定并提供的证据 | 当前状态 |
|---|---|---|
| 任务／划分／暴露 | 原始位置、图／schema／dump身份、论文或发布轨道选择、训练示例隔离、分组与分母 | MTQ域候选已对齐；论文运行ID、完整图参考验收未齐 |
| 共同输入与权限 | gold-linked或原NL的轨道、schema／实体候选、同等图访问、输出上限、反馈预算 | 协议约束已记；各块执行快照未冻结 |
| 行／列比较 | 有无ORDER BY、顺序、bag重复、列位置／别名映射，不能默默排序去重 | 作者实现与合成边界检查待核 |
| 类型与值 | bool与整数、整数／小数、NULL、日期时间精度／时区、列表／map、浮点容差 | 实际驱动表示与作者行为待核 |
| 图实体 | 节点／关系身份、标签属性、路径、序列化，不能以显示名冒实体键 | 图与驱动对照待核 |
| 失败与完整性 | 有效空答案、参考错误、拒答、超时、截断、未知完成状态均单列；拒答不计答对 | MTQ标签歧义已澄清；真实参考执行待资源 |
| 统计与成本 | 固定全部位置／图宏平均／覆盖／失败／配对区间；共享ID与结构族分组；全部尝试账本 | 新正式N未定，无新模型统计 |

先在独立合成边界例验证比较器，再在授权图宿主验证真实驱动／参考；两者均不能替代对方。不得根据已看TEST输出逐题改工具后混入冻结结果，也不以开发胜出作准入条件。基准不同协议应分块报告，不能用统一高分目标反推删题或放宽评分。

## 7. 本阶段验收、成本与发布范围

| 工作块 | 计划 | 已完成 | 失败／未完成 | 验收含义 |
|---|---:|---:|---|---|
| MTQ三个域原始文件 | 15 | 15 | 0 | 固定Git blob、字节数、SHA256通过 |
| MTQ作者Python来源 | 9 | 9 | 0 | 固定来源与AST通过，未执行 |
| 现行对应TRAIN／TEST文件 | 30 | 30 | 0 | 与三个域完整行多重集一致 |
| 域候选位置登记 | 7,059 | 7,059 | 0 | 仍非正式N／独立语义目标数 |
| 新正式预测 | N未定 | 0 | 派发0、失败0、未派发N未定 | 未准入，不把未知N写成0题完成 |

独立复核再次验证24个原始／作者文件共7,286,931字节、30个现行划分文件、三域完整行多重集、2份作者链接README、2份论文缓存、protocol／registry及封存脚本SHA；全部通过。canonical冻结32个源码SHA和适配分支46个运行源码SHA保持不变。本阶段模型调用／数据库查询／内网接口探测／SSH请求均0；预算inspect前后相同：实际累计5,192／12,000，余6,808、pending为空，余量仍仅原CypherBench授权范围。费用未知；未触发Text2GQL条件20,000额度，MTQ／ZOGRASCOPE也未借用该额度。

原始材料仅保存在`D:/ch7/reports/mtq_paper_cohort_contract_20261026`，包括protocol、source_receipts、released_split_receipts、candidate_positions、author_function_inventory、论文／源码缓存和independent_verification。紧凑汇总为`D:/ch7/reports/t1_mtq_zog_score_source_contract_summary_20261026.json`。论文仓库只提交自有审计脚本、本文和紧凑统计，不提交第三方完整语料／论文／逐题内容、凭据或一次性人类审批材料。

旧服务器仍按用户报告离线，本轮不据此推断其当前进程或锁；不复探内网接口、不恢复旧队列／服务／锁。UU已验证的小文件往返不等于模型路由或授权图宿主准入。后续继续离线评分合同与实际新任务参考准备；真实执行仍待可达推理路由及授权图数据库CPU宿主，不重复索取已提供令牌或已回答资源问题。T2–T5的要求保留，未自动启动。
