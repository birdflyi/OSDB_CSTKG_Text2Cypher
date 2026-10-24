# T1 其他公开候选：运行期间的离线审计

当前 CypherBench 正式方法与评分继续冻结。本文件只登记其他主表单元格的准备证据，不改变当前预测，也不授权额外模型调用。

## 21:59 增量：划分与数据包已进一步核对

| 项目 | 本轮确定的内容 | 对实验的影响 |
|---|---|---|
| ZOGRASCOPE | 固定提交的 IID 768、COMP 1,349、LEN 1,253 条；IID/COMP 无交集，完整覆盖常规 TEST 的 2,117 条 | 各划分单列，不按结果选范围 |
| ZOGRASCOPE 重复 | LEN 中 698 条在常规 TEST，555 条在常规 TRAIN；三种测试划分的并集为 2,672 个唯一 ID。LEN 的 1,253 行与原语料对应行完全相同 | 不把 3,370 个划分位置当独立问题。若采用完全相同的零样本输入/方法，可复用同题预测，再分别评分；训练或 ICL 上下文不同时不能复用 |
| ZOGRASCOPE 版本差异 | 论文 v2 表 2 为 IID 767、COMP 1,350；当前固定仓库为 768/1,349。CSV 的 template_id 与训练集有重叠，不能直接用该字段充当论文定义的匿名结构族 | 记录版本差异；正式前核查分区构建规则与统计族。不能据此直接断言作者划分错误，也不能将文献成绩当成本次同分母实测 |
| ZOGRASCOPE 输入 | 作者主表使用 gold entity linking；论文附录另报无 gold linking | 实体链接轨道明确标注，双方获得相同信息；不能直接混排不同轨道的成绩 |
| MindTheQuery | 已取得作者 annotation_protocol.txt；说明中的字段是 manual，实际 TEST 是 logical，且未给出清晰的字段映射/筛选实现 | 继续保留 1,829/1,316 等盘点数字；不自行将任一数字定为有效分母 |
| Text2GQL-Bench | 作者链接 ZIP 的 Cypher TEST 有 6 图、3,826 条唯一图-ID记录；已保存 133 个 JSON/CSV 文件并校验 ZIP CRC 与文件 SHA-256 | 可以推进离线 schema/数据审计；仍未部署数据库、未执行参考、未获额外预测预算 |

Text2GQL 六图题量：Financial_Payment 776；Information_Technology_IT_Asset_Management 726；Knowledge_Graph_Geography 642；Manufacturing_BOM(Bill_Of_Materials) 489；Manufacturing_Production_Process 596；Social_Network_Twitter 597。题量按当前下载对象的实际 Cypher TEST 文件统计，不采用跨语言总标注量。

数据包大小 953,926,289 字节，HTTP Last-Modified 为 2026-03-04。采用 If-Match 与 Range 读取目录及所需文件；没有完整下载 ZIP。ETag 用于固定该 HTTP 对象，不能冒称整个 ZIP 的 SHA-256。各已保存文件有独立 SHA-256，解压时核验 CRC；图源仍是 TuGraph，尚未证明 Neo4j 等价性。所有作者文件只作数据读取，没有执行作者代码，没有模型调用。

| 候选范围；尚非运行授权 | 两方法首轮调用数 | 双方每题一次反馈的额外上界 | 未包括 |
|---|---:|---:|---|
| ZOGRASCOPE，单一零样本输入轨道、2,672 个唯一题 | 5,344 | 5,344 | 开发、失败重试、具名方法、另一实体链接轨道 |
| Text2GQL Cypher TEST，3,826 题 | 7,652 | 7,652 | 开发、失败重试、其他语言轨道、具名方法 |
| MindTheQuery Automated，8,284 条盘点记录 | 16,568 | 16,568 | 轨道及参考资格尚未审定，不能当正式申请量 |

当前 12,000 次授权不覆盖这些新增运行。先解决协议和参考资格，再提出具体额度。范围选择依据互补证据与协议适用性，不依据当前 CypherBench 的胜负。

证据：Zograscope/split_protocol_inventory_20261024.json；protocol_audit_additional_assets_20261024.json；Text2GQL-Bench/zip_directory_inventory_20261024.json；Text2GQL-Bench/cypher_test_snapshot_inventory_20261024.json。文件均在本页末尾列出的证据目录下。

依据：[ZOGRASCOPE 论文 v2，第3、4节及附录B](https://arxiv.org/html/2503.05268v2)、[固定提交的作者说明](https://github.com/interact-erc/ZOGRASCOPE/blob/b2f91718b1324fdf8c759131d0d7c47c0eb33373/README.md)、[Text2GQL 数据准备说明](https://github.com/TuGraph-family/Awesome-Text2GQL/blob/7cd94dfa39102bf97753177e52ad6181a1f1e373/dataset_prep/README.md)。

| 候选 | 已核验内容 | 正式运行前还缺什么 |
|---|---|---|
| MindTheQuery | 作者仓库固定提交 `ec4054a1b7df7e6310efa95c56bb6e7cebc4d161`；官方 Automated 测试文件共8,284条、11个图；Manual 测试文件共1,829条、5个图 | 对齐论文与作者代码的评测轨道；核验手工标签含义、dump版本、实际schema和参考答案；另列调用预算 |
| ZOGRASCOPE | 作者仓库固定提交 `b2f91718b1324fdf8c759131d0d7c47c0eb33373`；提供 IID/COMP 标识清单、单独LEN划分、schema和多个Neo4j版本转储 | 盘点各划分题数、输入实体链接条件、训练与测试隔离；实际参考执行及任务结构审计；另列调用预算 |
| Text2GQL-Bench | 作者链接仓库固定提交 `7cd94dfa39102bf97753177e52ad6181a1f1e373`；提供数据下载入口和正式driver入口 | 固定真实数据包版本、Cypher测试轨道与题数；核验TuGraph/Neo4j语义和返回类型；确定图及运行环境；另列调用预算 |

以上三项均已保存仓库元数据与完整Git文件树。共73个选定文件、10,374,161字节通过Git blob与SHA-256核验。没有运行作者代码，没有安装新数据库，没有调用模型。未读取或下载仓库的`.env`。

## 必须保留的两项区别

### ZOGRASCOPE 的统计族键核验补充

对固定版本的标准 TRAIN+TEST 仅检查元数据：378 个原始 template_id 中，113 个对应多个节点数，104 个对应多个返回类型。因此，这个字段不能直接用作已验证的匿名结构族。此前观察到的 ID 重叠，也不能直接解释为作者结构划分泄露。正式统计前需要另行验证结构分组；不修改作者 IID/COMP/LEN 成员，不读取模型成绩来决定分组。

证据：`Zograscope/split_protocol_inventory_20261024.json`。本次没有执行作者代码、查询数据库或调用模型。

### Text2GQL 图 CSV 离线结构核验补充

| 图 | 节点行 | 边行 | 节点类型 | 边类型 | 边属性声明 | 重复的有类型端点对 |
|---|---:|---:|---:|---:|---:|---:|
| Financial_Payment | 8,420 | 17,184 | 8 | 8 | 0 | 0 |
| IT_Asset_Management | 815 | 3,107 | 7 | 8 | 11 | 0 |
| Knowledge_Graph_Geography | 1,100 | 9,715 | 7 | 7 | 0 | 0 |
| Manufacturing_BOM | 1,500 | 2,648 | 8 | 7 | 19 | 10 |
| Manufacturing_Production_Process | 1,030 | 1,595 | 9 | 8 | 6 | 0 |
| Social_Network_Twitter | 8,450 | 56,577 | 7 | 8 | 16 | 0 |

这些是 CSV 行和 schema 声明的盘点，不是已导入数据库的实体数。离线核验未发现重复节点主键、悬空端点、端点类型约束冲突、列宽问题或本次基础类型检查异常。BOM 的重复端点边保留，没有合并。空字段已计数；空字符串/NULL、日期和关系多重性的实际引擎语义仍待核验。

这为“异质、带属性、带时间数据”的互补性提供了数据层依据，但不证明测试题实际使用这些特征，也不证明当前服务能完整支持它们。下一步先做原生引擎/参考验收和任务特征盘点，不能按我们已经观察到的胜负决定选哪些图。

证据：`Text2GQL-Bench/offline_csv_graph_audit_20261024.json`；脚本 `D:\ch7\reports\audit_text2gql_csv_graphs_20261024.py`。没有部署数据库、执行作者代码或调用模型。

1. **MindTheQuery 的两个轨道不能混算。** 当前 Manual 测试文件中的 `logical [0/1]` 标签为：1共1,316条，0共85条，空白428条。标签含义和作者评测筛选规则仍待核对。不能把全部1,829条直接当作语义正确的NL—Cypher参考，也不能现在自行取1,316条作为正式分母。先核验协议，再在预测前固定纳入规则；不根据我们的方法成绩筛题。
2. **MindTheQuery 与 ZOGRASCOPE 的POLE图同源。** 两个仓库的 `pole-50.dump` 都是4,603,972字节，Git blob均为 `c284924aa7f16ed2ae8b0b3bcfceb141bb1f426e`。它们可以验证不同任务与结构划分，但不能据此声称两个独立图域。

CYQUARK论文对MindTheQuery报告的是五图结果；当前作者仓库同时提供11图Automated和5图Manual文件。需要进一步对齐具体轨道和过滤规则，不能将该论文报告值直接填成我们所下载全部文件的对应基线。

## 具名方法仍保留

| 方法 | 当前可用性结论 | 下一项工作 |
|---|---|---|
| Cypher-RI | 同名公开权重存在；作者归属、完整schema选择交互与推理入口尚未核实 | 继续核对论文/官方补充材料与权重关联；不冒称作者复现 |
| CypherSmith | 已核实ACL论文；当前搜索发现的同名PyPI工具与论文作者不同 | 不以该PyPI包替代论文方法；继续核对作者checkpoint、数据或输出 |
| CYQUARK | 论文链接的作者数据生成仓库已核实；最终训练权重/同题输出尚未核实 | 核对可复现训练与推理资源；生成器不能直接算已复现的专用模型 |

这些结论仅描述已核实资源，不证明资源在全网不存在。资源缺口不删除主表要求，也不阻塞当前已准入的独立核心比较。

累计12,000次授权仍限CypherBench阶段。其他基准先完成轨道、分母和数据核验，再提出明确的调用与计算需求；不使用当前余额擅自开跑。

证据目录：`D:\ch7\literature\public_candidate_metadata_20261024`。主要盘点：`MindTheQuery\test_split_inventory.json`、`MindTheQuery\manual_label_inventory.json`、`selected_asset_receipts.json`。

官方来源：[MindTheQuery作者仓库](https://github.com/endeavorXx/Mind-the-Query)、[ZOGRASCOPE作者仓库](https://github.com/interact-erc/ZOGRASCOPE)、[Text2GQL作者链接仓库](https://github.com/TuGraph-family/Awesome-Text2GQL)、[CYQUARK论文](https://arxiv.org/html/2606.14325v1)、[CypherSmith论文](https://aclanthology.org/2026.acl-long.1601/)、[Cypher-RI论文](https://papers.nips.cc/paper_files/paper/2025/file/906727703322b2e33b37c855fc15e8b7-Paper-Conference.pdf)。
