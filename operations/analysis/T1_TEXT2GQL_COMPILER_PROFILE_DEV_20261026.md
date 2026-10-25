# Text2GQL 通用后端编译适配：首轮独立验收

2026-10-26。这是 T1 的实现准备证据，不是模型预测成绩，也不是整张主表完成。

已从冻结的 `c614367` 单独建立 `codex/ch7-t1-tugraph-adapter`，工作树为 `D:/ch7/code/OSDB_CSTKG_Text2Cypher_t1_tugraph`。原 CypherBench 工作树及 32 个冻结源码哈希全部保持不变。

## 本轮可验收的内容

新版本 `typed-services-dev-0.3-native-profile` 增加显式 `tugraph-4.5.2` 编译选项。默认 Neo4j 查询路径保留；本版本尚未成为正式比较方法。

- 原生 schema 只承认真实声明的属性，不自动虚构 `eid/aliases/description/rid`。当前没有完成主键到组合身份的通用映射。
- 原生标识符使用经过独立验证的简单拼写；空格、保留词等未验证情形明确拒绝，不偷偷改 schema。
- 对有限 AND 条件及其否定显式添加非 NULL 操作数检查，保持 IR 已声明的 WHERE 三值逻辑选择语义。不能推导任意表达式或 OPTIONAL 的普遍等价性。
- REST 入口只接收编译器自己生成的选择调用，保留类型并一次性编码字面量。它不是任意原始 Cypher 的字符串替换器；中文、引号、反斜杠、换行及包含参数标记的字符串经小图执行检查。
- 同一 MATCH 内的连接模式改为确定性最长路径覆盖，保留每条边变量、方向、节点绑定及首次标签声明。用于绕开实测的平行模式排列丢失；不是逐测试题改写。
- 原生节点计数使用 `id(node)` 保持物理实体身份，绕开本小图观察到的节点对象 DISTINCT 计数问题。内部 ID 不作为新公共属性，也没有修改原数据或参考查询。

## 实际检查及保留的失败

独立材料沿用预先构造的四个 Person、四条 LINK 小图，不读取任何 benchmark TEST 问题、参考或模型配置。18 项选择调用在第一次执行前固定；三轮保留相同调用、ID 和预期答案，汇总脚本检查完全一致。

| 版本 | 原定检查 | 额外诊断 | 发现与处理 |
|---|---:|---:|---|
| compiler profile v1 | 17/18 | 0 | 两条平行边应有两种有序绑定，逗号模式只返回一种 |
| pattern diagnostic v2 | 不设正式分母 | 9 条，4 条符合预期 | 单链返回两种绑定；逗号模式少算；显式边 ID 不等检查仍不足以修复 |
| compiler profile v2 | 17/18 | 3 条，均符合预期 | 路径降低修复平行边，但 `count(DISTINCT node)` 将孤立节点也计入 |
| compiler profile v3 | **18/18** | 6 条，5 条符合预期 | 物理 ID 计数和声明主键计数均为 3；原生节点对象计数仍为 4，作为后端差异保留 |

另有一次 `pattern diagnostic v1` 在第一份 worker receipt 生成前退出；原目录只含输入与 worker，未生成数据库日志或查询响应。启动器错误保留，但没有保留到 worker 的根因堆栈，不能断言是端口故障。没有原地盲目重跑该目录；后续使用新目录和不同空闲端口。它新增查询 0 次。

本轮共 **72 次原生小图查询**，包含全部失败和额外诊断；不合并为一个人为扩大的成功率。23 项本地检查通过，包含原 schema 服务回归和新增编译边界检查。它们验证实现，不代表 benchmark EX 或人工语义标注。

## 尚未准入的范围

1. 原生主键／实体身份映射、集合组合与候选检索尚未接通；不能将以上单选择检查当完整 Ours 验收。
2. OPTIONAL／连续 MATCH 的原生降低尚未验证，当前明确 UNSUPPORTED；DATETIME 适配也未完成。保留必要功能要求，不因为本轮实现难度而删除实验或主张已支持。
3. Direct 与 Ours 的共同方言信息、真实 schema、实体权限及反馈预算还需形成同等协议，并在训练／独立开发材料上验收。
4. REST/Bolt 的答案表示与类型、作者子集与实际评分版本、全量 3,826 题及 11 条未知参考的处理、统计分组仍须冻结。参考审计成功数不填入方法主表。
5. 新增 Text2GQL 20,000 次调用申请仍待明确决定；本轮模型调用 **0**，累计仍 **5,192/12,000**，原剩余 6,808 仍限定 CypherBench。

本轮所有私有 CPU 服务子进程均正常退出，核对授权 hostname/owner、主锁不存在、相关进程为空、18870–18873 无监听。未清除旧锁或改动既有模型/GPU/系统服务。调度器未改变，T1 未完成，T2–T5 没有启动。

## 证据入口

- [紧凑机器汇总](D:/ch7/reports/t1_text2gql_compiler_stage_summary_20261026.json)，含源文件与逐轮 spec/receipt 哈希。
- 本地完整记录 `D:/ch7/reports/text2gql_compiler_fixture_v1_20261026/`、`v2`、`v3` 及 `text2gql_pattern_diagnostic_v1_20261026/`、`v2`；实际响应保存在各成功启动目录的 `responses/`，全部 SHA 与 receipt 核对。
- [远程闭环核验](D:/ch7/reports/text2gql_compiler_remote_closure_20261026.json)。私有客户端凭据留在远程私有目录，没有下载或发布。
- [六图参考审计](D:/ch7/discussion/T1_TEXT2GQL_NATIVE_REFERENCE_AUDIT_20261026.md)及[预算申请](D:/ch7/discussion/T1_TEXT2GQL_MODEL_RESOURCE_REQUEST_20261026.md)继续有效。
