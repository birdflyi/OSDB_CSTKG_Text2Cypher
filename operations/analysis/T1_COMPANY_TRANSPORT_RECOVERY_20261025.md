# T1：company 传输中断与审计恢复

## 当前更新：切换已核验的 SSH 客户端，继续原方法

第一份恢复队列完成 5 条新路径后再次发生 SSH 连接重置，于 company 393/694 条停止。没有继续盲目重启。第二条中断的 Ours 输出也已通过缓存恢复，新增模型调用 0；其最终结果与原全明细策略回放一致。两次缓存恢复都没有重新生成模型答案。

随后对 Paramiko 3.5 客户端做了实际检查：原 worker 源码哈希一致、图身份一致、保存的 Direct 答案一致、候选信息一致、写操作仍拒绝，前后节点数均为 581,306。没有替换远程执行器或读保护，只替换本地 SSH 传输客户端。已核验的 known_hosts 继续用于服务器身份检查。

当前队列改用 `infra_paramiko_queue_v1_20261025`，段名 `paramiko_company_v1`。状态审计与新完成图验收须使用 `--queue-state infra_paramiko_queue_v1_20261025`。前两份已停止的队列全部保留；不能据其停止记录判断当前队列。

运维变更验收：[变更清单](D:/ch7/reports/OPERATIONAL_TRANSPORT_AMENDMENT_PARAMIKO_V1_20261025.json)、[实际一致性检查](D:/ch7/reports/paramiko_transport_conformance_v1_20261025/result.json)。这证明已做的传输一致性检查通过，不保证后续网络不会再中断。模型调用重试规则、调用上限、SQL/Cypher 权限、查询时限与输出上限保持原冻结设置。

三个原包装层时钟及对应中断数据库完成状态保留为未知；失败重试与恢复开销保留。不同传输客户端的启动与往返开销需披露，不能当成方法加速收益。原计划中的持久化远程结果日志方案仍未实现，不冒称已部署。

## 第一段恢复的历史证据

company 队列在 02:11 因 SSH `Connection reset` 停止。387/694 条路径已有完整记录，前三图评分完整。原记录与停止文件全部保留。

| 检查项 | 证据与处理 |
|---|---|
| 中断题 | Direct，qid `885ff11b-d708-451e-80e4-493fdff56895` |
| 模型尝试 | slot 2534 读取超时 240.062 秒；等待 10.016 秒后，slot 2535 HTTP200 |
| 缓存完整性 | 成功响应内容与 reply_0 完全相同；重试请求逐字节相同；messages 与原 intent 相同 |
| 查询完整性 | 原冻结解释器从缓存产生的查询及参数，与原 query intent 相同 |
| 远程残留 | 已核验原 worker PID198329、Java198344、company 锁及指定主机；worker 阻塞在原输入管道 |
| 清理 | 对该已核验的空闲 worker 发送 SIGINT；其 finally 停止自己的数据库并移除自己的锁。没有手动删锁 |
| 缓存恢复 | 在同一已验收图上执行相同输出，成功；新增模型调用 0。恢复后账本仍为 2535 |
| 成本保留 | 超时、重试、未知原数据库完成状态保留；丢失的原包装层时钟为未知，不补零 |
| 继续执行 | 新队列目录 `infra_resume_queue_v2_20261025`，新段 `ssh_keepalive_company_v2`；跳过完整记录，继续剩余题 |

这次修改只涉及运维恢复：支持保存有物理重试历史的成功缓存，增加独立恢复/队列目录，并核验缓存对应的原请求。原源码、模型、提示、数据、任务、反馈、评分和分母不变。它不是新方法版本，也不增加自动无限重连。

新运维脚本：`operations/core_infra_resume_v2.py`。SHA-256：`8fd581da9a9b42846e138766decc5e08a4f1796bb10c7397e12cf5b71eba4c1b`。旧脚本和旧停止目录不覆盖。

当前状态检查必须针对新队列：完整性审计及之后的新图验收增加 `--queue-state infra_resume_queue_v2_20261025`。旧 v1 的停止记录不能用作新队列状态。前三图的完成回执仍在旧 v1，结果和验收哈希未变。

证据：[远程清理回执](D:/ch7/reports/t1_company_orphan_stop_receipt_20261025.json)、[缓存恢复计划](D:/ch7/experiments/public_benchmark_v1/core_main_v1_20261024/infra_recovery_company_cached_v1_20261025/plan.json)、[恢复完成回执](D:/ch7/experiments/public_benchmark_v1/core_main_v1_20261024/infra_recovery_company_cached_v1_20261025/complete.json)。


## 2026-10-25 04:02：再次断连及持久请求日志

- Paramiko 队列03:50停止；company645/694，最后一条 Ours 输出完整缓存。两条旧 OpenSSH 队列与本次失败均保留。现场无遗留 native worker、Java 或公共锁。
- 新传输层将原 native worker 置于远端独立 wrapper。每个请求有固定序号和内容哈希；请求、派发、结果和回执经 fsync 后原子发布。断连只重读同一 ID 的结果，单个 RPC 至多3次、间隔至少10秒。daemon 在原生执行后、结果落盘前死亡时仍为未知；禁止自动在新 daemon 重执行。
- 实际 company 检查通过：原答案和实体候选一致；写操作拒绝；节点数581306不变。主动丢弃响应后，重连取回逐字节相同回执；该 ID 的 native 写入为1次。相同 ID 更换请求内容被拒绝。原 native 源码及限制未改。
- 原缓存 qid edbd21b5-5437-4fb9-93f3-1edb37624385 恢复执行得到1行，与全明细一致，新增模型生成0。原未知状态与失败成本均保留。
- 运维实现和检查证据 a989a61 已推送；原冻结源码 c614367 保持32文件哈希一致。当前队列 `infra_journal_queue_v1_20261025`，进程59296/79824；交接与实际进程优先，不能根据这段历史快照重复启动。
- 新组件：operations/durable_native_journal_v1.py、journal_public_worker_v1.py、core_journal_resume_v1.py。验收：reports/OPERATIONAL_TRANSPORT_AMENDMENT_DURABLE_JOURNAL_V1_20261025.json；实际检查 reports/durable_journal_conformance_v2_20261025/result.json。
- 记录字段释义修正：继承的 `automatic_transport_retry:false` 仅表示不自动重启整条队列；它未描述新 RPC 的内部重连。实际 RPC 固定3次、10秒；可由代码和逐请求日志核验。保留原计划文件，不改写历史。模型重试仍由原冻结客户端管理并计入调用账本。
- 连接、日志持久化、重试和服务启动的额外时间计入传输与总成本。数据库自身时间仍由原 worker 返回。此项不是算法改进或速度收益；后续 T4 必须披露传输版本。实现阶段发生过一次 Paramiko `x` 模式不能写入的零模型部署错误；已用 `wx` 修正并重新检查，空部署临时文件保留。
