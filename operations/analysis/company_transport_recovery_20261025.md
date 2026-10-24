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
