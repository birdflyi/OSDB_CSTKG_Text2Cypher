# T1 磁盘配额中断与恢复检查

> 最新状态：2026-10-25 16:41 CST，用户已明确批准“允许归档该失效锁并继续”。重新核对进程、端口、锁、预算、原冻结及 64 KiB 写入／fsync／读回后，已将该锁按本页方案归档；原哈希不变。准入报告：`D:/ch7/reports/t1_approved_quota_recovery_20261025/admission.json`。新队列 `infra_journal_quota_recovery_queue_v5_20261025`、segment `journal_quota_recovery_20261025`；运行状态以后续实际进程和交接为准。下文“待批准”保留为历史方案，不能再当作未获批准。

时间：2026-10-25 15:10 CST。正式批次已停止。没有启动新队列、模型调用或数据库查询。

| 项目 | 已核对的证据 |
|---|---|
| 停止时间 | 14:40:18，原队列 `infra_journal_post_health_queue_v4_20261025` |
| 停止原因 | 候选检索的日志请求落盘发生 `OSError: [Errno 122] Disk quota exceeded`；不是模型调用错误 |
| geography | 710／732 条方法路径终结；余 22 条，来自 11 道未派发题 |
| 核心比较 | 计划 4,696；已派发并终结 3,092；尚未派发 1,604。movie、politics 均未派发 |
| 预算 | 累计 3,566／12,000；剩余 8,434；pending=0。原始账本不变 |
| 未完成生成 | 没有缺少 record 的模型 intent；下一题的候选上下文尚未创建 |
| 中断位置 | journal id 634；日志最后成功 id 633。全量证据复核通过：634 组已发布请求（id 0–633）均有匹配 dispatch／result／receipt，每组 native_writes=1。id 634 只有 3 个零字节临时文件，没有正式 request／dispatch／result／receipt，未执行该候选请求 |
| 原进程 | daemon 296235、native 296237、Java 296255 均不存在；未发现其他该实验 worker；17688 端口未监听 |
| 当前写入检查 | 在授权根目录一次写入 64 KiB、fsync、读回一致，通过；仅删除了此次检查自身的临时文件。这个小检查不证明后续大规模写入一直有空间 |
| 失效锁 | `.public-runtime.lock` 仍存在，69 字节，内容指向已退出的 native 296237／geography |

## 配额解释的边界

正式日志确认当时发生 EDQUOT。15:08 的只读检查显示本实验目录约 9.7 GiB，但不能据此认定本目录限额为 10 GiB。Ceph 在本目录及父目录没有返回 max_bytes 属性；挂载目录返回较大的共享配额，并存在当时的可用余量。其他写入负载、配额维度及时间变化未查清。不要把共享 `df` 空闲值当作本任务可持续写入额度的保证，也不要擅自修改共享配额或删除其他目录。

## 待批准的具体动作

用户接续规则要求“不自动清锁”，所以当前没有移走或删除失效锁。建议批准以下一次性恢复：

1. 再核对授权 hostname、execution_owner、原进程不存在、端口空闲、无其他 runner、账本 pending=0。
2. 再比较锁文件 SHA-256，必须为 `9916c1d031c1b412c3eecac8d02cb2ad5a48b50fc06cceb5c913d1f849b3717e`，且仍指向 296237／geography。
3. 将这一份锁**归档改名**为根目录中的 `.public-runtime.lock.archived-geography-20261025-144018`；目的文件必须不存在。保留原字节，不删除 journal、数据、预测或失败记录。
4. 重新做原冻结准入和存储检查。通过后建立新队列名，从未派发的 11 题继续 geography，再按原顺序 movie、politics。旧停止目录不重启，既有 710 条路径不重发。
5. 若锁身份、进程、预算或存储状态变化，立即停止此恢复；若再次配额失败，记录资源缺口，请求恢复／增加可用空间，不无限重试。

这不是放宽方法、评分或分母，也不把基础设施失败改成答对。旧两条已终结的基础设施失败继续保留。本次在下一题候选请求前停止，不能捏造新的模型错误或失败预测来填补未派发题。

## 证据位置

- `D:/ch7/reports/t1_disk_quota_audit_20261025/remote_inventory.json`：原始日志位置、空间和 id 634 文件状态。
- 同目录 `quota_owner_detail.json`：Ceph 属性与原锁内容。该次广义进程字符串匹配包含检查进程自身；不能据此认定有 worker 存活。
- 同目录 `storage_health_and_stale_lock.json`：排除检查进程自身后的精确 worker 检查、原三个 PID、端口、锁 SHA、小文件写入检查。
- 同目录 `local_unfinished_audit.json`：原预算和未派发题检查。
- 同目录 `session_evidence/`：已完成只读下载并核验 2,547 份原文件；远程证据不删除。`session_download_manifest.json` SHA-256：0733e38edcd94591f48a9a2f942f5f3d5be8b85d7beca7a0b25e1358a10d2398；`journal_evidence_acceptance.json` 为完整配对复核结果。
- `D:/ch7/reports/t1_core_integrity_cost_live_20261024.json`：全量进度及已完成四图成绩。

整张 T1 未完成。geography 尚无整图成绩；不报告局部 EX 或全 2,348 题结论。
