# T1：数据库接口验收通过，恢复原执行链路

**原冻结实验可以继续。** 数据库接口的实际验收已通过，一次入账的模型连通检查也通过。我们继续使用已验收的原 journal runner，按原顺序补完 geography、movie、politics。已验证的远端本地 IPC 保留为备用方案，本次正式预测不切换执行位置或预算权威。

| 检查 | 结果 |
|---|---|
| 原冻结源码 | c614367，32 个源码哈希不变；工作区干净 |
| 实际服务器验收 | 授权 hostname / owner 匹配；原冻结准入逻辑通过 |
| 冻结依赖 | 32 项源码、19 项数据、2,371 项证据校验通过 |
| 原 worker | 10 项依赖、geography 图 SHA 一致 |
| 输入字节 | 207 份已有 context 保持不变；实际候选检索重建的已用 context 为 3,600 字节，逐字节一致 |
| 只读护栏 | CREATE 被拒绝；前后节点数均为 773,489 |
| 退出清理 | 原 worker 正常退出；运行锁已由原 finally 清理；没有人工删除运行锁 |
| 模型检查 | 13:19 一次物理尝试，输出可用；不是新的主表样本 |
| 恢复前真实预算 | 3,267 / 12,000；剩余 8,733；pending=0 |
| 正式方法变动 | 无；原模型、提示、分母、评分、反馈和重试规则不变 |

## 准备阶段处理的问题

- 本地合成子进程默认使用 GBK。适配层显式指定 UTF-8 后，10 项本地检查通过。冻结 worker 源码没有修改。
- 一份旧恢复记录的 lookup 字段顺序与已用 context 不同。按原 journal 的规范 JSON 边界复核后，207 份 context 均一致。没有重写任何已用输入或原记录。
- 首次服务器验收因缺少 Git，在启动 worker 前停止。失败结果保留。我们在授权根目录内下载 Ubuntu 包并按 apt 元数据 SHA 校验，只解包到私有目录，没有全局安装或修改 GPU / 模型服务。真实 Git 检查得以保留。
- 第二次实际验收通过，耗时约 51.36 秒，没有模型调用或正式预测。这是执行接口检查，不是新的 benchmark 效果或效率结果。

Git 包为 `1:2.43.0-1ubuntu7.4`，SHA-256 为 `c79b7948ef9860c97ec74293a90178676e63368a0711c95c28a55af4edf1adab`。私有目录为授权根目录内的 private_git_noble_20261025/extracted；环境变量仅作用于该验收进程及子进程。

## 恢复方式

原执行链路的 journal adapter 文件哈希已重新核对。新准入再次检查原冻结文件、未知 intent、实际预算及服务器进程与锁。模型连通检查距离上次探测约八小时，只做一次，不重发旧失败请求。

本次使用新检查点目录 `infra_journal_post_health_queue_v4_20261025`，segment 为 `journal_post_health_1320`。runner 仍是原 core_journal_resume_v3.py；目录版本号表示新的运行记录，不表示新增模型方法或传输实现。

已有四图分数和 geography 的 413 条终态保留。拒答与两条基础设施失败继续留在分母。新运行只处理未完成路径；若再次达到三次基础设施失败，按原规则停止并检查，不自动无限重开。

远端副本不是预算权威，没有向服务器复制模型私有配置或 SSH 私钥。当前账本继续保存在 D 盘原路径，不发生跨端预算切换。备用本地 IPC 方案若以后启用，仍须完成执行权交接、实际输出同步和独立操作准入。

## 证据

- 实际验收：D:/ch7/reports/native_local_cutover_preflight_v2_20261025/remote_actual_v2/result.json。
- 模型检查：D:/ch7/experiments/public_benchmark_v1/endpoint_recovery_probe_20261025_1320/result.json。
- 新操作准入：D:/ch7/reports/T1_ORIGINAL_JOURNAL_HEALTH_RESUME_ADMISSION_20261025.json。

这次恢复不等于整张 T1 完成。具名方法、其他公开候选及 OSDB-STKG 独立任务仍按既有协议和资源要求推进。
