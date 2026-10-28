# 独立实验流水线

这是一条普通的 Python 流水线。启动后，领取许可、派发、重试、保存、切图和切换方法均由程序完成，不需要聊天代理逐批放行。聊天额度用尽不影响已启动的子进程。机器断电、进程被杀或真实服务下线仍会影响执行；程序不会伪装这些状态为正常运行。

## 执行顺序

1. 校验一次源码、输入、模型、预算、数据和执行准入；禁止启动第二个账本写入者。
2. 在打开数据库前，把全部已准入 method×benchmark 的首轮请求写入 `prepared_queue.jsonl`，包括原始请求体、逻辑身份和 SHA256。未准入的结果块写明缺项，不发送。
3. 对当前 method×benchmark，先生成和保存所有图的首轮模型回复，再依固定顺序执行数据库、必要的一次操作反馈、控制和评分。已有效返回的回复从完整历史账本复用，不重复生成。当前方法完成后自动进入下一方法。
4. 每个请求独立领取同模型端点许可。端点可动态加入、禁用或调整上限；禁止不同模型互相代答。

反馈只能由真实的非参考执行错误产生。因此，首轮请求可以全部提前准备，反馈请求不能凭空提前生成。它们在发送前写入 `feedback_queue/`，仍受同一总池、预算和重试规则约束。

## 当前候选派发规则

这些参数在新服务接入时固定进执行配置，不是对新服务容量的实测结论。

```json
{
  "global_cap": 128,
  "batch_size": 8,
  "batch_interval_seconds": 2,
  "prefetch_initial": true,
  "queue_wait_until_available": true,
  "continue_on_path_failures": true,
  "consecutive_failures": 32,
  "quarantine_seconds": 30,
  "retry": {
    "max_attempts": 3,
    "wait_seconds": 10,
    "before_write_only": false,
    "retry_unknown": true,
    "recovery_cap": 128,
    "recovery_min_cap": 128
  }
}
```

- 正常每2秒释放最多8条；上一批尚未返回也可继续派发。实际在途受全局和各端点上限共同约束。这里没有整批等待屏障。
- 最近8次终结中至少2次传输/资源失败时，单批8→4→2，间隔2→1→0.5秒。健康窗口反向恢复；不会永久降为单路。请求释放速率保持最多4条/秒，慢响应由实际在途上限约束。
- 失败请求延后至少10秒再进入许可队列；等待重试不占HTTP连接或模型许可，其他就绪任务继续。跨版本同体最多3次，未知及失败都计入批准额度；有效回复不重发。
- 连续基础设施失败触发端点短期隔离；429遵守服务端 Retry-After。没有健康检查、占位请求或隐藏SDK重试。已确认被回收的端点应禁用。
- 领取许可超时是排队事件，不是一次模型调用。启用持续排队后，程序继续等真实容量恢复，不再因这一点结束方法、消耗尝试号或需要聊天代理重新放行。
- 同一新执行配置把恢复许可上限设为全局上限，避免旧 `RecoveryGate` 的较小历史上限压住全部补发。小批节奏交给 `BatchPacer`，实际容量交给 `DynamicPool`。

## 失败与停止

普通传输失败按上述规则自动处理。穷尽尝试后保留失败；普通路径缺项写 `incomplete.json`，继续后续图或独立结果块，不能标成完整成功。

预算耗尽、手工DRAIN、冻结/归属不符、持久化失败、未终结已计费请求等属于需要明确处理的错误。程序排空已发送请求并留下问题记录，不无限重启、不删除失败、不擅自换模型。突然断电或被杀后的未知请求须先核验账本；`--resume`不会盲目重发一个中断块。

## 日志与结果

| 文件 | 内容 |
| --- | --- |
| `launch.json`、`stdout.log`、`stderr.log` | 独立进程PID、命令摘要、启动时间和异常输出 |
| `prepared_queue.jsonl`、各阶段`queue.jsonl` | 已冻结的完整首轮请求字节与逻辑身份；准备清单不等于实际派发数 |
| `journal.jsonl` | 派发意图、实际进入发送、重试预约、排队延后和终结事件 |
| `attempts/*/request.json`、`terminal.json` | 每次物理尝试的原始请求与HTTP响应、状态、时间和usage |
| `finals/*.json` | 每条逻辑路径结束后立即落盘的最终结果；不等整块结束才保存 |
| `pipeline.jsonl`、`issues.jsonl` | 阶段推进、缺项、停止原因及异常调用栈 |
| `capacity_samples.jsonl`、`capacity_timeseries.json`、`progress.json` | 每秒追加落盘的容量样本、终结汇总和最新状态；包含许可、实际HTTP在途、排队、隔离剩余时间和批次节奏，不冒称服务端解码并发 |
| `paths/`、`db_traces/`、评分与成本文件 | 最终答案、数据库证据、逐题评分和全部尝试成本；未知值不补0 |

`send_started`表示客户端进入发送动作，不保证服务端接收；是否写完、是否收到HTTP响应另有字段。原始终结文件和账本不可覆盖。模型成本按物理request_id去重；数据库重放和控制成本单列。

## 入口

仅准备，不打开服务、不领取调用额度：

```powershell
python -m engine.cli prepare-pipeline CONFIG.json --output NEW_PREPARATION_DIRECTORY
```

`prepared_plan.json`明确标注 `admitted_for_execution=false`。它不是执行授权；新API的模型、容量、配置和源码须与新冻结/准入一致。

接入新API并完成一次执行准入后，后台启动一次：

```powershell
python -m engine.cli launch-pipeline CONFIG.json --logs NEW_LOG_DIRECTORY
```

该命令返回后子进程继续。Windows隐藏窗口，其他系统独立会话；无需定时唤醒聊天来发送下一批。配置、准入和私有key引用都来自本机文件；日志不包含密钥。

查看与停发：

```powershell
python -m engine.cli monitor RUN_DIRECTORY
python -m engine.cli drain RUN_DIRECTORY
```

当前阶段只进行工程修改与离线验收。没有启动正式实验；等待用户提供新API后再接入。
