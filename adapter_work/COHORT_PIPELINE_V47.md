# 固定题集独立流水线

入口不需要会话持续参与。初轮统一连续生成；所有初轮完成后由独立命令消费；真实操作错误再进入至多一次 Direct 反馈。后处理不阻塞初轮请求。此版本不声明模型请求与 DB 全程重叠。

```powershell
python adapter_work/cohort_pipeline.py verify C:\absolute\pipeline.json
python adapter_work/cohort_pipeline.py generate C:\absolute\pipeline.json
python adapter_work/cohort_pipeline.py consume C:\absolute\pipeline-with-consumer.json
# 或一次启动两阶段：
python adapter_work/cohort_pipeline.py run C:\absolute\pipeline.json
```

`pipeline.json`：

```json
{
  "schema_version": "cohort_pipeline_v1",
  "output": "D:/new-run-directory",
  "physical_attempt_cap": 60000,
  "request_config": {"path": "D:/request.json", "sha256": "..."},
  "offline_config": {"path": "D:/offline.json", "sha256": "..."}
}
```

`generate` 可以没有 `offline_config`。后续追加该 pin 不修改已经封存的生成配置。若消费者代码版本不同，新增 `feedback_config` pin：它必须与原请求配置共享 inventory、bindings、budget_scope、ledger_dir 和 approval_path；只能使用新冻结 source 和当前资源策略，不能修改方法输入。运行前在对应不可变代码包内生成 source manifest。

请求配置沿用 `request_pipeline.run` 字段：`name, inventory, source, bindings, registry_path, key_dir, ledger_dir, approval_path, legacy_ledger, budget_scope, physical_attempt_cap, execute_authorized, run_dir`，以及可选 `retry, timeouts, protected_roots, recovery, drain_paths`。`run_dir` 固定为总输出目录下 `generation`。并发从动态 registry 取值，不写死 32。仅最多 global_cap 个任务同时进入调度器；长尾未终结继续占槽，任一任务终结马上补位，可以跨格子边界。未提交的队列不计入 admission timeout。

消费配置沿用 `offline_pipeline.run` 的 `inventory, source, backends, parallel, execute_authorized, output`。`output` 为总输出目录下 `consumption`。`source_finals=[]` 由程序注入真实生成凭据。新选择器固定为 `{"mode":"cohort","cohorts":[...与 inventory 完全相同的 pins...]}`，不能混入旧 `per_cell`。旧 20/5/1 题检查保留。恢复子集只在 `selection.selected_positions` 显式列出，不改变原分母。

每个后端配置新增 `scoring_contract` pin：文件含 `positions:[{source_position_id, task_id, graph, policy, reference_binding}]`。policy 必须绑定相同 cohort SHA。参考 binding 使用 `{path,sha256,line_1based,record_sha256}` 的裸参考 JSONL。先封存最终答案、验证凭据，再读取参考。CB/T2G 用冻结作者比较器；MTQ/ZOG 使用明确的我方指标及同事务参考；未知参考保留 R，不算错误。未终结保留 U；NA 单列；失败不会从分母消失。

跨旧预算复用必须显式给 `request_config.reuse_aliases` pin 和 `graph_snapshots[benchmark][graph]` SHA。别名列表每项：

```text
target_key, source_key, source_scope,
compatibility: {logical_model, graph, profile_sha256, public_input_sha256, graph_snapshot_sha256},
source_compatibility: 同上，
evidence: [所有旧 terminal.json 的 pin，以及来源上下文核验文件 pin]
```

程序核对所有旧计费尝试、请求字节、终结回复、输入、profile、模型及图快照。有效旧回复不重发，旧账本不改。初轮 aliases 不传给反馈。反馈与初轮先后持有同一个账本写锁，反馈最多使用初轮实际派发后剩余额度。

工程验证：100 请求/4 槽/长尾跨 cell 补位；六方法三题 18 位置完整假后端流程；Direct 操作错误自动反馈与旧失败保留；重复 task_id 不碰撞；别名上下文改变拒绝；失败/未知参考固定分母；原烟雾选择器不放宽。以上均为离线验证，不证明真实服务吞吐或数据库评分成功。

输出：`generation_receipt.json`、逐请求 request/terminal/final 与账本、`consumption/summary.json`、`feedback_queue.json`、可选反馈请求与消费目录、`completion.json`。已有目录不覆盖，硬失败保留所有证据；此 CLI 不自动猜测崩溃后未知请求是否执行，恢复需显式核对。
