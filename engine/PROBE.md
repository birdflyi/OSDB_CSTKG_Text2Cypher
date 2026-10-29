# 连续请求流水线

`python -m engine.probe CONFIG.json`读取已准备的公共请求，用现有动态池、调度器、传输器和账本发送。它不加载基准adapter，不启动数据库，不产生论文得分。

## 调度规则

- 请求按manifest顺序进入一个队列，所有cell共用同一池。某个请求终结并释放许可后，下一个等待请求即可执行；不等待上一个cell的长尾全部结束。
- 全局、端点和逻辑模型上限仍各自生效，注册表可热更新。配置的全局上限是本次程序的容量天花板，不写死为32或128。
- `family`和`serves`是运行者授权的路由声明。配置读取与许可领取使用同一规则。可选证据SHA只用于记录，缺少它不取消已有授权。
- 实际发送前仅改写`model`字段。逻辑请求、实际请求、请求摘要、requested_model和实际model均保留。
- 使用原有有界重试与端点恢复规则；普通HTTP失败留档继续。预算、身份、持久化错误或人工DRAIN仍停止新派发并排空。没有无限重试、定时空请求或额外探活。

## 与正式结果分开

manifest每行设置`purpose: stability_probe`及非空`probe_run_id`。其逻辑键与原实验及其他probe运行分别命名，同一probe身份仍保持幂等。

必须使用用途为`stability_probe`的独立批准子池。配置中的`physical_attempt_cap`是整批物理尝试上限，覆盖所有cell及重试；底层累计和子池上限同时生效。新增程序不会修改批准文件或挪用旧正式余额。

900个样本位置不等于900个独立模型请求：Direct-feedback共享Direct首轮，三个控制共享Ours计划；作者方法可能有多段生成。应在方法实现和全部请求准备完后，再确定物理尝试预算。

## 必需配置

| 字段 | 含义 |
|---|---|
| name / purpose / probe_run_id | 独立运行名称、用途和身份 |
| models | 允许的逻辑模型列表 |
| manifest_path / manifest_sha256 | `cli.load_items`可读取的请求清单及摘要 |
| prepared_request_count | 清单实际请求数 |
| code_files | `campaign.code_manifest()`在冻结时的完整结果 |
| run_dir | 全新输出目录；已有目录先审计，不隐式覆盖/重发 |
| registry_path / key_dir | 动态端点配置和私有密钥目录；密钥不放入manifest或Git |
| global_cap | 本进程容量天花板；smoke建议8–12，具体以配置为准 |
| approval_path / budget_scope | 已批准用途子池；批准文件`scope_purposes[budget_scope]`须为`stability_probe` |
| physical_attempt_cap | 全批物理尝试上限，至少覆盖全部首轮请求 |
| ledger_dir / legacy_ledger | 当前及历史实际计费证据目录 |

可配置`retry`、`timeouts`、`endpoint_policy`、`batch_size`、`batch_interval_seconds`及已有路径保护/迁移字段。批次间隔为0时无人工停顿，完成即补位。

## 输出与检查

逐请求`request.json`、`logical_request.json`和`terminal.json`保留原始请求、响应、失败及usage；`journal.jsonl`记录事件。
`cell_progress.json`分别报告planned、finished、api_ok、unknown和failed。请求已终结不等于成功。
`capacity_samples.jsonl`记录实际HTTP和许可水位；`summary.json`包含时间分布、预算与未知信息。`probe_result.json`不含论文得分，明确`offline_flow_tested=false`。
所有最终进度写完后再保存封印，后台观察者先退出，避免封印后继续修改派生文件。

## 已验证与限制

离线故障检查证明：A格长尾仍占许可时，A格另一请求完成即可让B格开始；两个真实账本槽的上限跨cell、跨重试且重启后仍生效；未知回复不会报为通过。
这些是假传输检查，不是服务端容量或在线稳定性实测。任意崩溃后完整自动重建尚未实现；当前入口拒绝隐式恢复已有目录，保留待审计证据。
