# Adapter 共同合同（CONTRACT v1，M01-B01 交付）

适用：所有 method × benchmark 格（A/B 组）。由 M01-B01（direct × CypherBench）先落地，主会话验收后其余格复用。
原则：**adapter 不碰网络、不碰数据库、不写调度器**。它只做三件事：准备请求、分类模型回复、在注入的运行时里消费。

---

## 1. 三个接口

```python
prepare(positions_path, *, limit=None) -> list[RequestSpec]
parse(reply: str, *, http_status=None, attempts=1, cost=None) -> Prediction
consume(positions, predictions, runtime) -> CellResult
```

### prepare

- 输入：V22 冻结题位文件（`adapter_positions_v22/cells/<method>__<benchmark>.positions.json`）。
- 逐题校验 `public_input_sha256`；不符抛 `IntegrityError`。
- 公共输入出现 `gold` / `reference` 键即抛 `IntegrityError`（防泄漏）。
- 输出 `RequestSpec`，**不发送**：

| 字段 | 含义 |
|---|---|
| `position_id` | 冻结题位 id（如 `B01-P01`） |
| `task_id` | 原始题号 |
| `graph` | 图名 |
| `category` | 冻结分类；未声明为 `None`，不得从 gold 反推 |
| `ordinal` / `full_flow_five` | 序号；是否 5 题子集 |
| `messages` | 公共输入的原样 user messages |
| `input_path` / `input_sha256` | 字节来源与摘要 |

### parse

只解析**模型预测**，不产生数据库答案；HTTP 200 不代表正确。

| 输入情形 | `status` |
|---|---|
| 非 2xx | `unknown` |
| 空串 | `format_error` |
| 含拒答措辞（"I can't" / "unable to" / "sorry" 等） | `refused` |
| 有代码围栏 → 取围栏内；有 Cypher 关键字 | `answered`，`cypher` 为去尾分号的语句 |
| 其余 | `format_error` |

`Prediction` 字段：`status`、`cypher`、`raw`、`http_status`、`attempts`、`cost`（未知为 `None`）。

### consume

`runtime` 由外层注入，四个方法：

```python
runtime.save_answer(position_id, cypher, raw)      # 先落方法答案
runtime.graph_lease(graph)                          # 图租约
runtime.db_client(lease, cypher) -> rows            # 执行；抛异常即失败
runtime.scorer(task_id, cypher, rows) -> float      # 评分
```

**顺序是合同的一部分**：`save_answer` → `db_client` → `scorer`。
数据库失败 → `run_error`，但答案已保存；不得把失败伪装成空集合。

`CellResult`：

| 字段 | 含义 |
|---|---|
| `positions` | 恒等于输入题位数（20），**分母不缩** |
| `answered` / `scored` | 执行成功且已评分的题数 |
| `refused` / `format_error` | 拒答 / 格式失败 |
| `run_error` | 执行失败（DB 异常等） |
| `not_dispatched` | 没有预测的题位 |
| `unknown` | 状态无法判定（如非 2xx） |
| `attempts` | 物理尝试总数（用于计费去重） |
| `cost` | 全部已知才求和；任一未知则为 `null` |
| `rows` | 逐题证据（position_id、status、cypher、error/score） |

---

## 2. 状态语义（不得混用）

| 状态 | 含义 | 允许的后续动作 |
|---|---|---|
| `not_dispatched` | 没发出去 | 可重派，保留题位 |
| `refused` | 模型拒答 | 保留，不计对 |
| `format_error` | 回复不可解析 | 保留，可重试但需新 attempt 记录 |
| `run_error` | 已发出、执行失败 | 保留，答案不丢 |
| `unknown` | 传输/状态未知 | 保留，**不得改写成失败** |
| `answered` | 执行成功 | 可评分 |

---

## 3. 验收命令

```powershell
cd D:\ch7\code\OSDB_CSTKG_Text2Cypher_engine_simplify\adapter_work\direct\cypherbench
python -m pytest tests -q
```

期望：`7 passed`，不联网、不连库、无副作用。

---

## 4. 给其他格的复用方式

1. 复制本目录结构，替换 `prepare` 的输入解析与 `parse` 的分类规则；
2. `consume` 一般不改，除非该基准需要特殊执行语义（如原生集合计数）；
3. 每个格必须交付 `deviations.json`，写明与本合同的差异及理由。
