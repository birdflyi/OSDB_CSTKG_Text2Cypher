# 本目录采用的合同：CONTRACT_V2

来源：`D:\ch7\reports\t1_preparation_20261029\delegation_bc_v23\CONTRACT_V2.md`
本文件只记录本格落地时的取舍，不复制全文。

## 接口

```python
prepare(positions_path) -> list[RequestSpec]
parse(reply, *, http_status, attempts, cost) -> Prediction
consume(positions, predictions, runtime) -> CellResult   # adapter_work/common/consumption.py
```

（RI 另有 `next_request(spec, reply, transport_unknown=False) -> NextStep`。）

## 运行时顺序（强制）

```
save_prediction → graph_lease → execute → save_final_answer
→ replay_controls → score_saved_answer
```

- 预测与最终答案分开保存，最终答案来自 `execute`，不是 Cypher 文本；
- `execute` 异常 → `run_error`，预测仍在，继续下一题；
- `score_saved_answer` 异常 → `scoring_error`、`value=null`，继续下一题；
- 分母恒为 20；方法状态与评分状态分列。

## 方法状态

`ready / refused / format_error / run_error / not_dispatched / unknown`
HTTP 200 不等于答对；非 2xx 记 `unknown`，不伪装成格式失败。

## 拒答判定

锚定句首的拒答措辞，且句中不含 Cypher 子句关键字；
`MATCH (n) WHERE n.note = 'sorry' RETURN n` 是查询，不是拒答。

## 本格差异

- 直接方法：`generation_stage=initial`，`protocol=chat`；
- 公共输入为 `messages` 时原样复用，不加指令；
- 参考集不进入 adapter，评分由 runtime 在 `SavedAnswerReceipt` 之后加载。

## 验收

```powershell
python -m pytest tests -q
```
