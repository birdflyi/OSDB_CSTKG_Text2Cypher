# M01-B01 Direct × CypherBench 适配交付

## 范围

本目录只覆盖 **direct × CypherBench** 一格。不发送 HTTP、不连接 Neo4j、不写调度器；请求的额度领取与实际发送由中央运行时负责。

## 文件

| 文件 | 作用 |
|---|---|
| `adapter.py` | `prepare` / `parse` / `consume` 三个接口 |
| `CONTRACT.md` | 共同合同（签名、字段、状态语义、验收命令） |
| `tests/test_adapter.py` | 7 项离线检查，假运行时，无网络 |
| `source_manifest.json` | 交付文件的 SHA256 |
| `paper_to_code.json` | 论文步骤 → 代码位置 |
| `deviations.json` | 与论文/合同的差异与理由 |
| `delivery.json` | 交付摘要、验收结果、未完成项 |

## 接口

```python
from adapter import prepare, parse, consume

specs = prepare(positions_path)                 # 20 个 RequestSpec，已校验 SHA
pred = parse(reply, http_status=200)            # answered / refused / format_error / unknown
result = consume(specs, {pid: pred}, runtime)   # 分母恒为 20
```

`runtime` 需提供 `save_answer`、`graph_lease`、`db_client`、`scorer` 四个方法；顺序固定为 **保存答案 → 执行 → 评分**。

## 验收

```powershell
python -m pytest tests -q
```

当前结果：**7 passed**，无模型调用、无数据库连接。

## 边界

- 不改 `engine/` 下任何文件；共享改动只以建议 patch 形式提出。
- 不自建 HTTP 循环、探活或重试。
- 20 题是适配抽检，不是总体准确率；`category` 为 `null` 时不得从 gold 反推。
