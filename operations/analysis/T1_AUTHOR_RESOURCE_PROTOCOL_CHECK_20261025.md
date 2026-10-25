# T1 作者资源与推理协议复核

时间：2026-10-25 14:15 CST。范围：公开来源核对。新增模型调用、训练和数据库查询均为 0。当前正式队列及冻结方法不变。

| 方法 | 本轮确认 | 还缺什么 | 如何进入主表 |
|---|---|---|---|
| CypherSmith | ACL 正文 PDF 可下载；18 页。附录 A 给任务指令和合成提示；附录 B 给部分训练参数 | 正文把目标模型写成 LLM（小于 10B），没有据此确定其模型 ID／checkpoint。数据生成器 Qwen2.5-32B-Instruct 不是该目标模型。此次正文链接检查未找到作者权重或仓库；清单链接本次返回 404 | 取得作者 checkpoint／数据与目标模型身份、推理配置后，验收同题执行；不能用通用 DeepSeek 换名复现 |
| Cypher-RI | NeurIPS 最终正文含 schema 交互协议：先生成选择 JSON，返回选中 schema，再续写查询。附录列出七图题数，与当前 2,348 题总数一致 | 正文和会议文章页本轮未给出可确认 Anonymous234 权重身份的链接；附录 checklist 的“已提供代码”声明仍需找到实际资源 | 先确认作者资源，再实现对应交互；单次完整 schema 普通提示不能标为该方法的完整复现 |
| CYQUARK | 作者论文明确公开的是数据生成库；论文中的专用模型由生成数据微调得到，并按底模与目标图报告 | 对应图／划分 checkpoint、训练数据版本或完整重训配置，及测试输入／推理输出 | 作者权重路线或明确标注的重新实现路线。现有通用模型接口不是其专用微调模型 |

## 对比较协议的影响

1. CypherSmith 正文第 5 页描述 CypherBench 为 8,534 TRAIN／2,488 TEST；当前固定官方版本为 8,295 TRAIN／2,348 TEST。该差异尚未解释。不能把正文 reported 数字直接放进我们的同条件排名，也不能推断其一定错用了数据。
2. CypherSmith 正文说微调包含相应 benchmark 的训练集。CYQUARK 也在目标图生成数据后微调。后续必须披露各方法训练暴露，不能将这些成绩写成与我们零新增训练相同的实验设置。
3. 保留全部具名候选行。不因资源未核清而删行；也不以名称相同代替身份验证。现有同底模核心比较继续，不等待外部资源。
4. 资源缺口首先是身份和协议，不只是显存。身份通过后才提出确切 GPU／作者推理接口需求。当前不新增训练授权，不自动消耗其他基准额度，不联系作者。
5. 实际资源仍未取得时，明确记录“未复现／资源待核验”。如拟按论文重做训练，另列数据、算力和验收需求，不能静默改名为作者复现。

## 证据

- [CypherSmith 官方文章页](https://aclanthology.org/2026.acl-long.1601/)，[正文](https://aclanthology.org/2026.acl-long.1601.pdf)。正文第 5–6、13–14 页已提取并渲染核对；全文链接与模型名检查已完成。PDF SHA-256：cea372c5da4fc9bd4eb700f325677b39b14cf4e0ac3b1e935aa51c78a067012f。
- [Cypher-RI 官方文章页](https://proceedings.neurips.cc/paper_files/paper/2025/hash/906727703322b2e33b37c855fc15e8b7-Abstract-Conference.html)，[正文](https://papers.nips.cc/paper_files/paper/2025/file/906727703322b2e33b37c855fc15e8b7-Paper-Conference.pdf)。PDF SHA-256：640aa139632eb300b7bdc27af52713f80c7379e14f5c72f711250970f51f3d1b。
- [CYQUARK 作者论文 v1](https://arxiv.org/html/2606.14325v1)，第 5.2 节、附录 A 及作者生成库链接。
- 原始 PDF、文本、链接清单和状态记录：D:/ch7/reports/author_resource_pdf_check_20261025/。清单 HTTP 404 和 OpenReview 网页验证阻塞只证明本次访问状态，不证明资源全球不存在。

本轮没有新的主表数字，也没有改变实验范围、分母、方法或测试任务。
