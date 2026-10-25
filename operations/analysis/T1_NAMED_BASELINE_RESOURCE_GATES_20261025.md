# T1 具名基线：作者身份与资源准入

2026-10-25 19:12 CST。当前同底模核心队列继续运行。本次只读取公开页面和文件，未执行作者代码、安装软件、下载模型权重、训练或发模型请求。

| 候选 | 本次可确认的证据 | 主表复现仍缺什么 |
|---|---|---|
| Cypher-RI | 作者 Yang Yang 的[论文列表](https://yangy.org/)对应论文条目链接 PDF；此次条目没有代码或模型链接。Anonymous234 同名仓库固定 revision 的 README 仅含许可证声明 | 作者与该权重的可追溯关联，以及与论文相符的 schema 选择／续写推理实现和配置 |
| CypherSmith，ACL 2026 | [ACL 官方页](https://aclanthology.org/2026.acl-long.1601/)列出 Zhang 等六位作者。PyPI 的[同名包](https://pypi.org/project/cyphersmith/)登记作者为 Aswin K V，主页指向另一套 Cypher 生成工具 | 论文对应的微调权重、目标模型身份、训练数据版本与推理配置。不能安装同名 PyPI 包后标成 ACL 方法复现 |
| CYQUARK | [作者论文页](https://francescoczz.github.io/publications.html)和[论文正文](https://arxiv.org/html/2606.14325v1)都指向[生成库](https://github.com/interact-erc/Text-To-Cypher-knowledge-graph-data-generation)；仓库身份链已确认 | 论文对应的图专用 checkpoint／输出，或完整的生成、筛选、微调和评测材料；生成库本身不等于已训练查询模型 |

CYQUARK 固定提交为 `013eab46e71aa1b5cfbd3ed91a44c030b17931ea`。完整非截断 Git 树有 35 个文件。本次检查 README、六份 Markdown 文档及 requirements；下载资产均与该树的 Git blob SHA-1 和本地 SHA-256 匹配。树中没有常见权重扩展名文件，也没有文件名包含 train、infer、finetun 或 eval 的文件。所查文本未给出权重下载链接。这些观察只描述该版本，不证明作者在其他位置没有发布资源，也不以文件名检查替代代码语义审计。

Cypher-RI 同名 Hugging Face 仓库的固定 `34ca521` README 共 31 字节，只有 Apache-2.0 声明，未提供作者或论文关联。OpenReview 网页本次进入验证页；公开 API 请求返回 HTTP 403。因此这条来源链仍未完成，不能将访问失败写成“作者没有代码”。第三方 GGUF 转换也不能补上作者身份链。

## 具体资源需求与后续处理

1. 如果我们已有作者配套资源，提供下载地址或本地目录即可。需要的是对应论文的代码／checkpoint／推理配置，或带方法版本和题目 ID 的作者原始输出。
2. 通用 DeepSeek 服务仍用于当前公平核心比较。它不能替代具名方法的专用微调模型。当前缺口先是模型与协议身份，尚不能据此指定一块 GPU 就宣称复现就绪。
3. 没有现成资源时，保留必要对照缺项，并明确比较作者输出、透明重实现或其他经核验近邻方法的路径。拟重训时先列出数据、训练算力、调用数量和验收条件，再申请资源；不静默更名或降低标准。
4. 外部方法的公开 reported 成绩仍单列。没有同题输出、训练暴露和评测版本对齐时，不把它们填成本项目实测，也不按当前 Ours 胜负选择复现对象。

证据目录：`D:/ch7/reports/t1_author_resource_identity_check_20261025_1910/`。其中保留公开 HTTP 状态、响应哈希、固定树和文档核验结果。当前 CypherBench 冻结、主分母、预算范围和运行队列均不变。
