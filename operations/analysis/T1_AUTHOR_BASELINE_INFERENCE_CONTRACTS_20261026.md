# T1 具名基线：正式来源、推理与资源合同

2026-10-26。本阶段完成来源及复现缺项核验，**没有完成三种作者方法的实验**。不改变 CypherBench 已冻结方法、2,348题分母、预测或评分，也不删除必要基线。

## 已取得的证据

固定清单28个来源请求全部HTTP200，原始字节合计7,384,605。包括三个正式网页、两篇正式PDF及CYQUARK作者生成库固定版本的全部23个非图片blob；后者均与既有完整35-blob树的Git对象SHA一致，12张图片未取。11个Python模块仅AST解析和静态阅读，未导入或运行。原论文、作者代码、全文提取、逐文件收据与渲染页仅存本地，不发布第三方全文。

三个HTML下载成功后，初版解析器将bytearray传入BeautifulSoup而报TypeError；原始capture_summary中的failed_sources指本地派生处理，**不是HTTP失败或源站无资源**。离线用原字节修复三份链接清单，未重发请求。独立核验文件明确HTTP失败0。后续打印非ASCII符号曾触发本机控制台编码异常，已写出的修复产物完整；公共脚本已修复这两个兼容问题，实际执行版本与protocol SHA一致的快照另存本地，不覆盖旧收据。

## 各方法的实际复现边界

| 方法与正式来源 | 已确定的合同 | 仍缺的可执行资源；T1状态 |
|---|---|---|
| [Cypher-RI，NeurIPS正式入口](https://proceedings.nips.cc/paper_files/paper/2025/hash/906727703322b2e33b37c855fc15e8b7-Abstract-Conference.html)；[正式PDF](https://papers.nips.cc/paper_files/paper/2025/file/906727703322b2e33b37c855fc15e8b7-Paper-Conference.pdf) | Qwen2.5-Coder-7B经RL；先简化schema，再生成JSON选择、补完整schema并继续生成。无效JSON／标签回退全schema。论文Table5七图数量相加2,348，与本项目各图计数一致 | Anonymous234等候选权重的作者关联、精确revision、实际推理控制器／分词器／解码和输出缺失；计数相同不证明逐题ID相同。作者复现未完成 |
| [CypherSmith，ACL正式入口](https://aclanthology.org/2026.acl-long.1601/)；[正式PDF](https://aclanthology.org/2026.acl-long.1601.pdf) | 数据生成与过滤后微调；目标解析器只记为小于10B的LLM。附录Qwen2.5-32B-Instruct是初始数据生成器，不是已确认的最终解析器。论文称CB测试2,488；另区分Text2Cypher的带列名EX-A与排序值EX | 最终底模身份、checkpoint、训练数据版本、逐题ID、推理与评分代码缺失。2,488与本项目2,348的140差额未定位；不能自动删140题或解释成笔误。作者复现未完成 |
| [CYQUARK，arXiv v1](https://arxiv.org/html/2606.14325v1)；[作者生成库固定版本](https://github.com/interact-erc/Text-To-Cypher-knowledge-graph-data-generation/tree/013eab46e71aa1b5cfbd3ed91a44c030b17931ea) | 目标图生成数据后微调Qwen3.5-0.8B／4B；9B用于改写／过滤。论文报告五seed均值，CB为七图AVG；该汇总不能直接与本项目按题micro EX排序 | 生成器身份已建立，完整解析器checkpoint、对应训练语料、seed输出／逐题评分和推理解码尚未取得。生成器并不是可直接回答自然语言的论文模型；作者复现未完成 |

Cypher-RI正式22页PDF与旧作者托管缓存逐字节完全相同，SHA256为`640aa139632eb300b7bdc27af52713f80c7379e14f5c72f711250970f51f3d1b`，解除这两个PDF版本是否一致的疑问；不能由此认证匿名HF权重。正文模板和Table5及Smith数据／评价／附录页已渲染目视复核。物理PDF页与浏览工具页码偏移分别登记，不能混用。

RI温度1.0标作rollout训练参数；Smith四种温度标作合成数据生成参数。没有证据把它们直接设为最终测试推理温度。论文训练硬件说明也不是本次必须申请同规模训练的理由；当前仅有推理授权，不开训练。

## 作者库和链接审计的范围

CYQUARK固定树35blob中，23个文本文件全部取得。`generate_batch.py`生成查询／proto-NL并写CSV；`filter.py`检查图模式，不能把文件名filter当作已具备论文9B语义过滤模型。`docs/Example.md`在生成完成后另要求LLM改写／过滤，两个template提供提示；README／论文互相链接建立的是生成库身份。

在这些固定文件中，没有取得checkpoint链接、解析器推理／训练入口或作者gold-vs-prediction评分器。字符串检索仅命中SupportedFeatures中一般性“evaluates”描述，经人工判定无关；该检索不能证明语义上的全球不存在。AST成功也不等于作者代码实际运行、图连接或正确性通过。

RI正式入口链接本次只有论文／BibTeX等出版入口；两PDF链接注释未含GitHub／HF／匿名模型链接。Smith页面的GitHub链接属于ACL网站自身仓库和构建版本，不能认成论文代码。CYQUARK页面其他GitHub链接含arXiv反馈与转换工具，均不混入作者资源。来源清单记录检索范围，不宣称全网无实现。候选OpenReview页面出现浏览验证挑战，本轮不绕过、不把无法读取当成作者未发布证据，也未反复访问旧robots受限会议页。

## 后续准入和成本

1. RI需作者关联的精确权重及schema检索／续写控制器；每段生成、回退和失败分别保留，再按实际物理调用计费。仅在DeepSeek上加选择schema提示只能另标“论文协议控制”，不能填完整RI行。
2. Smith需识别最终解析器与题目版本；数据生成器名称／通用小模型不能代填。Text2Cypher的EX-A／EX口径不移植成CB官方分数，也不因列名差异擅改已冻结评分。
3. CYQUARK需对应图／seed的模型与输出；记录目标图数据访问、合成／改写／过滤／训练暴露。训练目标图数据的专用模型应与同底模零训练核心分层披露。取得资源前不生成新训练数据、不请求训练GPU；五seed论文均值不冒一次本地推理或micro EX。
4. 用户已说明无现成作者文件；继续公开来源与补充入口核验，不重复索取同一批文件，不联系作者。若只能透明重实现，先形成具体方案、证据影响与资源要求，不能自动替代必要原方法或降低标准。

本阶段28来源计划／派发／取得28／28／28，原始下载失败0、未派发0；三份HTML离线解析故障已修复。三个作者方法正式N与适用矩阵仍待冻结；模型派发／完成／失败0，未派发N未定。真实账本前后5,194/12,000、余6,806、pending=[]；本阶段模型、DB、SSH及停用内网服务请求0。正式源码32文件及适配运行源码46文件SHA不变；T2–T5未启动，Text2GQL条件额度未触发。必要单元格继续保留未完成，整张T1未完成。

本地原始证据：`D:/ch7/reports/author_baseline_contracts_20261026/`。公开自有脚本在适配分支`operations/analysis/`；紧凑状态见[来源合同汇总](D:/ch7/reports/t1_author_baseline_contracts_20261026.json)。
