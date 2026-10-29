# 作者方法复现代码交付与验收规范

本文件用于编写缺失代码及离线验收，不授权训练、调用模型、连接数据库或跑测试集。目标是可复用的方法实现，不是只针对某个题库的脚本。代码找不到时保持“未取得作者实现”；不能声称作者没有发布，也不能拿同名的其他项目代替。

## 1. 三项工作包

| 优先级 | 方法 | 已有材料 | 需要交付的代码 |
|---|---|---|---|
| 1 | Cypher-RI | NeurIPS正文/附录/checklist；本项目schema选择/回退/追加的离线原语；待认证的HF候选 | 完整schema选择控制器、同一assistant rollout追加与继续生成、tokenizer/模板绑定、GRPO训练脚本及loss mask、预处理和推理接口 |
| 2 | CYQUARK | 作者生成器完整仓库，固定提交`013eab46e71aa1b5cfbd3ed91a44c030b17931ea`，共35文件；生成/过滤提示；论文训练配方 | 复用已有生成器；补训练数据到parser微调/推理的脚本、图schema适配器和输出记录。不能把生成器输出当训练后parser预测 |
| 3 | CypherSmith | ACL正文/附录/checklist；两种冲突过滤规则的显式离线实现 | 生成/语法与schema过滤/似然过滤/训练/推理模块；目标模型和规则仍未决，必须可配置并标明来源 |

RI、Smith尚未取得可确认的完整作者实现。CYQUARK已有代码，不应重写其生成器。作者提供材料后优先对照和替换独立重实现，保留差异记录。

## 2. 对接边界

每项交付一个小而清楚的模块，具备以下功能；函数名可以不同，但README应给出对应关系：

1. `prepare`：读取公共schema、任务/训练输入和显式配方，输出可审计的中间文件；离线模式不得构造网络/数据库客户端。
2. `next_request`或等价状态接口：返回需要的模型请求及后续状态，由本工程的动态池、发送器和账本执行。不得自带绕过全局额度的HTTP循环、重试或探活。
3. `parse_response`：保留原始输出，分别解析最终Cypher、schema选择、拒答、截断和格式错误。不能将reasoning文本或编译成功当最终答案。
4. `train`：显式入口，默认不启动。保存底模revision、tokenizer、模板、训练样本ID/摘要、完整参数与checkpoint身份。代码交付阶段只做假模型/小人工张量检查。
5. `predict`：显式入口，使用固定检查点和同基准公共输入。输出可交给对应原生引擎的Cypher；不得读评价参考或给预测注入正确答案。

训练型作者方法保留自己的模型身份；不能用当前DeepSeek生成器代替作者训练系统，再填成作者方法成绩。工程不同底模的请求不能互相代答。增加模型路由、训练资源或正式预算另行处理。

## 3. 数据集范围

| 方法 | 原论文的评估基准 | 本项目的新增适配 |
|---|---|---|
| RI | CypherBench、Neo4j-Text2Cypher；训练来自CB四个训练图 | Text2GQL、MTQ、ZOGRASCOPE、OSDB均为新增；Neo4j-Text2Cypher不是Text2GQL |
| Smith | CypherBench、Text2Cypher（Ozsoy等） | Text2GQL、MTQ、ZOGRASCOPE、OSDB均为新增 |
| CYQUARK | MTQ5图、CB7图、Text2GQL6图、ZOGRASCOPE1图 | OSDB及MTQ额外图为新增；不能用论文五图替代项目11图准备范围 |

对新基准要求我方实现schema、格式、训练输入和原生评分适配，不要求作者提供其未做过的训练中间结果。公开数据的`test`目录不能作为训练来源。使用目标图构造无标注合成训练数据时，必须披露图暴露与测试NL/gold隔离，不能称未见该图。

## 4. RI必须对照的细节

来源：[论文](https://papers.nips.cc/paper_files/paper/2025/file/906727703322b2e33b37c855fc15e8b7-Paper-Conference.pdf)，§2、§3.1–3.4及附录。

- 基模为论文指定的Qwen2.5-Coder-7B。精确revision与作者checkpoint身份未知，必须保留为未决，不能自行确认匿名HF候选。
- 在`</json>`处停止schema请求；按选定标签取schema；非法选择回退完整schema；追加`<schema>…</schema>`后继续同一assistant rollout，最终从`<answer>`解析Cypher。不得悄悄换成“再起一个user轮次”的近似实现。
- Chat接口未必支持该续写语义。请实现本地token序列/注入式后端接口并用假后端证明前缀和mask；不能假设现有API支持assistant prefill。
- GRPO：8 rollouts、采样温度1.0、学习率1e-6、clip0.2、初始KL0、BF16；取回schema token不参加生成loss。奖励的格式/schema/执行三项逐项映射到论文公式，代码中给出章节或公式编号。
- 论文正文训练batch128与附录1024冲突。分别配置，不默认认定某一个是作者实际值。训练筛选8295条及四图/ID需记录实际来源；不能按测试集胜率选择batch或样本。
- 交付假模型检查：合法/非法选择、未知标签、空选择、标签方向、token级续写、schema loss mask、EOS/截断、一次物理请求和一次逻辑rollout的区别。

## 5. Smith必须对照的细节

来源：[ACL论文](https://aclanthology.org/2026.acl-long.1601/)，§3、§4及附录B。

- 数据生成模型为Qwen2.5-32B-Instruct；论文生成温度0.6/0.7/0.8/0.9。输入为SynSQL-2.5M转换所得ontology，生成`instruction/ontology/question/reasoning/Cypher`，不能把生成器与目标微调模型混为一谈。
- 目标底模尚未明确取得。入口必须要求显式配置；若采用公开替代底模，标为我方复现设置，不声称还原原作者底模。
- 过滤模块分别支持去掉ontology、question、reasoning的上下文。第三项不是relation。记录一致token化下完整Cypher条件对数似然；不把不同长文本的总概率任意混比。
- 正文要求完整上下文胜过所有删减上下文，对应拒绝`p_full <= max(p_reduced)`；显示公式为`p_full < min(p_reduced)`。两者都可实现和单测，但未确认时不能替作者选定一个。不得在测试集上选有利版本。
- 保留样本S、被删减上下文训练变体、U\S筛选、两阶段中间ID与排除原因，不只输出最后518730这一论文计数。
- 论文配方：全参微调2 epochs、学习率2e-5、AdamW、5% warmup、cosine至峰值10%、BF16/ZeRO-3、每卡batch32、累积2、context4000。资源变化时可配置并披露，不将建议值写成作者实际值。
- 本项目CB2348与论文2488测试题的差异未解。保留各自ID与评分协议，不通过补/删140题来凑数。
- 交付人工数据/假模型检查：三种删减上下文、空/重复字段、语法/schema不一致、两种过滤分歧样例、数据划分交叉、训练数据来源、截断与缺失参数。

## 6. CYQUARK扩展交付

来源：[论文](https://arxiv.org/html/2606.14325v1)，§5.1–5.2、附录A和提示附录；[作者仓库](https://github.com/interact-erc/Text-To-Cypher-knowledge-graph-data-generation)。

- 沿用作者的图采样、proto-NL、改写和过滤流程；已有schema人工注释表不是可执行生成器schema。不得给ZOG每个label复制全部全局property，或凭名称猜identifier/name/聚合语义。
- 论文parser为Qwen3.5-0.8B/4B，改写/过滤为Qwen3.5-9B；学习率5e-6、weight decay0.01、1 epoch、batch16、5次run。实际种子和精确revision未知，必须显式配置并登记来源。
- 补齐dataset格式、训练/推理脚本、checkpoint与每图schema身份。没有训练后parser时，不能用未微调模型冒充CYQUARK。
- 仓库固定版本未附明确代码LICENSE。保留来源与许可缺项，不将论文CC许可套到代码上；代码复用与发布范围待确认。

## 7. 最低交付清单与验收

```text
README.md                       # 环境、入口、论文步骤到代码的映射
requirements.lock              # 精确依赖；不包含token、口令或机器私有配置
method/                         # 小模块；无import时网络、训练或数据库副作用
configs/paper-recipe.yaml        # 原文事实、作者建议、我方选择分别标明
configs/benchmark-adaptations/   # 5基准输入/schema/训练与推理差异
tests/                          # 人工样例、假模型、假数据库；准备阶段不跑真实任务
evidence/paper-to-code.json      # 每一步的论文位置、代码位置、实现状态
evidence/deviations.json         # 未决参数、缺资产、替代模型与实现差异
evidence/source_manifest.json    # 源码版本、上游commit、SHA与许可来源
```

验收将检查：导入无副作用；公共输入与参考隔离；同一底模身份明确；训练/测试无混入；参数来源可追溯；完整分母；错误不伪装为空答案；物理尝试经共同池/账本；原始输出先保存；训练型方法实际训练要求未被省略；同计划控制与作者模型比较分轨。

“代码完成”“离线测试通过”“训练资产齐备”“可以在线验收”“正式结果完成”分别登记。收到代码后先做上述离线审查；本阶段不会直接启动训练或模型/DB实验。
