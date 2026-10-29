# 并发实验引擎

目标很简单：任务共享一个动态模型额度池；等待不占工作线程；模型返回后立即归还额度，数据库执行使用另一组并发限制。

```text
任务 / 一次操作反馈
        |
        v
同模型等待队列 -> 全局额度 + 端点额度 -> 连接复用的 HTTP 请求
                                             |
                                  保存原始响应，归还模型额度
                                             |
                                  独立数据库队列 / 每请求 session
                                             |
                                  实际答案 / 评分委托 / 一次反馈
```

没有消息中间件或后台服务依赖。调度使用 `asyncio`；阻塞HTTP与数据库分别用有界线程池。核心运行时只用Python标准库；真实Neo4j worker启动时才需要现有Neo4j驱动。开发测试使用现有pytest。支持项目固定的Python3.9。

## 模块边界

| 文件 | 负责什么 |
|---|---|
| `pool.py` | 模型分组、两层额度、排队、热更新、端点隔离 |
| `scheduler.py` | 滚动调度、释放模型额度、独立DB队列、排空、一次反馈 |
| `transport.py` | 连接复用、确切发送字节、期限、原始响应和分阶段计时 |
| `ledger.py`、`journal.py` | 入批一次扫描旧账、O(1)新预留、单一所有权、崩溃证据 |
| `execution.py`、`workers/neo4j.py` | 单持久通道、请求ID分发、独立session、原只读执行合同 |
| `adapters/cypherbench.py` | 冻结消息格式、操作反馈、原解释器与评分委托 |
| `campaign.py`、`cli.py` | 生命周期、报告、只读核验和明确的在线准入 |
| `pipeline.py` | 结果块串行接续、共享动态池和HTTP连接、失败/未派发闸门 |
| `adapters/cypherbench_runtime.py`、`results.py` | 自有七图生命周期、真实答案、控制、主表与配对族区间 |
| `monitor.py` | 中文Rich看板；只读派生状态，不调用模型或分词 |
| `replay.py` | 只读历史输入、DB响应及评分回放；不调用模型或数据库 |

旧32个冻结文件不改。新模块不会自动导入旧runner、恢复旧队列或复写旧输出。

## 动态额度池

编辑端点注册文件。默认每10秒重读；解析失败保留上一版本并记录问题。

- 新端点按 `(name, url, model)` 加入。任务只能领取与自身冻结model相同的端点。
- `family`只作描述，不参与等价路由。不同服务名称若确为同一模型，需显式`serves`和已审查证据的`alias_evidence_sha256`。该摘要只是准入引用，不会自动验证权重。逻辑model与实际发送model分别保存，释放额度仍扣回原逻辑model。
- 停用或删除端点，只停止后续派发，在途继续完成。
- 降容后，如果已有在途超过新值，等待其自然降到上限以下。
- 同模型按等待顺序领取额度；没有某模型的端点，不会卡住其他模型的队列。
- 端点连续基础设施失败达到阈值，只隔离该端点。429/带Retry-After的503单列限流。
- 所有端点下线时等待，超过准入期限才退出未派发任务。不发送假负载保活。
- 全局上限是本次campaign的已批准护栏。新增端点不能绕过它；当前示例为128。

注册表不是物理GPU拓扑。多个URL/名称是否落在同一后端无法仅凭响应证明，不把容量相加解释为独立硬件。

## 表格准备与分阶段运行

当前`table1.json`的45格全部为准备状态；旧DS全量配置误绑Ling预算的运行绑定已移至历史字段。准备清单不授予运行或训练权限。

后续获准运行时，每格必须分别绑定`smoke`、`batch`或`full`配置。绑定内的`stage`必须与命令一致；默认冒烟不会使用全量配置。入口先校验所有绑定，再逐格保存独立首轮回复，随后逐格消费这些回复并评分。生成阶段不启动DB，消费阶段复用封存回复，操作反馈仍只来自实际执行错误。每格保留原准入文件，不合并或自行改签。

`pipeline.live_pipeline(..., phase="generate")`保存`generation_receipt.json`；`phase="consume", resume=True`核对同配置及交接摘要后继续。任意中途崩溃仍需显式恢复审计；这个两阶段接口不宣称修复所有崩溃情形。独立cell的初始请求仍依次进入池，不能把端点上限当持续实际并发。

`table_results.py`核对题ID、图、方法、模型与视图，拒绝冲突重复；Direct用initial，Direct+feedback和Ours用final。评分未知保留，只有全人口且全部评分终结才写measured。`attempt_costs.py`按原物理来源去重，保留失败、未知用量和未知费用。新Text2GQL消费入口采用共同的请求/终结/计费槽核对，方法答案保存后才产出未发送的反馈草稿。

## 账本与重试

新账本目录与旧账本、旧结果分离；审批绑定唯一新账本目录，防止换目录重置预算。一次读取旧槽并校验三种历史格式、路径映射和特殊探活终结名。旧槽从不写入。

新槽按物理尝试计费，包含前一槽SHA、请求SHA、审批SHA、逻辑ID、物理ID和子池。总数始终包含历史已计费量；多个子池共用同一账本权威对象，不互借。预留之后不删槽。重复ID同body只返回已有槽，`claim`阻止重复发送；同ID不同body报错。

逻辑ID不包含运行批次。物理ID增加稳定campaign名称命名空间，避免不同模型/新正式块从`a1`开始时撞上历史ID；这修正了手册中跨campaign身份不明确的问题。

传输在调用可能写字节之前就记`write_started`。部分写入失败仍是未知；不能因`request()`未正常返回就认定服务器没收到请求。HTTP500的响应已知，但模型生成结果可未知，两者分别保留。默认未知请求不自动重发；显式批准的恢复版本可以按下述有界规则重试。

默认新块只做一次物理尝试；如显式批准，可重试传输失败与HTTP5xx，包括已写出但无回复的请求。每次间隔至少10秒，同体跨恢复版本累计至多3次，所有物理尝试均计费。有效错答、长度截断、拒答与model不符不触发传输重试。历史“写出后也重试”的旧策略尚未被在线等价认证，CLI会拒绝把这种新运行冒充旧主表可比复跑。

本地端口/文件资源问题在模型请求前识别，不扣模型额度、不隔离端点。写出后的磁盘/进程异常保留不完整槽，核验后人工接续。系统TCP端口配置不会被引擎自动修改。

## 使用

从仓库根目录运行，固定Python：

```powershell
D:\ProgramData\Anaconda3\python.exe -m pytest engine/tests -q

# 只使用本机合成传输，不读取密钥、不访问任何模型/DB。
D:\ProgramData\Anaconda3\python.exe -m engine.cli bench-sched `
  --output D:\ch7\reports\engine-synthetic --items 512 --cap 128 --delay 0.1

# 仅在新建的临时合成账本中测量，不能传生产账本路径。
D:\ProgramData\Anaconda3\python.exe -m engine.cli bench-ledger `
  --output D:\ch7\reports\engine-synthetic

# 排空：停止接收新请求，继续落盘所有在途终结。
D:\ProgramData\Anaconda3\python.exe -m engine.cli drain <run_dir>

# 只读核验；resume-plan只生成计划，不发送请求。
D:\ProgramData\Anaconda3\python.exe -m engine.cli verify <new_ledger_dir>
D:\ProgramData\Anaconda3\python.exe -m engine.cli resume-plan `
  <new_ledger_dir> <sealed_manifest> <new_resume_plan.json>
```

`configs/`是禁用端点、未选正式model的示例。真实密钥从`key_dir/<key_ref>.txt`读取，不写入Git或请求证据。在线`run`需要单独active审批、精确代码/消息/model准入。单批run入口只允许明确批准的model-only预取；pipeline正式比较通过专用执行/评分adapter接线，不允许用生成文本代替EX。

## 验收边界

离线测试与合成峰值不能证明真实GPU容量。历史回放中，输入比较使用明确旧序列化器重建HTTP字节；旧请求文件自身的排版SHA另记，不能冒充网络抓包。

当前历史回放覆盖4696路径、4756物理请求、4750生成view。7个view缺少独立DB结果轨迹，完整L2仍未通过。原主表源树32/32冻结SHA匹配；TuGraph适配树有2个既存差异，不混为“原冻结损坏”。

2026-10-28已完成三路真实接口验收120/120、七个自有图完整属性/关系/方向检查7/7，以及真实RPC的Direct/编译计划/全明细三条读取接线。原模型结果与预算不覆盖。新模型复跑单列新版本，不声称补齐或替换缺失的历史轨迹。

## 串行结果块，块内并发

`python -m engine.cli pipeline <config>` 启动一个持久进程，结果块完成后直接进入下一块，无需定时回合或新的用户消息。每块必须是一个benchmark、一个method；多个model分别封印输入、排队和评分，不能互相代答。相同model的多个端点可以分流。

全流水线共享一个额度池、账本和keep-alive连接池；切图、切方法不主动销毁模型连接。数据库生命周期按七图顺序，每图一个自有worker，每个查询一个session，DB并发独立限制4。当前图内滚动派发；图切换和末尾排空仍有自然空档，不承诺始终占满128。

空闲连接在领取时检查本地socket可读/EOF状态，空闲超过2秒则退役并新建；这些检查不发送HTTP探活、不扣模型额度。初版跨图复用曾造成263条RemoteDisconnected未知，原记录和成本保留。显式恢复清单绑定旧账本及请求/终结SHA；已完成模型回复直接复用。2026-10-28人类另行批准记录失败后重试：未知传输失败可在新冻结策略下产生独立物理尝试，不覆盖旧文件。恢复队列起始16路，每8次重试若至少2次传输失败则减半，最低4路；空位滚动补发，不等整批结束。三次连续基础设施失败仍触发端点60秒隔离。反馈字节若与旧记录不同则停止审计。

当前生产配置：百灵128，DeepSeek108/105各16，全局128。DeepSeek通过显式chat_template_kwargs传thinking=true和effort50，两者top_p统一.95；这不证明与百灵high的能力等价。端点热增删或降容不会改变任务绑定的model。

块中断或存在未派发路径时停止，不缩分母。已有完成回执的块仅在显式`--resume`且计划不变时复用；有started而无completed的块必须先审计并另立恢复版本。

正式CypherBench适配器委托原32文件冻结解释器和官方纯比较器。Direct首轮/最终、Ours初轮/最终分别保存；有效错答不触发gold反馈，格式/执行错误至多一次反馈。最终答案先落盘，再回放共享计划全明细控制。简单正确策略与全明细共享结果，不是独立模型。全部2348题保留，完成后自动生成micro、七图macro、覆盖/失败及固定seed20261024的10000次分层配对族bootstrap。

`python -m engine.cli monitor <run_dir>` 显示中文看板：阶段、完成/计划、实际HTTP、许可占用、排队与HTTP峰值。每两秒读取派生计数，不调用模型、不分词。Text2GQL、MindTheQuery、ZOGRASCOPE、OSDB和具名作者方法的未完成合同在计划中标阻塞，不调用CypherBench适配器冒充跨benchmark支持。
