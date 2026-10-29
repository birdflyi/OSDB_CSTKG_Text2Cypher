"""Publish corrected preparation documents and pinned 45-cell material packs.

Only local files are read/written. No model/DB/training client is imported.
Previous editable documents are copied into the report before replacement;
historical results, freezes, approval files and budget records are not edited.
"""

import argparse
import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
import subprocess


ROOT = Path(__file__).resolve().parents[2]
DISCUSSION = Path("D:/ch7/discussion")
REPORTS = Path("D:/ch7/reports/t1_preparation_20261029")
STATE = Path("D:/ch7/state/T1_PREPARATION_MATRIX_20261029.json")


METHOD_GAPS = {
    "cypher-ri": "候选HF权重未获作者身份认证；缺tokenizer/chat模板/完整prompt/训练ID；批量128与1024未决。已实现离线schema选择/回退/同rollout追加原语，不当官方复现。",
    "cyphersmith": "未取得认证代码/数据/目标模型/训练检查点；过滤文字与公式不同；2488对2348题目ID未齐。两种过滤规则已显式实现，未替作者选定；暂存reported独立轨道，不永久删格。",
    "cyquark": "作者生成器已固定；论文训练并评估parser，不可因生成器定位删格。代码许可、图专用语料/训练脚本/检查点/种子/解码协议未齐。",
    "correct-detail": "2026-10-27路线§3已定义并有原生组件：必要身份/payload及完整集合运算。尚缺逐基准生产接线、范围验收和独立成本；共享Ours计划。",
    "simple-correct": "2026-10-27路线§3已定义：有严格互斥证明才加计数，否则完整身份集合。已有组件；现与Ours等价如实披露，尚缺逐基准生产接线。",
    "native-set-count": "已有同计划原生集合计数组件及OSDB108/108历史证据；仅适用类回放，不能删除整格或缩主方法分母；尚缺逐基准接线。",
}

DATA_GAPS = {
    "CypherBench": "七图/2348任务/官方评分历史材料保留；本轮已修路由与结果入口，尚无新运行准入。原9392子池绑定Ling，不能借给DS。",
    "Text2GQL": "原生六图/3826位置材料已备；3815已知参考及11项STRING/INT64异常保留。图生命周期/主评分协议/反馈与控制生产接线未齐。",
    "MindTheQuery": "11图离线store/schema和10113原问题公共输入包已备；认证、只读不可变图、主评分与完整运行接线未验收。",
    "ZOGRASCOPE": "Pole5与plain/gold_linked输入包已备；IID768/COMP1349/LEN1253分列，论文划分计数差异保留；认证/原生主评分和运行接线未验收。",
    "OSDB-STKG": "四项目事件图有历史证据，不是四种DBMS部署；新Reference投影135951行/64289658字节不能替代事件图。独立正式任务/结构留出/参考未定。",
}

CODE = {
    "CypherBench": ["engine/adapters/cypherbench.py", "engine/adapters/cypherbench_runtime.py", "engine/table_results.py"],
    "Text2GQL": ["operations/analysis/text2gql_receipt_consumer_dev_20261029.py", "operations/analysis/text2gql_response_boundary_dev_20261027.py", "operations/analysis/text2gql_reference_store_eval_20261027.py", "operations/analysis/text2gql_score_contract_dev_20261027.py"],
    "MindTheQuery": ["operations/analysis/property_graph_input_contract_dev_20261029.py", "operations/analysis/neo4j_reply_consumer_dev_20261029.py", "operations/analysis/neo4j_graph_preparation_dev_20261029.py", "operations/analysis/neo4j_reference_boundary_dev_20261029.py"],
    "ZOGRASCOPE": ["operations/analysis/property_graph_input_contract_dev_20261029.py", "operations/analysis/neo4j_reply_consumer_dev_20261029.py", "operations/analysis/neo4j_graph_preparation_dev_20261029.py", "operations/analysis/neo4j_reference_boundary_dev_20261029.py"],
    "OSDB-STKG": ["operations/analysis/osdb_common_input_v3.py", "operations/analysis/osdb_owned_graph_factory_dev.py", "operations/analysis/osdb_native_set_count_control_dev.py", "operations/analysis/osdb_independent_path_cost_dev.py"],
}


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def pin(relative):
    path = ROOT / relative
    return dict(file=relative, sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def prepare_contracts(previous):
    contracts = deepcopy(previous)
    contracts["stage"] = "PREPARATION_CONTRACTS_V14_NOT_RUNTIME_ADMISSION"
    for cell in contracts["cells"]:
        method, benchmark = cell["method"], cell["benchmark"]
        cell["execution_enabled"] = False
        cell["production_adapter_complete"] = benchmark == "CypherBench"
        cell["formal_ready"] = False
        cell["benchmark_contract"]["gap"] = DATA_GAPS[benchmark]
        cell["method_gap"] = METHOD_GAPS.get(method, "已有推理组件；保留同底模/同权限，尚需逐基准完整执行、评分、反馈与成本接线。")
        paths = [*CODE[benchmark], "engine/attempt_costs.py"]
        if method in ("cypher-ri", "cyphersmith", "cyquark"):
            paths.append("engine/author_protocols.py")
        if method in ("correct-detail", "simple-correct", "native-set-count"):
            paths.append("operations/analysis/neo4j_native_plan_dev_20261029.py")
            cell["shared_planner"] = "ours"
            cell["independent_model_method"] = False
        cell["prepared_code"] = [pin(name) for name in dict.fromkeys(paths)]
        cell["comparison_eligibility"] = dict(status="retain", symbol=None,
            incompatibility_evidence=None, omission_approved=False)
    identities = {(c["method"], c["benchmark"]) for c in contracts["cells"]}
    if len(identities) != 45 or len(contracts["cells"]) != 45:
        raise ValueError("The complete 9 by 5 matrix must be retained")
    return contracts


CORRECTED_METHODS = """# 方法准备结论（2026-10-29核查修正版）

本版替代此前“缺定义、可直接部署、可删除作者比较”的结论。当前仅准备45格，不启动实验。

| 方法 | 查实结论 | 剩余条件 |
|---|---|---|
| Direct / Ours | 有CypherBench实现及其他基准离线组件 | 逐基准完整执行/主评分/身份与独立准入；Ours独立算法增量尚未成立 |
| Direct+feedback | 已有一次非gold操作反馈；共享Direct首轮 | 跨适配器接线、全尝试成本、最终答案视图；不重复首轮计费 |
| correct-detail | 规则已定义：必要身份/payload、完整集合运算 | 逐基准回放接线及独立成本；不是新模型方法 |
| simple-correct | 规则已定义：有严格互斥证明才加计数，否则完整身份集合 | 与当前Ours等价须披露；逐基准接线 |
| native-set-count | 有同计划原生组件、旧OSDB108/108证据 | 逐类范围和原生表达；不支持类不能删除整格 |
| Cypher-RI | 会议论文、候选HF元数据及索引已得 | 候选未获作者身份认证；缺权重身份/tokenizer/模板/训练ID；批量128与1024冲突 |
| CypherSmith | 论文及训练/过滤材料已得 | 未取得认证代码/数据/目标模型/检查点；过滤文字与公式不同；2488对2348任务ID未齐 |
| CYQUARK | 作者生成器固定提交已得；论文训练并评估parser | 代码许可、图专用语料、训练脚本/检查点/种子/解码设置未齐 |

控制依据：T1_ROUTE_REVISION_AND_DISCRIMINATING_CONTROLS_V1_20261027.md §2–4。方法最终答案先封存，再跑同计划控制。原生控制不适用类单列，不缩主方法全量分母。完整数据库同权限轨道保留Direct原生能力。

## 可用于论文的限定表述

作者训练方法与本文同底模推理方法分轨报告。本次未取得的认证资源保持缺项；不把有界检索写成“作者确定未公开”，也不使用同名非作者项目替代。reported与measured分列，未经任务/划分/评分核对不直接排名。

CYQUARK论文§5.2/附录A明确评估经合成数据微调的Qwen parser，因此不能仅因仓库提供生成器就删除5格。CypherSmith暂列reported可以，但不是永久排除。RI候选Anonymous234/Cypher-RI不能称官方8B复现。

本轮离线实现RI标签选择/无效选择回退/同rollout追加schema原语；Smith两种冲突过滤规则必须显式选定，不默认采用任一规则。这些代码不是完整作者复现或训练授权。

来源：[RI会议论文](https://papers.nips.cc/paper_files/paper/2025/hash/906727703322b2e33b37c855fc15e8b7-Abstract-Conference.html)、[Smith论文](https://aclanthology.org/2026.acl-long.1601/)、[CYQUARK论文](https://arxiv.org/html/2606.14325v1)、[作者仓库](https://github.com/interact-erc/Text-To-Cypher-knowledge-graph-data-generation)。固定作者源码013eab46e71aa1b5cfbd3ed91a44c030b17931ea；论文许可不代替代码许可。

全部45格保留。灰色标记按T1_COMPARABILITY_LEGEND_20261029.md执行：—（已找到证据）仅用于指定任务/协议的不兼容；—（未找到证据）仍待核，不能省格。资源缺口不等于科学不适用；本轮没有新整格排除。
"""

CORRECTED_DECISIONS = """# 决策核查与纠正（2026-10-29）

本版替代此前评审包。当前仅准备45格，不运行实验。材料、离线代码、真实运行验收分开记。

| 决策 | 判定 | 修正要求 |
|---|---|---|
| D1 OSDB只用聚合Reference投影 | 需修改 | 工作稿§3.1/4.1/5.3含事件任务；保留事件图，Reference投影另列。新投影135951行/64289658字节，仍有event_id/time，但缺507739条EventAction。不是全部事件字段被折叠，也不是四种DBMS部署。 |
| D2 Smith只用reported | 有条件 | 可暂分轨引用；写“本次未取得认证资源”，不能永久排除必要同条件比较。 |
| D3 删除CYQUARK | 不成立 | 论文训练并评估parser；保留5格和训练资产缺口。 |
| D4 控制等定义 | 不成立 | 2026-10-27路线§3–4已有规则与组件；需接线，不应重问定义。 |
| D5 RI公开即可部署 | 需修改 | 候选尚未获作者身份认证；缺tokenizer/模板/控制器完整协议。 |
| D6 全生成后统一消费 | 有条件 | 独立首轮可先备料连续送料；操作反馈依赖执行错误。本轮入口已改为先逐格生成、后逐格消费，不改签准入；仅假传输检查，未在线部署。 |
| D7 同族端点互换 | 不成立 | family仅描述。别名必须显式登记并引用审查后的等价证据；每次保存逻辑model、发送model、实际字节与原始返回。记录served_model不等于证明权重相同。 |
| D8 保留不确定 | 成立 | 准备表可保留；最终表明确缺项/阻塞，不把不确定当完成。 |

| 有效性项 | 要求 |
|---|---|
| E1 底模 | 同底模推理对照与作者训练模型分轨；不同修订不互相代答 |
| E2 分母 | 冻结题ID/划分；失败/拒答/评分未知保留，不按成功题缩分母 |
| E3 评分 | 官方主评分与辅助比较分开；辅助通过不当主EX |
| E4 来源 | reported/measured分列，未经任务划分和评分对齐不排名 |
| E5 端点 | 权重/模板/推理设置等价证据；请求字节可追溯 |
| E6 控制 | 共享固定Ours计划，方法答案先保存；独立控制成本与共享规划账单去重 |

旧203项测试未覆盖路由字节错误、别名额度泄漏、评分分母或冒烟误绑正式配置。本轮专门修复并新增回归；测试数不代替真实端到端验收。

旧DS全量配置误用绑定Ling的9392子池。本阶段从表格解除运行绑定，保留原配置历史文件，不更改批准记录、不自行为DS签准入。当前不因端点空闲制造请求。
"""

CORRECTED_AUDIT = """# 作者方法源码核查（2026-10-29修正版）

本版替代之前将候选权重当官方、将数据生成方法整体排除的结论。证据包：author_resources_v13_catalog.json、author_reproduction_recipes_v13.json、author_cells_v13.json，均位于D:/ch7/reports/t1_preparation_20261029。

| 方法 | 固定材料 | 认证边界和未完成项 |
|---|---|---|
| Cypher-RI | NeurIPS正式PDF；HF候选34ca521350312184fd9f8833c4dd519163aff5a0元数据/权重索引 | 候选15.23GB BF16、约7.616B参数，不是作者身份桥；README仅许可说明，tokenizer/chat模板缺。未下载大权重、未部署；正文训练批量128与附录1024保留冲突 |
| CypherSmith | ACL正式PDF；生成Qwen2.5-32B-Instruct；518730训练样本与训练参数摘录 | 目标模型未明确；作者代码/语料/检查点本次未取得。文字剔除full<=max(reduced)，印刷式full<min(reduced)，不替作者选定；2488与本项目2348划分未对齐 |
| CYQUARK | 作者GitHub固定013eab46e71aa1b5cfbd3ed91a44c030b17931ea，35作者blob；论文和模板 | 作者实现是生成组件，论文还训练Qwen3.5-0.8B/4B parser；不能因此排除方法比较。固定树未见代码LICENSE，不以论文CC许可代替。图专用语料/训练脚本/权重/种子/解码待补 |

同名PyPI cyphersmith和haizhi-tech/CypherSmith Rust fuzzer不是所核ACL方法，不能作作者复现。OpenReview403、checklist404记录为未取得，不当成功附件。

CYQUARK论文图范围MTQ5/CB7/Text2GQL6/ZOG1，不是缩本项目MTQ11人口的许可。论文的默认生成配置不是“未训练模型”；图专用微调不可省略。

来源和HTTP/版本/SHA/许可详情见V13原始目录，原文件不改。需要认证作者资源，不能用反复检索或自拟prompt填补。资源缺口不等于任务不兼容；全部15作者格保留。
"""


def apply_corrections(output):
    output.mkdir(parents=True, exist_ok=False)
    before = output / "before"
    before.mkdir()
    documents = {
        "METHOD_CLOSURE_CONCLUSIONS_20261029.md": CORRECTED_METHODS,
        "METHOD_GAPS_AND_DECISIONS_20261029.md": CORRECTED_METHODS,
        "DECISION_REVIEW_PACK_20261029.md": CORRECTED_DECISIONS,
        "AUTHOR_METHODS_SOURCE_AUDIT_20261029.md": CORRECTED_AUDIT,
    }
    for name, text in documents.items():
        target = DISCUSSION / name
        shutil.copy2(target, before / name)
        target.write_text(text, encoding="utf-8")

    feasibility = DISCUSSION / "TABLE1_FEASIBILITY_V2_20261029.md"
    shutil.copy2(feasibility, before / feasibility.name)
    text = feasibility.read_text(encoding="utf-8")
    replacements = {
        "四库（Redis/DuckDB/IoTDB/Elasticsearch）部署与导入未完成": DATA_GAPS["OSDB-STKG"],
        "回放器未实现（零模型调用，约 0.5 天）": "规则和组件已存在；逐基准接线、适用范围和独立成本未齐",
        "复判 20 个\"不确定\"格：能跑→延后，不能跑→不可应用": "复判20个不确定格：资源缺口保留；明确任务不兼容证据才能建议不适用",
        "修 P1 统一入口准入，让 CypherBench 3 格可正式运行": "修入口/路由/评分，保留独立运行准入，不自行签发",
        "实现回放器（correct-detail / simple-correct，零模型调用）": "接已有同计划回放组件，完整保存控制成本",
        "冒烟：可运行格各抽 20 条，只测请求服务": "继续离线准备；本阶段不启动任何冒烟",
        "**当前能进冒烟的只有 3 格（60 条请求）**，其余要等适配器或复判。": "**当前没有格获本准备阶段运行授权**；3格是历史组件就绪，不是60条新调用批准。",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    feasibility.write_text(text, encoding="utf-8")

    shutil.copy2(STATE, before / STATE.name)
    matrix = load(STATE)
    matrix["preparation_revision"] = "v14-corrections"
    matrix["stage"] = "PREPARATION_ONLY"
    for dataset in matrix["datasets"]:
        dataset["gaps"] = DATA_GAPS[dataset["id"]]
    for cell in matrix["cells"]:
        cell["dataset_gap"] = DATA_GAPS[cell["benchmark"]]
        cell["experiment_enabled"] = False
        if cell["method"] in METHOD_GAPS:
            cell["method_gap"] = METHOD_GAPS[cell["method"]]
        elif cell["benchmark"] == "CypherBench":
            cell["method_gap"] = "入口/路由/评分错误已离线修复；完整正式底模、预算与代码准入尚未授权。"
    save(STATE, matrix)

    table_path = ROOT / "engine/configs/table1.json"
    shutil.copy2(table_path, before / "table1.json")
    table = load(table_path)
    for cell in table["cells"]:
        if cell.get("execution"):
            cell["historical_execution_binding"] = cell.pop("execution")
        cell["status"] = "blocked"
        benchmark = next(d for d in matrix["datasets"] if d["id"].lower() == cell["benchmark"]
                         or (d["id"] == "OSDB-STKG" and cell["benchmark"] == "osdb-stkg"))
        cell["unblock"] = dict(missing=[DATA_GAPS[benchmark["id"]],
            METHOD_GAPS.get(cell["method"], "准备阶段不派发；后续需冻结输入、同底模、主评分与独立预算准入。")])
    from engine.table import validate
    validate(table)
    table_path.write_text(json.dumps(table,ensure_ascii=False,indent=1)+"\n",encoding="utf-8")

    contracts = prepare_contracts(load(REPORTS / "execution_contracts_v13.json"))
    save(output / "execution_contracts_v14.json", contracts)
    cells = output / "cells"
    cells.mkdir()
    for cell in contracts["cells"]:
        save(cells / (cell["method"] + "__" + cell["benchmark"] + ".json"), cell)
    changed = [DISCUSSION / name for name in documents] + [feasibility, STATE, table_path]
    receipt = dict(phase="PREPARATION_ONLY", cells=45, model_calls=0, database_calls=0,
                   training_calls=0, scientific_cells_omitted=0,
                   files=[dict(path=str(p), sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in changed],
                   original_documents=str(before), artifact_and_author_evidence="author_resources_v13_catalog.json",
                   all_preparation_complete=False, full_production_adapters_remaining=4)
    save(output / "delivery.json", receipt)
    return receipt


def finalize_bundle(output):
    """Pin changed source and reuse V13 author assets; never include runtime keys."""
    output.mkdir(parents=True,exist_ok=True)
    (output/'cells').mkdir(exist_ok=True)
    contracts = prepare_contracts(load(REPORTS / 'execution_contracts_v13.json'))
    save(output/'execution_contracts_v14.json',contracts)
    for cell in contracts['cells']:
        save(output/'cells'/(cell['method']+'__'+cell['benchmark']+'.json'),cell)
    analysis_names = [
        'audited_model_reply_dev_20261029.py','neo4j_native_answer_contract_dev_20261029.py',
        'neo4j_native_selection_dev_20261029.py','neo4j_native_plan_dev_20261029.py',
        'neo4j_read_capture_dev_20261029.py','neo4j_reply_consumer_dev_20261029.py',
        'neo4j_reference_boundary_dev_20261029.py','property_graph_input_contract_dev_20261029.py',
        'text2gql_receipt_consumer_dev_20261029.py','text2gql_response_boundary_dev_20261027.py',
        'accept_neo4j_native_plan_20261029.py','accept_neo4j_read_capture_20261029.py',
        'accept_neo4j_reply_consumer_20261029.py','test_audited_text2gql_consumer_20261029.py',
        'prepare_corrected_matrix_v14_20261029.py',
    ]
    sources = {path.relative_to(ROOT).as_posix():path for path in (ROOT/'engine').rglob('*')
               if path.is_file() and path.suffix in ('.py','.json','.md')}
    sources.update({path.relative_to(ROOT).as_posix():path
                    for path in (ROOT/'experiment-harness/public_benchmark_v1').glob('*.py')})
    sources.update({'operations/analysis/'+name:ROOT/'operations/analysis'/name for name in analysis_names})
    for cell in contracts['cells']:
        for item in cell['prepared_code']:
            sources[item['file']] = ROOT/item['file']
    include_local_dependencies(sources)
    sources['preparation/execution_contracts_v14.json'] = output/'execution_contracts_v14.json'
    for name in ('author_resources_v13_catalog.json','author_reproduction_recipes_v13.json'):
        sources['preparation/'+name] = REPORTS/name
    for path in (output/'cells').glob('*.json'):
        sources['preparation/cells/'+path.name] = path
    manifest = {name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in sorted(sources.items())}
    save(output/'bundle_manifest.json',manifest)
    sources['bundle_manifest.json'] = output/'bundle_manifest.json'
    archive = output/'source_v14.tar.gz'
    with tarfile.open(archive,'w:gz') as stream:
        for name,path in sorted(sources.items()):
            stream.add(path,arcname=name,recursive=False)
    receipt = dict(kind='OFFLINE_SOURCE_AND_PREPARATION_NOT_RUNTIME_DEPLOYMENT',
        source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        files=len(manifest),cells=45,archive=str(archive),archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
        author_resources_reused='author_resources_v13_catalog.json',model_calls=0,DB_calls=0,training_calls=0,
        execution_enabled=False,source_manifest_sha256=hashlib.sha256((output/'bundle_manifest.json').read_bytes()).hexdigest())
    save(output/'bundle_receipt.json',receipt)
    return receipt


def include_local_dependencies(sources):
    """Include static local imports without importing or executing their code.

    Runtime engine and legacy harness files are already included as complete
    directories. This closes the older standalone analysis modules' imports;
    dynamic runtime configuration remains an explicit separate prerequisite.
    """
    pending = [path for path in sources.values() if path.suffix == '.py']
    visited = set()
    while pending:
        path = pending.pop()
        if path in visited:
            continue
        visited.add(path)
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        modules = []
        for node in ast.walk(tree):
            if isinstance(node,ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node,ast.ImportFrom) and node.level == 0 and node.module:
                modules.append(node.module)
        for module in modules:
            relative = Path(*module.split('.')).with_suffix('.py')
            for directory in (ROOT,ROOT/'operations/analysis',ROOT/'experiment-harness/public_benchmark_v1'):
                candidate = directory/relative
                if candidate.is_file():
                    name = candidate.relative_to(ROOT).as_posix()
                    if name not in sources:
                        sources[name] = candidate
                        pending.append(candidate)
                    break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--finalize",action='store_true')
    args = parser.parse_args()
    result = finalize_bundle(args.output) if args.finalize else apply_corrections(args.output)
    print(json.dumps(result, ensure_ascii=False, indent=2))
