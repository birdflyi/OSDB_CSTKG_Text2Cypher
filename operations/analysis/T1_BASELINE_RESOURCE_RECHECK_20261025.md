# T1具名方法资源：阻塞期间的有界复核

2026-10-25。此次只核对公开主来源，不运行作者代码、下载权重或调用模型；不把未找到链接解释为资源全球不存在。

| 对象 | 本次主来源核对 | 实验状态 |
|---|---|---|
| CypherSmith | ACL2026文章页确认作者为Zeyu Zhang、Kexuan Sun等；同名PyPI作者Aswin K V，不能视为该论文实现 | 专用权重／原推理模板／同题输出仍未验收 |
| Cypher-RI | 匿名HF库仍列15.2GB、4权重分片、提交34ca521；名称和文件本身不能建立与论文作者身份的关联 | 作者资源身份及schema-selection交互协议仍待证据 |
| CYQUARK | 论文v1明确链接作者data-generation库；当前HTML没有检索到huggingface链接 | 生成库公开不等于已取得对应微调权重或逐题预测 |

CypherSmith checklist PDF经网页工具未能读取；该读取失败不作为未公开资源的证据。第三方GGUF镜像和同名软件未替代作者资源，未请求或占用GPU，未从主表删除方法。

主来源：
- [CypherSmith ACL文章页](https://aclanthology.org/2026.acl-long.1601/)
- [同名PyPI项目作者信息](https://pypi.org/project/cyphersmith/)
- [Anonymous234/Cypher-RI文件页](https://huggingface.co/Anonymous234/Cypher-RI/tree/main)
- [CYQUARK论文v1及作者仓库链接](https://arxiv.org/html/2606.14325v1)

结论：本轮没有新增可以填进主表的复现数字。保留资源／协议缺项；不将reported数值填成实测，不将同名模型直接部署后称作作者复现。该离线核验不消耗12000调用额度。
