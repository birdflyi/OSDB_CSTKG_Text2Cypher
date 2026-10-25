# T1连接故障：本机只读日志核验

2026-10-25。核验窗口为北京时间01:30至07:10；本机时区为China Standard Time（UTC+8）。仅读取现有事件，不修改电源／网络配置，不发起实验端点探测、模型调用或数据库查询。

| 日志 | 查询条件 | 匹配事件 |
|---|---|---:|
| System／Kernel-Power | 42、107、506、507 | 0 |
| System／Power-Troubleshooter | 1 | 0 |
| NetworkProfile／Operational | 10000、10001 | 0 |
| WLAN-AutoConfig／Operational | 8000—8003 | 0 |
| Application／RasClient | 20225—20227 | 0 |

这些日志均为启用状态。System记录覆盖至本次检查，所选电源事件没有匹配。NetworkProfile最新记录在10月22日，WLAN最新记录在10月24日；不能凭这两份静默日志证明故障窗口内所有网络路径正常。本机最后启动时间为10月15日，这也不是未发生休眠的证明。

结论：目前没有所选事件将故障对应到本机休眠／唤醒或网络配置切换。该负检索不排除丢包、网关策略、没有记录在这些provider中的VPN／过滤器或服务端入口问题；不能据此宣布根因已定位或连接已恢复。本次SSH堆栈显示失败发生在TCP连接、认证和发送任务内容之前，因此不能用中文任务内容的编码解释这一类超时。

仅保存事件provider、ID、时间与日志范围；未导出事件原文、SSID、MAC或凭据。

证据：D:/ch7/reports/t1_windows_power_network_events_20261025.json；D:/ch7/reports/t1_windows_event_coverage_20261025.json。连接资源问题仍待说明，原批次不自动重启。
