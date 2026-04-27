# datalink-host

第一阶段开发骨架，包含：

- TCP 数据接收服务
- 控制连接服务骨架
- 相位展开与双路降采样处理链
- 基础 Windows/macOS 桌面 GUI
- 模拟发送端工具

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Windows 10 一键配置环境

PowerShell 下执行：

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup_windows_env.ps1
```

如果还想顺手放行联调所需端口：

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup_windows_env.ps1 -AddFirewallRules
```

## 启动 GUI

```bash
datalink-host-gui
```

## 一键本地调试

```bash
python -m datalink_host.debug_launcher
```

该入口会默认同时启动：

- GUI
- 本地运行时
- 模拟 TCP 发送端
- 假 DataLink 接收端

适合在 mac 上直接做联调和验收演示。

## 启动无界面运行时

```bash
datalink-host-runtime
```

默认行为：

- 数据链路默认工作在“主动连接设备”模式
- 默认尝试连接设备 `127.0.0.1:6340`
- 控制服务监听 `0.0.0.0:19001`
- 本地一键调试入口会自动切回监听模式，供模拟发送端接入

## 启动模拟发送端

```bash
datalink-host-sim-sender --host 127.0.0.1 --port 3677
```

## 启动假的 DataLink 接收端

```bash
datalink-host-sim-receiver --host 127.0.0.1 --port 16000
```

## 回放抓包文件

```bash
datalink-host-replay ./var/captures/session.dlhcap --host 127.0.0.1 --port 3677
```

## 当前状态

当前版本已实现：

- TCP 接收、控制连接、相位展开和双路降采样
- MiniSEED 本地写盘
- DataLink 远传客户端
- 基础 GUI、单通道分析页、PSD 和日志页
- 模拟发送端、假的 DataLink 接收端
- 原始 TCP 抓包与回放工具

## 控制连接示例

控制服务默认监听 `127.0.0.1:19001`，每条消息是一行 JSON。

当前 GUI 已支持直接配置以下协议项，便于现场联调：

- 数据接入模式（主动连接设备 / 监听设备连接）
- 本地监听地址 / 监听端口
- 设备 IP / 设备端口
- 数据端口
- 帧头值
- 帧头字节数
- 长度字段字节数
- 长度单位（字节/数值个数）
- 字节序（小端/大端）
- 通道排列（采样交织/按通道连续）

查询状态：

```bash
printf '%s\n' '{"type":"get_status"}' | nc 127.0.0.1 19001
```

更新双路降采样率：

```bash
printf '%s\n' '{"type":"set_config","payload":{"processing":{"data1_rate":100,"data2_rate":10}}}' | nc 127.0.0.1 19001
```

切换功能开关：

```bash
printf '%s\n' '{"type":"set_feature","payload":{"storage_enabled":true,"datalink_enabled":false}}' | nc 127.0.0.1 19001
```

更新存储配置：

```bash
printf '%s\n' '{"type":"set_config","payload":{"storage":{"enabled":true,"root":"E:\\data","file_duration_seconds":60,"network":"SC","station":"S0001","location":"10"}}}' | nc 127.0.0.1 19001
```

更新 DataLink 配置：

```bash
printf '%s\n' '{"type":"set_config","payload":{"datalink":{"enabled":true,"host":"10.2.12.61","port":16000,"ack_required":true,"send_data2":false}}}' | nc 127.0.0.1 19001
```

启用原始 TCP 抓包：

```bash
printf '%s\n' '{"type":"set_config","payload":{"capture":{"enabled":true,"path":"./var/captures/session.dlhcap"}}}' | nc 127.0.0.1 19001
```
