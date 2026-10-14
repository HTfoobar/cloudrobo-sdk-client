# CloudRobo 数据回流工具

跨平台后台监控客户目录，将完整批次直接上传 OBS，每秒向本地 Zenoh router
发布 `r2c_sdk.DataUploadProgress`。依赖 `hw-cloudrobo-core` 和
`hw-cloudrobo-r2c[client]`，复用现有配置、OBS SDK、Zenoh transport 和 Protobuf。
本工具不负责 router 后续转发及云端接收确认。

## 运行模式

工具使用顶层 `run_mode` 选择业务行为，`zenoh.mode` 只表示通信模式：

| run_mode | OBS 上传 | Zenoh 上报 | 配置示例 |
|---|---|---|---|
| upload_and_report（默认） | 真实上传 | 每秒真实进度 | [ingestion.yaml](examples/ingestion.yaml) |
| upload_only | 真实上传 | 未启用 | [upload-only.yaml](examples/upload-only.yaml) |
| mock_report | 不上传 | 每秒模拟进度 | [mock.yaml](examples/mock.yaml) |

旧配置未设置 `run_mode` 时保持默认行为。三个模式仍共享用户级单实例控制，
同一时刻仅运行一个。每个模式必须使用独立工作目录，不自动切换或搬迁目录。

```bash
cloudrobo ingestion start --config upload-only.yaml
cloudrobo ingestion start --config mock.yaml
# 切换模式需要先 stop，并指定该模式独立工作目录
cloudrobo ingestion start --config ingestion.yaml --run-mode upload_only --work-dir ./upload-work
```

## 安装和启动

需要 Python >=3.10。可安装完整 Client 或单独安装回流包：

```bash
pip install hw-cloudrobo-ingestion
# 可选的格式检查（不安装整个 LeRobot）
pip install 'hw-cloudrobo-ingestion[lerobot-validation]'

cloudrobo ingestion start --config ingestion.yaml
cloudrobo ingestion status
cloudrobo ingestion status --json
cloudrobo ingestion stop
```

开发环境安装后，需执行 R2C 自带的 Protobuf 编译命令生成新消息；正常 wheel
构建自动编译：

```bash
python -c 'from cloudrobo_r2c._build_commands import compile_protobuf_files; compile_protobuf_files()'
```

## 配置

默认模式下源目录、工作目录、OBS URI、`task_type` 和 `collect_method` 必填。
只上传模式允许省略两个分类字段，并忽略不使用的 Zenoh 配置；提供分类时仍校验。
Mock 模式不需要源目录或 OBS 凭证，工作目录及两个分类必填。分类必须为长度
1～64 的字符串（按 Unicode 字符计数）。
配置中的源/工作相对目录按原配置文件位置解释；CLI 路径按当前工作目录解释。
Windows 推荐使用 `C:/data/source` 或单引号路径。
OBS 使用现有 `cloudrobo config`、`HUAWEI_CLOUD_AK`、`HUAWEI_CLOUD_SK`、
`CLOUDROBO_OBS_ENDPOINT` 等配置，不将密钥写入回流数据库。

Zenoh 默认 client 模式连接 `tcp/127.0.0.1:7447`，source=`r2c/local`，
topic=`r2c/local/data_ingestion/upload_status`。支持 R2C ClientConfig 的 TLS、
connect 和 publisher 选项；身份不允许包含 `/` 或通配符。默认 BACKGROUND/DROP，
因此发布调用完成不表示 router 或云端已经接收，拥塞期间也可能丢样本。

### CLI 覆盖

`--config` 仍必需。优先级为 **显式 CLI 参数 > 配置文件 > 默认值**。
未传入的参数不覆盖 YAML；显式关闭和零值仍生效。后台使用合并后的配置，
原 YAML 不会改写。临时有效配置文件权限受限，启动成功或失败后删除。

| 配置 | CLI |
|---|---|
| 运行模式 | `--run-mode` |
| 路径 | `--source-dir`、`--work-dir`、`--obs-path` |
| 分类 | `--task-type`、`--collect-method` |
| 校验开关 | `--validate-lerobot` / `--no-validate-lerobot` |
| Zenoh 通信模式 | `--zenoh-mode` |
| 地址列表 | 可重复的 `--zenoh-endpoint` |
| 身份 | `--zenoh-project-id`、`--zenoh-device-id`、`--zenoh-client-id` |
| Mock 参数 | `--mock-file-count`、`--mock-file-size-bytes`、`--mock-upload-bps`、`--mock-batch-interval-seconds` |

CLI 地址整体替换 `zenoh.endpoints` 和实际的 `zenoh.connect.endpoints`，
保留 connect 超时等其他项。TLS、publisher 等高级设置只通过 YAML 配置。

## 客户目录约定

```text
source/
  batch-001/
    __UPLOAD_READY__       # 所有写入关闭后，最后创建的空文件
    meta/...
    data/...
    videos/...
```

每 2 秒递归发现就绪目录。标记创建后目录必须保持不变，批次不可嵌套、路径不可复用。
LeRobot V3 应在 `finalize()` 完成后于整个数据集根目录创建标记，不要仅标记 data
或 videos 子目录。无标记目录不会上传。

每批最多 5,000 个普通文件，不计标记；超限会拒绝整个批次。源目录只读，禁止
源目录与工作目录重叠、符号链接或 Windows junction。复制完成且可选校验通过后
才开始上传，OBS 保留相对源目录结构，标记不上传，本地删除不会删除 OBS 对象。

## 恢复和结果

真实上传模式的工作目录包含 `mode.json`、`batches.sqlite3`、`staging/`、
`checkpoints/` 和轮转日志。`mode.json` 绑定模式，不允许另一模式复用。
已有批次数据库且没有模式标记的目录按默认模式识别。
数据库只保存批次记录，不保存逐文件恢复进度；首次入队时间和批次 UUID 持久化。
文件 UUIDv5 从批次 UUID 和相对路径派生，重试/重启稳定。

成功或失败都清理数据副本。复制/上传错误按 5～300 秒退避重试，每次重新复制
并整批重传，可能覆盖之前上传成功的对象；OBS 上可能存在未完成批次的部分文件。
拒绝类错误本次运行不重复尝试，修正后下次启动重新检查。成功批次不会重传。
工作目录绑定源目录和 OBS 目标，改变任一项请使用新工作目录。

两个真实模式的记录完全独立，不共享成功历史、不跨模式补报。切回默认模式仅
恢复其自己的记录。两个模式使用同一源目录时，同一批次可能各上传一次，
后一次会覆盖同路径对象；若希望避免重复上传，可使用不同源目录。
只上传模式不初始化 Zenoh、不导入 Protobuf、不生成上报缓存，本地进度仍可查询。

`stop` 尽快停止当前子进程，超过 5 秒终止；未完成批次下次启动重新复制。
OBS SDK checkpoint 独立于副本，确认分段上传中止后删除；强制终止时可能有远端
残留分段，建议桶设置清理未完成 multipart 的生命周期策略作为兜底。
清理失败不丢记录，暂停该批次并继续其他任务。

每秒发送每批全量节点，不限制为 8 KB。状态变更即时发送，断连期间只保留最新
快照及待发布最终消息，不积压逐秒历史。超限等无法生成完整节点清单的错误通过
本地 status/log 展示；现有协议没有错误原因字段。

## 可选 LeRobot 校验

`validation.enabled: false`（默认）无需 PyArrow 校验依赖；开启时缺依赖会在启动
报错，不静默跳过。第一版针对标准 Parquet/MP4 的 `codebase_version: v3.0`，
以 LeRobot v0.4.3 的标准布局为依据，要求 `meta/tasks.parquet`。
检查 info/stats、Parquet schema/footer、episode 引用/范围、视频容器/视频流，
不自动修复、不逐帧解码、不评判训练质量；不支持 Lance 和内嵌 image 特征。
校验副本失败不上传、清理副本、记录拒绝原因。关闭校验可上传其他普通数据格式。

## Mock 模拟

Mock 不扫描源目录、不复制数据、不调用 OBS、不创建真实批次数据库或 checkpoint。
自动生成虚拟 MP4 文件，每轮包含新的批次 UUID、入队时间、模拟目标子目录及文件
UUID；顺序模拟 queued/uploading/done、进度、速率和 ETA，每秒发布全量消息。

默认 10 个文件、每个 10 MiB、速率 2 MiB/s，批次间隔 5 秒。文件数为整数
1～5,000，文件大小为 int64 范围内的正整数，速率为有限正数，间隔为有限非负数。
完成消息发布调用成功后才进入下一轮；router 不可用时只保留当前批次最新状态，
不会无限生成历史。模拟记录仅在内存中，重启从新批次开始。

使用真实本地 Zenoh 和同一 `r2c/local` 身份及 topic；协议没有 Mock 标志，
下游会按普通进度消息处理。CLI/status/log 明确显示 `mock_report`。
Mock 不支持 LeRobot 校验，配置开启时会报错；可用 `--no-validate-lerobot` 覆盖。

## 平台与验收

不依赖 Unix 信号完成生命周期控制，使用后台 subprocess、平台文件锁与认证的
loopback 控制通道。每个操作系统用户单实例，控制登记在 `~/.cloudrobo/ingestion`。
`CLOUDROBO_INGESTION_RUNTIME_DIR` 可指定独立登记位置（主要用于测试/受管理部署）；
不同登记目录会形成独立实例域。工作目录应由该用户独占，不要手动修改 SQLite。

跨平台 mock CI 与真实 OBS/router 联调分开。性能验收需在目标机器人及客户实际
数据上测量：单个 5,000 文件批次运行 30 分钟，序列化 p95 ≤200 ms、发布间隔
p95 ≤1.2 秒、OBS 吞吐相对关闭上报下降 ≤5%。通过单元测试不代表满足这些指标。

可复现的序列化微基准（输出机器信息、消息大小和 p95；不发送数据）：

```bash
python packages/cloudrobo-ingestion/benchmarks/serialize.py --files 5000 --iterations 100
```
