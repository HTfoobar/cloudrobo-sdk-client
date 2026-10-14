# CloudRobo 数据回流工具

跨平台后台监控客户目录，将完整批次直接上传 OBS，每秒向本地 Zenoh router
发布 `r2c_sdk.DataUploadProgress`。依赖 `hw-cloudrobo-core` 和
`hw-cloudrobo-r2c[client]`，复用现有配置、OBS SDK、Zenoh transport 和 Protobuf。
本工具不负责 router 后续转发及云端接收确认。

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

参见 [示例](examples/ingestion.yaml)。源目录、工作目录、OBS URI、`task_type` 和
`collect_method` 必填，两个分类必须为长度 1～64 的字符串（按 Unicode 字符计数）。
相对目录按配置文件位置解释；Windows 推荐使用 `C:/data/source` 或单引号路径。
OBS 使用现有 `cloudrobo config`、`HUAWEI_CLOUD_AK`、`HUAWEI_CLOUD_SK`、
`CLOUDROBO_OBS_ENDPOINT` 等配置，不将密钥写入回流数据库。

Zenoh 默认 client 模式连接 `tcp/127.0.0.1:7447`，source=`r2c/local`，
topic=`r2c/local/data_ingestion/upload_status`。支持 R2C ClientConfig 的 TLS、
connect 和 publisher 选项；身份不允许包含 `/` 或通配符。默认 BACKGROUND/DROP，
因此发布调用完成不表示 router 或云端已经接收，拥塞期间也可能丢样本。

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

工作目录包含 `batches.sqlite3`、`staging/`、`checkpoints/` 和轮转日志。
数据库只保存批次记录，不保存逐文件恢复进度；首次入队时间和批次 UUID 持久化。
文件 UUIDv5 从批次 UUID 和相对路径派生，重试/重启稳定。

成功或失败都清理数据副本。复制/上传错误按 5～300 秒退避重试，每次重新复制
并整批重传，可能覆盖之前上传成功的对象；OBS 上可能存在未完成批次的部分文件。
拒绝类错误本次运行不重复尝试，修正后下次启动重新检查。成功批次不会重传。
工作目录绑定源目录和 OBS 目标，改变任一项请使用新工作目录。

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
