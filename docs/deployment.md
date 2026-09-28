# 独立部署与回退

建议193目录：`/home/lijian/apps/xiaoyi-acps-adapter`。代码/release、配置、授权、凭据和状态分别管理，不混入隆耘项目。

```text
xiaoyi-acps-adapter/
  releases/<commit>/   # 从Git构建，不含生产凭据
  instances/<aic>/
    config/           # runtime.json、已审批acs.json
    policy/           # 管理员批准grants.json及审批审计
    credentials/      # client.pem、client.key、ca.pem，仅本机受控挂载
    state/            # 持久化SQLite，备份包含WAL或使用SQLite backup
    deploy/           # compose及固定镜像版本
```

健康端口绑定193的127.0.0.1，公网不暴露适配器控制接口。协议流量通过到215的AMQPS出站连接完成。9007证据及授权HTTP连接启用TLS验证，不走9008。

## 构建

```bash
docker build -t xiaoyi-acps-adapter:<commit> .
docker run --rm --entrypoint python xiaoyi-acps-adapter:<commit> -c 'import xiaoyi_adapter; print(xiaoyi_adapter.__version__)'
```

依赖使用requirements.lock。记录Git提交号、镜像ID、构建日志及测试结果。禁止把宿主机.env、私钥或业务令牌打入镜像。

若服务器访问Docker Hub/PyPI不稳定，可从受信任环境导入官方Python镜像，核对镜像层摘要；为目标Python/操作系统准备完整wheelhouse（含setuptools==84.0.0、wheel及其依赖），然后使用`deploy/Dockerfile.offline`构建。wheelhouse不入Git。跨平台下载时须显式包含Linux所需cffi/pycparser，并在目标镜像内执行pip check和完整测试；不能只用Windows测试代替。

## 准备实例

1. 使用现有注册平台或官方CLI完成ACS审批、AIC、EAB和clientAuth证书。
2. 复制`examples/runtime.echo.json`作为配置模板，填写自己获得的AIC和部署路径；示例占位符不能运行。
3. 状态目录归容器UID/GID所有。凭据目录只允许管理员和运行身份读取。授权目录建议root拥有、运行身份组可读，容器只读挂载。
4. 由受限宿主机管理员批准调用授权。示例（在安装本包的Linux管理环境执行）：

```bash
python -m xiaoyi_adapter.policyctl approve \
  --file /secure/policy/grants.json \
  --partner '<Partner AIC>' --leader '<Leader AIC>' \
  --capability test.echo --actor '<实际审核人>' \
  --reason 'Group回显接入验收' --purpose test --hours 24
```

该CLI的权限边界是宿主机访问权限；不能将其无鉴权包装为公网接口。配置文件、审批审计仅管理员可写。后续Registry授权接口提供相同Policy结构，适配器用自身证书读取。

5. 启动前运行`xiaoyi-adapter check --config ...`，核对ACS、证书、AIC及Inbox一致性。
6. 明确9007证据服务已授权此身份对。先停旧同AIC实例、保留其状态目录，再启动新实例；不并行运行相同身份。

## 运维

- `/health/live`：服务进程存活。
- `/health/ready`：Inbox连接、有效授权及主循环状态；不等于每个业务系统或所有平台权限已验收。
- `xiaoyi-adapter inspect --config ...`：只读列出Group状态及审计标识，不打印业务正文或密钥。
- 跟踪证书有效期，提前续期。首版续期后需无活动Group窗口安全重建实例。
- 定期备份状态与授权审计，保留镜像摘要。数据库升级前做可恢复备份，本版schema_version=1。

## 回退

没有活动Group时可停止新实例，恢复原镜像、原配置、原状态目录并启动原实例。跟屁虫接管测试失败且仍有活动Group时，先保留现场并通过原Leader正常解散；不能直接覆盖数据库、伪造closed或并行启动第二个同AIC实例。

新项目镜像的构建和候选部署不要求重建隆耘API、Registry或天眼适配器。若需变更215的共享证据授权配置，应先安排独立变更及回退方案。

## 短会话遗漏连接观测后的正式恢复

新版入组在9007确认专用连接归属前不会向Leader报告connected=true。此前版本若在观察前即完成短会话，证据接口可能对连接/Channel返回`unknown`，不能自动标记closed。正式恢复仅用于这种已失败的历史Group，不能替代常规DISBAND。

先停用该实例并保留状态库；精确核对Leader已dissolved、协议退出Publisher Ack已记录、任务已终态、9007显示Group双方队列、Exchange和ACL均不存在、RabbitMQ当前仅一条该Partner连接且它正消费长期Inbox。将215管理端的只读连接归属核对和193 Leader终态核对写入带时间戳、三方AIC/Group标识的JSON证明，放入实例私有证据目录。不得仅凭`connected=false`或队列不存在推断连接已清理。

在停止实例的状态目录上运行`python -m xiaoyi_adapter.repair`，先用`--dry-run`核验，再带真实审核人和原因执行。该工具重新通过9007 mTLS读证据，检查证明不超过60秒、精确身份和Inbox连接归属、无未完成业务或待发消息；只为指定Group写入`formal-unobserved-connection`终态和持久审计。它不会修改平台MQ/ACL，也不会把该Group计为自动解散通过。若任何条件不足，保留leaving并升级人工排查。
