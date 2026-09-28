# 0.1.0 候选版验证记录

验证日期：2026-09-28。结论：候选代码、镜像与隔离测试准备完成，尚未完成真实 ACPs Group 联调，不能作为生产验收通过证明。

## 版本及位置

- 仓库：https://github.com/lijian-888/xiaoyi-acps-adapter
- 分支：`codex/group-partner-runtime`
- 镜像对应源码提交：`e818663814065479a7d9735119dff2d3359f9472`
- 193 镜像：`xiaoyi-acps-adapter:0.1.0-e818663`
- 镜像 ID：`sha256:a353516c28f66f9da407962db172489b8d0a3118524b2771891ee57d2c68f1bf`
- 193 源码目录：`/home/lijian/apps/xiaoyi-acps-adapter/releases/e818663814065479a7d9735119dff2d3359f9472`
- 镜像运行用户：`10001:10001`；实际 Python：`3.14.7`。

构建使用已缓存的官方 Python 基础镜像，跨主机转移后核对 RootFS 层摘要一致。最后一次构建采用 `deploy/Dockerfile.offline` 和完整 wheelhouse，Docker 构建网络关闭。镜像不包含生产凭据。

## 实测

- Windows Python 3.12：53 项单元测试通过。
- 193 最终 Linux 镜像：53 项单元测试通过，退出码 0。
- Linux 测试容器使用 `--network none --read-only`，仅临时目录可写；没有连接真实 MQ 或业务服务。
- 最终 Linux 镜像 `pip check`：`No broken requirements found.`
- 测试覆盖邀请校验、任务幂等与状态隔离、连接器、动态授权、固定解散截止时间、条件删除、Publisher Ack、传输引用与完整证据校验。

测试使用隔离替身，不能据此宣称真实注册、证书、邀请、任务返回或自动解散已验收。

## 现场保留

未启动新适配器生产实例，未替换跟屁虫，未重建隆耘、天眼或平台组件。193 原跟屁虫健康接口实测 ready=true、inboxConnected=true、activeGroups=0。原小亿与 Registry 网站以及隆耘 API 仍在运行。

## 联调前置阻断

215 共享 MQ Auth 的 `EVIDENCE_ALLOWED_PAIRS` 当前只包含隆耘 Leader 与天眼 Partner。需要经批准保留原授权并新增精确身份对：

- Leader：`1.2.156.3088.1.0001.00001.ZJM2XM.50E81A.0XHW`
- Partner：`1.2.156.3088.1.0001.00001.80NSQB.2JZQPQ.0C61`（跟屁虫）

该变更属于共享平台配置，可能需要重建 `mq-auth-server-blue`；本轮未执行。还须验证本次新 Group 的参与记录及连接归属观察可用。不能因授权缺失跳过资源校验或伪造 closed。

获准变更后：先完成共享服务备份与授权复核，再在跟屁虫无活动 Group 时受控切换实例。禁止两个实例同时使用跟屁虫身份。随后执行真实 Inbox 邀请、回显任务与结果比对、正常 DISBAND、自动 closed、精确资源清理及长期 Inbox 保留核验。失败保留现场，按正式流程收尾后回退。

## 尚未交付的集成

- 注册网站中的动态调用授权编辑页与专用授权 API 尚未接线；当前提供受限宿主机审批工具和授权读取契约。
- 真实平台端到端验收、真实超时负向场景及重启恢复联测待执行。
- 自动证书续签、单成员退出但 Group 继续运行的资源验收，不在本候选版已验收范围。

详细限制见 README、业务接口契约和部署文档。
