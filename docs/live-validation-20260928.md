# 跟屁虫首个生产入口回归（2026-09-28）

范围：新通用适配器仅接管跟屁虫 Partner AIC `1.2.156.3088.1.0001.00001.80NSQB.2JZQPQ.0C61`；隆耘 API、原农巡天眼适配器及 Registry 未重建。193 同一时刻仅运行一个跟屁虫身份：旧 `genpichong-partner` 已停止，新 `xiaoyi-acps-echo-partner` 运行。原容器和独立状态目录保留供受控回退。

- 代码提交：`71ab7fa731346b55915b2cf207506d49af08ab5b`；镜像 `xiaoyi-acps-adapter:0.1.0-71ab7fa`，镜像短 ID `226d2caa98f0`。193 离线构建、容器内 58 项测试及 `pip check` 通过。
- 215 共享 9007 证据服务仅新增已审批的隆耘 Leader＋跟屁虫 Partner 精确身份对；原隆耘＋农巡天眼身份对保留。变更前 overlay 备份位于 215 原目录，`mq-auth-server-blue` 健康，农巡天眼适配器未重建。
- 首次手工 Group `group-universal-echo-20260928T081047Z-6c70881e`：输入与 Partner Product 相等，任务最终完成且 Group 自动关闭；测试脚本使用非 `xiaoyi-` 前缀，无法从仅收录小亿会话的投影器读取，致使等待路径超时。该脚本错误不是 Partner 业务失败，但此测试不用于网页入口验收。
- 首次小亿网页 Group `group-xiaoyi-3499f028150e46e189ac0796bcb823b2`：网页任务成功、Leader dissolved，但旧版 Partner 在过快的解散时序下停在 `leaving`。9007 对从未观测到的专用连接返回 `unknown`，不能据此判定资源已清理。215 RabbitMQ 管理端只读核对仅剩长期 Inbox 连接且消费者为 1；Leader dissolved、退出响应已确认、无待发消息和未结任务。停用新适配器期间运行带新鲜证据的正式恢复 CLI，dry-run 与实际执行均通过，审计 operationId 为 `manual-recovery-3373d43f-9ed7-456e-b774-506dbbeefcb8`。此历史记录**不计为自动解散成功**。
- 修复：只有 9007 证据同时证明专用 Connection、Channel、Consumer、双方队列、Exchange、两级 ACL 和长期 Inbox 已精确存在，且 `connectionIdentityObserved=true`，才向 Leader 上报 `connected=true`。这避免短会话在平台记录连接归属前就被解散。
- 修复后真实网页请求：jobId `da944d74fa8547b8b9c6cd48e5c965eb`，sessionId `xiaoyi-2b540277e30f434b9ad9c3d6dedfe414`，groupId `group-xiaoyi-2b540277e30f434b9ad9c3d6dedfe414`。`test.echo` 返回文本与输入 `echo-join-barrier-20260928-71ab7fa` 完全相同；Partner 任务 `completed`、`unsettled=false`，网页 job `succeeded`、`cleanup_status=dissolved`。
- 本次 Group Partner `joined_at=2026-09-28T08:39:35.868797+00:00`，`closed_at=2026-09-28T08:39:50.048928+00:00`，终态 operationId `automatic-disband-df46edab-2ddf-4830-a8f3-39ec1e589191`；`connection_identity_observed=true`、`exit_confirmed=true`、`error=null`。9007 最终读回 evidenceId `515dc34e0d98428280de547aed908e6e`，专用连接/Channel/Consumer、双方临时队列、Exchange、两级 ACL 均为 `absent`，`evidenceComplete=true`；长期 Inbox `present`、消费者 1。
- 最终 193 容器健康且自动重启计数 0；`ready=true`、`inboxConnected=true`、`authorizationValid=true`、`activeGroups=0`、`pendingOutbox=0`。215 证据服务健康，原农巡天眼适配器运行中。

边界：这证明通用框架的首个 Echo Connector、真实 Group 调用和自动收尾。对其他公司的 HTTP/插件连接器、异步长任务、证书续期、授权撤销和高并发仍需逐能力接入与验收；不能因跟屁虫通过便宣称所有智能体已自动兼容。网站当前显示的业务结果仍有 `awaiting-completion` Product 状态，最终任务终态需看 job `succeeded` 与 Partner `completed`，后续可改进网站状态呈现。
