# 设计与验收

## 部署关系

193运行独立适配器实例，每AIC一容器、一套本地证书、一份状态库。215继续提供已有Registry、CA、RabbitMQ、9007管理/证据服务及天眼专用适配器。隆耘Leader负责建组、邀请、任务确认和平台资源清理。

注册平台负责身份、ACS、审批、目录上下架；适配器负责协议执行。原小亿展示目录不变更，不自动把注册通过解释为业务调用授权。

## 关键一致性

- Inbox邀请验证协议、有效期、目标成员、Leader授权、精确Exchange及容量；邀请Token只保存摘要。
- Task绑定Leader、Group、Session、Task四元组，跨组/跨会话查询拒绝。消息ID冲突不能改写原任务。
- SQLite事务原子保存消息回执、任务状态和Outbox。发消息等待RabbitMQ Publisher Ack后才更新confirmed；Ack不表示对方处理完成。
- 业务执行先持久化dispatching。进程中断后通过请求ID查询，不盲目再次启动有副作用的业务。
- 180秒是默认能力期限，可按异步能力配置。结果不明确保留状态和名额。
- 文件授权最长到expires_at；适配器无权续期。管理员可撤销特定Leader。新业务执行受授权约束，查询、完成与必要清理可以继续。
- 暂时无法访问授权服务时只允许未过期缓存；明确HTTP拒绝或本地授权文件删除立即失效。

## 退出

1. 校验管理命令发送者和精确Group，持久化DISBAND ID、received_at、固定300秒deadline及退出响应ID。
2. 已接受任务先完成、取消确认或进入明确终态。有未确认业务不释放名额。
3. 持久化并发送connected=false，等待Publisher Ack；只证明协议退出响应被MQ确认。
4. 等消费回调完成ACK后取消本Group消费者，用独立通道被动查询精确Partner队列。
5. 只有消费者和消息数均为0才调用`delete(if_unused=True, if_empty=True)`，条件竞态不强删。
6. 关闭本Group专属连接。关闭超时不删除引用；仅本地transport已关闭且平台精确证据确认Connection/Channel/Consumer消失，才能协调移除。
7. 9007完整证据确认双方队列、Group Exchange、两级ACL均消失，Inbox保留且消费者为1，才写closed和终态审计。
8. 过期保留leaving/finalize_pending。重复消息、回调和重启不延长deadline；人工正式恢复需独立审计流程，首版不提供快捷强制关闭按钮。

## 真实联调前置条件

- 跟屁虫当前无活动Group，现有同AIC实例和新实例不能同时启动。
- 保存旧部署及状态目录，验证回退命令后再接管跟屁虫运行身份；私钥留在193原受控位置。
- 215证据服务批准精确身份对：隆耘Leader AIC + 跟屁虫Partner AIC。不得放开任意AIC。
- 新实例校验本地ACS、clientAuth证书、MQ信任链；Inbox仅一个消费者。
- 入组后先由9007实际观察到精确连接身份，再进入DISBAND。
- 现有隆耘Leader的调用权限也要允许该已审批Partner及test.echo；不得修改天眼运行实例。

## 验收分层

本地测试覆盖状态机、幂等、终态保护、跨Session隔离、真实业务响应映射、丢失POST响应恢复、授权撤销、证据错误、条件删除竞态、Publisher Nack和固定退出期限。

真实平台验收需另行记录：镜像摘要、双方AIC、sessionId、groupId、Inbox邀请、joined和connected=true、TaskCommand/result/product ID、原样回显结果、Leader complete、DISBAND ID、退出Ack、closed审计、精确资源证据、Inbox保留及容器身份。

随后需要故障联测：Group连接断开、进程中断恢复、重复start、业务超时、授权撤销、300秒退出超时。通过回显只代表协议和该连接器可用，不能代表所有第三方API已通过验收。
