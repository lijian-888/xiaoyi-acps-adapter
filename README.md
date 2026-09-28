# 小亿通用 ACPs Group Partner 适配器

同一套代码和镜像，按智能体 AIC 分别部署。每个实例使用独立证书、Inbox、MQ连接和状态库。适配器处理 Group 协议，连接器调用真实业务。当前版本为 **0.1.0 联调候选版**，不能将单元测试通过视为真实平台接入验收通过。

## 已实现

- Group-only：AMQPS + SASL EXTERNAL、长期 Inbox、精确成员邀请、专属 Group 连接。
- AIP `start/get/continue/complete/cancel`，官方 SDK 消息模型及资源命名。
- 一份 SQLite 状态库绑定一个 AIC；消息去重、任务归属隔离、持久化 Outbox、稳定产出物ID、Publisher Ack。
- 同步与异步业务、JSON Schema校验、默认180秒任务期限及并发限制。
- 三种连接器：原样回显 `echo`、统一HTTP业务接口 `standard-http`、已有JSON API字段映射 `json-http`。
- 按 Leader AIC 和能力ID授权；有批准人、用途、原因及有效期的授权文件每30秒重载；支持HTTPS/mTLS授权接口读取。
- 接到DISBAND持久化固定截止时间；业务未收尾时不谎报取消；协议退出确认与资源验收分开；条件删队列；仅在精确平台证据齐备后进入closed。
- 健康检查、运行身份校验、结构化审计、非root容器、资源上限、日志轮转。

## 当前交付边界

1. **保留现有天眼和隆耘实现**，本仓库不导入它们的应用代码。跟屁虫作为首个验收对象。
2. 采用已运行平台的`acps-sdk==2.1.0`模型。没有直接使用SDK的默认退出方法，因为其无条件队列删除不满足本项目的安全要求。
3. 动态授权模块已支持受限宿主机管理员审批文件及HTTP读取。**现有注册网站的授权编辑页面和专用授权HTTP接口尚未集成**。注册审批、运行调用授权、目录上架三者保持独立。
4. 215的证据服务当前只批准“隆耘Leader＋天眼Partner”。跟屁虫必须获得精确证据读取授权，且Group成员登记可用，才能执行完整自动退出验收。
5. 普通旧HTTP接口不会自动获得可靠幂等、查询、取消能力。复杂OAuth、长作业或多步骤业务要实现并审查对应连接器。新能力通过显式连接器注册加入构建；运行时不接受网页上传或任意动态导入代码。
6. 证书首期通过现有CLI申请和运维流程续期。实例启动校验证书身份、有效期和clientAuth；自动续签与在线换证尚未实现。
7. 小文件可作为能力定义中的受限结构化字段；大文件使用业务方受控引用。本版不自动下载任意URL，也不提供文件托管服务。
8. 首版严格自动验收以整个Group正常DISBAND为准。单个Partner主动退出而其他成员继续运行，需要另行定义成员级资源验收契约，不能套用整个Group资源全消失的条件。

## 本地开发

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.lock
.\.venv\Scripts\python -m pip install --no-deps -e .
.\.venv\Scripts\python -m unittest discover -s tests -v
```

单元测试可以在Windows执行；实际服务使用Linux容器，借助文件锁避免同一状态目录启动两份运行时。

## 如何接入业务

- API可以按约定实现：使用`standard-http`，见[业务接口契约](docs/business-contract.md)。
- 保留已有JSON API：参考`examples/connector.legacy.json`，配置请求字段、成功条件、结果字段。
- 复杂业务：实现`Connector`的execute/poll/resume/cancel，并在`build_connectors()`显式注册；不修改Group核心。
- 使用现有注册平台完成ACS审批、AIC、EAB和证书申请。适配器本身不生成AIC，也不存储Registry管理员账号。

## 部署

参见[部署与回退](docs/deployment.md)、[设计与验收](docs/design-and-acceptance.md)。真实生产调用只能在Group完整联调通过后开放。

## 官方参考

- [智能体开发指南](https://github.com/AIP-PUB/ACPs-community/blob/main/acps-docs/tutorials/agent-development.md)
- [AIP SDK教程](https://github.com/AIP-PUB/ACPs-community/blob/main/acps-docs/tutorials/aip-sdk-tutorial.md)
- [AIC工具与身份验证](https://github.com/AIP-PUB/ACPs-community/blob/main/acps-sdk/acps_sdk/aic/README.md)

原AtomGit入口本次抓取不可用，使用相同组织项目的GitHub文档作参考。业务HTTP契约、动态授权JSON及9007证据扩展属于本项目约定，不冒充ACPs官方新增规范。
