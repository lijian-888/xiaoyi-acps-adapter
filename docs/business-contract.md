# 业务连接器契约 v1

本契约连接适配器和业务系统。智能体之间始终使用ACPs Group，不开放Direct RPC入口。

## 统一业务接口

### 启动

`POST /v1/capabilities/{capabilityId}/execute`

```json
{"requestId":"由适配器持久化生成的幂等键","input":{"text":"请查询"}}
```

HTTP头同时带`Idempotency-Key: requestId`。业务系统必须将这个键和真正执行动作持久化关联，相同键重复提交不能重复执行。同键不同输入应拒绝。仅适配器发送这个Header不能证明业务端已经实现幂等。

同步成功返回：

```json
{"state":"succeeded","text":"实际分析正文","output":{"count":3},"settled":true}
```

异步接受返回HTTP 200或202：

```json
{"state":"working","job_id":"实际业务任务号","settled":false}
```

返回中的state表示业务状态，不直接复用ACPs completed：业务succeeded先转换成awaiting-completion，待Leader complete才进入completed。

### 状态查询及丢失响应恢复

`GET /v1/tasks/{requestId}`

必须能用客户端requestId查询，包括首次启动已成功、但HTTP响应丢失的场景。找不到不能直接解释为“从未执行”。适配器不会在响应不明时盲目重复POST。

### 补充输入

`POST /v1/tasks/{jobId-or-requestId}/continue`

请求包含新的操作requestId和input，幂等键独立于原start操作。仅在awaiting-input或awaiting-completion接受。连接器声明不支持时，保留当前任务并返回明确错误。

### 取消

`POST /v1/tasks/{jobId-or-requestId}/cancel`

只有返回`{"state":"canceled","settled":true}`才能将协议任务标记canceled。停止适配器协程、关闭浏览器、HTTP超时不等于取消了远端业务。

### 返回类型

| 字段 | 说明 |
| --- | --- |
| state | working / awaiting-input / succeeded / failed / canceled |
| text | 展示正文或需要补充输入的提示 |
| output | 经过能力output_schema约束的结构化产出 |
| job_id | 可选的真实业务任务号 |
| error_code | 可选的稳定错误码，不放密码、内部响应或堆栈 |
| settled | true代表本次业务阶段已明确落定，false表示仍有未确认操作 |

能力清单必须为公开返回字段指定严格的Schema，建议`additionalProperties=false`。字段映射连接器只返回配置的response_fields。结果通过Group可被有权限的群成员读取，不适合携带其他租户数据或凭据。

## 已有API连接器

`json-http`首版支持固定相对路径的POST、请求字段重命名、嵌套返回字段读取、业务成功码判断。示例把`input.text`变为`question`，并将`data.answer`变为输出text。

它不假设任意API都支持轮询、取消、追加输入或幂等。对于真实副作用动作，如果接口缺乏查询能力且返回丢失，任务进入需核对状态、占用名额，不自动重试或伪称失败后重新启动。

复杂OAuth或特殊异步API需开发已审查的Python连接器。插件应返回BusinessResult，不能直接操作MQ、ACL、其他AIC私钥或协议状态库。

## 文件

首版可在input/output的JSON Schema里定义小文件内容或受控下载引用，受MQ消息体总大小上限限制。适配器不自动抓取URL，避免把业务参数变成任意内网请求。是否允许公开下载必须由业务方和上架策略明确。
