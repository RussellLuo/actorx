# 调用语义

[English](runtime.md) | 中文

Send/Call/Stream 都按同一顺序执行：**核对契约 → 选定并固定代次 → 等待 READY → 经 NATS 投递**。目标通过 actorx mailbox 执行业务，同进程也走相同路径。地址模型见[核心概念](concepts.zh.md)。

## 完成、顺序与错误

| 调用或观察 | 含义 |
| --- | --- |
| Send 成功 | 目标代已受理，handler 可以尚未完成 |
| Call 返回 | 收到有效最终结果或确定业务错误；不推定副作用回滚 |
| Stream 成功 | 收到有效成功终止，序号连续，全部 chunk 已消费且活动回调已退出 |
| 本地校验、容量或明确发布前失败 | 本次业务帧未发送，返回原错误 |
| 精确目标在 mailbox 前拒绝 | 本次未受理的确定错误，不透明重试 |
| 可能已发送，缺少所需 ACK、结果或终止 | ErrOutcomeUnknown，并保留取消、期限或传输原因 |

普通业务 error 只结束该次请求，状态修改保留，同代可继续处理后续请求。Send ACK 后的 handler 错误由核心异步日志报告，不撤销 ACK；panic 按核心规则停止实例，完整退出由 [Wait](lifecycle.zh.md) 观察。

同一实例的 handler 串行执行，跨发送者及并发调用没有全局顺序。Counter 等 Send ACK 后再 Call，通过同步 handler 观察前序增加；ACK 本身不是业务完成屏障。StartTask 的外部工作可与后续 handler 重叠，不能用该顺序推定异步任务完成。

## 期限与取消

无 deadline 时默认五秒，显式期限最多六十秒。契约读取、选代、激活和等待消耗同一总期限；各阶段不重新计时。Send 受理后跟随目标实例寿命，调用方取消只结束 ACK 等待。Call/Stream 尽力传播取消，接收方期限提供兜底；取消不证明 handler、任务或副作用已经停止。

断线不缓冲或重放业务。出现结果未知时，应用通过业务查询、幂等或补偿处理；框架不透明重发，也不保存可查询的业务结果。精确期限编码及取消身份校验见 [wire 契约](contract.zh.md)。

## Stream

`addr.Stream[C](ctx, msg, yield)` 按序串行调用 yield。缺块、重复、乱序、非法 chunk 或终止均不能成功。有效业务错误是确定失败，可停止消费排队 chunk，不撤销已消费内容；没有有效终止时保留 ErrOutcomeUnknown。有效终止后的取消不额外增加远端结果不明；回调错误保留原 Go error，缺终止证据时同时保留未知结果。

所有返回路径都等待活动回调退出。取消或失败后不启动新回调，已观察的业务/协议错误不会被随后等待超时覆盖。阻塞回调继续占窗口、关联和 Runtime 工作预算，并阻止 Close 成功；网络取消仍可在回调阻塞期间传播。回调应及时返回，或用捕获的 context 自行响应取消，不在其中同步等待外层 Stream 或 Runtime.Close。

每流窗口为4条/64 KiB编码 payload，包含活动回调；Runtime 入站/出站默认合计最多16流，可通过 TransportLimits.Streams 调整。消费完成才按真实编码长度归还额度；`Responder.WriteConsumed` 额外等待该消费确认。部分消费后失败不自动续传，断线取消既有入站流，重连不恢复；迟到帧不能恢复关联或再次触发消费。序号、窗口确认与累计预算详见 [wire 契约](contract.zh.md#统一信封)及[资源表](contract.zh.md#时间及开发资源配置)。

长流用 `actorx.HandleStream` 的短 handler 启动 `actorx.StartTask(ctx, ...)`：worker 使用快照和任务 context 执行 I/O 或下游 Stream，通过 `reply.Write` 转交内容，串行完成回调调用 `reply.Complete(err)`。状态只在串行 handler/完成回调修改，同一 Actor 可继续处理普通 Notify。启动任务前取得 Addr/Self，worker 不保留分布式上下文视图。

Stream 使用上述五秒默认、六十秒最长总期限，长订阅须显式选择期限。取消流只结束请求，不自动退订 Self 或停止协作 Actor，结束责任见[生命周期](lifecycle.zh.md#对象寿命)。

## 契约与容量

调用方和工作进程使用稳定包路径的共享 DTO，消息、结果/chunk 的具名 Go 类型身份、schema 和模式都必须匹配；同一 Type 的完整契约不可变，新增 handler 也须用新 Type，见[契约升级](deployment.zh.md#契约升级)。嵌套 Addr 自动关联接收 Runtime。

wire 使用严格 JSON 子集，单个 payload/chunk 最多64 KiB。自定义 codec、执行能力、非法数值或不支持的形状会被拒绝；完整兼容表、深度/节点/分配限制统一见 [wire 参考](contract.zh.md#schema-子集与指纹)。

本地容量不足返回 ErrFull。SDK 溢出只有异步诊断，不能证明某请求未执行；明确拒绝也依赖有效回复路径，满载时仍可能结果未知。`rt.Diagnostics()` 提供故障观察。网络工作与核心 handler/任务分别记账，只在实际结清时释放；完整上限见[资源表](contract.zh.md#时间及开发资源配置)。

## 容量配置

创建各个 worker 或调用方 Runtime 时设置容量，未填写的字段保留默认值：

```go
rt, err := dist.NewRuntime(ctx,
    dist.WithActor(CounterSpec),
    dist.WithExecutionLimits(dist.ExecutionLimits{
        Instances:       256,
        MailboxCapacity: 64,
    }),
    dist.WithTransportLimits(dist.TransportLimits{
        Requests: 256,
        Streams:  32,
    }),
)
```

| 字段 | 默认值 | 作用范围 |
| --- | ---: | --- |
| ExecutionLimits.Instances | 64 | 业务实例，包含本地子 Actor；也用于独立计数的分布式激活容量 |
| ExecutionLimits.MailboxCapacity | 16 | 每个 Actor 的排队工作与任务完成预留槽 |
| ExecutionLimits.Pending | 32 | 每个 Actor 已受理但未结清的请求，包含 Send 和 Deferred 请求 |
| ExecutionLimits.Tasks | 4 | 每个 Actor 的异步任务，直到结果交接或丢弃完成 |
| TransportLimits.Requests | 128 | 入站、出站分别计算的未结清网络请求；也用于业务核心调用方和共享回复关联容量 |
| TransportLimits.Streams | 16 | 入站、出站合计的流数，包含活动消费回调 |

工作从队列取出时释放 mailbox 容量，请求结清时才释放 Pending。Deferred handler 可以已经返回、mailbox 已空，但回复仍占 Pending。StartTask 同时占用任务槽并预留 mailbox 完成槽。

Instances 用同一个值配置执行实例和激活容量，两处独立计数：创建失败或已退出但尚未释放的激活仍可占容量，本地子 Actor 则只占执行实例容量。Stream 也占请求额度；Send ACK 等待上限为 Requests 的一半、向上取整。激活及控制请求与业务请求共享回复关联。

字段为零使用默认值，不表示无限制。负数或资源容量算术溢出在读取类型声明、连接网络前返回 actorx.ErrSetup。同组 Option 后者整体替换前者，再为零字段补默认值。配置在 Runtime 寿命内固定，不修改其他进程或服务端配额。

Factory 配置和创建并发继续由内部管理。请求/回复缓冲与分派队列随网络容量配套增长；wire 窗口、payload 大小、分派 worker 数及 Registry I/O/存储仍有独立限制。配置是容量上限，不保证受理，其他预算也可能先返回 ErrFull。默认资源表及内部推导规则见 [wire 参考](contract.zh.md#时间及开发资源配置)；提高 Instances 时须检查存储余量。

## 错误分类

使用 errors.Is 判断框架及 context 错误，不解析诊断文字。远端任意业务 error 只还原文字，不保留自定义 Go 类型；框架/context 分类经下游包装继续保留，并携带最多四项来源。收到外层确定回复不抹去下游的不确定性。

| dist 错误 | 含义 |
| --- | --- |
| ErrTypeUnavailable | 没有响应类型激活请求的 Factory，不授权替换已有代 |
| ErrNotActivated | 冷句柄控制未找到当前代 |
| ErrContract | 完整契约、schema 或模式不兼容；可同时包装核心分类 |
| ErrProtocol | wire 版本或协议帧不受支持 |
| ErrOwnerUnavailable | 原拥有者不可达，归属保留 |
| ErrActivationUnknown | 激活或归属更新不明，需核实原 key/generation |
| ErrOutcomeUnknown | 可能已发送业务请求，缺少该操作所需完成证据 |
| ErrExitUnknown | 原代终态缺失、过期或无法确认 |

本地设置、关闭、停止、容量、消息、模式和结果类型错误沿用 actorx.ErrSetup/ErrClosed/ErrStopped/ErrFull/ErrUnhandled/ErrMode/ErrReplyType 等分类。结果未知可同时包含 context.Canceled 或 DeadlineExceeded；不能仅凭超时推定未执行。业务查询、幂等及补偿由应用定义。
