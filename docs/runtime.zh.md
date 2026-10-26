# 运行语义参考

[English](runtime.md) | 中文

本页集中说明作者侧绑定、异步执行与调用侧通信语义。完整程序见[快速入门](getting-started.zh.md)，启动和退出见[生命周期](lifecycle.zh.md)。

## 绑定与调用

常规作者通过 `//actorx:actor` 和 actorx-gen 生成消息绑定，首次使用见[快速入门](getting-started.zh.md)。底层 `Actor` 接口要求 `Bindings() []actorx.Binding`；生成器提供这个方法，也可按[手写绑定](#手写绑定)自行实现。Spawn 调用一次 Bindings，校验并复制条目后安装；nil、typed nil、Bindings panic、无效或重复绑定均返回可用 `errors.Is(err, actorx.ErrSetup)` 判断的错误。

| 绑定 | Handler 签名 | 支持的调用 |
| --- | --- | --- |
| `Handle` | `func(actorx.Context, Q) (R, error)` | Send / Call |
| `HandleMessage` | `func(actorx.Context, M) error` | Send / Call；成功 Call 的结果是 nil |
| `HandleDeferred` | `func(actorx.Context, Q, actorx.Reply[R]) error` | Send / Call；返回 nil 后等待 Complete |
| `HandleStream` | `func(actorx.Context, Q, actorx.StreamReply[C]) error` | 仅 Stream |

首参由 Runtime 提供，实现标准 `context.Context`，携带请求的期限、取消和值，以及所属 actor 的能力。调用方的 Addr 方法仍接收标准 context。

消息按具体类型匹配：类型别名相同，T 与 *T 不同；接口消息、重复类型和 nil handler 无法装配。生命周期通知只有特殊的 HandleMessage 绑定，见[生命周期](lifecycle.zh.md#绑定生命周期通知)。

`Call[R]` 的 R 在收到结果时检查，类型错配返回 `ErrReplyType`，业务副作用可能已发生。error-only handler 可用 `Call[any]` 等待完成；无类型 nil 可由接口 R 接收，不能由具体值或指针 R 接收；带类型 nil 保留其动态类型。

本地消息、结果和 chunk 不深拷贝；交付后不得并发修改共享的 slice、map 或指针内容。handler 不可抢占，不要同步等待自己或形成同步 Call 环。

## 调用方的取消与完成

| 操作 | context 的作用 | 成功边界 |
| --- | --- | --- |
| Send | 约束受理；入队后执行跟随 actor 生命周期。 | 已进入目标 mailbox，业务错误通过 slog 报告。 |
| Call | 约束请求执行与结果等待。 | 一次最终回复；普通业务 error 返回给调用方。 |
| Stream | 约束请求执行与分片消费。 | 成功终止且已接受的分片消费结束。 |

Call / Stream 在执行前已取消时可跳过 handler；执行中取消只能请求协作停止。Send 成功后，调用方再取消不会撤回消息。**等待超时不证明业务未执行**，框架不会自动重试，业务应先定义幂等或查询确认方式。

## 异步任务与延迟回复

`actorx.StartTask(ctx, work, complete, discard)` 管理属于当前 actor 的异步任务。worker 使用输入快照，完成回调在 actor 上串行执行；完整接线见 [profile](../examples/profile/main.go)。

```go
cancel, err := actorx.StartTask(ctx,
    func(ctx context.Context) (Result, error) {
        return client.Fetch(ctx) // 使用快照和并发安全客户端。
    },
    func(result Result, err error) bool {
        reply.Complete(result, err)
        return true // 接管结果；false 则交给 discard。
    },
    nil, // 纯值无需释放；资源结果应提供 discard。
)
```

仅从 Started、普通 handler 或串行完成回调启动任务。框架预留一个任务名额和一个 mailbox 完成槽；业务消息与完成回调共用 FIFO。容量不足返回 ErrFull，不启动 worker。Stopping / Stopped 不能启动新任务；nil 或无 actor 归属的 Context 返回 ErrSetup。

| 结果路径 | 归属 |
| --- | --- |
| complete 返回 true | 结果已被接管；保存在 actor 内的资源由生命周期释放。 |
| complete 返回 false | discard 释放结果。 |
| complete panic，或 Stop 导致完成无法交付 | discard 释放未交接结果。 |
| worker 尚未返回的资源 | worker 自行释放。 |

无需释放的纯值可用 nil discard。discard 可能在 mailbox 外执行，不得依赖 actor 继续处理消息，也不得并发操作其可变状态。完成回调在返回 true 前保存资源又 panic 时，框架不能撤销字段写入；discard 与生命周期释放必须共用幂等操作。[真实文件示例](../example_task_test.go)演示这种资源交接。

任务默认继承请求的取消、期限和值，actor 停止始终取消任务。返回的 cancel 仅请求取消，不删除任务或证明工作结束；请求取消后完成回调仍可能执行，迟到结果是否过期由业务 revision 判断。要调整父 context，同时保留 actor 归属：

```go
taskCtx := ctx.WithContext(context.WithoutCancel(ctx))
// 再用 actorx.StartTask(taskCtx, ...)；actor 停止仍会取消任务。
```

`WithContext(parent)` 也可传入派生期限或 `context.Background()`。它只改变任务父 context，不改变原 Call / Stream 的等待期限。计时器、连接等待等放在受控 work 中，并在返回前释放。

`Reply.Complete(value, err)` 与 `StreamReply.Complete(err)` 最多成功一次；取消、停止或重复完成返回 false。延迟 handler 返回非 nil error 会结束请求。Reply 是一次调用的本地回复能力，不应持久化或作为消息数据传递。延迟 Send 不占调用方 Call 名额，仍占目标的未完成请求名额。

## Stream

调用方逐段消费，回调在调用者 goroutine 上串行执行：

```go
err := addr.Stream[TextChunk](ctx, Ask{Topic: "Actors"},
    func(chunk TextChunk) error {
        fmt.Print(chunk.Text)
        return nil
    })
```

生产方在 StartTask worker 中调用 `stream.Write(ctx, chunk, payloadBytes)`，例如文本声明 `len(chunk.Text)`；窗口满时 Write 阻塞，不能在状态 handler 中生产。worker 返回后，由串行完成回调 `stream.Complete(err)`。终止信号不占数据窗口，成功结束前排空已接受的 chunk；部分输出后仍可能返回错误。完整程序见 [chat](../examples/chat/main.go)。

回调返回 error 时，Stream 原样返回该错误并取消请求；context 取消和 actor 停止也会结束消费。活跃回调返回前 Stream 无法返回，因此回调自身必须有合理时间边界。nil 回调返回 ErrSetup，模式不匹配在 handler 执行前拒绝，chunk 类型错配返回 ErrReplyType。

本地字节预算由生产者声明保留的 payload 大小，每项必须在 0 到配置上限之间。缓冲 payload 至多为 `items * chunkBytes`，另加正在消费的一项。框架不计算任意 Go 对象的真实堆占用，不计对象头或外部 SDK 缓冲；不能用小 size 携带大对象。

## 容量与背压

| 配置 | 默认值 | 范围 |
| --- | --- | --- |
| `WithInstanceLimit(n)` | 0（不限） | Runtime 全部实例，含构造、停止中及 ctx.Spawn 子对象，完整退出后释放。 |
| `WithMailboxCapacity(n)` | 64 | 每 actor 排队消息与预留完成槽。 |
| `WithTaskLimit(n)` | 16 | 每 actor 未结清任务，包括等待交接的结果。 |
| `WithPendingLimit(n)` | 256 | 每 actor 未完成请求，包括 Send。 |
| `WithCallLimit(n)` | 256 | 整个 Runtime 的调用方 Call / Stream 关联。 |
| `WithStreamLimits(items, chunkBytes)` | 8 / 64 KiB | 每流缓冲项数与每项声明大小。 |

除实例上限允许0表示不限外，其余配置必须为正；负实例上限返回 ErrSetup。消息或任务受理遇到容量不足立即返回 ErrFull；流窗口满时 Write 等待消费或取消。提高调用方总容量不会放宽任一目标的请求上限；Stream 的调用方名额保留到回调和缓冲消费结束。实例上限在读取 Bindings 前检查，满载 Spawn 返回零 Addr 与 ErrFull；失败回滚后可复用槽，Stopped 尚未完成时不能复用。

同步信号演示见[容量与背压示例](../examples/README.zh.md#容量与背压)。

## 错误速查

下表说明框架产生的错误；业务 handler 仍可返回自己的 error。使用 `errors.Is` 匹配哨兵错误、`errors.As` 读取本地 PanicError，不依赖完整错误文本。

| 错误 | 含义与处理 |
| --- | --- |
| `ErrFull` | 本次受理因容量不足被拒绝；限流或等待业务定义的重试时机。 |
| `ErrStopped` | 地址无效、目标已不可用或请求因实例停止而结清；不能据此排除已开始的副作用。 |
| `ErrClosed` | 所属 Runtime / Channel 已关闭或正在关闭；使用其拥有者的关闭流程。 |
| `ErrUnhandled` | 目标没有此具体消息类型的普通绑定；核对 T / *T 与消息契约。 |
| `ErrMode` | 绑定不支持所选 Send / Call / Stream 模式；改用匹配模式。 |
| `ErrReplyType` | 回复或 chunk 与调用方类型参数不兼容；修正契约，副作用可能已发生。 |
| `ErrSetup` | 实例、绑定、配置或回调无效；修正装配。 |
| `ErrChunkSize` | Write 的声明大小为负数或超限；修正或拆分分片。 |
| `context.Canceled` / `DeadlineExceeded` | 当前调用或等待取消、超时；不自动重试。 |

Stop / Wait 的地址能力错误、Exit.Reason 与等待 error 的区别见[生命周期](lifecycle.zh.md)。

| 异常位置 | 行为 |
| --- | --- |
| 普通 handler 返回 error | 结束本次请求，actor 继续处理后续消息。 |
| 普通 handler 或任务完成回调 panic | 停止实例，最终原因包含带堆栈的 PanicError。 |
| 任务 work panic | 转成 PanicError 作为 err 交给完成回调；停止导致无法交付时按 discard 路径结清。 |
| discard panic | 记录到实例最终退出原因，继续后续清理。 |
| 生命周期 error / panic | 参与启动失败或最终退出原因，后续清理继续。 |

## 静态读取消息描述

工具可用 `Describe(bindings)` 读取普通消息类型与模式，不构造实例、不创建 Runtime 或执行 handler：

```go
descriptions, err := actorx.Describe((*Counter)(nil).Bindings())
if err != nil {
    return err
}
for _, d := range descriptions {
    fmt.Println(d.MessageType(), d.ResultType())
}
```

Describe 与 Spawn 共用校验，先校验所有绑定，再排除生命周期条目。失败返回匹配 ErrSetup 的错误，无部分结果；空集合或仅生命周期集合返回空描述。输出按绑定顺序排列，切片独立，不保留 handler 或业务对象。

`MessageType()` 是消息类型，`ResultType()` 是回复或 chunk 类型，无结果时为 nil；`Validate(SendKind/CallKind/StreamKind)` 校验模式。`Binding.Descriptor()` 可读取单个绑定的描述；`Request[Q,R]()`、`Message[M]()`、`Streaming[Q,C]()` 可在没有 handler 时声明同样的类型与模式。

生成 Bindings 支持在具体指针 nil receiver 上只构造条目。手写 Bindings 必须保证 nil 与真实 receiver 的描述一致，不读状态或取得资源；每个实例绑定自己的 receiver，不缓存捕获其他实例的闭包或并发修改共享切片。指针接收者可直接捕获 `c.Read`，值接收者须在延迟调用闭包中访问对象，避免构造时隐式解引用 nil。[手写示例](../example_describe_test.go)与[生成示例](../examples/counter/describe_test.go)展示两条路径。Spawn 仍要求非 nil 对象。

<a id="可选生成器"></a>

## 生成器参考

在应用文件中添加指令并标记类型：

```go
//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen

//actorx:actor
type Counter struct{ n int }
```

在目标 package 运行 `go generate .`，或直接 `go run github.com/go-aie/actorx/cmd/actorx-gen`。首次接入见[快速入门](getting-started.zh.md)。默认按包生成一个 `actorx.gen.go`，支持 `-output 文件名` 与 `-check`。

- 类型标记选择公开、直接声明的方法，跨文件收集，支持 T / *T；私有方法、`//actorx:ignore` 与嵌入提升的方法不绑定。
- 只选择少数方法时使用 `//actorx:handler`，同一类型不能混用两种选择。签名不符合四类 handler、重复消息或已有手写 Bindings 均报错。
- 生命周期按事件类型识别并生成 HandleMessage，不依赖方法名。生成器只生成绑定，不构造业务对象。
- 值接收者延迟到分派时读取当前对象，不在装配时保存状态副本。输出按类型与方法排序，原子写入，拒绝覆盖手写文件。

生成遵循 GOOS / GOARCH 与 build tags，将源 `//go:build`、旧式 `// +build` 和文件名平台条件保留为输出条件。同一输出内类型和方法必须有一致的规范化条件表达式；不求解逻辑等价、不取交集或自动拆分变体，输出文件名也不能附加其他条件。混合条件请手写 Bindings。

生成前用待生成的 Bindings 签名参与类型检查，支持 `var _ actorx.Actor = (*T)(nil)` 的首次生成与后续检查，不忽略无效接口断言。泛型 actor 和 cgo package 暂不支持；手写 Bindings 保留完整功能。

## 手写绑定

需要直接控制绑定，或使用生成器尚不支持的类型和构建条件时，可自行实现 `Bindings`。例如为计数器绑定普通请求：

```go
func (c *Counter) Bindings() []actorx.Binding {
    return []actorx.Binding{actorx.Handle(c.Add)}
}
```

手写与生成绑定使用相同 API，满足同一个 Actor 接口；同一类型只保留一份 Bindings。四类 handler 的选择见[绑定与调用](#绑定与调用)。

Bindings 只构造条目，不执行 handler、读取状态或取得资源；为每个实例绑定自己的 receiver。支持静态描述时，还需遵守[nil receiver 约定](#静态读取消息描述)。完整手写示例见 [example_describe_test.go](../example_describe_test.go)。

## Channel 扩展接口

Channel 连接 Runtime 与 mailbox 投递、回复能力。Runtime 默认使用 `NewLocalChannel()`，通过 `WithChannel` 可提供其他实现；业务 handler 继续使用 Context 和 Addr。

```go
type Channel interface {
    Open(context.Context) error
    Bind(context.Context, string, Receiver) (release func() error, err error)
    Deliver(context.Context, Delivery, Responder) error
    Close() error
}
```

Runtime 独占成功打开的 Channel。Open 失败须回滚自身工作，拒绝重复认领时不能影响已有拥有者。Bind 返回可重复调用的释放函数。启动回滚和关闭顺序见[生命周期](lifecycle.zh.md#关闭-runtime)。

| 能力 | 契约 |
| --- | --- |
| `Delivery` | 携带具体实例 target、唯一 correlation ID、Kind 和 Message；`Validate` 检查投递信封，消息类型和调用模式另行校验。 |
| `Receiver` | `Receive` 进入 mailbox，成功证明受理；`Bindings` 提供普通消息描述，不含 handler 或生命周期通知。 |
| `Responder` | `Complete` 单次结算；`Write` 将 chunk 排入有界窗口，`WriteConsumed` 另等待消费。 |

写入可能阻塞，应放在请求所属工作中，不能阻塞共享接收回调。核心继续负责串行执行、请求/任务完成和完整退出。完整接口说明见 [channel.go](../channel.go)。

## Context 扩展与迁移

`actorx.Context` 是接口，包含标准 context 方法以及 WithContext、Stop、Spawn、State。核心持有私有本地实现；零值是 nil 接口，不能直接调用方法。`StartTask` 在预留资源前校验归属，nil、typed-nil、空嵌入和无执行归属均返回 ErrSetup。

创建 Runtime 时安装 `WithContextFactory`。Spawn 在安装有效 Bindings 后、Channel.Bind 和 Started 前，用本次 Actor 调用工厂一次，保存返回的 `ContextWrapper`；业务 Actor 只需 Bindings。例如，应用可以捕获共享服务：

```go
type appContext struct {
    actorx.Context
    service *Service // 不可变，或由其所有者同步。
}

factory := func(actorx.Actor) (actorx.ContextWrapper, error) {
    return func(ctx actorx.Context) actorx.Context {
        return &appContext{Context: ctx, service: service}
    }, nil
}
rt, err := actorx.NewRuntime(initCtx, actorx.WithContextFactory(factory))
```

NewRuntime 只保存策略，不调用工厂。未配置时保持纯本地行为；显式 nil 的最终工厂在打开 Channel 前返回 ErrSetup。重复选项由后者覆盖，不叠加工厂链。已配置工厂必须返回非 nil wrapper，需要保持原 Context 时返回恒等函数。Bindings 无效或实例预算已满时不调用工厂，Describe 也不调用。

工厂可读取应用自己的私有 Actor 适配器，捕获稳定实例环境。wrapper 须嵌入本次提供的有效 `actorx.Context`，包外不能从头实现私有归属方法。handler 仍接收 `actorx.Context`，按需断言可选能力。生成器识别该接口及其类型别名，拒绝旧指针签名和下游扩展接口；绑定选择、生成命令和 `-check` 均保持原样。已删除 ContextDecorator 及其自动检测。

工厂和 wrapper 均须有界，不做 I/O、不获取资源、不启动任务、不调用 handler、不读写业务可变状态；环境在安装后保持不变或由所有者同步。并发 Spawn 可以并发调用同一个工厂。工厂调用、包装及私有归属提取均不持 Runtime/actor 内部锁。核心不提供工厂资源清理协议，环境生命周期由其所有者负责；不能在工厂中等待当前实例启动或本 Runtime.Close。

wrapper 必须保留期限、取消和值，指向本次提供的同一个本地上下文；其他实例或旧调用的 Context 会被拒绝。WithContext 创建新的本地上下文并调用同一已保存 wrapper，不重跑工厂、不重新读取 Actor、不叠加旧包装层。Background 保留动态能力，nil parent 会 panic。本地 ctx.Spawn 的子对象及孙对象用自己的 Actor 经过同一 Runtime 工厂；实例环境独立，不复制父 wrapper，也不新增停止传播。

| 工厂 / 包装失败位置 | 行为 |
| --- | --- |
| Spawn 工厂绑定 | 不调用 Channel.Bind 或生命周期 handler；返回零 Addr，回滚完整退出后释放实例名额。 |
| Started | 跳过 handler，Spawn 进入原生命周期失败回滚。 |
| 普通 handler | 跳过 handler，当前请求以设置错误结算，记录错误并停止实例。 |
| WithContext | panic 携带设置错误；发生在 handler 中时沿用其原 panic 边界。 |
| Stopping / Stopped | 记录错误并跳过该回调，继续后续清理和退出阶段。 |

无效返回值产生 ErrSetup；工厂 error 与 ErrSetup 组合并保留原错误链；工厂或 wrapper panic 保留 PanicError 和堆栈，并与 ErrSetup 组合。仅在私有归属提取边界将空嵌入转发 panic 视为无效上下文；work、complete、discard 保持原 panic 边界。handler 内的 panic 外层仍为 PanicError，不承诺其错误链匹配 ErrSetup。

退出包装失败不会绕过 worker、discard 或完整退出预算。如果被跳过的 Stopping 原本负责解除 worker 阻塞，Wait/Close 可以超时，实例名额仍被占用，直到实际工作结束。串行 complete 使用原闭包及捕获的上下文，不额外重建或包装。工厂阻塞期间仍占用实例预算，并计入 Spawn/Close 等待；工厂返回后沿用原关闭复查，Runtime 已关闭时不进入 Started、不发布地址。

| 旧 API | 当前 API |
| --- | --- |
| `func(ctx *actorx.Context, ...)` | `func(ctx actorx.Context, ...)` |
| `ctx.StartTask(work, complete, discard)` | `actorx.StartTask(ctx, work, complete, discard)` |
| `ctx.Context` | 直接把 `ctx` 传给标准 `context.Context` 参数。 |
| `actorx.Context{}` / `&actorx.Context{}` | 从 Runtime 取得有效 Context；接口零值是 nil。 |
| Runtime/实例 association API | 配置 WithContextFactory，在返回的 wrapper 中嵌入本次提供的 Context。 |
| Actor ContextDecorator | 将实例绑定移入 ContextFactory，将每次执行包装移入 ContextWrapper。 |

已移除 WithRuntimeAssociation、Runtime.SpawnAssociated、Context.RuntimeAssociation 和 Context.InstanceAssociation。StartTask 从传入上下文取得取消、期限和值，并保留原 actor 归属；State 仍是本地生命周期快照，Spawn 仍是本地创建。
