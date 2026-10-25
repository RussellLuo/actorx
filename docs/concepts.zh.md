# 核心概念

[English](concepts.md) | 中文 · [文档目录](README.zh.md)

Actorx 将有状态组件组织为 actor：普通 Go struct 保存状态，具名方法处理消息，调用方通过地址通信。先运行[快速入门](getting-started.zh.md)，再用本页理解各部分的关系。

## Actor 作者侧与使用侧

| 部分 | 作者侧 | 使用侧 |
| --- | --- | --- |
| 状态与消息 | 定义 struct、普通 Go 消息及 handler。 | 发送消息获取结果，不直接访问状态对象。 |
| `Actor` | 标记类型，编写具名消息处理方法。 | 将一个新对象交给 `Runtime.Spawn`。 |
| `Context` | handler 获得的上下文，提供任务、创建实例和自身停止能力。 | 调用 Addr 时传入标准 `context.Context`。 |
| `Addr` | 可保存其他 actor 的地址进行协作。 | 使用 Send / Call / Stream，以及原始本地地址的 Stop / Wait。 |
| `Runtime` | 执行 handler、管理所属任务。 | 创建实例，最终 Close 并等待关闭。 |

常规作者用 `//actorx:actor` 标记类型，由生成器把普通 Go 方法接入 Runtime，具体步骤见[快速入门](getting-started.zh.md)。交给 Spawn 后，状态对象由实例独占，调用方不得继续读写可变字段或重复 Spawn 同一对象。实例资源在生命周期中取得和释放。

## 消息与串行状态

```text
调用方 → Addr → Channel → 有界 mailbox → handler
```

消息按具体 Go 类型匹配 handler。一个 actor 的 handler 和任务完成回调串行执行，因此它们可以直接更新自己的状态；不同 actor 可并发运行。

| 调用 | 适用场景 | 成功意味着什么 |
| --- | --- | --- |
| `Send` | 提交无需结果的消息。 | 目标 mailbox 已受理，handler 可能尚未执行。 |
| `Call[R]` | 等待一次结果或业务操作完成。 | 得到类型为 R 的回复。 |
| `Stream[C]` | 边生成边消费多个分片。 | 分片消费完成，且生产方成功结束。 |

绑定类型决定可用模式，见[运行语义参考](runtime.zh.md#绑定与调用)。本地值不深拷贝，双方要约定 slice、map 和指针数据的所有权。不要在 handler 中同步等待自己，也不要形成同步 Call 环。

## 异步工作

耗时 I/O 或计算放进 `ctx.StartTask(work, complete, discard)`，让 handler 尽快返回：

```text
handler：读取状态、准备输入快照
    → work：在 worker 中执行耗时工作
    → complete：回到 actor 串行处理结果、更新状态或回复
```

worker 不能访问可变 actor 状态。等待期间 actor 可处理其他消息，因此结果是否仍然适用由业务判断。没有被接管的结果通过 discard 释放；取消只请求协作停止，任务结清和资源释放仍由所属 actor 管理。

一次延迟回复使用 `Reply`，多段输出使用 `StreamReply`。它们与任务执行是不同职责：StartTask 管工作，Reply / StreamReply 管结果交付。继续阅读 [profile](../examples/profile/README.zh.md)和 [chat](../examples/chat/README.zh.md)。

## 实例与生命周期

每次 Spawn 创建一个独立实例，等待 Started 成功后返回 Addr。Addr 固定指向该实例，实例退出后不会重绑到替代者；`IsZero()` 只判断零值，不探测运行状态。

| 操作或通知 | 职责 |
| --- | --- |
| Started / Stopping / Stopped | 作者初始化资源、解除工作阻塞、完成收尾。 |
| `Addr.Stop` / `Context.Stop` | 使用侧请求停止 / 作者侧请求自身停止。 |
| `Addr.Wait` | 确认原始本地实例完整退出，读取最终 Exit。 |
| `Runtime.Close` | 停止并等待所属实例，最后关闭 Channel。 |

`ctx.Spawn` 可在同一 Runtime 创建协作实例，但不会形成自动级联停止的父子关系。资源归属、启动失败和关闭顺序见[生命周期](lifecycle.zh.md)。

## 本地投递

Runtime 默认使用根包的 LocalChannel，通过 Channel 接口将请求送入目标 mailbox。本地投递直接传递普通 Go 值，调用方通过 Addr 使用实例，业务 handler 无需管理投递过程。

<a id="静态读取消息描述"></a>

静态读取消息清单等工具能力集中在[运行语义参考](runtime.zh.md#静态读取消息描述)，无需在第一次编写 Actor 时掌握。

下一步按[示例学习路径](../examples/README.zh.md)学习异步查询、流式输出和实例协作。
