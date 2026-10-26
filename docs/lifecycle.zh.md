# 生命周期

[English](lifecycle.md) | 中文 · [文档目录](README.zh.md)

作者通过 Started / Stopping / Stopped 管理实例资源，调用方通过 Stop / Wait 管理一个原始本地实例，通过 Runtime.Close 关闭整个运行时。可观察的完整程序见[生命周期示例](../examples/lifecycle/README.zh.md)。

## 状态与启动

`Runtime.Spawn(value)` 安装绑定，等待 Started 成功后才开放普通投递并返回 Addr。失败则等待完整回滚，返回零 Addr 和最终错误；nil、typed nil、Bindings panic、无效绑定等装配问题匹配 ErrSetup。

| `ctx.State()` | 含义 |
| --- | --- |
| `StateStarted` | 收件绑定就绪，执行 Started，尚未开放普通投递。 |
| `StateRunning` | 处理普通 handler 与串行任务完成回调。 |
| `StateStopping` | 已关闭受理，当前 handler 返回后进入退出流程。 |
| `StateStopped` | 自有工作、回滚和解绑完成，执行最终 Stopped。 |

状态是并发安全的观察快照，不替代投递检查，也不表示业务 Ready。StateStopped 可能先于 Stopped 返回，**完整退出以 Wait 为准**。无 actor 关联的 Context 返回 StateStopped。

Started 使用实例生命周期 context，可启动 StartTask，但不能等待自己的 mailbox、任务完成回调或自身退出。完成回调等到 Running 才执行；Spawn 不等待异步业务初始化，需要时由应用另行定义就绪协议。

进入 Started 前失败不执行生命周期，仅回滚运行时资源。一旦进入启动阶段，即使没有 Started handler 或 Started error / panic，也会执行 Stopping 和 Stopped，清理需容忍部分初始化。启动期间 Stop / Close 会取消 context，等待用户代码返回后回滚。Spawn 没有单独的超时参数，无法强制打断用户代码。

## 绑定生命周期通知

普通具名方法接收对应事件类型，签名统一为 `func(actorx.Context, Event) error`：

```go
func (s *Session) Started(ctx actorx.Context, _ actorx.Started) error {
    s.messages = 0
    return nil
}

func (s *Session) Stopping(ctx actorx.Context, msg actorx.Stopping) error {
    // 解除 worker 的阻塞，不在这里等待这些 worker。
    return nil
}

func (s *Session) Stopped(ctx actorx.Context, msg actorx.Stopped) error {
    // worker、未交接结果回滚和收件解绑已结束，完成最终收尾。
    return nil
}
```

与普通 handler 一样，生成器根据事件参数类型为生命周期方法生成绑定。沿用 `//actorx:actor` 或 `//actorx:handler` 选择规则，修改后重新生成即可；方法名推荐与事件一致，也允许自定义名称。底层接线见[手写绑定参考](runtime.zh.md#手写绑定)。

每阶段最多一个 handler，均可省略，也允许只有生命周期绑定的实例。指针事件、业务返回值、延迟回复和 Stream 签名无效。

通知由 Runtime 按阶段直接分发，与业务 handler 串行执行，不占 mailbox 或调用名额，不会抢占活跃 handler。它们不能通过 Addr / Channel 投递，也不属于普通 CallDescriptor。作为普通消息查找时返回 ErrUnhandled，其他取消、停止与容量检查仍可能先失败。

## 通过地址或 handler context 停止

`addr.Stop(cause)` 关闭受理并请求取消，不等待退出；返回 nil 只表示停止请求已受理。它不依赖业务消息、mailbox 或调用容量。自定义 Stop / Shutdown / Logout 消息仍是普通业务消息，不隐式触发停止。

原始本地地址通过所属 Runtime 提供 Stop / Wait，普通 Go 值复制保留两种能力。ExportAddr / ImportAddr 只传实例身份，导回源 Runtime 也不恢复能力。导入地址调用 Stop / Wait 返回说明原因的普通错误，不依赖精确文本分类。

| 情况 | Stop 结果 |
| --- | --- |
| Runtime 尚未关闭，重复停止同一实例，包括其退出后 | 返回 nil，保留首次停止原因。 |
| 所属 Runtime 已开始关闭 | ErrClosed。 |
| 零值或非法地址 | ErrStopped。 |

作者可在 Started、普通 handler 或串行完成回调中调用 `ctx.Stop(cause)` 请求自身停止。它不等待、不打断后续代码，应及时返回；当前 Call / Stream 会以 ErrStopped 结清。Stopping / Stopped 中重复调用不覆盖首次原因，nil 或无 actor 关联的 Context 安全无操作。

## 通过地址等待退出

```go
if err := addr.Stop(nil); err != nil {
    return err
}
exit, err := addr.Wait(ctx)
if err != nil {
    return err // 本次等待失败，尚未确认完整退出。
}
return exit.Reason
```

`Wait(ctx) (Exit, error)` 被动等待，不会停止实例。等待成功时 error 为 nil，Exit.Reason 是正常退出的 nil 或最终失败原因。例如实例以 DeadlineExceeded 退出，该错误位于 Exit.Reason，而不是等待 error。

原始地址支持并发、重复、晚到等待，Runtime.Close 后仍可读取结果。一个等待者取消不影响其他等待者，可换 context 重试；已发布结果优先于已取消 context。零值或非法地址返回 ErrSetup，导入地址缺少能力，如上所述。

不要在 handler 内 Wait 自身。无法返回的 handler、worker、discard 或 Stopped 都会阻止完整退出。Stopped.Reason 仅为进入该阶段的快照，最终原因以 Wait 为准。共享 error 与 PanicError 的可变诊断数据由调用方只读使用。

## 停止顺序

Stop 关闭入口、取消自有工作并结清未决请求；排队业务消息不再执行。随后依次：

1. 等待当前 Started / handler 返回，并完成请求取消与结果结清。
2. 执行 Stopping，解除 worker 阻塞。
3. 等待自有 worker，discard 回滚未交接结果。
4. 释放底层收件绑定。
5. 进入 StateStopped，执行 Stopped。
6. 固定最终 Exit.Reason，发布 Wait 的完成结果。

Stopping / Stopped 的 error 或 panic 会加入最终原因，后续清理继续。退出 handler 收到已取消的 context，不能启动 StartTask 或等待自身 mailbox；外部收尾 I/O 应直接使用独立、有界的 context。Stream 调用方回调和缓冲消费由调用方管理，不阻止 actor 本身退出。

## 实例资源所有权

| 时点 | 责任 |
| --- | --- |
| 注入共享依赖 | 应用拥有并释放，实例不关闭共享依赖。 |
| Started | 取得实例资源并及时保存字段；尚未交接的局部资源用 defer 回滚。 |
| Stopping | 容忍部分初始化，取消业务工作并解除 worker 阻塞，操作保持有界。 |
| Stopped | 在 worker、discard 和解绑后释放最终业务资源。 |
| 进入启动前失败 | 不执行生命周期；构造时提前取得的独占资源由调用方释放，或将获取移到 Started。 |

Bindings 不取得资源。框架不提供业务清理登记表；多资源的释放顺序和单个 handler 内的异常隔离用普通 Go 控制流与 defer 安排。

任务结果交接和 discard 规则见[运行语义](runtime.zh.md#异步任务与延迟回复)。提前保存结果后 panic 时，discard 与生命周期释放要共享幂等操作；调用方的失败释放与启动回滚同理。完整资源接线见[真实文件示例](../example_task_test.go)。

## 在 handler 内创建 actor

```go
func (s *Session) Started(ctx actorx.Context, _ actorx.Started) error {
    worker, err := ctx.Spawn(&Worker{})
    if err != nil {
        return err // Worker 已完整回滚；Session 选择传播失败。
    }
    s.worker = worker
    return nil
}
```

`Context.Spawn` 与 Runtime.Spawn 共用启动路径，在同一 Runtime 创建独立实例。只从 Started、普通 handler 或串行完成回调调用；创建期间暂停当前执行，不重入。目标初始化不得同步依赖创建者的后续执行，避免启动、Call 或 Wait 循环。

单次请求取消或创建者停止不会打断创建等待，也不会自动停止目标。目标启动失败是否传播由业务选择，专属依赖需显式 Stop / Wait；Runtime.Close 覆盖全部所属实例。成功启动结果一旦固定，即使实例在 Spawn 返回前退出，也返回成功，最终原因通过 Wait 读取。见 [composition](../examples/composition/README.zh.md)。

nil 或无归属 Context 返回 ErrSetup；创建者正在停止或已停止返回 ErrStopped，否则 Runtime 已关闭返回 ErrClosed。

## 关闭 Runtime

`rt.Close(ctx)` 拒绝新 Spawn 与投递，停止并等待全部所属实例，包括正在绑定或执行 Started 的 Spawn，随后关闭 Channel。成功认领的 Channel 由 Runtime 独占管理，应用通过 Runtime 关闭；Open 失败由 Channel 自行回滚，Runtime 不关闭未认领的 Channel。

Close 超时只结束本次等待，后台关闭继续，可再次 Close 等待。超时错误匹配 `ctx.Err()`，包含未退出实例 ID、阶段和队列/请求/任务计数供定位阻塞。取消不等于工作完成或资源清理结束。

Close 返回关闭等待或 Channel 关闭错误，不汇总各 actor 的 Exit.Reason；需要观察业务退出原因时保留原始地址并 Wait。监督、自动重启、远程死亡监控、Ready 与 Drain 协议不属于当前生命周期 API。
