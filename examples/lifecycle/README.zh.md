# 生命周期

[English](README.md) | 中文

本地会话示例，演示生命周期通知、状态查询与显式停止。

## 运行

需要 Go 1.27+。在项目根目录执行：

```sh
go run ./examples/lifecycle
```

预期输出：

```text
started: state=Started
spawn returned
message: hello, state=Running
stopping: state=Stopping, reason=<nil>
stopped: state=Stopped, messages=1, reason=<nil>
done: err=<nil>
```

## 工作流程

`Started` 在 Spawn 返回前初始化会话状态。调用方通过 `Call` 等待 `Receive` 处理一条消息，再请求停止。`Stopping` 在等待 worker 前运行；`Stopped` 在自有工作与资源清理结束后输出最终消息数。各方法通过 `ctx.State()` 查询当前状态。

`addr.Stop(nil)` 请求正常停止，并返回受理错误。`addr.Wait(ctx)` 等待包括最终消息 handler 在内的完整退出，将 `Exit.Reason` 与等待错误分开返回。原始本地地址的值复制支持重复和晚到等待，Runtime.Close 后仍可读取；导入地址不能 Stop / Wait。示例通过 Call 确定输出顺序，无需 sleep。

`main.go` 中的 `Logout` handler 调用 `ctx.Stop(nil)` 请求自身停止。可以把 `run` 中通过地址停止和等待退出的部分替换为：

```go
if err := addr.Send(context.Background(), Logout{}); err != nil {
    return err
}
exit, err := addr.Wait(context.Background())
if err != nil {
    return err
}
return exit.Reason
```

`Logout` 是普通业务消息，由 handler 显式调用 `ctx.Stop(nil)` 请求停止。调用方用 Send 投递，再用 Wait 确认退出；当前请求中的 Call 会以 ErrStopped 结清。多个 actor 的创建、通信和回收见 [composition](../composition/README.zh.md)。

示例统一使用 `Started / Stopping / Stopped` 方法名。生命周期阶段由通知类型决定，也支持自定义方法名。`//actorx:actor` 同时生成业务消息与生命周期通知绑定。两者共用 actor 的串行执行循环；生命周期通知在阶段边界直接分发，不入队。修改 handler 后，在项目根目录运行 `go generate ./examples/lifecycle`。

启动失败、`Stopped` 清理、I/O 归属和 handler 限制见[生命周期专题](../../docs/lifecycle.zh.md)。
