# Actor 协作

[English](README.md) | 中文

Session 创建专属 Worker，请它将消息转成大写，并在会话结束时显式回收它。建议先阅读 [lifecycle](../lifecycle/README.zh.md)。

需要 Go 1.27+，在项目根目录运行：

```sh
go run ./examples/composition
```

```text
worker: HELLO
worker stopped
session stopped
```

`Session.Started` 通过 `ctx.Spawn` 在同一 Runtime 创建 Worker。Session 保存其 `Addr`，通过类型化 Call 通信，双方不直接访问对方状态。依赖方向为 Session → Worker，没有同步 Call 环。Spawn 等待 Started 成功；如果应用在其中启动异步工作，其业务就绪需要另行确认。

创建关系不自动级联停止。`Session.Stopping` 显式停止 Worker，并等待其完整退出后再结束 Session。停止阶段的 context 已取消，因此这里使用独立且有期限的 context。等待目标是另一个不依赖 Session 的 actor，不能等待自身。应用退出时，Runtime.Close 仍负责关闭全部实例。

完整流程见 [main.go](main.go)。修改 handler 后运行 `go generate ./examples/composition`。
