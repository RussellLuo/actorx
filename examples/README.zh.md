# Actorx 学习路径

[English](README.md) | 中文

首次使用先完成[快速入门](../docs/getting-started.zh.md)，概念和语义查阅见[文档目录](../docs/README.zh.md)。

需要 Go 1.27+，以下命令均在项目根目录运行。核心示例无需服务端、网络、模型或凭据。

| 顺序 | 示例 | 新增知识 | 运行 |
| --- | --- | --- | --- |
| 1 | [counter](counter/README.zh.md) | 状态归属、串行消息、Send / Call、生成绑定 | `go run ./examples/counter` |
| 2 | [profile](profile/README.zh.md) | `ctx.StartTask`、串行完成回调、延迟回复 | `go run ./examples/profile` |
| 3 | [chat](chat/README.zh.md) | StreamReply、逐段消费与取消 | `go run ./examples/chat` |
| 4 | [lifecycle](lifecycle/README.zh.md) | 生命周期通知、State、Stop / Wait、自身停止 | `go run ./examples/lifecycle` |
| 5 | [composition](composition/README.zh.md) | 创建其他 actor、通过地址协作、显式回收依赖 | `go run ./examples/composition` |

每个示例的业务定义和运行入口集中在 `main.go`，生成绑定单独保存在 `actorx.gen.go`。lifecycle 的 README 另附自身停止的短代码片段；并发、取消与退出边界由根目录的核心测试验证。

## 容量与背压

根目录的 [example_limits_test.go](../example_limits_test.go) 包含两个可执行示例，建议在 counter 与 chat 之后阅读：

- `Example_mailbox`：设置 `WithMailboxCapacity(1)`，第一个任务正在执行，第二个填满队列，第三次 Send 返回 `ErrFull`。受控阻塞仅用于观察队列，业务 I/O 应放入 `ctx.StartTask` 的 worker。
- `Example_stream`：设置 `WithStreamLimits(1, 1)`，第一个分片停留在消费回调，第二个填满缓冲，第三次写入必须等待消费继续。生产过程在 `ctx.StartTask` 的 worker 中执行。

同步信号建立执行顺序，无需 sleep。流的字节数由生产者声明，本例使用 `len(text)`。在项目根目录运行：

```sh
go test -v . -run '^Example_(mailbox|stream)$'
```

## 资源归属

[ExampleContext_StartTask](../example_task_test.go) 在任务 worker 中取得临时文件，在串行完成回调中接管，再由 Stopped 释放。未能接管的结果通过 discard 回滚。两条路径共用幂等释放操作，也覆盖完成回调保存文件后 panic 的情况。相关失败边界见[运行语义](../docs/runtime.zh.md)。

```sh
go test -v . -run '^ExampleContext_StartTask$'
```

```text
resource acquired
resource released
```

## 更新与验证

修改标记 actor 的 handler 后运行 `go generate ./examples/...`。运行 `go test -race -timeout 60s ./...` 和 `go vet ./...`，检查核心、生成器和可执行测试示例，再运行上表中的程序。主线示例由生成器提供消息绑定；直接控制绑定的用法见[手写绑定参考](../docs/runtime.zh.md#手写绑定)。
