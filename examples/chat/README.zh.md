# 流式聊天

[English](README.md) | 中文

逐词输出模拟回答。无需模型、网络或凭据。

需要 Go 1.27+，在项目根目录运行：

```sh
go run ./examples/chat
```

```text
answer: Actors use messages to coordinate work.
```

生产方通过 `StreamReply.Write` 写入分片，用 `Complete` 结束流。调用方用 `for chunk, err := range addr.Stream[TextChunk](ctx, ask)` 逐段消费，无需先收集成一个完整回复。开始迭代才启动请求；建立与终态错误通过错误项交付，部分输出后仍可能失败。

`Write` 可能等待慢速消费者，因此生产过程放在 `actorx.StartTask` 中执行。其用法由 [profile](../profile/README.zh.md) 介绍，本例在此基础上增加流式交付，不再引入计时器或额外任务状态。

需要提前停止时，可用 `break`、外层函数 `return` 或取消传给 `Stream` 的 context。提前退出会请求取消并清理消费侧资源，不证明生产方已经结束；`Runtime.Close` 会等待所属工作结束。迭代器复用与循环体取消边界见 [Stream 语义](../../docs/runtime.zh.md#stream)。

修改 handler 后，运行 `go generate ./examples/chat`。

有界窗口与慢消费者的同步演示见[容量与背压专题](../README.zh.md#容量与背压)。
