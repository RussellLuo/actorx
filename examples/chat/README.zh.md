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

生产方通过 `StreamReply.Write` 写入分片，用 `Complete` 结束流。调用方在 `Addr.Stream` 的回调中逐段消费，无需先收集成一个完整回复。

`Write` 可能等待慢速消费者，因此生产过程放在 `ctx.StartTask` 中执行。其用法由 [profile](../profile/README.zh.md) 介绍，本例在此基础上增加流式交付，不再引入计时器或额外任务状态。

需要提前停止时，取消传给 `Stream` 的 context。取消只请求停止，`Runtime.Close` 会等待所属工作结束。

修改 handler 后，运行 `go generate ./examples/chat`。

有界窗口与慢消费者的同步演示见[容量与背压专题](../README.zh.md#容量与背压)。
