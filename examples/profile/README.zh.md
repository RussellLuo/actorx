# 用户资料查询

[English](README.md) | 中文

查询模拟的远程用户目录，一次返回完整资料。无需网络或凭据。

需要 Go 1.27+，在项目根目录运行：

```sh
go run ./examples/profile
```

```text
profile: alice (alice@example.com)
```

`Directory.Lookup` 用 `ctx.StartTask` 发起查询，随后返回，让 actor 继续处理消息。完成回调在 actor 上串行执行，清除加载状态并提交延迟回复。查询期间 `GetStatus` 仍能响应。同一时间只接受一次查询。

流式回答与提前取消见 [chat](../chat/README.zh.md)。修改 handler 后，运行 `go generate ./examples/profile`。
