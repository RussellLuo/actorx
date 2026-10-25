# 计数器

[English](README.md) | 中文

本地计数器，演示 `Send` 与 `Call` 共用同一个 actor mailbox。

`Runtime.Spawn` 等待 Started 成功后返回 Addr；失败完成回滚后返回零值地址。业务对象交给实例后由其独占，通过消息查询状态。

## 运行

需要 Go 1.27+。在项目根目录执行：

```sh
go run ./examples/counter
```

预期输出：

```text
counter: 5
reset: 0
```

## 工作流程

`Counter` 用字段保存计数。`Send(Add{2})` 将更新入队，随后 `Call[int](Add{3})` 返回 `5`；调用 `Reset` 后再用 `Get` 确认计数归零。

`//actorx:actor` 标记让生成器为公开 handler 生成绑定。修改 handler 后，在项目根目录运行 `go generate ./examples/counter` 更新 `actorx.gen.go`。

首次创建独立项目见[快速入门](../../docs/getting-started.zh.md)。工具需要读取消息清单时，可选读[静态描述参考](../../docs/runtime.zh.md#静态读取消息描述)及其[可运行示例](describe_test.go)。
