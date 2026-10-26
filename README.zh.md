# Actorx

[English](README.md) | 中文

Actorx 是一个 Go Actor 框架，帮助你构建通过消息协作的有状态组件。

它让组件按顺序处理消息，支持异步任务和流式结果，并提供完整的启动与退出管理。

## 开始使用

以计数器为例：用一个对象保存计数，通过消息请求它累加并返回结果。

```go
import "github.com/go-aie/actorx"

type Add struct {
    N int
}

//actorx:actor
type Counter struct {
    count int
}

func (c *Counter) Add(_ actorx.Context, msg Add) (int, error) {
    c.count += msg.N
    return c.count, nil
}
```

`Counter` 保存状态，`Add` 定义消息，具名方法处理对应消息。创建实例后，通过地址与它通信。下面省略常规错误处理：

```go
ctx := context.Background()
rt, _ := actorx.NewRuntime(ctx)
defer rt.Close(ctx)

addr, _ := rt.Spawn(&Counter{})
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 5
```

每个 Actor 按顺序处理消息，业务状态在自己的处理方法中更新。

示例使用生成的消息绑定，项目准备和完整程序见[快速入门](docs/getting-started.zh.md)，仓库内运行方式见[计数器示例](examples/counter/README.zh.md)。通过[示例学习路径](examples/README.zh.md)，继续了解异步查询、流式输出、组件协作和资源管理。

## 深入了解

| 文档 | 内容 |
| --- | --- |
| [快速入门](docs/getting-started.zh.md) | 编写 Actor，通过消息调用它，并运行完整示例。 |
| [核心概念](docs/concepts.zh.md) | 组件如何通过消息协作，状态与任务由谁管理。 |
| [运行语义](docs/runtime.zh.md) | 消息处理、异步任务、错误和容量控制的具体约定。 |
| [生命周期](docs/lifecycle.zh.md) | 组件初始化、停止与资源清理。 |

更多阅读入口和验证说明见[文档目录](docs/README.zh.md)。

分布式逻辑 Actor 见 [Actorx Distributed](dist/README.zh.md)：NATS 通信、条件归属、固定代次和显式恢复。

## 许可证

[MIT](LICENSE)
