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

示例使用生成的消息绑定。项目配置、绑定生成和完整运行步骤见[快速入门](docs/getting-started.zh.md)。

## 深入了解

| 文档 | 内容 |
| --- | --- |
| [核心概念](docs/concepts.zh.md) | 理解 Actor、消息、地址，以及状态和任务的归属。 |
| [示例学习路径](examples/README.zh.md) | 从计数器开始，学习异步任务、流式输出、生命周期和组件协作。 |
| [运行语义](docs/runtime.zh.md) | 查阅消息处理、取消、错误和容量限制。 |
| [生命周期](docs/lifecycle.zh.md) | 了解组件初始化、停止与资源清理。 |

完整文档见[文档目录](docs/README.zh.md)。

## 分布式扩展

需要分布式逻辑 Actor 时，参见 [Actorx Distributed](https://github.com/go-aie/actorx-dist)：NATS 通信、条件归属、固定代次和显式恢复。

## 许可证

[MIT](LICENSE)
