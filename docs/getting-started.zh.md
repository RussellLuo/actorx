# 快速入门

[English](getting-started.md) | 中文 · [文档目录](README.zh.md)

本页用一个完整计数器串起 Actor 作者侧与使用侧。需要 Go 1.27+，无需外部服务。

## 创建独立项目

下面使用已检出的 Actorx 源码。将 `/absolute/path/to/actorx` 改为仓库的绝对路径：

```sh
mkdir counter-app
cd counter-app
go mod init example.com/counter-app
go mod edit -require=github.com/RussellLuo/actorx@v0.0.0
go mod edit -replace=github.com/RussellLuo/actorx=/absolute/path/to/actorx
```

`v0.0.0` 是本地占位版本，replace 让应用消费当前源码。这套步骤不验证发布依赖；改用发布版本时，选择包含所需 API 的真实版本、移除 replace，再单独验证。

将下面的源文件保存为 `main.go`，随后生成消息绑定并运行：

```go
//go:generate go run github.com/RussellLuo/actorx/cmd/actorx-gen
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/RussellLuo/actorx"
)

type Add struct{ N int }

//actorx:actor
type Counter struct{ n int }

func (c *Counter) Add(_ actorx.Context, msg Add) (int, error) {
    c.n += msg.N
    return c.n, nil
}

func run() error {
    ctx := context.Background()
    rt, err := actorx.NewRuntime(ctx)
    if err != nil {
        return err
    }
    defer rt.Close(ctx)

    addr, err := rt.Spawn(&Counter{})
    if err != nil {
        return err
    }
    n, err := addr.Call[int](ctx, Add{N: 5})
    if err != nil {
        return err
    }
    fmt.Println("counter:", n)
    return nil
}

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}
```

生成并运行：

```sh
go generate .
go run .
```

`go generate .` 根据文件中的指令运行 actorx-gen，创建 `actorx.gen.go`，将处理方法绑定到消息类型。生成文件与 `main.go` 一起编译；类型标记本身不会自动生成代码。

预期输出：

```text
counter: 5
```

## 作者侧：状态、消息和处理方法

`Counter.n` 是实例私有状态，`Add` 是普通 Go 消息。handler `Counter.Add` 负责更新状态并返回结果，首参由框架提供。

`//actorx:actor` 选择这个类型的公开处理方法。actorx-gen 根据消息参数和返回值生成绑定，让 Counter 满足 `actorx.Actor` 接口；业务代码只需维护消息、状态和处理方法。

一个 actor 串行处理消息，由自己的处理方法更新状态。本地消息不深拷贝；使用 map、slice 或指针时，调用双方需要约定可变数据的归属。

## 使用侧：创建、调用和关闭

1. `NewRuntime(ctx)` 创建运行时。
2. `Spawn(&Counter{})` 创建计数器实例并返回地址。随后通过消息使用对象，不再直接读写字段，也不重复 Spawn 同一对象。
3. `Call[int](ctx, Add{N: 5})` 请求累加 5，等待并取得整数结果。
4. `defer rt.Close(ctx)` 在应用结束时关闭运行时并回收所属实例。

示例检查每一步返回的 error。更多调用模式和错误约定见[运行语义参考](runtime.zh.md)。

<a id="可选生成绑定"></a>

## 更新处理方法

新增或修改公开 handler 后，在应用目录重新运行 `go generate .`，再运行应用。`actorx.gen.go` 保存生成的绑定；类型标记与生成指令保留在业务源文件中。

辅助公开方法用 `//actorx:ignore` 排除。类型选择、签名和生成检查等约定见[生成器参考](runtime.zh.md#生成器参考)。需要直接控制底层绑定时，可选读[手写绑定](runtime.zh.md#手写绑定)。

## 继续学习

先阅读[核心概念](concepts.zh.md)，再依次运行 [profile](../examples/profile/README.zh.md)、[chat](../examples/chat/README.zh.md)、[lifecycle](../examples/lifecycle/README.zh.md)和 [composition](../examples/composition/README.zh.md)。它们分别增加异步任务、流、生命周期和实例协作。

仓库内扩展计数器见 [counter](../examples/counter/README.zh.md)。
