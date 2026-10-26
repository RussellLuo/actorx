# 快速入门

[English](getting-started.md) | 中文

下面用一个计数器说明如何编写 Actor，并通过消息调用它：计数器保存当前计数，收到累加消息后更新状态，再把结果返回给调用方。

需要 Go 1.27+，无需外部服务。在空目录中准备示例：

```sh
mkdir counter-app
cd counter-app
go mod init example.com/counter-app
go get github.com/go-aie/actorx
```

## 1. 定义消息和 Actor

`Add` 表示一次累加请求，`Counter` 保存计数。将以下代码保存为 `main.go`：

```go
//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/go-aie/actorx"
)

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

`Counter.Add` 接收 `Add` 消息，将 `N` 加到当前计数上，并返回新的计数。首参 `actorx.Context` 由运行时提供；这个例子不需要使用它。

`//actorx:actor` 标记让生成器为 `Counter` 的公开处理方法生成消息绑定。你只需编写状态、消息和处理方法，绑定代码在后面的步骤中生成。

## 2. 创建实例并调用

应用通过 `Runtime` 创建和管理 Actor 实例。`Spawn` 返回实例的地址，调用方通过地址发送消息。在同一个 `main.go` 中继续添加：

```go
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

`Call[int]` 发送 `Add{N: 5}`，等待处理方法返回整数结果。同一实例的消息串行执行；后续调用会继续使用该实例保存的计数。对象交给 `Spawn` 后，通过消息访问它的状态。

`defer rt.Close(ctx)` 在应用结束时关闭运行时，停止并等待它管理的实例退出。示例把错误返回到 `main`，在完成清理后统一报告。

## 3. 生成绑定并运行

文件开头的 `go:generate` 指令调用 actorx-gen。执行以下命令，生成 `actorx.gen.go`，再运行程序：

```sh
go generate .
go run .
```

预期输出：

```text
counter: 5
```

生成文件把 `Counter.Add` 绑定到 `Add` 消息，与 `main.go` 一起编译。

<a id="可选生成绑定"></a>

## 更新处理方法

新增或修改公开处理方法后，重新运行 `go generate .` 更新绑定，再运行程序。辅助公开方法可用 `//actorx:ignore` 排除；更多选择和签名规则见[生成器参考](runtime.zh.md#生成器参考)。

## 继续学习

仓库内的 [counter](../examples/counter/README.zh.md) 在同一场景中增加了查询、重置和 `Send` 调用。阅读[核心概念](concepts.zh.md)，了解状态、地址、任务和 Runtime 的关系后，可以按需要继续运行：

- [profile](../examples/profile/README.zh.md)：在异步任务中查询数据，完成后回复。
- [chat](../examples/chat/README.zh.md)：逐段返回结果并支持取消。
- [lifecycle](../examples/lifecycle/README.zh.md)：观察启动、停止和资源清理。
- [composition](../examples/composition/README.zh.md)：创建另一个 Actor，通过地址协作并回收实例。
