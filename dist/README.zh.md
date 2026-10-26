# Actorx Distributed

[English](README.md) | 中文

Actorx Distributed（Go 包名 `dist`）是一个基于 [actorx](../README.zh.md) 的 Go 分布式 Actor 框架，支持逻辑寻址、按需激活、唯一归属、跨进程 Send/Call/Stream，以及停止和完整退出管理。

业务沿用 actorx 的有状态对象、串行消息处理、异步任务和流式结果。dist 通过 NATS 与 JetStream KV 管理通信和归属，让调用方按类型和业务 ID 访问对象。

## 开始使用

以跨进程计数器为例：Actor 保存计数，收到 Add 消息后累加并返回结果。

```go
import (
    "github.com/go-aie/actorx"
    "github.com/go-aie/actorx/dist"
)

type Add struct {
    N int `json:"n"`
}

//actorx:actor
type Counter struct {
    count int
}

func (c *Counter) Add(_ actorx.Context, msg Add) (int, error) {
    c.count += msg.N
    return c.count, nil
}

var CounterSpec = dist.ActorSpec[*Counter]{
    Type: "counter",
    New: func(dist.ActorKey) *Counter {
        return &Counter{}
    },
}
```

Counter 定义业务行为，CounterSpec 声明逻辑类型和构造函数。工作进程注册 Spec，并保持运行以处理后续请求；下面省略常规错误处理及等待退出的代码：

```go
ctx := context.Background()
worker, _ := dist.NewRuntime(ctx, dist.WithActor(CounterSpec))
defer worker.Close(ctx)
```

另一个调用进程创建自己的 Runtime，无需注册 Actor。以下假设已取得请求上下文 ctx，且目标 ID 尚未使用：

```go
client, _ := dist.NewRuntime(ctx)
defer client.Close(ctx)

addr, _ := client.Addr("counter", "orders-42")
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 5
```

`Addr("counter", "orders-42")` 只取得逻辑地址；首次调用才查找或激活对象，框架保证同一逻辑身份的唯一归属。地址选定一次激活后始终固定该代；实例退出后，使用新地址选择下一代。

Actor 内寻址使用 `dist.MustFrom(ctx).Addr(type, id)`，Self 可取得当前代回信地址，详见[核心概念](docs/concepts.zh.md)。上述代码需要生成 Bindings；完整定义、绑定和进程入口见 [Counter 示例](examples/counter/README.zh.md)。

## 实际运行

需要 Go 1.27。当前示例从本仓库源码运行，dist 通过 go.mod 的本地 replace 使用上一级目录的核心源码。迁移后的核心发布后，再固定其版本并移除 replace，以验证发布模块的使用。

先从 [NATS 官方 v2.10.18 发布页](https://github.com/nats-io/nats-server/releases/tag/v2.10.18)下载适合系统和架构的压缩包。以下以 macOS Apple Silicon 为例，下载、解压并将可执行文件加入当前终端 PATH；其他平台在同一发布页选择对应包：

```sh
curl -fL -o /tmp/nats-server.tar.gz \
  https://github.com/nats-io/nats-server/releases/download/v2.10.18/nats-server-v2.10.18-darwin-arm64.tar.gz
tar -xzf /tmp/nats-server.tar.gz -C /tmp
export PATH="/tmp/nats-server-v2.10.18-darwin-arm64:$PATH"
```

进入本仓库的 dist 目录，启动开发服务。配置已开启 JetStream，监听本机4222端口；存储目录须可写：

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

保持服务运行，在另外两个终端中同样进入 dist 目录：

```sh
# 终端 2：启动工作进程，等待 "Counter worker ready"。
go run ./examples/counter -role worker
# 终端 3：运行调用方，新 ID 依次输出 5、10。
go run ./examples/counter -role client -id orders-42
```

client 每轮先 Send 增加2，再 Call 增加3并返回累计值，共执行两轮，因此输出5、10。同一 worker 仍存活时，使用相同 ID 再运行 client，输出15、20。可再启动一个 worker，共同提供该类型的创建能力；Actor 间组合调用见 [Client → Button → Counter](examples/click/README.zh.md)。

client 关闭不停止远端对象；worker 用 Ctrl-C 优雅关闭并等待所属实例退出。状态保存在 worker 内存中，实例退出后不保留。失联时没有自动接管，强制退出后的归属恢复见[部署指南](docs/deployment.zh.md#显式恢复与存储修复)。

## 深入了解

| 文档 | 内容 |
| --- | --- |
| [核心概念](docs/concepts.zh.md) | 逻辑对象、激活代次、地址与上下文。 |
| [调用语义](docs/runtime.zh.md) | Send/Call/Stream、期限、错误和容量。 |
| [生命周期](docs/lifecycle.zh.md) | Stop/Wait、对象寿命和关闭责任。 |
| [部署指南](docs/deployment.zh.md) | 配置、权限、契约升级与显式恢复。 |

实现细节见[架构与核心接入](docs/architecture.zh.md)、[wire 契约与资源限制](docs/contract.zh.md)、[Registry 协议](docs/registry.zh.md)。真实发布版本消费、实际跨主机网络与复制配置尚未验证。
