# Actorx 文档

[English](README.md) | 中文

首次使用从[快速入门](getting-started.zh.md)开始，完成一个 Actor 的编写、调用和关闭。核心需要 Go 1.27+，仅依赖标准库。

| 文档 | 阅读目的 |
| --- | --- |
| [快速入门](getting-started.zh.md) | 从独立项目开始，编写 Actor、生成绑定并运行完整计数器。 |
| [核心概念](concepts.zh.md) | 理解状态、消息、地址、异步任务与 Runtime 的归属。 |
| [运行语义参考](runtime.zh.md) | 查阅 handler 选择、取消、错误、背压、静态描述与生成器约定。 |
| [生命周期](lifecycle.zh.md) | 初始化资源、创建协作实例、停止并确认完整退出。 |

学习主线是 **快速入门 → 核心概念 → 示例**。运行语义和生命周期按需查阅。

[示例学习路径](../examples/README.zh.md)依次提供 counter、profile、chat、lifecycle 和 composition，另有容量、背压与资源交接专题。每个示例都有运行命令与预期输出。

## 当前范围

核心提供有界串行执行、类型化绑定、异步任务、延迟回复、流和本地生命周期管理。

监督、自动重启、逻辑身份发现、自动激活和放置不属于当前核心能力。

## 仓库验证

在仓库根目录运行：

```sh
go test -race -timeout 60s ./...
go vet ./...
```

这些命令运行核心与生成器测试、可执行测试示例及静态检查。完整示例程序的运行方式见[示例学习路径](../examples/README.zh.md)。

分布式逻辑身份及激活属于独立验收的 [Actorx Distributed 模块](https://github.com/go-aie/actorx-dist/blob/main/README.zh.md)。核心 [Channel 扩展接口](runtime.zh.md#channel-扩展接口)集中在运行语义参考中。
