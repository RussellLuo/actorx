# Actorx

English | [中文](README.zh.md)

Actorx is an actor framework for Go that helps you build stateful components that collaborate through messages.

Components process messages in order, with support for asynchronous tasks, streaming results, and lifecycle management from startup through complete shutdown.

## Get started

A counter keeps its count in an object. Send it a message to add a value and receive the new total.

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

`Counter` holds state, `Add` defines a message, and a named method handles that message. Create an instance and communicate through its address. Routine error handling is omitted here:

```go
ctx := context.Background()
rt, _ := actorx.NewRuntime(ctx)
defer rt.Close(ctx)

addr, _ := rt.Spawn(&Counter{})
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 5
```

Each actor processes messages in order and updates its state inside its own handlers.

The example uses generated message bindings. See [Getting started](docs/getting-started.md) for project setup, binding generation, and steps to run the complete program.

## Learn more

| Document | Contents |
| --- | --- |
| [Core concepts](docs/concepts.md) | Understand actors, messages, addresses, and ownership of state and tasks. |
| [Example guide](examples/README.md) | Start with a counter, then learn asynchronous tasks, streaming, lifecycle, and collaboration. |
| [Runtime reference](docs/runtime.md) | Look up message handling, cancellation, errors, and capacity limits. |
| [Lifecycle](docs/lifecycle.md) | Learn about component initialization, shutdown, and resource cleanup. |

See the [documentation guide](docs/README.md) for the full documentation.

## Distributed extension

For distributed logical Actors, see [Actorx Distributed](https://github.com/go-aie/actorx-dist): NATS messaging, conditional ownership, fixed generations and explicit recovery.

## License

[MIT](LICENSE)
