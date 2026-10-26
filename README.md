# Actorx

English | [中文](README.zh.md)

Actorx is an actor framework for Go that helps you build stateful components that collaborate through messages.

Components process messages in order, with support for asynchronous tasks, streaming results, and lifecycle management from startup through complete shutdown.

## Get started

A counter keeps its count in an object. Send it a message to add a value and receive the new total.

```go
import "github.com/go-aie/actorx"

type Add struct{ N int }

//actorx:actor
type Counter struct{ n int }

func (c *Counter) Add(_ actorx.Context, msg Add) (int, error) {
    c.n += msg.N
    return c.n, nil
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

The example uses generated message bindings. See [Getting started](docs/getting-started.md) for project setup and a complete program, or the [counter example](examples/counter/README.md) to run it from the checkout. Follow the [example guide](examples/README.md) for asynchronous lookup, streaming, collaboration, and resource management.

## Learn more

| Document | Contents |
| --- | --- |
| [Getting started](docs/getting-started.md) | Write an actor, call it through messages, and run the complete example. |
| [Core concepts](docs/concepts.md) | How components collaborate through messages and own their state and tasks. |
| [Runtime reference](docs/runtime.md) | Rules for message handling, asynchronous tasks, errors, and capacity limits. |
| [Lifecycle](docs/lifecycle.md) | Component initialization, shutdown, and resource cleanup. |

See the [documentation guide](docs/README.md) for further reading and validation instructions.

For distributed logical Actors, see [Actorx Distributed](dist/README.md): NATS messaging, conditional ownership, fixed generations and explicit recovery.
