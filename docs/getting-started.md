# Getting started

English | [中文](getting-started.zh.md)

This counter shows how to write an actor and call it through messages. It keeps a count, updates it when an addition message arrives, and returns the result to the caller.

Requires Go 1.27+, with no external service needed. Start in an empty directory:

```sh
mkdir counter-app
cd counter-app
go mod init example.com/counter-app
go get github.com/go-aie/actorx
```

## 1. Define a message and an actor

`Add` represents an addition request, and `Counter` holds the count. Save the following as `main.go`:

```go
//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/go-aie/actorx"
)

type Add struct{ N int }

//actorx:actor
type Counter struct{ n int }

func (c *Counter) Add(_ actorx.Context, msg Add) (int, error) {
    c.n += msg.N
    return c.n, nil
}
```

`Counter.Add` receives an `Add` message, adds `N` to the count, and returns the new count. The runtime supplies the first argument, `actorx.Context`; this example does not need to use it.

The `//actorx:actor` marker selects Counter's public handlers for binding generation. You write the state, messages, and handler methods; a later step generates the bindings.

## 2. Create an instance and call it

The application uses a `Runtime` to create and manage actor instances. `Spawn` returns an instance's address, which the caller uses to send messages. Continue in the same `main.go`:

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

`Call[int]` sends `Add{N: 5}` and waits for an integer result from the handler. Messages to the same instance execute serially, and later calls continue with its saved count. Once the object is passed to `Spawn`, access its state through messages.

`defer rt.Close(ctx)` closes the runtime when the application finishes, stopping and waiting for its instances to exit. The example returns errors to `main` so they are reported after cleanup.

## 3. Generate bindings and run

The file's `go:generate` directive invokes actorx-gen. Generate `actorx.gen.go`, then run the program:

```sh
go generate .
go run .
```

Expected output:

```text
counter: 5
```

The generated file binds `Counter.Add` to the `Add` message and compiles alongside `main.go`.

<a id="optional-generate-bindings"></a>

## Update handlers

After adding or changing public handlers, rerun `go generate .` to update the bindings before running the program. Exclude public helpers with `//actorx:ignore`; see the [generator reference](runtime.md#generator-reference) for more selection and signature rules.

## Continue learning

The repository's [counter](../examples/counter/README.md) extends the same scenario with queries, resets, and `Send`. Read [Core concepts](concepts.md) for the relationships between state, addresses, tasks, and Runtime, then explore these examples as needed:

- [profile](../examples/profile/README.md): fetch data in an asynchronous task and reply on completion.
- [chat](../examples/chat/README.md): stream results and support cancellation.
- [lifecycle](../examples/lifecycle/README.md): observe startup, shutdown, and resource cleanup.
- [composition](../examples/composition/README.md): create another actor, collaborate through its address, and stop the instance.
