# Getting started

English | [中文](getting-started.zh.md)

This complete counter connects the actor author's work with the caller's work. Requires Go 1.27+, with no external service needed.

## Create an application module

Use a local Actorx checkout. Replace `/absolute/path/to/actorx` with its absolute path:

```sh
mkdir counter-app
cd counter-app
go mod init example.com/counter-app
go mod edit -require=github.com/RussellLuo/actorx@v0.0.0
go mod edit -replace=github.com/RussellLuo/actorx=/absolute/path/to/actorx
```

`v0.0.0` is a local placeholder; replace points the application at current source. These steps do not validate a released dependency. To consume a release, select an actual version containing the required APIs, remove replace, and verify separately.

Save the source below as `main.go`, then generate its message bindings and run it:

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

Generate and run:

```sh
go generate .
go run .
```

`go generate .` runs actorx-gen through the file directive and creates `actorx.gen.go`, connecting handler methods to message types. This generated file compiles alongside `main.go`; the type marker alone does not generate code.

Expected output:

```text
counter: 5
```

## Author: state, messages, and handlers

`Counter.n` is private instance state; `Add` is an ordinary Go message. The `Counter.Add` handler updates state and returns a result. The runtime supplies its first argument.

`//actorx:actor` selects this type's public handlers. actorx-gen uses their message parameters and return values to generate bindings, making Counter satisfy `actorx.Actor`. Business code maintains messages, state, and handler methods.

An actor handles messages serially and updates state through its own handlers. Local messages are not deep-copied; callers and handlers must agree on ownership when using maps, slices, or pointers.

## Caller: create, call, and close

1. `NewRuntime(ctx)` creates the runtime.
2. `Spawn(&Counter{})` creates a counter instance and returns its address. Use messages afterward; do not access mutable fields or Spawn the same object again.
3. `Call[int](ctx, Add{N: 5})` requests an addition of 5 and waits for the integer result.
4. `defer rt.Close(ctx)` closes the runtime and cleans up its instances when the application finishes.

The example checks the error from each step. See the [runtime reference](runtime.md) for more call modes and error rules.

<a id="optional-generate-bindings"></a>

## Update handlers

After adding or changing public handlers, rerun `go generate .` in the application directory before running the application. Generated bindings live in `actorx.gen.go`; the marker and generation directive stay in business source.

Exclude public helpers with `//actorx:ignore`. See the [generator reference](runtime.md#generator-reference) for selection, signatures, and generated-file checks. For direct control of low-level bindings, see the optional [handwritten bindings](runtime.md#handwritten-bindings) reference.

## Continue learning

Read [Core concepts](concepts.md), then run [profile](../examples/profile/README.md), [chat](../examples/chat/README.md), [lifecycle](../examples/lifecycle/README.md), and [composition](../examples/composition/README.md). They add owned tasks, streaming, lifecycle, and actor collaboration.

The repository's [counter](../examples/counter/README.md) adds more messages.
