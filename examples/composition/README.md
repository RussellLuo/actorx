# Actor composition

English | [中文](README.zh.md)

A Session creates a dedicated Worker, asks it to uppercase a message, and explicitly stops it when the session ends. Read [lifecycle](../lifecycle/README.md) first.

From the project root, with Go 1.27+:

```sh
go run ./examples/composition
```

```text
worker: HELLO
worker stopped
session stopped
```

`Session.Started` uses `ctx.Spawn` to create Worker in the same Runtime. Session stores its `Addr` and communicates through a typed Call; neither actor directly accesses the other's state. The dependency points from Session to Worker, with no synchronous Call cycle. Spawn waits for Started; it does not establish readiness of asynchronous work an application might start there.

Creation does not establish automatic shutdown propagation. `Session.Stopping` explicitly stops Worker and waits for its complete exit before Session exits. It uses an independent, bounded context because the stopping context is already canceled. This wait is for another actor that does not depend on Session; waiting for oneself would deadlock. Runtime.Close remains responsible for shutting down all instances when the application exits.

See [main.go](main.go) for the complete flow. After changing handlers, run `go generate ./examples/composition`.
