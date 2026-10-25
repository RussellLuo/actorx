# Counter

English | [中文](README.zh.md)

A local counter showing how `Send` and `Call` share one actor mailbox.

`Runtime.Spawn` waits for Started and returns Addr; failure returns a zero address after rollback. The instance exclusively owns its state object; query state through messages.

## Run

Requires Go 1.27+. From the project root:

```sh
go run ./examples/counter
```

Expected output:

```text
counter: 5
reset: 0
```

## How it works

`Counter` keeps the count in a field. `Send(Add{2})` queues an update; the following `Call[int](Add{3})` returns `5`. Calling `Reset` and then `Get` confirms the count is zero.

The `//actorx:actor` marker selects public handlers for generated bindings. After changing handlers, run `go generate ./examples/counter` from the project root to update `actorx.gen.go`.

See [Getting started](../../docs/getting-started.md) to create an application module. Tools that need message metadata can consult the [metadata reference](../../docs/runtime.md#reading-message-metadata) and [runnable example](describe_test.go).
