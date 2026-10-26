# Learning Actorx

English | [中文](README.zh.md)

Start with [Getting started](../docs/getting-started.md). Use the [documentation guide](../docs/README.md) for concepts and reference pages.

Requires Go 1.27+. Run these commands from the project root. The core examples need no services, network, model, or credentials.

| Order | Example | New concept | Run |
| --- | --- | --- | --- |
| 1 | [counter](counter/README.md) | State ownership, serial messages, Send / Call, generated bindings | `go run ./examples/counter` |
| 2 | [profile](profile/README.md) | `actorx.StartTask`, serial completion, deferred reply | `go run ./examples/profile` |
| 3 | [chat](chat/README.md) | StreamReply, incremental consumption, cancellation | `go run ./examples/chat` |
| 4 | [lifecycle](lifecycle/README.md) | Notices, State, Stop / Wait, self-stop | `go run ./examples/lifecycle` |
| 5 | [composition](composition/README.md) | Create another actor, communicate by address, reclaim a dependency | `go run ./examples/composition` |

Each example keeps its business definitions and entry point in `main.go`, with generated bindings in `actorx.gen.go`. The lifecycle README includes a short self-stop snippet; root core tests verify concurrency, cancellation, and shutdown boundaries.

## Capacity and backpressure

The root [example_limits_test.go](../example_limits_test.go) contains two executable examples to read after counter and chat:

- `Example_mailbox`: with `WithMailboxCapacity(1)`, the first job is executing, the second fills the queue, and a third Send returns `ErrFull`. The controlled blocker only demonstrates queue occupancy; application I/O belongs in `actorx.StartTask` workers.
- `Example_stream`: with `WithStreamLimits(1, 1)`, the first chunk stays in the consumer callback, the second fills the buffer, and the third write must wait for consumption to continue. Production runs in `actorx.StartTask` workers.

Signals establish ordering without sleeps. The producer declares the payload byte size, using `len(text)` here. Run from the project root:

```sh
go test -v . -run '^Example_(mailbox|stream)$'
```

## Resource ownership

[ExampleStartTask](../example_task_test.go) acquires a temporary file in a task worker, adopts it in a serial completion, and releases it with Stopped. A result that cannot be adopted goes through discard. Both paths share an idempotent release operation, including when completion panics after saving the file. See [runtime semantics](../docs/runtime.md) for those failure boundaries.

```sh
go test -v . -run '^ExampleStartTask$'
```

```text
resource acquired
resource released
```

## Updating and validation

After changing marked actor handlers, run `go generate ./examples/...`. Run `go test -race -timeout 60s ./...` and `go vet ./...` to check the core, generator, and executable test examples, then run the programs listed above. The main examples use generated bindings. See [handwritten bindings](../docs/runtime.md#handwritten-bindings) for direct binding control.
