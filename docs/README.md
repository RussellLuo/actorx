# Actorx documentation

English | [中文](README.zh.md)

Start with [Getting started](getting-started.md) to write, call, and shut down an actor. The core requires Go 1.27+ and depends only on the standard library.

| Document | Purpose |
| --- | --- |
| [Getting started](getting-started.md) | Write an actor, generate its bindings, and run a complete counter from an application module. |
| [Core concepts](concepts.md) | Understand ownership of state, messages, addresses, tasks, and Runtime. |
| [Runtime reference](runtime.md) | Look up handlers, cancellation, errors, backpressure, metadata, and generator rules. |
| [Lifecycle](lifecycle.md) | Initialize resources, create collaborating actors, stop, and confirm complete exit. |

The learning path is **Getting started → Core concepts → Examples**. Consult runtime and lifecycle details as needed.

The [example guide](../examples/README.md) walks through counter, profile, chat, lifecycle, and composition, with additional capacity, backpressure, and resource handoff examples. Each example includes commands and expected output.

## Current scope

The core provides bounded serial execution, typed bindings, owned tasks, deferred replies, streaming, and local lifecycle management.

Supervision, automatic restart, logical identity discovery, automatic activation, and placement are outside the current core.

## Repository validation

From the repository root:

```sh
go test -race -timeout 60s ./...
go vet ./...
```

These commands run the core and generator tests, executable test examples, and static checks. To run the complete example programs, follow the [example guide](../examples/README.md).

Distributed logical identity and activation live in [Actorx Distributed](../dist/README.md), an independently tested module. The runtime reference covers the [Channel extension interface](runtime.md#channel-extension-interface).
