# Actorx Distributed

English | [中文](README.zh.md)

Actorx Distributed (Go package `dist`) is a distributed Actor framework for Go built on [actorx](../README.md). It provides logical addressing, on-demand activation, unique ownership, cross-process Send/Call/Stream, and lifecycle management through complete shutdown.

Business code keeps actorx stateful objects, serial message handling, asynchronous tasks and streaming results. dist uses NATS and JetStream KV for communication and ownership so callers access objects by type and business ID.

## Get started

A distributed counter keeps its count in an Actor. An Add message increments it and returns the new total.

```go
import (
    "github.com/go-aie/actorx"
    "github.com/go-aie/actorx/dist"
)

type Add struct{ N int `json:"n"` }

//actorx:actor
type Counter struct{ value int }

func (c *Counter) Add(_ actorx.Context, msg Add) (int, error) {
    c.value += msg.N
    return c.value, nil
}

var CounterSpec = dist.ActorSpec[*Counter]{
    Type: "counter",
    New:  func(dist.ActorKey) *Counter { return &Counter{} },
}
```

Counter defines business behavior; CounterSpec declares its logical type and constructor. A worker registers the Spec and stays running to handle requests. Routine error handling and waiting for shutdown are omitted here:

```go
ctx := context.Background()
worker, _ := dist.NewRuntime(ctx, dist.WithActor(CounterSpec))
defer worker.Close(ctx)
```

A separate caller creates its own Runtime without registering Actors. The following assumes a request context ctx and a previously unused target ID:

```go
client, _ := dist.NewRuntime(ctx)
defer client.Close(ctx)

addr, _ := client.Addr("counter", "orders-42")
n, _ := addr.Call[int](ctx, Add{N: 5})
fmt.Println(n) // 5
```

`Addr("counter", "orders-42")` only obtains a logical handle. The first call finds or activates the object, with unique ownership for the logical identity. Once selected, that address stays on the same generation. Obtain a fresh address to select another generation after exit.

Inside an Actor, use `dist.MustFrom(ctx).Addr(type, id)` for addressing and Self for a reply address on the current generation; see [concepts](docs/concepts.md). These snippets require generated Bindings. Complete definitions, bindings and process entry points are in the [Counter example](examples/counter/README.md).

## Run the example

Requires Go 1.27. Run from this source checkout; dist uses the core source in the parent directory through go.mod's local replacement. After publishing the migrated core, pin its version and remove the replacement for released-module consumption.

Download the archive for your OS and architecture from the [official NATS v2.10.18 release](https://github.com/nats-io/nats-server/releases/tag/v2.10.18). For macOS Apple Silicon, download, extract and add the executable to this terminal's PATH as follows. Other platforms can select their matching package on the same release page:

```sh
curl -fL -o /tmp/nats-server.tar.gz \
  https://github.com/nats-io/nats-server/releases/download/v2.10.18/nats-server-v2.10.18-darwin-arm64.tar.gz
tar -xzf /tmp/nats-server.tar.gz -C /tmp
export PATH="/tmp/nats-server-v2.10.18-darwin-arm64:$PATH"
```

From the checkout's dist directory, start the development server. Its configuration enables JetStream and listens on local port 4222. The storage directory must be writable:

```sh
nats-server -c examples/nats.conf -sd /tmp/dist-counter
```

Leave the server running. Open two more terminals, each in the dist directory:

```sh
# Terminal 2: start a worker and wait for "Counter worker ready".
go run ./examples/counter -role worker
# Terminal 3: a fresh ID prints 5, then 10.
go run ./examples/counter -role client -id orders-42
```

Each client round sends an increment of two, then calls an increment of three and reads the total. Two rounds print 5, then 10. While the same worker stays alive, repeat with the same ID to print 15, then 20. Start another worker to share creation capacity for the Type. See [Client → Button → Counter](examples/click/README.md) for Actor composition.

Closing a client leaves remote objects alive. Use Ctrl-C for graceful worker shutdown and join its owned instances. State lives in worker memory and disappears on instance exit. Unreachable owners are not replaced automatically; see [explicit recovery](docs/deployment.md#explicit-recovery-and-storage-repair) after forced exit.

## Learn more

| Guide | Contents |
| --- | --- |
| [Concepts](docs/concepts.md) | Logical objects, activation generations, addresses and contexts. |
| [Call semantics](docs/runtime.md) | Send/Call/Stream, deadlines, errors and capacity. |
| [Lifecycle](docs/lifecycle.md) | Stop/Wait, object lifetime and cleanup responsibility. |
| [Deployment](docs/deployment.md) | Configuration, permissions, contract upgrades and explicit recovery. |

Implementation details live in [architecture and core integration](docs/architecture.md), [wire contract and limits](docs/contract.md), and the [Registry protocol](docs/registry.md). Released-version consumption, physical cross-host networking and replication remain unverified.
