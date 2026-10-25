# Lifecycle

English | [中文](README.zh.md)

A local session showing lifecycle notices, state queries, and explicit shutdown.

## Run

Requires Go 1.27+. From the project root:

```sh
go run ./examples/lifecycle
```

Expected output:

```text
started: state=Started
spawn returned
message: hello, state=Running
stopping: state=Stopping, reason=<nil>
stopped: state=Stopped, messages=1, reason=<nil>
done: err=<nil>
```

## How it works

`Started` initializes session state before Spawn returns. A `Call` waits for `Receive` to process one message before the caller requests shutdown. `Stopping` runs before worker waiting; `Stopped` reports the final message count after owned work and resource cleanup finish. Each method reads its current state through `ctx.State()`.

`addr.Stop(nil)` requests a normal stop and returns an acceptance error. `addr.Wait(ctx)` waits for complete exit, including the final message handler, and returns `Exit.Reason` separately from a waiting error. Original local address copies support repeated and late waits, even after Runtime.Close; imported addresses cannot Stop or Wait. The example uses Call to make the output order deterministic, without sleeps.

The `Logout` handler in `main.go` calls `ctx.Stop(nil)` to request self-stop. Replace the address Stop and Wait section in `run` with:

```go
if err := addr.Send(context.Background(), Logout{}); err != nil {
    return err
}
exit, err := addr.Wait(context.Background())
if err != nil {
    return err
}
return exit.Reason
```

`Logout` is an ordinary business message whose handler explicitly calls `ctx.Stop(nil)`. The caller uses Send, then Wait to confirm exit; a Call being handled during self-stop settles with ErrStopped. See [composition](../composition/README.md) for creating, calling, and reclaiming another actor.

The example uses the standard `Started` / `Stopping` / `Stopped` method names. The message type selects the lifecycle phase, so custom names remain supported. `//actorx:actor` generates both business and lifecycle notice bindings. Both use the actor's serial execution loop; lifecycle notices are dispatched directly at phase boundaries without queueing. After changing handlers, run `go generate ./examples/lifecycle` from the project root.

See the [lifecycle guide](../../docs/lifecycle.md) for startup failure, `Stopped` cleanup, I/O ownership, and handler restrictions.
