# Streaming chat

English | [中文](README.zh.md)

Stream a simulated answer word by word. No model, network, or credentials are needed.

From the project root, with Go 1.27+:

```sh
go run ./examples/chat
```

```text
answer: Actors use messages to coordinate work.
```

The producer writes chunks with `StreamReply.Write` and ends the stream with `Complete`. The caller consumes each chunk through the `Addr.Stream` callback, without collecting a single reply first.

`Write` may block when the reader is slow, so production runs in `ctx.StartTask`, introduced in [profile](../profile/README.md). This example adds streaming to that foundation, with no timer or extra task state.

To stop reading early, cancel the context passed to `Stream`. Cancellation requests a stop; `Runtime.Close` waits for owned work to finish.

After changing handlers, run `go generate ./examples/chat`.

For a synchronized demonstration of a bounded window and slow consumption, see [capacity and backpressure](../README.md#capacity-and-backpressure).
