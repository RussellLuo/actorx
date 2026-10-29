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

The producer writes chunks with `StreamReply.Write` and ends the stream with `Complete`. The caller consumes each chunk with `for chunk, err := range addr.Stream[TextChunk](ctx, ask)`, without collecting a single reply first. The request starts when iteration begins; setup and terminal errors arrive as error items, which may follow partial output.

`Write` may block when the reader is slow, so production runs in `actorx.StartTask`, introduced in [profile](../profile/README.md). This example adds streaming to that foundation, with no timer or extra task state.

To stop reading early, use `break`, return from the enclosing function, or cancel the context passed to `Stream`. Early exit requests cancellation and cleans up consumption; it does not prove the producer has finished. `Runtime.Close` waits for owned work to finish. See [Stream semantics](../../docs/runtime.md#stream) for iterator reuse and loop-body cancellation.

After changing handlers, run `go generate ./examples/chat`.

For a synchronized demonstration of a bounded window and slow consumption, see [capacity and backpressure](../README.md#capacity-and-backpressure).
