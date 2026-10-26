# Runtime reference

English | [中文](runtime.zh.md)

This page covers author-side bindings and asynchronous execution, and caller-side communication. See [Getting started](getting-started.md) for a complete program and [Lifecycle](lifecycle.md) for startup and exit.

## Bindings and calls

The normal author workflow uses `//actorx:actor` and actorx-gen to generate message bindings; see [Getting started](getting-started.md). The underlying `Actor` interface requires `Bindings() []actorx.Binding`. The generator provides it, or authors can use [handwritten bindings](#handwritten-bindings). Spawn calls it once, validates and copies the entries, then installs them. Nil, typed nil, Bindings panic, invalid entries, and duplicate bindings return errors matching `actorx.ErrSetup` through `errors.Is`.

| Binding | Handler signature | Calls |
| --- | --- | --- |
| `Handle` | `func(actorx.Context, Q) (R, error)` | Send / Call |
| `HandleMessage` | `func(actorx.Context, M) error` | Send / Call; a successful Call result is nil |
| `HandleDeferred` | `func(actorx.Context, Q, actorx.Reply[R]) error` | Send / Call; nil return leaves the request waiting for Complete |
| `HandleStream` | `func(actorx.Context, Q, actorx.StreamReply[C]) error` | Stream only |

Runtime supplies the first argument. It implements standard `context.Context`, carrying request deadlines, cancellation, values, and actor ownership. Caller-side Addr methods take standard contexts.

Messages match concrete types: aliases are identical, while T and *T differ. Interface messages, duplicate types, and nil handlers fail setup. Lifecycle notices have special HandleMessage bindings; see [Lifecycle](lifecycle.md#binding-lifecycle-notices).

`Call[R]` checks R when the result arrives. A mismatch returns `ErrReplyType` after possible business side effects. Use `Call[any]` to wait for error-only handlers. Untyped nil is accepted by an interface R, not a concrete value or pointer R; typed nil retains its dynamic type.

Local messages, results, and chunks are not deep-copied. Do not concurrently mutate shared slices, maps, or pointer contents after handoff. Handlers cannot be preempted; do not synchronously wait for yourself or create synchronous Call cycles.

## Caller cancellation and completion

| Operation | Context controls | Success means |
| --- | --- | --- |
| Send | Admission; accepted execution follows actor lifetime. | The target mailbox admitted it; business errors are reported through slog. |
| Call | Request execution and result waiting. | One final reply; ordinary business errors return to the caller. |
| Stream | Request execution and chunk consumption. | Successful termination and consumption of accepted chunks. |

Call / Stream canceled before execution may skip the handler. Once execution starts, cancellation requests cooperation. Canceling after Send succeeds does not retract the message. **Timeout does not prove an operation never ran.** Actorx never retries automatically; define idempotency or confirmation queries before retrying.

## Tasks and deferred replies

`actorx.StartTask(ctx, work, complete, discard)` manages work owned by the current actor. Workers use input snapshots; completions run serially on the actor. See [profile](../examples/profile/main.go) for complete wiring.

```go
cancel, err := actorx.StartTask(ctx,
    func(ctx context.Context) (Result, error) {
        return client.Fetch(ctx) // Use snapshots and a concurrency-safe client.
    },
    func(result Result, err error) bool {
        reply.Complete(result, err)
        return true // Adopt the result; false hands it to discard.
    },
    nil, // Plain values need no release; resources need discard.
)
```

Start tasks only from Started, a normal handler, or a serial completion. Actorx reserves a task slot and a mailbox completion slot; business messages and completions share FIFO order. Exhaustion returns ErrFull without starting a worker. Stopping / Stopped cannot start work. Nil or invalid Context returns ErrSetup.

| Result path | Ownership |
| --- | --- |
| complete returns true | The result was adopted; resources retained by the actor are released through lifecycle cleanup. |
| complete returns false | discard releases the result. |
| complete panics, or Stop prevents delivery | discard releases the unadopted result. |
| Resources not yet returned by work | The worker releases them. |

Plain values can use nil discard. Discard may run outside the mailbox; it must not depend on further actor messages or concurrently touch mutable actor state. If completion stores a resource and panics before returning true, Actorx cannot undo field writes. Discard and lifecycle cleanup must share an idempotent release operation. The [real file example](../example_task_test.go) demonstrates this handoff.

Tasks inherit request cancellation, deadlines, and values; actor shutdown always cancels owned work. The returned cancel requests cancellation without deleting the task or proving completion. Completion can still run after request cancellation; business revision checks decide whether late results remain relevant. To adjust the parent while retaining actor ownership:

```go
taskCtx := ctx.WithContext(context.WithoutCancel(ctx))
// Then use actorx.StartTask(taskCtx, ...); actor shutdown still cancels the task.
```

`WithContext(parent)` also accepts a derived deadline or `context.Background()`. It changes the task parent, not the original Call / Stream wait. Keep timers and connection waits inside controlled work and release them before returning.

`Reply.Complete(value, err)` and `StreamReply.Complete(err)` succeed at most once, returning false after cancellation, stop, or previous completion. A non-nil deferred handler error ends its request. Reply is a local capability for one invocation; do not persist it or send it as message data. Deferred Send consumes no caller-side Call slot but retains a target pending-request slot.

## Stream

Callers consume chunks serially on the calling goroutine:

```go
err := addr.Stream[TextChunk](ctx, Ask{Topic: "Actors"},
    func(chunk TextChunk) error {
        fmt.Print(chunk.Text)
        return nil
    })
```

Produce with `stream.Write(ctx, chunk, payloadBytes)` in a StartTask worker; text can declare `len(chunk.Text)`. Write blocks on a full window, so never produce inside a state handler. After the worker returns, its serial completion calls `stream.Complete(err)`. Termination uses no data-window slot. Successful completion drains accepted chunks, and an error may follow partial output. See [chat](../examples/chat/main.go).

Callback errors return unchanged and cancel the request. Context cancellation and actor stop also end consumption. Stream cannot return until an active callback returns, so callbacks need reasonable time bounds. A nil callback returns ErrSetup; incompatible modes fail before handler execution; incompatible chunks return ErrReplyType.

Local byte accounting uses the producer-declared retained payload size, between zero and the per-chunk limit. Buffered payload is at most `items * chunkBytes`, plus one chunk being consumed. Actorx does not calculate arbitrary Go heap size or account for object headers and external SDK buffers. Do not declare a small size for a large object.

## Capacity and backpressure

| Option | Default | Scope |
| --- | --- | --- |
| `WithInstanceLimit(n)` | 0 (unlimited) | All Runtime instances, including construction, stopping and ctx.Spawn children, until complete exit. |
| `WithMailboxCapacity(n)` | 64 | Queued messages and reserved completions per actor. |
| `WithTaskLimit(n)` | 16 | Unsettled tasks per actor, including results awaiting handoff. |
| `WithPendingLimit(n)` | 256 | Unfinished requests per actor, including Send. |
| `WithCallLimit(n)` | 256 | Caller-side Call / Stream associations across Runtime. |
| `WithStreamLimits(items, chunkBytes)` | 8 / 64 KiB | Buffered items per stream and declared size per item. |

The instance limit permits zero for unlimited; other settings must be positive. Negative instance limits return ErrSetup. Message and task admission return ErrFull immediately when capacity is exhausted; stream Write waits for consumption or cancellation. Raising caller capacity does not relax target limits. A Stream retains its caller slot until callbacks and draining finish. The instance limit is checked before Bindings; full Spawn returns zero Addr and ErrFull. Rollback releases capacity; unfinished Stopped retains it.

See the synchronized [capacity and backpressure examples](../examples/README.md#capacity-and-backpressure).

## Error reference

The table describes framework errors; business handlers can return their own errors. Match sentinels with `errors.Is` and inspect local PanicError with `errors.As`, rather than matching complete text.

| Error | Meaning and response |
| --- | --- |
| `ErrFull` | Capacity rejected this admission; throttle or use a business-defined retry policy. |
| `ErrStopped` | Invalid address, unavailable target, or a request settled by actor shutdown; earlier side effects may have started. |
| `ErrClosed` | Runtime / Channel is closing or closed; follow its owner's shutdown flow. |
| `ErrUnhandled` | No ordinary binding for this concrete message; check T / *T and the message contract. |
| `ErrMode` | Binding does not support the selected Send / Call / Stream mode; use the matching mode. |
| `ErrReplyType` | Reply or chunk is incompatible with the caller's type parameter; fix the contract. Side effects may have occurred. |
| `ErrSetup` | Invalid instance, binding, configuration, or callback; fix assembly. |
| `ErrChunkSize` | Write declared a negative or excessive size; correct or split the chunk. |
| `context.Canceled` / `DeadlineExceeded` | This invocation or wait was canceled or timed out; no automatic retry. |

See [Lifecycle](lifecycle.md) for Stop / Wait capability errors and the distinction between Exit.Reason and waiting errors.

| Failure site | Behavior |
| --- | --- |
| Ordinary handler returns error | Ends this request; the actor keeps processing messages. |
| Ordinary handler or completion panics | Stops the actor; its final reason includes PanicError with a stack. |
| Task work panics | Converts to PanicError and passes it to completion as err; if shutdown prevents delivery, settles through discard. |
| Discard panics | Records the failure in the final exit reason and continues cleanup. |
| Lifecycle error / panic | Contributes to startup failure or final exit; later cleanup continues. |

## Reading message metadata

Tools can use `Describe(bindings)` to read ordinary message types and modes without constructing an instance, creating Runtime, or executing handlers:

```go
descriptions, err := actorx.Describe((*Counter)(nil).Bindings())
if err != nil {
    return err
}
for _, d := range descriptions {
    fmt.Println(d.MessageType(), d.ResultType())
}
```

Describe shares Spawn's validation: validate every binding, then omit lifecycle entries. Failure matches ErrSetup with no partial output. Empty or lifecycle-only collections return empty descriptions. Results follow binding order, use independent slices, and retain no handler or business object.

`MessageType()` identifies the message; `ResultType()` identifies a reply or chunk, or nil for error-only bindings. `Validate(SendKind/CallKind/StreamKind)` checks modes. `Binding.Descriptor()` describes one binding. `Request[Q,R]()`, `Message[M]()`, and `Streaming[Q,C]()` declare the same types and modes without a handler.

Generated Bindings supports construction on a concrete nil pointer. Handwritten Bindings must describe identical types and modes for nil and live receivers without reading state or acquiring resources. Bind each instance's own receiver; do not cache closures capturing another instance or concurrently modify shared slices. Pointer receivers can capture `c.Read` directly. Value receivers need a deferred invocation closure to avoid dereferencing nil during construction. See [handwritten](../example_describe_test.go) and [generated](../examples/counter/describe_test.go) examples. Spawn still requires a non-nil object.

<a id="optional-generator"></a>

## Generator reference

Add a directive to an application file and mark the type:

```go
//go:generate go run github.com/go-aie/actorx/cmd/actorx-gen

//actorx:actor
type Counter struct {
    count int
}
```

Run `go generate .` in the target package, or invoke `go run github.com/go-aie/actorx/cmd/actorx-gen` directly. See [Getting started](getting-started.md) for first setup. The default output is one package-level `actorx.gen.go`; `-output filename` and `-check` are supported.

- Type markers select public, directly declared methods across files, supporting T / *T receivers. Private, `//actorx:ignore`, and promoted methods are excluded.
- To select individual methods, use `//actorx:handler`; do not mix both modes on one type. Unsupported signatures, duplicate messages, or existing handwritten Bindings fail generation.
- Lifecycle event types produce HandleMessage without relying on method names. The generator writes bindings without constructing business objects.
- Value receivers read the current object at dispatch time instead of capturing a state copy during assembly. Output is sorted by type and method, written atomically, and never overwrites handwritten files.

Generation follows GOOS / GOARCH and build tags, preserving source `//go:build`, legacy `// +build`, and filename platform conditions in the output constraint. Types and methods in one output must have identical normalized expressions. It does not solve logical equivalence, intersect constraints, or split variants. Output filenames cannot add extra constraints. Use handwritten Bindings for mixed conditions.

Before generation, pending Bindings signatures participate in type checking, supporting first generation and later checks with `var _ actorx.Actor = (*T)(nil)` without ignoring invalid assertions. Generic actors and cgo packages are unsupported; handwritten Bindings retains full functionality.

## Handwritten bindings

Implement `Bindings` directly when you need explicit binding control or types and build conditions the generator does not support. For example, bind an ordinary counter request:

```go
func (c *Counter) Bindings() []actorx.Binding {
    return []actorx.Binding{actorx.Handle(c.Add)}
}
```

Handwritten and generated bindings use the same API and satisfy the same Actor interface. Keep one Bindings implementation per type. See [Bindings and calls](#bindings-and-calls) for the four handler shapes.

Bindings only constructs entries without executing handlers, reading state, or acquiring resources. Bind each instance's own receiver. Static metadata reads also require the [nil-receiver contract](#reading-message-metadata). See [example_describe_test.go](../example_describe_test.go) for a complete handwritten example.

## Channel extension interface

Channel connects Runtime to mailbox delivery and replies. Runtime uses `NewLocalChannel()` by default; supply another implementation with `WithChannel`. Business handlers continue to use Context and Addr.

```go
type Channel interface {
    Open(context.Context) error
    Bind(context.Context, string, Receiver) (release func() error, err error)
    Deliver(context.Context, Delivery, Responder) error
    Close() error
}
```

Runtime exclusively owns a successfully opened Channel. Open must roll back its own work on failure and leave an existing owner intact if a duplicate claim is rejected. Bind returns an idempotent release function. Startup rollback and shutdown ordering are covered in [Lifecycle](lifecycle.md#closing-runtime).

| Capability | Contract |
| --- | --- |
| `Delivery` | Carries the concrete instance target, unique correlation ID, Kind and Message. `Validate` checks the envelope; message types and modes are checked separately. |
| `Receiver` | `Receive` enters the mailbox; success proves admission. `Bindings` exposes ordinary message descriptors, without handlers or lifecycle notices. |
| `Responder` | `Complete` settles once. `Write` queues a bounded chunk; `WriteConsumed` also waits for consumption. |

Writes can block: use request-owned work rather than a shared receive callback. The core retains responsibility for serial execution, request/task completion and complete exit. Full interface details are in [channel.go](../channel.go).

## Context extensions and migration

`actorx.Context` is an interface with standard context methods plus WithContext, Stop, Spawn and State. Runtime owns its private local implementation. Its zero value is a nil interface; methods on nil are invalid. `StartTask` validates ownership and returns ErrSetup for nil, typed nil, empty embedded contexts or missing ownership before reserving any resources.

Install `WithContextFactory` when creating a Runtime. A factory receives the instance Actor once after valid Bindings are installed, before Channel.Bind and Started, and returns its `ContextWrapper`. Business Actors only need Bindings. For example, an application can capture a service:

```go
type appContext struct {
    actorx.Context
    service *Service // Immutable or synchronized by its owner.
}

factory := func(actorx.Actor) (actorx.ContextWrapper, error) {
    return func(ctx actorx.Context) actorx.Context {
        return &appContext{Context: ctx, service: service}
    }, nil
}
rt, err := actorx.NewRuntime(initCtx, actorx.WithContextFactory(factory))
```

NewRuntime saves the policy without calling it. Omitting the option keeps local contexts; an explicitly nil final factory returns ErrSetup before the channel opens. Repeated options replace the policy rather than compose it. A configured factory must return a non-nil wrapper; use an identity function to preserve the local context. Invalid Bindings or exhausted instance capacity prevent factory execution. Describe does not call the factory.

The factory can read an application's private Actor adapter to capture stable instance environment. Each returned wrapper embeds the exact supplied `actorx.Context`; external packages cannot implement its private ownership method from scratch. Handlers still receive `actorx.Context` and may assert optional capabilities. The generator accepts that interface and its aliases, and rejects pointer signatures or downstream extension interfaces. Binding selection, generation commands and `-check` are unchanged. ContextDecorator and its automatic detection have been removed.

Factories and wrappers must be bounded and perform no I/O, resource acquisition, task starts, handler calls or business-state access. Their environment is fixed at installation or synchronized by its owner. Concurrent Spawns may call the factory concurrently. Factory calls, wrapping and private ownership extraction run without Runtime/actor locks. Core supplies no factory resource cleanup; the environment owner manages its lifetime. Do not wait for the current instance to start or for this Runtime.Close inside the factory.

Wrappers must preserve deadlines, cancellation and values and refer to the exact supplied context; a context from another instance or earlier invocation is rejected. WithContext creates a fresh local context and invokes the same saved wrapper, without rerunning the factory, rereading Actor or stacking previous wrappers. Background preserves capabilities; nil parent panics. Local ctx.Spawn children and grandchildren use the same Runtime factory with their own Actor, retaining independent instance environments and no automatic stop propagation.

| Factory / wrapper failure | Result |
| --- | --- |
| Spawn factory binding | Skip Channel.Bind and all lifecycle handlers; return zero Addr after rollback and release the instance slot at complete exit. |
| Started | Skip the handler; fail Spawn through normal lifecycle rollback. |
| Ordinary handler | Skip the handler; settle the request with the setup error, record it and stop the instance. |
| WithContext | Panic carrying the setup error; a surrounding handler uses the existing panic boundary. |
| Stopping / Stopped | Record the error, skip that callback, and continue the remaining cleanup and exit stages. |

Invalid returns produce ErrSetup. A factory error is joined with ErrSetup and retains its original error chain. A factory or wrapper panic preserves PanicError and its stack, joined with ErrSetup. Only ownership extraction recovers empty-embedding forwarding panics as invalid contexts; business work, completion and discard keep their existing panic boundaries. A handler panic remains an outer PanicError, whose error chain need not match ErrSetup.

Failed shutdown wrapping does not bypass workers, discard or complete-exit accounting. If a skipped Stopping callback was supposed to unblock a worker, Wait/Close can time out and the instance slot remains occupied until that work actually ends. Serial completion keeps its original closure and captured context; it is not wrapped again. A blocked factory remains counted in instance capacity and Spawn/Close waits; after it returns, the normal close check prevents Started or address publication if the Runtime closed.

| Previous API | Current API |
| --- | --- |
| `func(ctx *actorx.Context, ...)` | `func(ctx actorx.Context, ...)` |
| `ctx.StartTask(work, complete, discard)` | `actorx.StartTask(ctx, work, complete, discard)` |
| `ctx.Context` | Pass `ctx` directly as `context.Context`. |
| `actorx.Context{}` / `&actorx.Context{}` | Receive a valid Context from Runtime; the interface zero value is nil. |
| Runtime/instance association APIs | Configure WithContextFactory and embed the supplied Context in its returned wrapper. |
| Actor ContextDecorator | Move instance binding into ContextFactory and request wrapping into ContextWrapper. |

The removed association APIs are WithRuntimeAssociation, Runtime.SpawnAssociated, Context.RuntimeAssociation and Context.InstanceAssociation. StartTask uses the passed context for cancellation, deadlines and values while retaining its original actor ownership. State remains a local lifecycle snapshot; Spawn remains local creation.
