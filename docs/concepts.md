# Core concepts

English | [中文](concepts.zh.md)

Actorx organizes stateful components as actors: ordinary Go structs keep state, named methods handle messages, and callers communicate through addresses. Run [Getting started](getting-started.md) first, then use this page to understand the parts.

## Actor authors and callers

| Part | Author | Caller |
| --- | --- | --- |
| State and messages | Define structs, ordinary Go messages, and handlers. | Send messages for results instead of accessing the state object. |
| `Actor` | Mark the type and write named message handlers. | Give a fresh object to `Runtime.Spawn`. |
| `Context` | Handler context providing tasks, instance creation, and self-stop. | Pass standard `context.Context` to Addr operations. |
| `Addr` | Keep other actor addresses for collaboration. | Use Send / Call / Stream, and Stop / Wait on original local addresses. |
| `Runtime` | Runs handlers and manages owned work. | Creates instances and eventually closes them. |

The normal author workflow marks a type with `//actorx:actor` and lets the generator connect ordinary Go methods to Runtime. See [Getting started](getting-started.md) for the steps. After Spawn, the instance exclusively owns the state object; callers must not access its mutable fields or Spawn the same object again. Acquire and release instance resources through lifecycle handlers.

## Messages and serial state

```text
Caller → Addr → Channel → bounded mailbox → handler
```

Concrete Go message types select handlers. Each actor runs handlers and task completion callbacks serially, so they can update its state directly. Different actors can run concurrently.

| Call | Use | Success means |
| --- | --- | --- |
| `Send` | Submit a message without waiting for a result. | The target mailbox admitted it; the handler may not have run. |
| `Call[R]` | Wait for one result or business operation completion. | A reply of type R was received. |
| `Stream[C]` | Consume chunks as they are produced. | Chunks were consumed and the producer ended successfully. |

Bindings determine supported modes; see the [runtime reference](runtime.md#bindings-and-calls). Local values are not deep-copied, so agree on ownership of slices, maps, and pointers. Never synchronously wait for yourself inside a handler or create synchronous Call cycles.

## Asynchronous work

Put slow I/O or computation in `actorx.StartTask(ctx, work, complete, discard)` and return promptly from the handler:

```text
handler: read state and prepare an input snapshot
    → work: perform slow work in a worker
    → complete: return to serial actor execution to update state or reply
```

Workers must not access mutable actor state. The actor may process other messages while work runs, so the business decides whether a result is still relevant. Discard releases results that were not adopted. Cancellation requests cooperation; the owning actor still manages task settlement and resource release.

Use `Reply` for one deferred result and `StreamReply` for multiple chunks. These have separate responsibilities from tasks: StartTask owns work; Reply / StreamReply delivers results. Continue with [profile](../examples/profile/README.md) and [chat](../examples/chat/README.md).

## Instances and lifecycle

Each Spawn creates an independent instance and waits for Started before returning Addr. An Addr stays bound to that instance and never rebinds to a replacement. `IsZero()` only checks the zero value; it does not probe whether the target is running.

| Operation or notice | Responsibility |
| --- | --- |
| Started / Stopping / Stopped | Initialize resources, unblock work, and finish cleanup. |
| `Addr.Stop` / `Context.Stop` | Request shutdown from a caller / request self-stop. |
| `Addr.Wait` | Confirm complete exit of the original local instance and read Exit. |
| `Runtime.Close` | Stop and wait for owned instances, then close Channel. |

`ctx.Spawn` creates collaborators in the same Runtime without automatic parent-child shutdown. See [Lifecycle](lifecycle.md) for ownership, startup failure, and shutdown ordering.

## Local delivery

Runtime defaults to the root package's LocalChannel, routing requests through the Channel interface into the target mailbox. Local delivery passes ordinary Go values directly. Callers use Addr; business handlers do not manage delivery.

<a id="reading-message-metadata"></a>

Tooling features such as [reading message metadata](runtime.md#reading-message-metadata) live in the runtime reference and are not prerequisites for writing a first actor.

Next, follow the [example guide](../examples/README.md) for asynchronous lookup, streaming, and actor collaboration.

## Context capabilities

`actorx.Context` is an interface supplied by Runtime. It embeds standard `context.Context` and provides WithContext, Stop, Spawn, and State. Start owned work through `actorx.StartTask(ctx, ...)`. Spawn creates an independent local instance; State describes its local lifecycle.

Configure `WithContextFactory` on a Runtime to add optional capabilities. Spawn calls the factory once with the instance Actor, then uses its returned `ContextWrapper` for each execution. Extensions embed the supplied Context and retain its private execution ownership. WithContext replaces the parent and reuses that wrapper, including with Background; local children use the same factory with their own Actor. See the [extension and migration rules](runtime.md#context-extensions-and-migration).
