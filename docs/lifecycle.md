# Lifecycle

English | [中文](lifecycle.zh.md)

Authors manage instance resources with Started / Stopping / Stopped. Callers manage original local instances with Stop / Wait, and close the whole runtime with Runtime.Close. The [lifecycle example](../examples/lifecycle/README.md) makes the sequence observable.

## States and startup

`Runtime.Spawn(value)` installs bindings and waits for Started before admitting ordinary messages and returning Addr. Failure waits for complete rollback and returns a zero Addr with the final error. Setup problems such as nil, typed nil, Bindings panic, and invalid bindings match ErrSetup.

| `ctx.State()` | Meaning |
| --- | --- |
| `StateStarted` | Channel binding is ready; Started runs before ordinary admission opens. |
| `StateRunning` | Processes ordinary handlers and serial task completions. |
| `StateStopping` | Admission is closed; shutdown proceeds after the active handler returns. |
| `StateStopped` | Owned work, rollback, and binding release finished; final Stopped runs. |

State is a concurrency-safe observation, not an admission check or business Ready signal. StateStopped may precede Stopped's return; **Wait confirms complete exit**. An unassociated Context returns StateStopped.

Started uses the actor lifetime context. It may start tasks, but cannot wait for its own mailbox, task completions, or exit. Completions wait until Running. Spawn does not wait for asynchronous business initialization; define a separate readiness protocol if needed.

Failure before startup entry skips lifecycle notices and rolls back runtime resources only. Once startup is entered, Stopping and Stopped run even without a Started handler or after its error / panic. Cleanup must tolerate partial initialization. Stop / Close during startup cancels its context and waits for user code before rollback. Spawn has no separate timeout parameter and cannot forcibly interrupt user code.

## Binding lifecycle notices

Named methods receive event types with the signature `func(actorx.Context, Event) error`:

```go
func (s *Session) Started(ctx actorx.Context, _ actorx.Started) error {
    s.messages = 0
    return nil
}

func (s *Session) Stopping(ctx actorx.Context, msg actorx.Stopping) error {
    // Unblock workers; do not wait for those workers here.
    return nil
}

func (s *Session) Stopped(ctx actorx.Context, msg actorx.Stopped) error {
    // Workers, unadopted-result rollback, and binding release finished.
    return nil
}
```

As with ordinary handlers, the generator creates lifecycle bindings from event parameter types. Use the same `//actorx:actor` or `//actorx:handler` selection rules and regenerate after changes. Matching method names are recommended; custom names are supported. For low-level wiring, see [handwritten bindings](runtime.md#handwritten-bindings).

Each phase permits at most one handler, and all are optional. Lifecycle-only actors are valid. Pointer events, business return values, deferred replies, and streaming signatures are invalid.

Runtime dispatches notices directly at phase boundaries, serially with business handlers, without mailbox or caller slots. Notices never preempt an active handler. They cannot be delivered through Addr / Channel or described as ordinary CallDescriptor values. Ordinary lookup returns ErrUnhandled; other cancellation, shutdown, or capacity checks may fail first.

## Stopping through an address or handler context

`addr.Stop(cause)` closes admission and requests cancellation without waiting for exit. A nil result confirms acceptance only. It uses no business message, mailbox capacity, or caller slot. User-defined Stop / Shutdown / Logout messages remain ordinary business messages without implicit lifecycle effects.

Original local addresses provide Stop / Wait through their owning Runtime. Ordinary Go value copies retain both capabilities. ExportAddr / ImportAddr transfers identity only; importing into the source Runtime does not restore capabilities. Stop / Wait on imported addresses returns an explanatory ordinary error; do not classify it by exact text.

| Situation | Stop result |
| --- | --- |
| Runtime remains open; repeated stop, including after instance exit | nil, preserving the first stop cause. |
| Owning Runtime has begun closing | ErrClosed. |
| Zero or malformed address | ErrStopped. |

Authors can call `ctx.Stop(cause)` from Started, a normal handler, or a serial completion. It neither waits nor interrupts subsequent code; return promptly. The current Call / Stream is settled with ErrStopped. Repeated calls in Stopping / Stopped preserve the first cause; nil or unassociated Context is a safe no-op.

## Waiting through an address

```go
if err := addr.Stop(nil); err != nil {
    return err
}

exit, err := addr.Wait(ctx)
if err != nil {
    return err // This wait failed; complete exit is not yet confirmed.
}
return exit.Reason
```

`Wait(ctx) (Exit, error)` waits passively without stopping the instance. On a successful wait, error is nil and Exit.Reason is nil for normal exit or the final failure cause. If an actor exits with DeadlineExceeded, that error appears in Exit.Reason, not the waiting error.

Original addresses support concurrent, repeated, and late waits, including after Runtime.Close. Canceling one waiter does not affect others; retry with a new context. Published results take precedence over canceled contexts. Zero or malformed addresses return ErrSetup; imported addresses lack capability as described above.

Never Wait for yourself inside a handler. A blocked handler, worker, discard, or Stopped prevents complete exit. Stopped.Reason is only a phase-entry snapshot; use Wait for the final reason. Treat shared mutable diagnostic data in errors and PanicError as read-only.

## Shutdown order

Stop closes admission, cancels owned work, and settles pending requests. Queued business messages do not execute. Shutdown then:

1. Waits for the active Started / handler and request cancellation / result settlement.
2. Runs Stopping to unblock workers.
3. Waits for owned workers and discards unadopted results.
4. Releases the channel binding.
5. Enters StateStopped and runs Stopped.
6. Fixes the final Exit.Reason and publishes Wait's result.

Stopping / Stopped errors or panics contribute to the final reason without skipping later cleanup. Exit handlers receive a canceled context. They cannot start tasks or wait for their own mailbox; external finalization I/O should use a separate bounded context directly. Caller-owned Stream loop bodies and draining do not prevent actor exit. Stopping iteration requests cancellation; use Wait to observe actor exit.

## Instance resource ownership

| Point | Responsibility |
| --- | --- |
| Shared injected dependencies | The application owns and releases them; instances do not close shared dependencies. |
| Started | Acquire instance resources and save fields promptly; defer rollback for local resources not transferred. |
| Stopping | Tolerate partial initialization, cancel business work, and unblock workers with bounded operations. |
| Stopped | Release final business resources after workers, discard, and binding release. |
| Failure before startup entry | No lifecycle handler runs; callers release exclusive constructor resources, or move acquisition into Started. |

Bindings acquires no resources. Actorx has no business cleanup registry; arrange release order and exception isolation inside handlers with ordinary Go control flow and defer.

See the [runtime reference](runtime.md#tasks-and-deferred-replies) for task adoption and discard. If completion stores a resource and panics, discard and lifecycle cleanup must share an idempotent release operation. The same applies to caller failure cleanup and startup rollback. See the [real file example](../example_task_test.go).

## Creating actors from a handler

```go
func (s *Session) Started(ctx actorx.Context, _ actorx.Started) error {
    worker, err := ctx.Spawn(&Worker{})
    if err != nil {
        return err // Worker rollback finished; Session chooses to propagate.
    }
    s.worker = worker
    return nil
}
```

`Context.Spawn` shares Runtime.Spawn's startup path and creates an independent instance in the same Runtime. Call it only from Started, normal handlers, or serial completions. It pauses current execution without reentry. Target initialization must not synchronously depend on the creator's further execution; avoid startup, Call, and Wait cycles.

Request cancellation or creator shutdown does not interrupt the wait or automatically stop the target. Startup failure propagation is a business choice. Explicitly Stop / Wait dedicated dependencies; Runtime.Close covers all owned instances. Once startup success is fixed, Spawn remains successful even if the actor exits before return. Read the final reason with Wait. See [composition](../examples/composition/README.md).

Nil or unassociated Context returns ErrSetup; a stopping or stopped creator returns ErrStopped; otherwise a closed Runtime returns ErrClosed.

## Closing Runtime

`rt.Close(ctx)` rejects new Spawn and delivery, stops and waits for all owned instances, including Spawn during binding or Started, then closes Channel. Runtime exclusively owns a successfully claimed Channel; applications close it through Runtime. Failed Open rolls back its own work; Runtime never closes an unclaimed Channel.

A Close timeout only ends this wait; background shutdown continues and later Close calls can wait again. Timeout errors match `ctx.Err()` and include unfinished IDs, phases, and queue / request / task counts for diagnosis. Cancellation does not prove work or cleanup finished.

Close returns waiting or Channel-close errors, not an aggregate of actor Exit.Reason values. Keep original addresses and Wait to observe business exit causes. Supervision, automatic restart, remote death monitoring, Ready, and Drain protocols are outside the current lifecycle API.
