# Call semantics

English | [中文](runtime.zh.md)

Send/Call/Stream follow one path: **validate the contract → select and fix a generation → wait for READY → send through NATS**. The target runs business code through actorx mailboxes, including same-process calls. See [concepts](concepts.md) for the address model.

## Completion, ordering and errors

| Call or observation | Meaning |
| --- | --- |
| Successful Send | Target generation admitted the message; its handler may still be running |
| Call returns | Valid final result or definite business error; no rollback implied |
| Successful Stream | Valid successful termination, consecutive sequences, all chunks consumed and active callback returned |
| Local validation, capacity or explicit pre-publish failure | This business frame was not sent; return the original error |
| Exact target refuses before mailbox admission | Definite non-admission for this attempt; no transparent retry |
| Possible send without required ACK, result or termination | ErrOutcomeUnknown plus cancellation, deadline or transport cause |

Ordinary business errors end only the request, retain state changes and leave the generation available. Handler errors after Send ACK use core asynchronous logging without revoking admission. Panic stops the instance under core rules; [Wait](lifecycle.md) observes complete exit.

Handlers within an instance run serially; concurrent calls and different senders have no global order. Counter waits for Send ACK before Call and uses its synchronous handler to observe the preceding increment. ACK itself is no business-completion barrier. StartTask external work can overlap subsequent handlers; this ordering does not prove task completion.

## Deadlines and cancellation

Requests default to five seconds without a deadline; explicit deadlines are capped at sixty seconds. Contract reads, selection, activation and response waits consume one total deadline, never reset per stage. Admitted Send follows target instance lifetime; caller cancellation only ends the ACK wait. Call/Stream propagate cancellation best effort, with receiving deadlines as a fallback. Cancellation does not prove handlers, tasks or effects have stopped.

Disconnected business messages are neither buffered nor replayed. Applications handle uncertain outcomes through business queries, idempotency or compensation. The framework neither transparently resends nor stores queryable business results. See the [wire contract](contract.md) for deadline encoding and cancellation identity checks.

## Stream

`addr.Stream[C](ctx, msg, yield)` invokes yield serially in order. Missing, duplicate, out-of-order or malformed chunks/termination cannot succeed. Valid business errors are definite failures and may stop queued consumption without undoing consumed data. Without valid termination, ErrOutcomeUnknown remains. Cancellation after valid termination adds no remote uncertainty. Callback errors retain the original Go error, plus uncertainty when terminal evidence is absent.

Every return path joins an active callback. Cancellation/failure starts no further callback, and an observed business/protocol error survives a later timeout during that join. Blocked callbacks retain their window, correlation and Runtime work reservation and prevent Close success; network cancellation can still propagate while they block. Callbacks should return promptly or use captured context for cancellation. Never synchronously wait for the enclosing Stream or Runtime.Close inside a callback.

Each stream reserves four items/64 KiB encoded payload, including its active callback; a Runtime allows at most 16 incoming/outgoing streams combined. Credit returns only after consumption, using actual encoded bytes. `Responder.WriteConsumed` additionally waits for this acknowledgement. Partial failure never resumes automatically; disconnect cancels existing incoming streams and reconnect does not restore them. Late frames cannot recreate associations or consumers. See the [wire envelope](contract.md#unified-envelope) and [resource table](contract.md#time-and-development-resource-profile) for sequences, window acknowledgements and aggregate budgets.

For long streams, use a short `actorx.HandleStream` handler to start `actorx.StartTask(ctx, ...)`. The worker uses snapshots and its task context for I/O or downstream Stream, forwards chunks through `reply.Write`, and the serial completion calls `reply.Complete(err)`. State changes stay in serial handlers/completions, leaving the Actor able to handle ordinary Notify. Obtain Addr/Self before starting the task; workers do not retain distributed context views.

Streams share the five-second default and sixty-second maximum total deadline; choose an explicit subscription deadline. Canceling a stream ends only that request, without unsubscribing Self or stopping collaborators. See [lifetime responsibility](lifecycle.md#object-lifetime).

## Contracts and capacity

Callers and workers use shared DTOs at stable package paths. Named Go identities, schemas and modes must match for messages and results/chunks. One Type has an immutable complete contract; adding a handler requires a new Type. See [contract upgrades](deployment.md#contract-upgrades). Nested Addr values automatically bind the receiving Runtime.

Wire values use a strict JSON subset with a 64 KiB limit per payload/chunk. Custom codecs, execution capabilities, invalid numbers and unsupported shapes fail. The [wire reference](contract.md#schema-subset-and-fingerprint) owns the complete compatibility table and depth/node/allocation limits.

Local capacity failures return ErrFull. SDK overflow provides only asynchronous diagnostics, never evidence that a particular request did not execute. Explicit refusal also requires a valid reply path; saturated systems may still produce uncertainty. Observe faults through `rt.Diagnostics()`. Network work and core handlers/tasks have separate budgets, released only when actual work settles. See the [complete limits](contract.md#time-and-development-resource-profile).

## Error classifications

Use errors.Is for framework and context errors rather than parsing diagnostics. Remote arbitrary business errors retain text, not custom Go types. Framework/context classifications survive downstream wrapping with at most four source records. A definite outer response does not erase downstream uncertainty.

| dist error | Meaning |
| --- | --- |
| ErrTypeUnavailable | No Factory responds to type activation; this does not authorize replacement of an existing generation |
| ErrNotActivated | Cold control found no current generation |
| ErrContract | Full contract, schema or mode mismatch; may also wrap core classifications |
| ErrProtocol | Unsupported wire version or protocol frame |
| ErrOwnerUnavailable | Original owner is unreachable; ownership is retained |
| ErrActivationUnknown | Uncertain activation or ownership update requiring original key/generation reconciliation |
| ErrOutcomeUnknown | Business request may have been sent without the operation's required completion evidence |
| ErrExitUnknown | Original terminal evidence is missing, expired or unverifiable |

Local setup, closure, stop, capacity, message, mode and result-type errors reuse actorx.ErrSetup/ErrClosed/ErrStopped/ErrFull/ErrUnhandled/ErrMode/ErrReplyType and related classifications. Uncertainty may include context.Canceled or DeadlineExceeded; timeout alone never proves non-execution. Applications define business queries, idempotency and compensation.
