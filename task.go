package actorx

import (
	"context"
	"sync"
)

type taskWork struct {
	once          sync.Once
	deliver, drop func()
}

// StartTask starts an asynchronous task owned by this actor, reserving one
// mailbox slot for its completion. work runs in a worker and must use snapshots,
// not mutable actor state. complete runs serially on the actor; true transfers
// ownership of value, false calls discard. discard may be nil for plain values.
// Call only from Started, a handler or a serial completion callback.
// Started may start work; completions wait until Running. Stopping and Stopped
// cannot start work, even with an independent parent context.
// If delivery is lost to Stop, discard runs without the mailbox. Addr.Wait joins
// discard as well.
//
// The task inherits ctx's cancellation, deadline and values; actor shutdown
// always cancels it. The returned CancelFunc requests cancellation without
// waiting for completion. A nil or invalid Context returns ErrSetup without
// reserving resources, including wrappers with nil embedded ownership;
// work and complete must be non-nil. ErrFull means no worker was started.
//
// Unblock workers in Stopping; all lifecycle handlers must be bounded.
// Before returning true, complete must leave value safe for discard if it
// panics. If it stores value earlier, discard and lifecycle cleanup must share an
// idempotent release operation; the runtime cannot undo business field writes.
func StartTask[T any](
	ctx Context,
	work func(context.Context) (T, error),
	complete func(T, error) bool,
	discard func(T),
) (context.CancelFunc, error) {
	local := contextOwner(ctx)
	if local == nil || work == nil || complete == nil {
		return nil, ErrSetup
	}

	a := local.actor

	a.mu.Lock()
	if !a.entered || a.stopped {
		a.mu.Unlock()
		return nil, ErrStopped
	}
	if a.rt.ctx.Err() != nil {
		a.mu.Unlock()
		return nil, ErrClosed
	}
	if len(a.queue)+a.reserved >= a.rt.cfg.mailbox || len(a.work) >= a.rt.cfg.workers {
		a.mu.Unlock()
		return nil, ErrFull
	}
	if err := ctx.Err(); err != nil {
		a.mu.Unlock()
		return nil, err
	}

	// Reserve completion capacity before the worker can produce a value.
	jobCtx, cancel := context.WithCancel(ctx)
	stopOwner := context.AfterFunc(a.ctx, cancel)
	w := &taskWork{}
	a.work[w] = struct{}{}
	a.reserved++
	a.workers.Add(1)
	a.mu.Unlock()

	go func() {
		defer a.workers.Done()

		value, err := func() (out T, err error) {
			defer func() {
				if v := recover(); v != nil {
					err = panicError(v)
				}
			}()

			return work(jobCtx)
		}()

		// Delivery and shutdown compete for one settlement and one ownership decision.
		settle := func(deliver bool) {
			w.once.Do(func() {
				defer func() {
					cancel()
					stopOwner()

					a.mu.Lock()
					delete(a.work, w)
					a.mu.Unlock()
				}()

				adopted := false
				defer func() {
					if !adopted && discard != nil {
						a.safely(func() { discard(value) })
					}
				}()

				if deliver {
					adopted = complete(value, err)
				}
			})
		}
		w.deliver = func() { settle(true) }
		w.drop = func() { settle(false) }

		// Replace the reserved slot with a completion without reopening admission.
		a.mu.Lock()
		a.reserved--
		if a.stopped {
			a.mu.Unlock()

			w.drop()
			return
		}

		a.queue = append(a.queue, item{work: w})
		a.signal()
		a.mu.Unlock()
	}()

	return cancel, nil
}
