package actorx

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
)

type item struct {
	msg     any
	req     *request
	binding Binding
	work    *taskWork
}

// Actor supplies instance-bound message and lifecycle handlers.
// Bindings only constructs entries: it must not read state, perform I/O or acquire resources.
// Each instance must bind its own receiver and must not share mutable binding storage.
// For pointer implementations, constructing Bindings on a nil receiver must be safe
// and yield the same descriptions as a live receiver. Spawn still rejects nil actors.
type Actor interface {
	Bindings() []Binding
}

// actor is the Runtime-owned execution state.
type actor struct {
	mu             sync.Mutex
	rt             *Runtime
	contextWrapper ContextWrapper
	id             string
	ctx            context.Context
	cancel         context.CancelFunc

	// mu protects lifecycle flags, phase, cause and the state collections.
	// bindings become immutable before channel binding and startup.
	entered bool // Started was entered, so shutdown must dispatch lifecycle notices.
	started bool // Started succeeded and ordinary message admission opened.
	stopped bool // Admission is closed; stopDone joins the first Stop's cancellation.
	state   State

	// phase is diagnostic only and does not control lifecycle transitions.
	phase    string
	cause    error
	bindings []Binding
	queue    []item
	reserved int
	requests map[*request]struct{}
	work     map[*taskWork]struct{}

	workers        sync.WaitGroup
	wake           chan struct{}
	ready          chan error
	stopDone       chan struct{}
	exit           *exitState
	releaseBinding func() error
}

func newActor(rt *Runtime) *actor {
	ctx, cancel := context.WithCancel(context.Background())
	return &actor{
		rt:       rt,
		id:       newID(),
		ctx:      ctx,
		cancel:   cancel,
		phase:    phaseSetup,
		requests: make(map[*request]struct{}),
		work:     make(map[*taskWork]struct{}),
		wake:     make(chan struct{}, 1),
		ready:    make(chan error, 1),
		stopDone: make(chan struct{}),
		exit:     &exitState{done: make(chan struct{})},
	}
}

// addr returns the address of this actor instance.
func (a *actor) addr() Addr {
	return Addr{
		id:      a.id,
		channel: a.rt.calls,
		exit:    a.exit,
		rt:      a.rt,
	}
}

// reason returns the current cause snapshot for lifecycle notifications.
func (a *actor) reason() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.cause
}

func (a *actor) signal() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// stop is the shared internal entry, including Close and startup/panic failures.
func (a *actor) stop(cause error) {
	requests, first := a.beginStop(cause)
	if first {
		a.settleStop(requests)
	}
}

// beginStop fixes admission and the first cause under mu. Addr.Stop also holds
// Runtime.mu here so acceptance is ordered with Close and startup publication.
func (a *actor) beginStop(cause error) ([]*request, bool) {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return nil, false
	}

	a.stopped = true
	a.state = StateStopping
	a.cause = errors.Join(a.cause, cause)
	requests := make([]*request, 0, len(a.requests))
	for r := range a.requests {
		requests = append(requests, r)
	}

	a.mu.Unlock()
	return requests, true
}

// Cancellation and responders may block or reenter the Runtime. No Runtime or
// Actor lock may be held here; run waits on stopDone before dispatching Stopping.
func (a *actor) settleStop(requests []*request) {
	a.cancel()
	for _, r := range requests {
		r.finish(nil, ErrStopped)
	}

	close(a.stopDone)
	a.signal()
}

func (a *actor) enqueue(r *request) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.started || a.stopped {
		return ErrStopped
	}
	if a.rt.ctx.Err() != nil {
		return ErrClosed
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}

	b, found := a.binding(reflect.TypeOf(r.msg))
	if !found || b.phase != noLifecyclePhase {
		return ErrUnhandled
	}
	if err := b.descriptor.Check(r.msg, Kind(r.mode)); err != nil {
		return err
	}
	if len(a.queue)+a.reserved >= a.rt.cfg.mailbox || len(a.requests) >= a.rt.cfg.pending {
		return ErrFull
	}

	a.requests[r] = struct{}{}
	a.queue = append(a.queue, item{msg: r.msg, req: r, binding: b})
	a.signal()
	return nil
}

// binding uses the same immutable table for business messages and lifecycle notices.
// The table is installed before the execution loop starts.
func (a *actor) binding(typ reflect.Type) (Binding, bool) {
	for _, b := range a.bindings {
		if b.descriptor.MessageType() == typ {
			return b, true
		}
	}

	return Binding{}, false
}

func (a *actor) run() {
	if a.entered {
		a.start()
		a.runMailbox()
	}

	// Stop closes admission before it finishes publishing request results.
	// Do not enter Stopping until those results have been settled.
	<-a.stopDone
	if a.entered {
		a.setPhase(phaseStopping)
		a.record(a.dispatch(item{msg: Stopping{Reason: a.reason()}}))
	}

	a.cleanup()
	if a.entered {
		a.mu.Lock()
		a.state = StateStopped
		a.phase = phaseStopped
		a.mu.Unlock()

		a.record(a.dispatch(item{msg: Stopped{Reason: a.reason()}}))
	}

	a.finish()
}

func (a *actor) runMailbox() {
	for {
		a.mu.Lock()
		if a.stopped {
			a.mu.Unlock()
			return
		}
		if len(a.queue) == 0 {
			a.phase = phaseIdle
			a.mu.Unlock()

			<-a.wake
			continue
		}

		next := a.queue[0]
		a.queue[0] = item{}
		a.queue = a.queue[1:]
		a.phase = phaseHandler
		if next.work != nil {
			a.phase = phaseCompletion
		}

		a.mu.Unlock()

		if err := a.dispatch(next); err != nil && next.req != nil {
			next.req.finish(nil, err)
		}
	}
}

// dispatch is the sole handler invocation path. Lifecycle notices carry the actor
// context and no business request. The execution loop dispatches them at phase
// boundaries, independently of mailbox admission and capacity.
func (a *actor) dispatch(next item) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = panicError(v)
			if next.req != nil || next.work != nil {
				a.record(err)
				a.stop(nil)
			}
		}
	}()

	if next.work != nil {
		next.work.deliver()
		return nil
	}

	ctx := a.ctx
	if next.req != nil {
		select {
		case <-next.req.done:
			return nil
		default:
		}

		ctx = next.req.ctx
	} else {
		// Lifecycle handlers are optional and use the same binding table.
		next.binding, _ = a.binding(reflect.TypeOf(next.msg))
	}
	if next.binding.invoke == nil {
		return nil
	}

	handlerCtx, err := a.newContext(ctx)
	if err != nil {
		if next.req != nil {
			a.record(err)
			next.req.finish(nil, err)
			a.stop(nil)
		}
		return err
	}
	return next.binding.invoke(handlerCtx, next.msg, next.req)
}

func (a *actor) record(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && !errors.Is(a.cause, err) {
		a.cause = errors.Join(a.cause, err)
	}
}

func (a *actor) setPhase(phase string) {
	a.mu.Lock()
	a.phase = phase
	a.mu.Unlock()
}

func (a *actor) safely(fn func()) {
	defer func() {
		if v := recover(); v != nil {
			err := panicError(v)
			a.record(err)
			slog.Error("actor cleanup panic", "id", a.id, "error", err)
		}
	}()

	fn()
}

func (a *actor) cleanup() {
	a.setPhase(phaseWorkers)
	a.workers.Wait()

	// Workers can leave queued completions behind; release their unadopted values.
	a.setPhase(phaseRollback)
	a.mu.Lock()
	jobs := make([]*taskWork, 0, len(a.work))
	for w := range a.work {
		jobs = append(jobs, w)
	}

	a.queue = nil
	a.mu.Unlock()

	for _, w := range jobs {
		w.drop()
	}

	if a.releaseBinding != nil {
		a.safely(func() { a.record(a.releaseBinding()) })
	}
}

func (a *actor) finish() {
	a.rt.mu.Lock()
	a.mu.Lock()
	a.state = StateStopped
	a.phase = phaseDone
	a.exit.result = Exit{Reason: a.cause}
	a.mu.Unlock()

	// Fix the result before publishing the exit signal. A Close
	// snapshot must either include this actor or observe its completed exit.
	close(a.exit.done)
	delete(a.rt.actors, a.id)
	a.rt.mu.Unlock()
}
