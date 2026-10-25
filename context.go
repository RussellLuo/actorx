package actorx

import "context"

// Context carries a handler's cancellation, deadline, values, and actor ownership.
// The runtime supplies it when invoking a handler; it can be passed directly to
// APIs that accept context.Context. The zero value cannot Spawn or StartTask.
//
// Use StartTask only from Started, a handler or a serial completion callback.
// Workers receive a standard context.Context and must not access mutable actor state.
type Context struct {
	context.Context
	actor *actor
}

// WithContext returns a new context with the same actor and a different parent.
// Use it to apply a derived deadline or to detach work from a request with
// context.WithoutCancel(ctx). Actor shutdown still cancels owned tasks.
func (ctx *Context) WithContext(parent context.Context) *Context {
	if parent == nil {
		panic("actorx: nil parent context")
	}

	return &Context{Context: parent, actor: ctx.actor}
}

// Stop requests this actor's shutdown through the shared internal stop path.
// It does not wait for exit; the current handler must still return. A nil or
// unassociated Context is a safe no-op. Repeated calls preserve the first cause.
func (ctx *Context) Stop(cause error) {
	if ctx != nil && ctx.actor != nil {
		ctx.actor.stop(cause)
	}
}

// Spawn creates an independent instance in this actor's Runtime and waits for
// Started. Call only from Started, a handler or a serial task completion callback.
// The current handler pauses without reentry; request cancellation and stopping
// the creator do not interrupt this wait or automatically stop the new instance.
// Target initialization must not depend on the creator's further execution.
// Failure returns a zero Addr after target rollback. A nil/unassociated Context
// returns ErrSetup; a stopping/stopped owner returns ErrStopped. Otherwise the
// Runtime's close boundary is checked by the shared Spawn path (ErrClosed).
func (ctx *Context) Spawn(value Actor) (Addr, error) {
	if ctx == nil || ctx.actor == nil {
		return Addr{}, ErrSetup
	}

	a := ctx.actor
	a.mu.Lock()
	stopped := a.stopped || a.state == StateStopped
	a.mu.Unlock()

	if stopped {
		return Addr{}, ErrStopped
	}

	return a.rt.Spawn(value)
}
