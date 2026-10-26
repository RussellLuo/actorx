package actorx

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-aie/actorx/internal/channelutil"
)

// Context carries a handler's cancellation, deadline, values, and actor ownership.
// Runtime supplies it to handlers. Extensions must embed a valid framework
// Context to preserve its private execution ownership. The zero value is nil;
// methods cannot be called on a nil interface. Use StartTask for owned work.
type Context interface {
	context.Context

	// WithContext replaces the parent while preserving this actor's capabilities.
	// A nil parent or failed wrapping panics. Shutdown still cancels owned tasks.
	WithContext(context.Context) Context

	// Stop requests shutdown without waiting for this handler or complete exit.
	Stop(error)

	// Spawn creates an independent local instance and waits for Started. Call
	// from Started, a handler or serial completion; there is no stop propagation.
	Spawn(Actor) (Addr, error)

	// State is a concurrent-safe local lifecycle snapshot, not a readiness test.
	State() State

	actorContext() *localContext
}

// ContextWrapper adds capabilities to one instance's execution contexts. Runtime
// calls it before each bound handler, including lifecycle handlers, and when
// WithContext derives a context. It must be bounded, perform no I/O or resource
// acquisition, and preserve the supplied context's cancellation, deadline,
// values and ownership.
// Its environment must be immutable or synchronized by its owner. No Runtime or
// actor lock is held while it runs. Return the supplied Context or a wrapper
// embedding it, never a context retained from another invocation.
//
// Invalid results return ErrSetup; a wrapper panic also preserves PanicError.
// Failure skips the callback: Started rolls back, an ordinary request fails and
// stops the actor, and shutdown continues its remaining stages. WithContext
// panics with the setup error because it has no error result.
type ContextWrapper func(Context) Context

// ContextFactory binds a wrapper to a Spawn's Actor once, after valid Bindings
// are installed and before Channel.Bind or Started. It is installed with
// WithContextFactory; NewRuntime and Describe never call it. Concurrent Spawns
// may call the factory concurrently, without Runtime or actor locks held.
//
// Like ContextWrapper, it must be bounded, perform no I/O, acquire no resources,
// and neither invoke handlers nor access mutable business state. Capture only
// immutable or synchronized instance environment in the returned wrapper.
//
// A nil wrapper, error or panic fails Spawn with ErrSetup before lifecycle entry;
// errors retain their chain and panics retain PanicError. Return an identity
// wrapper to keep the local context. Core provides no factory resource cleanup.
type ContextFactory func(Actor) (ContextWrapper, error)

type localContext struct {
	context.Context
	actor *actor
}

func (ctx *localContext) actorContext() *localContext { return ctx }

// contextOwner validates only the private execution seam. Empty embedded
// interfaces can panic during method promotion; recover only at this boundary,
// never around business work or completion/discard callbacks.
func contextOwner(ctx Context) (local *localContext) {
	defer func() {
		if recover() != nil {
			local = nil
		}
	}()

	if channelutil.IsNil(ctx) {
		return nil
	}

	local = ctx.actorContext()
	if local == nil || local.actor == nil || local.actor.rt == nil || channelutil.IsNil(local.Context) {
		return nil
	}

	return local
}

// newContext is the single creation and wrapping path. Compare the precise
// local context, not just the actor: an old request has different parent data.
func (a *actor) newContext(parent context.Context) (ctx Context, err error) {
	local := &localContext{Context: parent, actor: a}
	if a.contextWrapper == nil {
		return local, nil
	}

	defer func() {
		if v := recover(); v != nil {
			ctx = nil
			err = errors.Join(ErrSetup, panicError(v))
		}
	}()

	ctx = a.contextWrapper(local)
	if contextOwner(ctx) != local {
		return nil, fmt.Errorf("%w: context wrapper must preserve the supplied context", ErrSetup)
	}

	return ctx, nil
}

// WithContext returns a context with the same actor, a different parent, and
// capabilities from the same wrapper. Use context.WithoutCancel(ctx) to detach work
// from a request. Actor shutdown still cancels owned tasks. A nil parent panics;
// a wrapping failure panics with its setup error. The factory is not called again.
func (ctx *localContext) WithContext(parent context.Context) Context {
	if parent == nil {
		panic("actorx: nil parent context")
	}

	derived, err := ctx.actor.newContext(parent)
	if err != nil {
		panic(err)
	}

	return derived
}

// Stop requests this actor's shutdown without waiting for exit. The current
// handler must still return. Repeated calls preserve the first cause.
func (ctx *localContext) Stop(cause error) {
	if ctx != nil && ctx.actor != nil {
		ctx.actor.stop(cause)
	}
}

// Spawn creates an independent local instance in this actor's Runtime and waits
// for Started. Call only from Started, a handler or a serial task completion.
// The creator pauses without reentry; its cancellation or stop does not interrupt
// this wait or stop the child. Initialization must not depend on the creator's
// further execution. Failure returns a zero Addr after rollback. A stopping owner
// returns ErrStopped; the shared Runtime close boundary returns ErrClosed.
// The child uses the same Runtime's ContextFactory with its own Actor as input;
// it does not inherit the creator's wrapper or instance identity.
func (ctx *localContext) Spawn(value Actor) (Addr, error) {
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
