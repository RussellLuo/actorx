package actorx

import (
	"context"

	"github.com/go-aie/actorx/internal/channelutil"
)

// Kind selects admission-only Send, a final result, or a stream. It is channel
// plumbing; business callers select the corresponding Addr method.
type Kind uint8

const (
	SendKind Kind = iota
	CallKind
	StreamKind
)

// Responder is a bounded reply capability, not a business address. Complete is
// one-shot. Write may block and must not run on a shared receive callback.
// Channels must deliver replies by correlation ID, never retain an Actor/request.
type Responder interface {
	// Addresses imports decoded addresses into the owner without exposing an Actor.
	Addresses() AddressImporter

	// Complete settles at most once. Successful stream termination follows all
	// accepted writes; the caller drains them before returning success.
	Complete(any, error) bool

	// Write accepts a chunk into a bounded window; it need not wait for consumption.
	// ctx bounds admission, including waiting behind other writers or for capacity.
	Write(context.Context, any, int) error

	// WriteConsumed uses ctx to bound admission and waiting for the iterator's
	// consumption or discard receipt. Cancellation ends the wait without revoking
	// an accepted chunk or releasing its data or consumption budget. Values are
	// not copied; a context error does not permit reusing their backing storage.
	// A body that continues acknowledges nil; early exit or discard acknowledges
	// context.Canceled, and a type mismatch acknowledges ErrReplyType. An active
	// body must exit before acknowledgement. Nil proves successful consumption;
	// context errors do not distinguish admission, wait cancellation or discard
	// and must not trigger automatic retries. If receipt and cancellation are both
	// ready, either may win. Complete cancels the request context, so writes using
	// it may return context.Canceled even after successful producer completion.
	// Use a request-owned worker, never a shared receive callback.
	WriteConsumed(context.Context, any, int) error
}

// Receiver validates and enqueues into the ordinary actor mailbox. A nil result
// proves admission, not handler completion. The context bounds target execution
// for Call/Stream and admission only for Send. Delivery.Correlation must be unique;
// Delivery.Target must match the bound instance.
type Receiver interface {
	// Addresses imports decoded addresses into the owner without exposing an Actor.
	Addresses() AddressImporter

	Receive(context.Context, Delivery, Responder) error

	// Bindings returns an independent snapshot of all ordinary bindings, without
	// handlers or runtime-only lifecycle notices. Channels use these descriptions
	// to validate message types and invocation modes before calling Receive.
	Bindings() []CallDescriptor
}

// Delivery describes one logical invocation. Context carries its deadline and
// cancellation; replies belong exclusively to the supplied Responder.
type Delivery struct {
	Target      string
	Correlation string
	Kind        Kind
	Message     any
}

// Validate checks the common delivery envelope and reply capability. Message
// types and modes are checked against the target's CallDescriptor separately;
// wire-specific lengths and encoding limits remain with the network channel.
func (d Delivery) Validate(reply Responder) error {
	if !ValidInstanceID(d.Target) || d.Correlation == "" || d.Kind > StreamKind || channelutil.IsNil(reply) {
		return ErrMode
	}

	return nil
}

// Channel is the single implementation boundary used by Runtime. Open claims
// this instance once and initializes it; a failed Open rolls back its own work
// and cannot be retried. A rejected duplicate Open must leave the owner intact.
// Bind succeeds only when ready and returns an idempotent release. Deliver never
// retries: Send success proves mailbox admission; Call/Stream complete via the
// Responder. Close rejects new work, is idempotent, and waits for owned work to
// exit. Runtime calls Close only after its own successful Open.
// Implementations can share envelope and binding checks through Delivery.Validate
// and ValidateBinding; wire formats and transport resource ownership stay private.
type Channel interface {
	Open(context.Context) error
	Bind(context.Context, string, Receiver) (release func() error, err error)
	Deliver(context.Context, Delivery, Responder) error
	Close() error
}

type actorReceiver struct{ actor *actor }

func (r actorReceiver) Addresses() AddressImporter { return r.actor.rt.calls }

func (receiver actorReceiver) Receive(ctx context.Context, delivery Delivery, sink Responder) error {
	if err := delivery.Validate(sink); err != nil {
		return err
	}

	a := receiver.actor
	if delivery.Target != a.id {
		return ErrStopped
	}

	id, kind, msg := delivery.Correlation, delivery.Kind, delivery.Message
	parent := ctx
	if kind == SendKind {
		parent = a.ctx
	}

	reqCtx, cancel := context.WithCancel(parent)
	r := &request{
		owner:  a,
		id:     id,
		mode:   mode(kind),
		msg:    msg,
		ctx:    reqCtx,
		cancel: cancel,
		done:   make(chan struct{}),
		sink:   sink,
	}
	if err := ctx.Err(); err != nil {
		cancel()
		return err
	}
	if err := a.enqueue(r); err != nil {
		cancel()
		return err
	}

	r.watch(parent)
	return nil
}

func (receiver actorReceiver) Bindings() []CallDescriptor {
	var out []CallDescriptor
	for _, b := range receiver.actor.bindings {
		if b.phase == noLifecyclePhase {
			out = append(out, b.Descriptor())
		}
	}

	return out
}
