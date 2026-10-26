package actorx

import (
	"context"
	"fmt"
	"reflect"
)

type mode uint8

const (
	sendMode mode = iota
	callMode
	streamMode
)

// noLifecyclePhase marks bindings without a lifecycle phase, never an actor state.
const noLifecyclePhase State = ^State(0)

// Binding describes one concrete message or lifecycle event type.
// Construct it with a Handle function.
type Binding struct {
	descriptor CallDescriptor
	phase      State // noLifecyclePhase for ordinary messages; otherwise a lifecycle phase.
	invoke     func(Context, any, *request) error
	invalid    bool
}

func bindingFor[Q any]() Binding {
	t := reflect.TypeFor[Q]()
	phase := lifecycleType(t)
	return Binding{
		descriptor: Message[Q](),
		phase:      phase,
		invalid:    t.Kind() == reflect.Interface || phase != noLifecyclePhase,
	}
}

// lifecycleType validates binding and invocation descriptions. It recognizes pointer
// forms too so setup rejects them instead of registering ordinary messages.
func lifecycleType(t reflect.Type) State {
	if t == nil {
		return noLifecyclePhase
	}

	var seen map[reflect.Type]bool
	for t.Kind() == reflect.Pointer {
		// Defined pointer types may be recursive (type P *P).
		if t.Name() != "" {
			if seen[t] {
				return noLifecyclePhase
			}
			if seen == nil {
				seen = make(map[reflect.Type]bool)
			}

			seen[t] = true
		}

		t = t.Elem()
	}

	switch t {
	case reflect.TypeFor[Started]():
		return StateStarted
	case reflect.TypeFor[Stopping]():
		return StateStopping
	case reflect.TypeFor[Stopped]():
		return StateStopped
	}

	return noLifecyclePhase
}

// Handle binds a synchronous request and reply handler for the concrete type Q.
func Handle[Q, R any](h func(Context, Q) (R, error)) Binding {
	b := bindingFor[Q]()
	b.descriptor = Request[Q, R]()
	b.invalid = b.invalid || h == nil
	b.invoke = func(ctx Context, msg any, r *request) error {
		out, err := h(ctx, msg.(Q))
		r.finish(out, err)
		return nil
	}
	return b
}

// HandleMessage binds a handler that completes without a reply value. Exact
// Started, Stopping and Stopped value types bind runtime-owned lifecycle notices.
func HandleMessage[M any](h func(Context, M) error) Binding {
	b := bindingFor[M]()
	if b.phase != noLifecyclePhase && b.descriptor.MessageType().Kind() != reflect.Pointer {
		b.invalid = false
	}

	b.invalid = b.invalid || h == nil
	b.invoke = func(ctx Context, msg any, r *request) error {
		err := h(ctx, msg.(M))
		if r == nil {
			return err
		}

		r.finish(nil, err)
		return nil
	}
	return b
}

// Reply is a one-shot local capability. Complete returns false after cancellation,
// actor exit, or a previous completion. It must not be persisted or sent as data.
type Reply[R any] struct{ req *request }

func (r Reply[R]) Complete(value R, err error) bool {
	if r.req == nil {
		return false
	}

	return r.req.finish(value, err)
}

// HandleDeferred leaves the request open after the handler returns successfully.
// The handler normally uses StartTask; its serial completion calls Complete.
func HandleDeferred[Q, R any](h func(Context, Q, Reply[R]) error) Binding {
	b := bindingFor[Q]()
	b.descriptor = Request[Q, R]()
	b.invalid = b.invalid || h == nil
	b.invoke = func(ctx Context, msg any, r *request) error {
		return h(ctx, msg.(Q), Reply[R]{r})
	}
	return b
}

// StreamReply writes to a bounded caller-owned window. Write may block and must
// run in a StartTask worker, never in the actor's state handler. bytes is
// the retained payload size, supplied by the producer (for text, len(text)).
// Values are not copied.
type StreamReply[C any] struct{ req *request }

func (s StreamReply[C]) Write(ctx context.Context, value C, bytes int) error {
	if s.req == nil || s.req.mode != streamMode {
		return ErrMode
	}

	return s.req.sink.Write(ctx, value, bytes)
}

func (s StreamReply[C]) Complete(err error) bool {
	if s.req == nil {
		return false
	}

	return s.req.finish(nil, err)
}

// HandleStream binds a streaming handler. Only Addr.Stream can invoke it;
// Send and Call fail before execution.
func HandleStream[Q, C any](h func(Context, Q, StreamReply[C]) error) Binding {
	b := bindingFor[Q]()
	b.descriptor = Streaming[Q, C]()
	b.invalid = b.invalid || h == nil
	b.invoke = func(ctx Context, msg any, r *request) error {
		return h(ctx, msg.(Q), StreamReply[C]{r})
	}
	return b
}

// install validates and copies all entries before any channel or lifecycle work.
func (a *actor) install(bindings []Binding) error {
	if err := validateBindings(bindings); err != nil {
		return err
	}

	a.bindings = append([]Binding(nil), bindings...)
	return nil
}

// validateBindings is shared by installation and static description. Validate
// lifecycle entries too, before a metadata caller can filter them out.
func validateBindings(bindings []Binding) error {
	seen := make(map[reflect.Type]bool, len(bindings))

	for _, b := range bindings {
		if b.phase == noLifecyclePhase &&
			b.descriptor.Validate(CallKind) != nil &&
			b.descriptor.Validate(StreamKind) != nil {
			return fmt.Errorf("%w: invalid invocation description", ErrSetup)
		}
		if b.invalid || b.descriptor.MessageType() == nil || b.invoke == nil || seen[b.descriptor.MessageType()] {
			return fmt.Errorf("%w: invalid or duplicate message %v", ErrSetup, b.descriptor.MessageType())
		}

		seen[b.descriptor.MessageType()] = true
	}

	return nil
}

// Descriptor returns the immutable invocation types and modes of this binding.
// It carries no handler or operation name.
func (b Binding) Descriptor() CallDescriptor { return b.descriptor }
