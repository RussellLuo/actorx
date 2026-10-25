package actorx

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// LocalChannel passes ordinary Go values without encoding or copying them.
// The sender and receiver must respect the ownership of shared mutable values.
type LocalChannel struct {
	mu        sync.Mutex
	targets   map[string]*localTarget
	attempted bool
	opened    bool
	closed    bool
}

type localTarget struct {
	receiver Receiver
	bindings map[reflect.Type]CallDescriptor
}

var _ Channel = (*LocalChannel)(nil)

// NewLocalChannel creates an unopened channel for one Runtime.
func NewLocalChannel() *LocalChannel { return &LocalChannel{} }

func (ch *LocalChannel) Open(ctx context.Context) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.attempted || ch.closed {
		return ErrClosed
	}

	ch.attempted = true
	if err := ctx.Err(); err != nil {
		ch.closed = true
		return err
	}

	ch.targets = make(map[string]*localTarget)
	ch.opened = true
	return nil
}

func (ch *LocalChannel) Bind(ctx context.Context, id string, receive Receiver) (func() error, error) {
	ch.mu.Lock()
	opened, closed := ch.opened, ch.closed
	ch.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	if !opened {
		return nil, ErrPhase
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateBinding(id, receive); err != nil {
		return nil, err
	}

	// Freeze descriptions once at Bind, outside the channel lock: Receiver is
	// an external capability and may reenter the channel while describing itself.
	target := &localTarget{receiver: receive, bindings: make(map[reflect.Type]CallDescriptor)}
	for _, descriptor := range receive.Bindings() {
		typ := descriptor.MessageType()
		if descriptor.Validate(CallKind) != nil && descriptor.Validate(StreamKind) != nil {
			return nil, fmt.Errorf("%w: invalid invocation description", ErrSetup)
		}
		if _, exists := target.bindings[typ]; exists {
			return nil, fmt.Errorf("%w: duplicate message %v", ErrSetup, typ)
		}

		target.bindings[typ] = descriptor
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ch.targets[id] != nil {
		return nil, fmt.Errorf("%w: duplicate instance", ErrSetup)
	}

	ch.targets[id] = target
	var once sync.Once
	return func() error {
		once.Do(func() {
			ch.mu.Lock()
			delete(ch.targets, id)
			ch.mu.Unlock()
		})
		return nil
	}, nil
}

func (ch *LocalChannel) Deliver(ctx context.Context, d Delivery, sink Responder) error {
	ch.mu.Lock()
	target, opened, closed := ch.targets[d.Target], ch.opened, ch.closed
	ch.mu.Unlock()

	if closed {
		return ErrClosed
	}
	if !opened {
		return ErrPhase
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.Validate(sink); err != nil {
		return err
	}
	if target == nil {
		return ErrStopped
	}

	descriptor, ok := target.bindings[reflect.TypeOf(d.Message)]
	if !ok {
		return ErrUnhandled
	}
	if err := descriptor.Check(d.Message, d.Kind); err != nil {
		return err
	}

	return target.receiver.Receive(ctx, d, sink)
}

func (ch *LocalChannel) Close() error {
	ch.mu.Lock()
	ch.closed = true
	ch.targets = nil
	ch.mu.Unlock()
	return nil
}
