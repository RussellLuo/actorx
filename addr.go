package actorx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
)

// Addr points to one specific instance and never rebinds to a replacement.
// Its private fields intentionally have no default JSON representation.
type Addr struct {
	id      string
	channel *callState
	exit    *exitState
	rt      *Runtime
}

// IsZero reports whether addr is the zero value.
// It does not report whether the target actor is running.
func (addr Addr) IsZero() bool {
	return addr == Addr{}
}

// Stop requests shutdown of the original local instance without waiting for exit.
// A nil result means admission is closed, not that cleanup has finished. Repeated
// stops preserve the first cause. Imported addresses have no control capability;
// a local address returns ErrClosed once its Runtime begins closing.
func (addr Addr) Stop(cause error) error {
	if addr.channel == nil || !ValidInstanceID(addr.id) {
		return ErrStopped
	}
	if addr.rt == nil || addr.exit == nil {
		return fmt.Errorf("actorx: address has no local lifecycle capability")
	}

	rt := addr.rt
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return ErrClosed
	}

	a := rt.actors[addr.id]
	if a == nil {
		// Completion and removal share this lock. The address itself retains
		// only its exit record and Runtime, never the completed execution object.
		select {
		case <-addr.exit.done:
			rt.mu.Unlock()
			return nil
		default:
			rt.mu.Unlock()
			return ErrStopped
		}
	}

	requests, first := a.beginStop(cause)
	rt.mu.Unlock()

	if first {
		a.settleStop(requests)
	}

	return nil
}

func (addr Addr) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("actorx: Addr requires ExportAddr/ImportAddr or a channel-aware JSON codec")
}

func (addr *Addr) UnmarshalJSON([]byte) error {
	return fmt.Errorf("actorx: Addr requires ExportAddr/ImportAddr or a channel-aware JSON codec")
}

// request belongs only to the receiving actor. It never owns a caller window.
type request struct {
	owner     *actor
	id        string
	mode      mode
	msg       any
	ctx       context.Context
	cancel    context.CancelFunc
	once      sync.Once
	watchMu   sync.Mutex
	stopWatch func() bool
	done      chan struct{}
	sink      Responder
}

func (r *request) finish(value any, err error) bool {
	won := false
	r.once.Do(func() {
		won = true
		r.watchMu.Lock()
		if r.stopWatch != nil {
			r.stopWatch()
			r.stopWatch = nil
		}

		r.cancel()

		// Settle the reply before removing the target record. Complete exit must not
		// precede a local caller's result publication.
		if r.mode != sendMode {
			r.sink.Complete(value, err)
		}

		r.owner.mu.Lock()
		delete(r.owner.requests, r)
		close(r.done)
		r.owner.mu.Unlock()
		r.watchMu.Unlock()
	})
	if won && r.mode == sendMode && err != nil &&
		!errors.Is(err, ErrStopped) && !errors.Is(err, context.Canceled) {
		slog.Error("actor Send failed", "id", r.owner.id, "error", err)
	}

	return won
}

func (r *request) watch(parent context.Context) {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()

	select {
	case <-r.done:
		return
	default:
	}

	r.stopWatch = context.AfterFunc(parent, func() { r.finish(nil, parent.Err()) })
}

// caller is a caller-side association. It has no receiving Actor or request pointer.
type caller struct {
	id        string
	channel   *callState
	mode      mode
	ctx       context.Context
	cancel    context.CancelFunc
	once      sync.Once
	watchMu   sync.Mutex
	stopWatch func() bool
	done      chan struct{}
	value     any
	err       error
	stream    *streamBuffer
	release   func()
}

func (c *caller) finish(value any, err error) bool {
	won := false
	c.once.Do(func() {
		won = true
		c.value, c.err = value, err

		// Streams retain the caller window until the iterator's current body
		// and cleanup have finished, even if the producer has already stopped.
		if c.mode != streamMode {
			c.release()
		}

		c.watchMu.Lock()
		if c.stopWatch != nil {
			c.stopWatch()
			c.stopWatch = nil
		}

		c.cancel()
		c.channel.mu.Lock()
		delete(c.channel.calls, c.id)
		close(c.done)
		c.channel.mu.Unlock()
		c.watchMu.Unlock()

		if c.stream != nil {
			// Publish the producer's terminal result after every accepted write.
			// Consumption and its acknowledgements belong to the iterator.
			c.stream.seal()
		}
	})

	return won
}

func (c *caller) watch(parent context.Context) {
	c.watchMu.Lock()
	defer c.watchMu.Unlock()

	select {
	case <-c.done:
		return
	default:
	}

	c.stopWatch = context.AfterFunc(parent, func() { c.finish(nil, parent.Err()) })
}

func (addr Addr) prepare(ctx context.Context, msg any, m mode) (*caller, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if addr.channel == nil || addr.id == "" {
		return nil, ErrStopped
	}

	return addr.channel.deliver(ctx, addr.id, msg, m)
}

// Send admits a message without waiting for its handler. ctx controls admission;
// after admission, the request follows the actor's lifetime.
func (addr Addr) Send(ctx context.Context, msg any) error {
	_, err := addr.prepare(ctx, msg, sendMode)
	return err
}

func replyAs[R any](v any) (R, error) {
	var zero R
	if v == nil && reflect.TypeFor[R]().Kind() == reflect.Interface {
		return zero, nil
	}

	out, ok := v.(R)
	if !ok {
		return zero, fmt.Errorf("%w: got %T, want %v", ErrReplyType, v, reflect.TypeFor[R]())
	}

	return out, nil
}

// Call waits for one reply of type R or for cancellation or actor shutdown.
func (addr Addr) Call[R any](ctx context.Context, msg any) (R, error) {
	var zero R
	r, err := addr.prepare(ctx, msg, callMode)
	if err != nil {
		return zero, err
	}

	<-r.done
	if r.err != nil {
		return zero, r.err
	}

	return replyAs[R](r.value)
}
