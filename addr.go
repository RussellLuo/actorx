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
	if won && r.mode == sendMode && err != nil && !errors.Is(err, ErrStopped) && !errors.Is(err, context.Canceled) {
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

// consumedChunk keeps a chunk's budget reserved until the caller callback returns.
// mu arbitrates cancellation before the callback starts; done joins it afterward.
type consumedChunk struct {
	value              any
	done               chan error
	mu                 sync.Mutex
	started, abandoned bool
}

func (v *consumedChunk) cancelled(err error) error {
	v.mu.Lock()
	if !v.started {
		v.abandoned = true
		v.mu.Unlock()
		return err
	}

	v.mu.Unlock()

	// An active callback retains its window even after cancellation.
	<-v.done
	return err
}

type streamBuffer struct {
	mu       sync.Mutex
	chunks   chan any
	maxBytes int
}

func (s *streamBuffer) write(ctx context.Context, r *caller, value any, bytes int) error {
	if bytes < 0 || bytes > s.maxBytes {
		return ErrChunkSize
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-r.done:
		return context.Canceled
	default:
	}

	select {
	case <-r.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	case s.chunks <- value:
		return nil
	}
}

// Stream consumes chunks serially on the calling goroutine. The callback must
// return before Stream can return. Callback errors are returned unchanged.
func (addr Addr) Stream[C any](ctx context.Context, msg any, yield func(C) error) error {
	if yield == nil {
		return ErrSetup
	}

	r, err := addr.prepare(ctx, msg, streamMode)
	if err != nil {
		return err
	}

	// Keep the caller's window budget until its callback and final drain finish,
	// even if the producer has already sent its terminal result.
	defer func() {
		r.finish(nil, context.Canceled)
		r.release()
	}()

	consume := func(v any) (err error) {
		if chunk, ok := v.(*consumedChunk); ok {
			chunk.mu.Lock()
			if chunk.abandoned {
				chunk.mu.Unlock()
				return context.Canceled
			}

			chunk.started = true
			chunk.mu.Unlock()

			v = chunk.value
			defer func() { chunk.done <- err }()
		}

		chunk, err := replyAs[C](v)
		if err != nil {
			return err
		}

		return yield(chunk)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case v := <-r.stream.chunks:
			if err := consume(v); err != nil {
				return err
			}
		case <-r.done:
			// Finish closes done before waiting for a blocked writer. Taking mu now
			// establishes that all accepted writes precede the terminal result.
			r.stream.mu.Lock()
			r.stream.mu.Unlock()

			for {
				if err := ctx.Err(); err != nil {
					return err
				}

				select {
				case v := <-r.stream.chunks:
					if err := consume(v); err != nil {
						return err
					}
				default:
					return r.err
				}
			}
		}
	}
}
