package actorx

import (
	"context"
	"sync"
)

// callState is Runtime-owned coordination, shared by every channel implementation.
// It owns caller associations and budgets; it is not another transport layer.
type callState struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	channel Channel
	closed  bool
	pending int
	limit   int
	calls   map[string]*caller

	streamItems int
	chunkBytes  int
}

func newCallState(ch Channel, c config) *callState {
	ctx, cancel := context.WithCancel(context.Background())
	return &callState{
		channel:     ch,
		calls:       make(map[string]*caller),
		ctx:         ctx,
		cancel:      cancel,
		limit:       c.calls,
		streamItems: c.streamItems,
		chunkBytes:  c.chunkBytes,
	}
}

func (ch *callState) release() {
	ch.mu.Lock()
	ch.pending--
	ch.mu.Unlock()
}

// correlationReply routes by ID; it holds neither the caller record nor a target.
type correlationReply struct {
	ch *callState
	id string
}

func (r correlationReply) Addresses() AddressImporter { return r.ch }

func (r correlationReply) lookup() *caller {
	r.ch.mu.Lock()
	defer r.ch.mu.Unlock()

	return r.ch.calls[r.id]
}

func (r correlationReply) Complete(v any, err error) bool {
	if c := r.lookup(); c != nil {
		return c.finish(v, err)
	}

	return false
}

func (r correlationReply) Write(ctx context.Context, v any, bytes int) error {
	c := r.lookup()
	if c == nil {
		return context.Canceled
	}
	if c.stream == nil {
		return ErrMode
	}

	return c.stream.write(ctx, c, v, bytes)
}

func (r correlationReply) WriteConsumed(ctx context.Context, value any, bytes int) error {
	c := r.lookup()
	if c == nil {
		return context.Canceled
	}
	if c.stream == nil {
		return ErrMode
	}

	v := &consumedChunk{value: value, done: make(chan error, 1)}
	if err := c.stream.write(ctx, c, v, bytes); err != nil {
		return err
	}

	select {
	case err := <-v.done:
		return err
	case <-ctx.Done():
		return v.cancelled(ctx.Err())
	case <-c.done:
		return v.cancelled(context.Canceled)
	}
}

func (ch *callState) deliver(ctx context.Context, target string, msg any, m mode) (*caller, error) {
	id := newID()
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return nil, ErrClosed
	}

	var c *caller
	if m != sendMode {
		// Reserve and register before Deliver: a channel may reply synchronously.
		if ch.pending >= ch.limit {
			ch.mu.Unlock()
			return nil, ErrFull
		}

		reqCtx, cancel := context.WithCancel(ctx)
		c = &caller{
			id:      id,
			channel: ch,
			mode:    m,
			ctx:     reqCtx,
			cancel:  cancel,
			done:    make(chan struct{}),
			release: ch.release,
		}
		if m == streamMode {
			c.stream = &streamBuffer{chunks: make(chan any, ch.streamItems), maxBytes: ch.chunkBytes}
		}

		ch.pending++
		ch.calls[id] = c
	}

	ch.mu.Unlock()

	parent := ctx
	if c != nil {
		parent = c.ctx
		c.watch(ctx)
	} else {
		// Channel shutdown bounds admission waits without tying an admitted
		// Send's execution to the caller's context.
		var cancel context.CancelFunc
		parent, cancel = context.WithCancel(ctx)
		stop := context.AfterFunc(ch.ctx, cancel)
		defer func() {
			stop()
			cancel()
		}()
	}

	err := ch.channel.Deliver(parent, Delivery{
		Target:      target,
		Correlation: id,
		Kind:        Kind(m),
		Message:     msg,
	}, correlationReply{ch, id})
	if err != nil {
		if c != nil {
			c.finish(nil, err)
			if m == streamMode {
				c.release()
			}
		}

		return nil, err
	}

	return c, nil
}

func (ch *callState) stop() {
	ch.mu.Lock()
	ch.closed = true
	calls := make([]*caller, 0, len(ch.calls))
	for _, c := range ch.calls {
		calls = append(calls, c)
	}

	ch.mu.Unlock()

	ch.cancel()
	for _, c := range calls {
		c.finish(nil, ErrClosed)
	}
}
