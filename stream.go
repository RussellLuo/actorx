package actorx

import (
	"context"
	"iter"
	"sync/atomic"
)

// streamChunk transfers an accepted value and its optional consumption receipt
// to the iterator. Only the iterator acknowledges consumption or discard.
type streamChunk struct {
	value    any
	consumed chan error
}

func (c streamChunk) acknowledge(err error) {
	if c.consumed != nil {
		c.consumed <- err
	}
}

// streamBuffer serializes writes and sealing through writeGate. Accepted chunks
// remain queued until the iterator consumes or discards them.
type streamBuffer struct {
	writeGate chan struct{}
	chunks    chan streamChunk
	maxBytes  int
}

func (s *streamBuffer) write(ctx context.Context, c *caller, chunk streamChunk, bytes int) error {
	if bytes < 0 || bytes > s.maxBytes {
		return ErrChunkSize
	}

	// Serialize writes with sealing, while allowing a queued writer to leave
	// independently when its context or the producer lifetime ends.
	select {
	case <-c.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	case s.writeGate <- struct{}{}:
	}
	defer func() { <-s.writeGate }()

	// A ready gate may win selection concurrently with cancellation or sealing.
	// Recheck both lifetimes before entering the send to a possibly closed queue.
	select {
	case <-c.done:
		return context.Canceled
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-c.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	case s.chunks <- chunk:
		return nil
	}
}

// seal runs once, after caller.done closes to unblock writers. The write gate
// ensures channel closure follows all accepted writes, including concurrent ones.
func (s *streamBuffer) seal() {
	s.writeGate <- struct{}{}
	close(s.chunks)
	<-s.writeGate
}

// stopStream ends consumption, settling every unconsumed receipt. It can run
// before an error body so that body may join writers without blocking cleanup.
// Repeating it during deferred cleanup is safe: finish is one-shot and the sealed
// queue has already been emptied. The caller window remains held by closeStream.
func (c *caller) stopStream() {
	c.finish(nil, context.Canceled)
	for chunk := range c.stream.chunks {
		chunk.acknowledge(context.Canceled)
	}
}

// closeStream belongs to the iterator, or to admission if setup fails. It runs
// after the current loop body exits, including during panic unwinding.
func (c *caller) closeStream() {
	c.stopStream()
	c.release()
}

// Stream returns a single-use iterator that starts the request when iterated.
// Chunks and a possible final error are yielded serially on the iterating
// goroutine. Accepted chunks precede the producer's final result; successful
// completion yields no extra item. Repeated or concurrent use yields ErrSetup.
//
// Stopping iteration cancels the request; an active loop body must cooperate
// with ctx itself. Keep msg and its referenced data stable until use.
func (addr Addr) Stream[C any](ctx context.Context, msg any) iter.Seq2[C, error] {
	var used atomic.Bool

	return func(yield func(C, error) bool) {
		var zero C
		if !used.CompareAndSwap(false, true) {
			yield(zero, ErrSetup)
			return
		}

		c, err := addr.prepare(ctx, msg, streamMode)
		if err != nil {
			yield(zero, err)
			return
		}
		defer c.closeStream()

		fail := func(err error) {
			c.stopStream()
			yield(zero, err)
		}

		consume := func(chunk streamChunk) bool {
			value, err := replyAs[C](chunk.value)
			if err != nil {
				// No data body runs for a type error. Settle its receipt before
				// entering the error body, which may itself wait for the writer.
				chunk.acknowledge(err)
				fail(err)
				return false
			}

			// Acknowledge only after the body exits. A false yield or panic
			// abandons this chunk; neither may publish successful consumption.
			consumed := context.Canceled
			defer func() { chunk.acknowledge(consumed) }()

			more := yield(value, nil)
			if more {
				consumed = nil
			}

			return more
		}

		for {
			if err := ctx.Err(); err != nil {
				fail(err)
				return
			}

			select {
			case <-ctx.Done():
				fail(ctx.Err())
				return
			case chunk, ok := <-c.stream.chunks:
				if !ok {
					// Queue closure follows producer completion. Read its result
					// only after draining, unless the caller has canceled.
					err := ctx.Err()
					if err == nil {
						err = c.err
					}
					if err != nil {
						fail(err)
					}
					return
				}

				if !consume(chunk) {
					return
				}
			}
		}
	}
}
