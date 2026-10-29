package actorx

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type iteratorChannel struct {
	Channel
	deliver func(context.Context, Responder) error
}

func (ch iteratorChannel) Deliver(ctx context.Context, _ Delivery, reply Responder) error {
	return ch.deliver(ctx, reply)
}

func iteratorAddr(t *testing.T, deliver func(context.Context, Responder) error) Addr {
	t.Helper()

	ch := newCallState(iteratorChannel{deliver: deliver}, config{
		calls:       1,
		streamItems: 2,
		chunkBytes:  8,
	})
	t.Cleanup(ch.stop)

	return Addr{id: newID(), channel: ch}
}

func assertIteratorIdle(t *testing.T, addr Addr) {
	t.Helper()

	addr.channel.mu.Lock()
	defer addr.channel.mu.Unlock()

	if addr.channel.pending != 0 || len(addr.channel.calls) != 0 {
		t.Fatal("iterator retained caller state", addr.channel.pending, len(addr.channel.calls))
	}
}

func iteratorError[C comparable](t *testing.T, chunks iter.Seq2[C, error], want error) {
	t.Helper()

	var zero C
	n := 0
	for value, err := range chunks {
		n++
		if value != zero || !errors.Is(err, want) {
			t.Fatalf("iterator error item = (%v, %v), want (%v, %v)", value, err, zero, want)
		}
	}

	if n != 1 {
		t.Fatalf("error yielded %d times", n)
	}
}

func TestStreamIteratorLazyAndSingleUse(t *testing.T) {
	var deliveries atomic.Int32
	addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
		deliveries.Add(1)
		if err := reply.Write(ctx, 7, 1); err != nil {
			return err
		}
		reply.Complete(nil, nil)
		return nil
	})

	chunks := addr.Stream[int](context.Background(), struct{}{})
	if deliveries.Load() != 0 {
		t.Fatal("iterator delivered before use")
	}
	assertIteratorIdle(t, addr)

	n := 0
	for value, err := range chunks {
		n++
		if value != 7 || err != nil {
			t.Fatal(value, err)
		}

		// A concurrent attempt must fail without disturbing the active body.
		done := make(chan struct{})
		go func() {
			defer close(done)
			iteratorError(t, chunks, ErrSetup)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent iteration did not return")
		}
	}

	if n != 1 || deliveries.Load() != 1 {
		t.Fatal("unexpected chunks or deliveries", n, deliveries.Load())
	}

	iteratorError(t, chunks, ErrSetup)
	assertIteratorIdle(t, addr)
}

func TestStreamIteratorErrors(t *testing.T) {
	sentinel := errors.New("producer failed")
	for _, tc := range []struct {
		name  string
		count int
		final error
	}{
		{"empty", 0, nil},
		{"empty failure", 0, sentinel},
		{"ordered", 2, nil},
		{"partial failure", 2, sentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Complete before iteration receives anything: accepted chunks must
			// still precede the final error, with no EOF or extra success item.
			addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
				for value := 1; value <= tc.count; value++ {
					if err := reply.Write(ctx, value, 1); err != nil {
						return err
					}
				}

				reply.Complete(nil, tc.final)
				return nil
			})

			n, failures := 0, 0
			for value, err := range addr.Stream[int](context.Background(), struct{}{}) {
				if err == nil {
					n++
					if failures != 0 || n > tc.count || value != n {
						t.Fatal("unexpected data item", value, n, failures)
					}
					continue
				}

				failures++
				if value != 0 || err != tc.final || n != tc.count {
					t.Fatal("unexpected terminal item", value, err, n)
				}
			}

			wantFailures := 0
			if tc.final != nil {
				wantFailures = 1
			}
			if n != tc.count || failures != wantFailures {
				t.Fatal("missing or repeated items", n, failures)
			}
			assertIteratorIdle(t, addr)
		})
	}

	addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
		return reply.Write(ctx, 7, 1)
	})

	iteratorError(t, addr.Stream[string](context.Background(), struct{}{}), ErrReplyType)
	assertIteratorIdle(t, addr)

	rejected := iteratorAddr(t, func(context.Context, Responder) error { return sentinel })
	iteratorError(t, rejected.Stream[int](context.Background(), struct{}{}), sentinel)
	assertIteratorIdle(t, rejected)

	ctx, cancel := context.WithCancel(context.Background())
	chunks := rejected.Stream[int](ctx, struct{}{})
	cancel()
	iteratorError(t, chunks, context.Canceled)
	iteratorError(t, (Addr{}).Stream[int](context.Background(), struct{}{}), ErrStopped)
}

func TestStreamIteratorConsumptionCleanup(t *testing.T) {
	for _, action := range []string{"continue", "break", "return", "panic", "wrong type"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			wrote := make(chan error, 1)
			addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
				go func() {
					err := reply.WriteConsumed(ctx, 7, 1)
					wrote <- err
					reply.Complete(nil, err)
				}()
				return nil
			})

			if action == "wrong type" {
				iteratorError(t, addr.Stream[string](ctx, struct{}{}), ErrReplyType)
			} else {
				const sentinel = "loop panic"
				businessErr := errors.New("consumer failed")
				err := func() error {
					defer func() {
						v := recover()
						if action == "panic" && v != sentinel {
							t.Errorf("panic = %v, want %v", v, sentinel)
						} else if action != "panic" && v != nil {
							panic(v)
						}
					}()

					for value, err := range addr.Stream[int](ctx, struct{}{}) {
						if value != 7 || err != nil {
							t.Fatal(value, err)
						}

						select {
						case err := <-wrote:
							t.Fatal("consumption acknowledged inside loop body", err)
						default:
						}
						iteratorError(t, addr.Stream[int](ctx, struct{}{}), ErrFull)

						if action == "panic" {
							panic(sentinel)
						}
						if action == "break" {
							break
						}
						if action == "return" {
							return businessErr
						}
						continue
					}

					return nil
				}()
				if action == "return" && err != businessErr {
					t.Fatal("caller lost its business error", err)
				}
			}

			select {
			case err := <-wrote:
				switch {
				case action == "continue" && err != nil:
					t.Fatal("normal consumption failed", err)
				case action == "wrong type" && !errors.Is(err, ErrReplyType):
					t.Fatal("type error lost", err)
				case (action == "break" || action == "return" || action == "panic") &&
					!errors.Is(err, context.Canceled):
					t.Fatal("early exit did not cancel writer", err)
				}
			case <-ctx.Done():
				t.Fatal("writer still blocked", ctx.Err())
			}
			assertIteratorIdle(t, addr)
		})
	}
}

func TestStreamIteratorDelayedCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			var deliveries atomic.Int32
			addr := iteratorAddr(t, func(context.Context, Responder) error {
				deliveries.Add(1)
				return nil
			})

			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
			}
			defer cancel()

			chunks := addr.Stream[int](ctx, struct{}{})
			if !deadline {
				cancel()
			}
			<-ctx.Done()
			iteratorError(t, chunks, ctx.Err())
			iteratorError(t, chunks, ErrSetup)
			if deliveries.Load() != 0 {
				t.Fatal("expired iterator delivered a request")
			}
			assertIteratorIdle(t, addr)
		})
	}
}

func TestStreamIteratorYieldFalse(t *testing.T) {
	for _, terminal := range []string{"success", "failure", "wrong type", "cancel"} {
		t.Run(terminal, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
				if err := reply.Write(ctx, 7, 1); err != nil {
					return err
				}

				var next any = 8
				if terminal == "wrong type" {
					next = "wrong type"
				}
				if err := reply.Write(ctx, next, 1); err != nil {
					return err
				}

				var final error
				if terminal == "failure" {
					final = errors.New("producer failed")
				}
				reply.Complete(nil, final)
				return nil
			})

			calls := 0
			// Invoke the sequence directly so even an erroneous extra yield is
			// counted, rather than hidden by range's own panic after break.
			addr.Stream[int](ctx, struct{}{})(func(value int, err error) bool {
				calls++
				if calls != 1 || value != 7 || err != nil {
					t.Error("unexpected yield", calls, value, err)
				}
				if terminal == "cancel" {
					cancel()
				}

				return false
			})

			if calls != 1 {
				t.Fatal("yield count", calls)
			}
			assertIteratorIdle(t, addr)
		})
	}
}

func TestStreamIteratorEarlyExitUnblocksWriter(t *testing.T) {
	for _, action := range []string{"break", "return", "panic"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			entered, full := make(chan struct{}), make(chan struct{})
			wrote := make(chan error, 1)
			requestContext := make(chan context.Context, 1)
			addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
				requestContext <- ctx
				go func() {
					if err := reply.Write(ctx, 7, 1); err != nil {
						wrote <- err
						return
					}
					select {
					case <-entered:
					case <-ctx.Done():
						wrote <- ctx.Err()
						return
					}

					// The active body plus two queued chunks fills the window.
					for i := 0; i < 2; i++ {
						if err := reply.Write(ctx, 8+i, 1); err != nil {
							wrote <- err
							return
						}
					}
					close(full)
					wrote <- reply.Write(ctx, 10, 1)
				}()
				return nil
			})

			businessErr := errors.New("consumer failed")
			panicValue := &struct{ message string }{"loop panic"}
			err := func() error {
				defer func() {
					if v := recover(); v != nil {
						if action != "panic" || v != panicValue {
							panic(v)
						}
					} else if action == "panic" {
						t.Error("loop panic did not propagate")
					}
				}()

				for value, err := range addr.Stream[int](ctx, struct{}{}) {
					if value != 7 || err != nil {
						t.Fatal("unexpected item after early exit", value, err)
					}
					close(entered)
					select {
					case <-full:
					case <-ctx.Done():
						t.Fatal("writer did not fill window", ctx.Err())
					}
					select {
					case err := <-wrote:
						t.Fatal("write escaped bounded window", err)
					default:
					}
					switch action {
					case "return":
						return businessErr
					case "panic":
						panic(panicValue)
					}
					break
				}

				return nil
			}()
			if action == "return" && err != businessErr {
				t.Fatal("caller lost its business error", err)
			}

			select {
			case err := <-wrote:
				if err != context.Canceled {
					t.Fatal("blocked writer was not canceled", err)
				}
			case <-ctx.Done():
				t.Fatal("writer still blocked", ctx.Err())
			}
			if err := (<-requestContext).Err(); err != context.Canceled {
				t.Fatal("request was not canceled", err)
			}
			assertIteratorIdle(t, addr)
		})
	}
}

func TestStreamIteratorActiveConsumptionBarrier(t *testing.T) {
	for _, terminal := range []string{"cancel", "complete"} {
		t.Run(terminal, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wrote := make(chan error, 1)
			var reply Responder
			var requestCtx context.Context
			addr := iteratorAddr(t, func(ctx context.Context, r Responder) error {
				reply, requestCtx = r, ctx
				go func() { wrote <- r.WriteConsumed(ctx, 7, 1) }()
				return nil
			})

			data, failures := 0, 0
			for value, err := range addr.Stream[int](ctx, struct{}{}) {
				if err != nil {
					failures++
					if terminal != "cancel" || value != 0 || err != context.Canceled {
						t.Fatal("unexpected terminal item", value, err)
					}
					continue
				}

				data++
				if data != 1 || value != 7 {
					t.Fatal("unexpected data item", value, data)
				}
				if terminal == "cancel" {
					cancel()
				} else if !reply.Complete(nil, nil) {
					t.Fatal("producer completion rejected")
				}
				await(t, requestCtx.Done())
				iteratorError(t, addr.Stream[int](context.Background(), struct{}{}), ErrFull)

				// Give the waiting writer an opportunity to observe cancellation.
				// It must join this body rather than acknowledge consumption early.
				select {
				case err := <-wrote:
					t.Fatal("active consumption acknowledged early", err)
				case <-time.After(20 * time.Millisecond):
				}
			}

			wantFailures := 0
			if terminal == "cancel" {
				wantFailures = 1
			}
			if data != 1 || failures != wantFailures {
				t.Fatal("unexpected item counts", data, failures)
			}

			select {
			case err := <-wrote:
				if err != nil {
					t.Fatal("completed loop body was not acknowledged", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("consumption remained blocked after body exit")
			}
			assertIteratorIdle(t, addr)
		})
	}
}

func TestStreamIteratorCancelWithCompletedProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
		for _, value := range []int{1, 2} {
			if err := reply.Write(ctx, value, 1); err != nil {
				return err
			}
		}
		reply.Complete(nil, errors.New("producer failed"))
		return nil
	})

	data, failures := 0, 0
	for value, err := range addr.Stream[int](ctx, struct{}{}) {
		if err == nil {
			data++
			if data != 1 || value != 1 {
				t.Fatal("continued draining after cancellation", value)
			}
			cancel()
			continue
		}

		failures++
		if value != 0 || err != context.Canceled {
			t.Fatal("cancellation lost to producer result", value, err)
		}
	}

	if data != 1 || failures != 1 {
		t.Fatal("unexpected item counts", data, failures)
	}
	assertIteratorIdle(t, addr)
}

// queuedConsumption starts two ordered WriteConsumed calls and completes before
// Deliver returns, so neither loop body can start before the producer terminates.
func queuedConsumption(t *testing.T, final error) (Addr, <-chan error) {
	t.Helper()

	wrote := make(chan error, 2)
	addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
		c := reply.(correlationReply).lookup()
		for value := 1; value <= 2; value++ {
			go func() { wrote <- reply.WriteConsumed(ctx, value, 1) }()
			awaitQueuedChunks(t, c, value)
		}
		if !reply.Complete(nil, final) {
			t.Fatal("producer completion rejected")
		}
		await(t, ctx.Done())
		// The producer's context has ended, but accepted chunks still belong
		// to the iterator. Neither consumption may be acknowledged yet.
		select {
		case err := <-wrote:
			t.Fatal("producer completion abandoned accepted chunk", err)
		case <-time.After(20 * time.Millisecond):
		}
		return nil
	})

	return addr, wrote
}

func awaitQueuedChunks(t *testing.T, c *caller, count int) {
	t.Helper()

	deadline := time.After(3 * time.Second)
	for len(c.stream.chunks) != count {
		select {
		case <-deadline:
			t.Fatal("writer did not queue chunks", count)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestStreamIteratorQueuedConsumptionAfterCompletion(t *testing.T) {
	failure := errors.New("producer failed")
	for _, final := range []error{nil, failure} {
		name := "success"
		if final != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			addr, wrote := queuedConsumption(t, final)
			data, failures := 0, 0
			for value, err := range addr.Stream[int](context.Background(), struct{}{}) {
				if err != nil {
					failures++
					if value != 0 || err != final || data != 2 {
						t.Fatal("unexpected terminal item", value, err, data)
					}
					continue
				}

				data++
				if value != data || data > 2 || failures != 0 {
					t.Fatal("accepted chunks were not drained in order", value, data)
				}
			}

			wantFailures := 0
			if final != nil {
				wantFailures = 1
			}
			if data != 2 || failures != wantFailures {
				t.Fatal("unexpected item counts", data, failures)
			}

			for range 2 {
				select {
				case err := <-wrote:
					if err != nil {
						t.Fatal("consumed chunk was not acknowledged", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("consumed writer still blocked")
				}
			}
			assertIteratorIdle(t, addr)
		})
	}
}

func TestStreamIteratorDiscardQueuedConsumption(t *testing.T) {
	for _, action := range []string{"break", "cancel", "panic", "wrong type"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			addr, wrote := queuedConsumption(t, nil)
			firstResult := context.Canceled
			if action == "cancel" {
				firstResult = nil // The first body completes before cancellation ends iteration.
			}
			if action == "wrong type" {
				firstResult = ErrReplyType
				iteratorError(t, addr.Stream[string](ctx, struct{}{}), ErrReplyType)
			} else {
				const panicValue = "loop panic"
				func() {
					defer func() {
						v := recover()
						if action == "panic" && v != panicValue {
							t.Errorf("panic = %v, want %v", v, panicValue)
						} else if action != "panic" && v != nil {
							panic(v)
						}
					}()
					data, failures := 0, 0
					for value, err := range addr.Stream[int](ctx, struct{}{}) {
						if err != nil {
							failures++
							if action != "cancel" || value != 0 || err != context.Canceled {
								t.Fatal("unexpected terminal item", value, err)
							}
							continue
						}
						data++
						if data != 1 || value != 1 {
							t.Fatal("unexpected item", value, err)
						}
						if action == "panic" {
							panic(panicValue)
						}
						if action == "cancel" {
							cancel()
							continue
						}
						break
					}

					if data != 1 || (action == "cancel" && failures != 1) {
						t.Fatal("unexpected item counts", data, failures)
					}
				}()
			}

			counts := make(map[error]int)
			for range 2 {
				select {
				case err := <-wrote:
					if errors.Is(err, ErrReplyType) {
						err = ErrReplyType
					}
					counts[err]++
				case <-time.After(3 * time.Second):
					t.Fatal("discarded writer still blocked")
				}
			}

			// Writers may return in either order: the current chunk gets its
			// consumption result, and the remaining chunk is always discarded.
			counts[firstResult]--
			counts[context.Canceled]--
			for err, count := range counts {
				if count != 0 {
					t.Fatal("unexpected consumption results", err, count)
				}
			}
			assertIteratorIdle(t, addr)
		})
	}
}

func TestStreamIteratorAdmissionFailureDiscardsAcceptedChunk(t *testing.T) {
	failure := errors.New("delivery rejected")
	wrote := make(chan error, 1)
	addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
		c := reply.(correlationReply).lookup()
		go func() { wrote <- reply.WriteConsumed(ctx, 7, 1) }()
		// Deliver fails only after a chunk has entered the caller's queue.
		awaitQueuedChunks(t, c, 1)
		return failure
	})

	iteratorError(t, addr.Stream[int](context.Background(), struct{}{}), failure)
	select {
	case err := <-wrote:
		if err != context.Canceled {
			t.Fatal("rejected delivery did not discard accepted chunk", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rejected delivery retained writer")
	}
	assertIteratorIdle(t, addr)
}

func TestStreamIteratorWriteContextAfterAcceptance(t *testing.T) {
	wrote := make(chan error, 1)
	addr := iteratorAddr(t, func(_ context.Context, reply Responder) error {
		c := reply.(correlationReply).lookup()
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			err := reply.WriteConsumed(ctx, 7, 1)
			wrote <- err
			reply.Complete(nil, err)
		}()

		// Cancel only after acceptance: the iterator still owns the receipt.
		awaitQueuedChunks(t, c, 1)
		cancel()
		select {
		case err := <-wrote:
			t.Fatal("write context revoked accepted chunk", err)
		case <-time.After(20 * time.Millisecond):
		}
		return nil
	})

	data := 0
	for value, err := range addr.Stream[int](context.Background(), struct{}{}) {
		data++
		if data != 1 || value != 7 || err != nil {
			t.Fatal("accepted chunk was not consumed", value, err)
		}
	}

	if data != 1 {
		t.Fatal("missing accepted chunk")
	}
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal("consumption was not acknowledged", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consumed writer still blocked")
	}
	assertIteratorIdle(t, addr)
}

func TestStreamIteratorCanceledWriteDoesNotQueue(t *testing.T) {
	addr := iteratorAddr(t, func(_ context.Context, reply Responder) error {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := reply.Write(ctx, 1, 1); err != context.Canceled {
			t.Fatal("canceled Write was accepted", err)
		}
		if err := reply.WriteConsumed(ctx, 2, 1); err != context.Canceled {
			t.Fatal("canceled WriteConsumed was accepted", err)
		}
		reply.Complete(nil, nil)
		return nil
	})

	for value, err := range addr.Stream[int](context.Background(), struct{}{}) {
		t.Fatal("canceled write produced an item", value, err)
	}
	assertIteratorIdle(t, addr)
}

func TestStreamIteratorCompletionUnblocksFullWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	entered, full := make(chan struct{}), make(chan struct{})
	wrote := make(chan error, 1)
	var reply Responder
	addr := iteratorAddr(t, func(ctx context.Context, r Responder) error {
		reply = r
		go func() {
			if err := r.Write(ctx, 1, 1); err != nil {
				wrote <- err
				return
			}

			select {
			case <-entered:
			case <-ctx.Done():
				wrote <- ctx.Err()
				return
			}
			for _, value := range []int{2, 3} {
				if err := r.Write(ctx, value, 1); err != nil {
					wrote <- err
					return
				}
			}
			close(full)
			wrote <- r.Write(context.Background(), 4, 1)
		}()
		return nil
	})

	data := 0
	for value, err := range addr.Stream[int](ctx, struct{}{}) {
		data++
		if value != data || data > 3 || err != nil {
			t.Fatal("unexpected item", value, err)
		}
		if data != 1 {
			continue
		}

		close(entered)
		await(t, full)

		// Complete must unblock the next write while this body keeps the
		// accepted chunks queued for later consumption.
		completed := make(chan bool, 1)
		go func() { completed <- reply.Complete(nil, nil) }()
		select {
		case ok := <-completed:
			if !ok {
				t.Fatal("producer completion rejected")
			}
		case <-ctx.Done():
			t.Fatal("producer completion blocked on full window", ctx.Err())
		}
		select {
		case err := <-wrote:
			if err != context.Canceled {
				t.Fatal("blocked write escaped producer completion", err)
			}
		case <-ctx.Done():
			t.Fatal("writer remained blocked after completion", ctx.Err())
		}
	}

	if data != 3 {
		t.Fatal("accepted chunks were not drained", data)
	}
	assertIteratorIdle(t, addr)
}

func TestStreamIteratorErrorBodyCanJoinDiscardedWriters(t *testing.T) {
	for _, reason := range []string{"cancel", "wrong type"} {
		t.Run(reason, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wrote := make(chan error, 2)
			addr := iteratorAddr(t, func(ctx context.Context, reply Responder) error {
				c := reply.(correlationReply).lookup()
				for value := 1; value <= 2; value++ {
					go func() { wrote <- reply.WriteConsumed(ctx, value, 1) }()
					awaitQueuedChunks(t, c, value)
				}
				if reason == "cancel" {
					cancel()
				}

				return nil
			})

			failures, typeErrors := 0, 0
			for value, err := range addr.Stream[string](ctx, struct{}{}) {
				failures++
				want := context.Canceled
				if reason == "wrong type" {
					want = ErrReplyType
				}
				if value != "" || !errors.Is(err, want) {
					t.Fatal("unexpected error item", value, err)
				}

				// Consumption has ended before this error item is delivered, but
				// its loop body still holds the caller window until it exits.
				iteratorError(t, addr.Stream[int](context.Background(), struct{}{}), ErrFull)
				for range 2 {
					select {
					case err := <-wrote:
						if errors.Is(err, ErrReplyType) {
							typeErrors++
						} else if err != context.Canceled {
							t.Fatal("unexpected discarded result", err)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("error body blocked joining discarded writers")
					}
				}
			}

			wantTypeErrors := 0
			if reason == "wrong type" {
				wantTypeErrors = 1
			}
			if failures != 1 || typeErrors != wantTypeErrors {
				t.Fatal("unexpected error counts", failures, typeErrors)
			}
			assertIteratorIdle(t, addr)
		})
	}
}

// observedStreamWriteContext reports entry into a context-aware write wait.
// Done may be evaluated more than once as admission progresses.
type observedStreamWriteContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *observedStreamWriteContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestStreamIteratorStopUnblocksQueuedWriters(t *testing.T) {
	for _, terminal := range []string{"producer complete", "caller cancel", "channel stop"} {
		t.Run(terminal, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var reply Responder
			addr := iteratorAddr(t, func(_ context.Context, r Responder) error {
				reply = r
				return nil
			})

			c, err := addr.prepare(ctx, struct{}{}, streamMode)
			if err != nil {
				t.Fatal(err)
			}
			closeStream := sync.OnceFunc(c.closeStream)
			defer closeStream()

			for value := 1; value <= 2; value++ {
				if err := reply.Write(context.Background(), value, 1); err != nil {
					t.Fatal(err)
				}
			}

			const writers = 8
			results := make(chan error, writers)
			for i := range writers {
				writeCtx := &observedStreamWriteContext{
					Context: context.Background(),
					waiting: make(chan struct{}),
				}
				write := reply.Write
				if i%2 != 0 {
					write = reply.WriteConsumed
				}
				go func() { results <- write(writeCtx, 3+i, 1) }()
				await(t, writeCtx.waiting)
			}

			// No consumer frees capacity: stopping must release both the writer
			// awaiting queue space and writers still awaiting admission.
			stopped := make(chan bool, 1)
			go func() {
				switch terminal {
				case "producer complete":
					stopped <- reply.Complete(nil, nil)
				case "caller cancel":
					cancel()
					stopped <- true
				case "channel stop":
					addr.channel.stop()
					stopped <- true
				}
			}()
			select {
			case ok := <-stopped:
				if !ok {
					t.Fatal("producer completion rejected")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("stopping blocked behind queued writers")
			}

			for range writers {
				select {
				case err := <-results:
					if err != context.Canceled {
						t.Fatal("unaccepted writer was not canceled", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("stopping retained queued writer")
				}
			}
			if n := len(c.stream.chunks); n != 2 {
				t.Fatal("unaccepted writes changed queue", n)
			}

			closeStream()
			assertIteratorIdle(t, addr)
		})
	}
}

func TestStreamIteratorWriteAdmissionCancellation(t *testing.T) {
	for _, consumed := range []bool{false, true} {
		name := "Write"
		if consumed {
			name = "WriteConsumed"
		}
		t.Run(name, func(t *testing.T) {
			for _, action := range []string{"already canceled", "cancel while waiting", "deadline while waiting"} {
				t.Run(action, func(t *testing.T) {
					var reply Responder
					addr := iteratorAddr(t, func(_ context.Context, r Responder) error {
						reply = r
						return nil
					})

					c, err := addr.prepare(context.Background(), struct{}{}, streamMode)
					if err != nil {
						t.Fatal(err)
					}
					closeStream := sync.OnceFunc(c.closeStream)
					defer closeStream()

					for value := 1; value <= 2; value++ {
						if err := reply.Write(context.Background(), value, 1); err != nil {
							t.Fatal(err)
						}
					}

					// Keep the window full and one writer pending independently of
					// the second writer's cancellation or deadline.
					firstCtx := &observedStreamWriteContext{
						Context: context.Background(),
						waiting: make(chan struct{}),
					}
					first := make(chan error, 1)
					go func() { first <- reply.Write(firstCtx, 3, 1) }()
					await(t, firstCtx.waiting)

					ctx, cancel := context.WithCancel(context.Background())
					if action == "deadline while waiting" {
						cancel()
						ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
					}
					defer cancel()

					if action == "already canceled" {
						cancel()
					}

					secondCtx := &observedStreamWriteContext{
						Context: ctx,
						waiting: make(chan struct{}),
					}
					second := make(chan error, 1)
					write := reply.Write
					if consumed {
						write = reply.WriteConsumed
					}
					go func() { second <- write(secondCtx, 4, 1) }()
					if action != "already canceled" {
						await(t, secondCtx.waiting)
						if action == "cancel while waiting" {
							cancel()
						}
					}

					<-ctx.Done()
					select {
					case err := <-second:
						if err != ctx.Err() {
							t.Fatal("canceled writer returned", err, "want", ctx.Err())
						}
					case <-time.After(3 * time.Second):
						t.Fatal("canceled writer blocked behind another write before admission")
					}

					select {
					case err := <-first:
						t.Fatal("second writer's cancellation affected first writer", err)
					default:
					}
					if n := len(c.stream.chunks); n != 2 {
						t.Fatal("canceled write changed accepted chunks", n)
					}

					closeStream()
					select {
					case err := <-first:
						if err != context.Canceled {
							t.Fatal("stream closure did not cancel blocked writer", err)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("stream closure retained blocked writer")
					}
					assertIteratorIdle(t, addr)
				})
			}
		})
	}
}
