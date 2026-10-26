package actorx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type get struct{}

type add struct{ N int }

type wait struct{}

func runtimeFor(t *testing.T, opts ...Option) *Runtime {
	t.Helper()

	rt, err := NewRuntime(context.Background(), opts...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := rt.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	return rt
}

type bindingsFunc func() []Binding

func (f bindingsFunc) Bindings() []Binding { return f() }

func spawnFor(t *testing.T, rt *Runtime, f func() []Binding) *actor {
	t.Helper()
	var a *actor
	// Capture execution state only when Started receives its runtime Context.
	// Bindings has no execution pointer, and the helper also handles early exit.
	addr, err := rt.Spawn(bindingsFunc(func() []Binding {
		entries := append([]Binding(nil), f()...)
		for i, b := range entries {
			if b.phase == StateStarted {
				invoke := b.invoke
				entries[i].invoke = func(ctx Context, msg any, r *request) error { a = ctx.actorContext().actor; return invoke(ctx, msg, r) }
				return entries
			}
		}
		return append(entries, HandleMessage(func(ctx Context, _ Started) error { a = ctx.actorContext().actor; return nil }))
	}))
	if err != nil {
		t.Fatal(err)
	}
	if a == nil || addr != a.addr() {
		t.Fatal("Spawn returned a different instance")
	}
	return a
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
}

func call[R any](t *testing.T, a *actor, msg any) R {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	v, err := a.addr().Call[R](ctx, msg)
	if err != nil {
		t.Fatal(err)
	}

	return v
}

func TestSerialBindingsAndTypes(t *testing.T) {
	rt := runtimeFor(t, WithMailboxCapacity(256))
	n := 0
	active := 0
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			Handle(func(ctx Context, msg add) (int, error) {
				active++
				if active != 1 {
					t.Error("concurrent handler")
				}

				n += msg.N
				active--
				return n, nil
			}),
			Handle(func(ctx Context, _ get) (int, error) { return n, nil }),
			HandleMessage(func(ctx Context, _ wait) error {
				n = 0
				return nil
			}),
		}
	})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := a.addr().Send(context.Background(), add{1}); err != nil {
				t.Error(err)
			}
		}()
	}

	wg.Wait()

	if v := call[int](t, a, get{}); v != 100 {
		t.Fatal(v)
	}
	if _, err := a.addr().Call[string](context.Background(), add{1}); !errors.Is(err, ErrReplyType) {
		t.Fatal(err)
	}
	if call[int](t, a, get{}) != 101 {
		t.Fatal("reply mismatch should not undo side effects")
	}
	if v := call[any](t, a, wait{}); v != nil {
		t.Fatal(v)
	}
	if _, err := a.addr().Call[int](context.Background(), wait{}); !errors.Is(err, ErrReplyType) {
		t.Fatal(err)
	}
	if err := a.addr().Send(context.Background(), &get{}); !errors.Is(err, ErrUnhandled) {
		t.Fatal(err)
	}
}

func TestNilRepliesAndNonComparableMessages(t *testing.T) {
	rt := runtimeFor(t)
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			Handle(func(ctx Context, _ []int) (*int, error) { return nil, nil }),
			HandleMessage(func(ctx Context, _ get) error { return nil }),
		}
	})

	if v := call[*int](t, a, []int{1}); v != nil {
		t.Fatal(v)
	}

	v := call[any](t, a, []int{1})
	if _, ok := v.(*int); !ok {
		t.Fatalf("typed nil lost: %T", v)
	}
	if _, err := a.addr().Call[*int](context.Background(), get{}); !errors.Is(err, ErrReplyType) {
		t.Fatal(err)
	}
}

func TestFullStopAndUncooperativeHandler(t *testing.T) {
	rt := runtimeFor(t, WithMailboxCapacity(1))
	entered := make(chan struct{})
	release := make(chan struct{})
	var ran atomic.Int32
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleMessage(func(ctx Context, _ wait) error {
				close(entered)
				<-release
				return nil
			}),
			HandleMessage(func(ctx Context, _ get) error {
				ran.Add(1)
				return nil
			}),
		}
	})

	if err := a.addr().Send(context.Background(), wait{}); err != nil {
		t.Fatal(err)
	}

	await(t, entered)

	// Keep the handler blocked while filling its single queued-message slot.
	pending := make(chan error, 1)
	go func() {
		_, err := a.addr().Call[any](context.Background(), get{})
		pending <- err
	}()
	waitQueue(t, a, 1)
	if err := a.addr().Send(context.Background(), get{}); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}

	a.addr().Stop(nil)
	if err := <-pending; !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}

	select {
	case <-a.exit.done:
		t.Fatal("Done before handler exit")
	default:
	}

	if err := a.addr().Send(context.Background(), get{}); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}

	close(release)
	waitExitFor(t, a.addr())

	if ran.Load() != 0 {
		t.Fatal("queued handler ran after Stop")
	}
}

func waitQueue(t *testing.T, a *actor, n int) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		size := len(a.queue)
		a.mu.Unlock()

		if size == n {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("queue size never reached", n)
}

type nilActor struct{}

func (*nilActor) Bindings() []Binding { panic("must not be called") }

func TestBindingsRejectInvalidActorsAndEntries(t *testing.T) {
	rt := runtimeFor(t)
	var ptr *nilActor
	var f bindingsFunc
	for _, value := range []Actor{nil, ptr, f} {
		if addr, err := rt.Spawn(value); !addr.IsZero() || !errors.Is(err, ErrSetup) {
			t.Fatal(addr, err)
		}
	}
	count := 0
	addr, err := rt.Spawn(bindingsFunc(func() []Binding { count++; panic("bindings") }))
	var pe *PanicError
	if !addr.IsZero() || !errors.Is(err, ErrSetup) || !errors.As(err, &pe) || count != 1 {
		t.Fatal(addr, err, count)
	}
	b := HandleMessage(func(Context, get) error { return nil })
	for _, entries := range [][]Binding{
		{HandleMessage[any](func(Context, any) error { return nil })},
		{HandleMessage[get](nil)}, {{}}, {b, b},
	} {
		if addr, err := rt.Spawn(bindingsFunc(func() []Binding { return entries })); !addr.IsZero() || !errors.Is(err, ErrSetup) {
			t.Fatal(addr, err)
		}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.actors) != 0 {
		t.Fatal("failed Bindings retained execution state")
	}
}

func TestPanicTerminatesButBusinessErrorDoesNot(t *testing.T) {
	rt := runtimeFor(t)
	business := errors.New("business")
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleMessage(func(ctx Context, _ get) error { return business }),
			HandleMessage(func(ctx Context, _ wait) error { panic("handler") }),
		}
	})

	if _, err := a.addr().Call[any](context.Background(), get{}); err != business {
		t.Fatal(err)
	}
	if _, err := a.addr().Call[any](context.Background(), wait{}); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}

	waitExitFor(t, a.addr())

	var pe *PanicError
	if !errors.As(a.reason(), &pe) || len(pe.Stack) == 0 {
		t.Fatal(a.reason())
	}
}

func TestSpawnCloseBoundaryAndTimeout(t *testing.T) {
	rt := runtimeFor(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	spawned := make(chan error, 1)
	go func() {
		_, err := rt.Spawn(bindingsFunc(func() []Binding {
			close(entered)
			<-release
			return nil
		}))
		spawned <- err
	}()
	await(t, entered)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rt.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := rt.Spawn(bindingsFunc(func() []Binding { return nil })); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}

	close(release)
	if err := <-spawned; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}

	rt.mu.Lock()
	remaining := len(rt.actors)
	rt.mu.Unlock()
	if remaining != 0 {
		t.Fatal("rollback retained execution state")
	}

	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentSpawnCloseAndUniqueIDs(t *testing.T) {
	rt := runtimeFor(t)
	ids := sync.Map{}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			a, err := rt.Spawn(bindingsFunc(func() []Binding { return nil }))
			if err != nil {
				if !errors.Is(err, ErrClosed) {
					t.Error(err)
				}

				return
			}
			if _, exists := ids.LoadOrStore(a.id, true); exists {
				t.Error("duplicate ID")
			}

			a.Stop(nil)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		_ = rt.Close(context.Background())
	}()
	wg.Wait()

	rt2 := runtimeFor(t)
	a := spawnFor(t, rt2, func() []Binding { return nil })
	old := a.addr()
	a.addr().Stop(nil)
	waitExitFor(t, a.addr())

	b := spawnFor(t, rt2, func() []Binding { return nil })
	if old.id == b.id {
		t.Fatal("reused ID")
	}
	if err := old.Send(context.Background(), get{}); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func TestChannelOwnershipAndPendingBound(t *testing.T) {
	ch := NewLocalChannel()
	rt := runtimeFor(t, WithChannel(ch), WithCallLimit(1))
	if _, err := NewRuntime(context.Background(), WithChannel(ch)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleDeferred(func(ctx Context, _ get, _ Reply[int]) error {
			close(entered)
			return nil
		})}
	})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := a.addr().Call[int](ctx, get{})
		result <- err
	}()
	await(t, entered)

	if _, err := a.addr().Call[int](context.Background(), get{}); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}

	// Caller cancellation is independent of target settlement after P2-01.
	// Keep the target completion barrier, rather than treating Call's return as
	// evidence that the receiving side has already finished cancellation.
	a.mu.Lock()
	var target *request
	for r := range a.requests {
		target = r
	}

	a.mu.Unlock()

	if target == nil {
		t.Fatal("missing admitted target request")
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	rt.calls.mu.Lock()
	pending, records := rt.calls.pending, len(rt.calls.calls)
	rt.calls.mu.Unlock()

	if pending != 0 || records != 0 {
		t.Fatal("caller cancellation retained an association", pending, records)
	}

	await(t, target.done)
	a.mu.Lock()
	n := len(a.requests)
	a.mu.Unlock()

	if n != 0 {
		t.Fatal(n)
	}
}

func TestDeferredCompletionAndCancellation(t *testing.T) {
	rt := runtimeFor(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var duplicate atomic.Bool
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleDeferred(func(ctx Context, _ wait, r Reply[int]) error {
				_, err := StartTask(ctx,
					func(context.Context) (int, error) {
						close(entered)
						<-release
						return 42, nil
					},
					func(v int, err error) bool {
						if !r.Complete(v, err) {
							t.Error("first completion lost")
						}

						duplicate.Store(!r.Complete(99, nil))
						return true
					},
					nil,
				)
				return err
			}),
			Handle(func(ctx Context, _ get) (int, error) { return 7, nil }),
		}
	})

	result := make(chan int, 1)
	go func() {
		v, err := a.addr().Call[int](context.Background(), wait{})
		if err != nil {
			t.Error(err)
		}

		result <- v
	}()
	await(t, entered)

	if call[int](t, a, get{}) != 7 {
		t.Fatal("I/O blocked handler")
	}

	close(release)
	if v := <-result; v != 42 {
		t.Fatal(v)
	}

	// The reply can arrive before the completion callback finishes. A serial
	// handler barrier makes its duplicate-completion check observable here.
	call[int](t, a, get{})
	if !duplicate.Load() {
		t.Fatal("duplicate reply accepted")
	}
}

func TestTaskReservationAndRollbackAfterQueuedCompletion(t *testing.T) {
	rt := runtimeFor(t, WithMailboxCapacity(2))
	started := make(chan struct{})
	finishWork := make(chan struct{})
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	rolled := make(chan struct{})
	var complete atomic.Bool
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleMessage(func(ctx Context, _ wait) error {
				_, err := StartTask(ctx.WithContext(context.Background()),
					func(context.Context) (int, error) {
						close(started)
						<-finishWork
						return 1, nil
					},
					func(int, error) bool {
						complete.Store(true)
						return true
					},
					func(int) { close(rolled) },
				)
				return err
			}),
			HandleMessage(func(ctx Context, _ get) error {
				close(blocked)
				<-unblock
				return nil
			}),
			HandleMessage(func(ctx Context, _ add) error { return nil }),
		}
	})

	if err := a.addr().Send(context.Background(), wait{}); err != nil {
		t.Fatal(err)
	}

	await(t, started)

	if err := a.addr().Send(context.Background(), get{}); err != nil {
		t.Fatal(err)
	}

	await(t, blocked)

	// One queued message plus the worker's reserved completion fills capacity.
	if err := a.addr().Send(context.Background(), add{}); err != nil {
		t.Fatal(err)
	}
	if err := a.addr().Send(context.Background(), add{}); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}

	// Queue the completion behind the blocked handler, then stop before delivery.
	close(finishWork)
	waitQueue(t, a, 2)
	a.addr().Stop(nil)
	close(unblock)
	waitExitFor(t, a.addr())
	await(t, rolled)

	if complete.Load() {
		t.Fatal("completion ran after Stop")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.work) != 0 || a.reserved != 0 || len(a.requests) != 0 {
		t.Fatal("leaked owned records")
	}
}

func TestStopUnblocksWorkerBeforeWaiting(t *testing.T) {
	rt := runtimeFor(t)
	unblock := make(chan struct{})
	entered := make(chan struct{})
	rolled := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleMessage(func(Context, Stopping) error { close(unblock); return nil }), HandleMessage(func(ctx Context, _ wait) error {
			_, err := StartTask(ctx.WithContext(context.Background()),
				func(context.Context) (int, error) {
					close(entered)
					<-unblock
					return 1, nil
				},
				func(int, error) bool { return false },
				func(int) { close(rolled) },
			)
			return err
		})}
	})

	if err := a.addr().Send(context.Background(), wait{}); err != nil {
		t.Fatal(err)
	}

	await(t, entered)

	a.addr().Stop(nil)
	waitExitFor(t, a.addr())
	await(t, rolled)
}

func TestWorkerPanicAndDiscard(t *testing.T) {
	rt := runtimeFor(t)
	discarded := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleDeferred(func(ctx Context, _ get, r Reply[int]) error {
			_, err := StartTask(ctx,
				func(context.Context) (int, error) { panic("worker") },
				func(v int, err error) bool {
					r.Complete(v, err)
					return false
				},
				func(int) { close(discarded) },
			)
			return err
		})}
	})

	_, err := a.addr().Call[int](context.Background(), get{})
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatal(err)
	}

	await(t, discarded)
}

func TestSendContextEndsAtAdmission(t *testing.T) {
	rt := runtimeFor(t)
	blocked := make(chan struct{})
	release := make(chan struct{})
	ran := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleMessage(func(ctx Context, _ wait) error {
				close(blocked)
				<-release
				return nil
			}),
			HandleMessage(func(ctx Context, _ get) error {
				if ctx.Err() != nil {
					t.Error(ctx.Err())
				}

				close(ran)
				return nil
			}),
		}
	})

	a.addr().Send(context.Background(), wait{})
	await(t, blocked)

	ctx, cancel := context.WithCancel(context.Background())
	if err := a.addr().Send(ctx, get{}); err != nil {
		t.Fatal(err)
	}

	cancel()
	close(release)
	await(t, ran)
}

func TestTaskCapacityRejectsBeforeWorkAndLateResourceRollback(t *testing.T) {
	rt := runtimeFor(t, WithTaskLimit(1))
	started := make(chan struct{})
	release := make(chan struct{})
	discardStarted := make(chan struct{})
	discardFinish := make(chan struct{})
	var extraRan atomic.Bool
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleMessage(func(ctx Context, _ wait) error {
				_, err := StartTask(ctx.WithContext(context.Background()),
					func(context.Context) (int, error) {
						close(started)
						<-release
						return 1, nil
					},
					func(int, error) bool { return true },
					func(int) {
						close(discardStarted)
						<-discardFinish
					},
				)
				return err
			}),
			HandleMessage(func(ctx Context, _ get) error {
				_, err := StartTask(ctx.WithContext(context.Background()),
					func(context.Context) (int, error) {
						extraRan.Store(true)
						return 0, nil
					},
					func(int, error) bool { return true },
					nil,
				)
				return err
			}),
		}
	})

	if err := a.addr().Send(context.Background(), wait{}); err != nil {
		t.Fatal(err)
	}

	await(t, started)

	if _, err := a.addr().Call[any](context.Background(), get{}); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}
	if extraRan.Load() {
		t.Fatal("work started without a reservation")
	}

	a.addr().Stop(nil)
	close(release)
	await(t, discardStarted)

	// Done must join resource discard even when the worker returns after Stop.
	select {
	case <-a.exit.done:
		t.Fatal("Done before rollback finished")
	default:
	}

	close(discardFinish)
	waitExitFor(t, a.addr())
}

func TestReplyCancellationCompletionStopRace(t *testing.T) {
	rt := runtimeFor(t)
	for i := 0; i < 50; i++ {
		replies := make(chan Reply[int], 1)
		a := spawnFor(t, rt, func() []Binding {
			return []Binding{HandleDeferred(func(ctx Context, _ get, r Reply[int]) error {
				replies <- r
				return nil
			})}
		})
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := a.addr().Call[int](ctx, get{})
			result <- err
		}()
		r := <-replies
		var wg sync.WaitGroup
		for _, f := range []func(){cancel, func() { r.Complete(1, nil) }, func() { a.addr().Stop(nil) }} {
			wg.Add(1)
			go func(f func()) {
				defer wg.Done()

				f()
			}(f)
		}

		wg.Wait()
		err := <-result
		if err != nil && !errors.Is(err, ErrStopped) && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if r.Complete(2, nil) {
			t.Fatal("reply completed twice")
		}

		waitExitFor(t, a.addr())
	}
}

func TestDeferredSendHasNoCallerWaitSlot(t *testing.T) {
	rt := runtimeFor(t, WithPendingLimit(1))
	started := make(chan struct{})
	release := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleDeferred(func(ctx Context, _ get, r Reply[int]) error {
			_, err := StartTask(ctx,
				func(context.Context) (int, error) {
					close(started)
					<-release
					return 1, nil
				},
				func(v int, err error) bool {
					r.Complete(v, err)
					return true
				},
				nil,
			)
			return err
		})}
	})

	if err := a.addr().Send(context.Background(), get{}); err != nil {
		t.Fatal(err)
	}

	await(t, started)

	rt.calls.mu.Lock()
	n := rt.calls.pending
	rt.calls.mu.Unlock()

	if n != 0 {
		t.Fatal("Send allocated caller wait slot", n)
	}

	close(release)
	a.addr().Stop(nil)
	waitExitFor(t, a.addr())
}

func TestCallContextDeadlineAndCleanupPanic(t *testing.T) {
	rt := runtimeFor(t)
	observed := make(chan time.Time, 1)
	cleaned := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleMessage(func(Context, Stopping) error { defer close(cleaned); panic("stop cleanup") }), HandleMessage(func(ctx Context, _ get) error {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Error("missing deadline")
			}

			observed <- deadline
			<-ctx.Done()
			return ctx.Err()
		})}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	expected, _ := ctx.Deadline()
	if _, err := a.addr().Call[any](ctx, get{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if deadline := <-observed; !deadline.Equal(expected) {
		t.Fatal(deadline, expected)
	}

	a.addr().Stop(nil)
	waitExitFor(t, a.addr())
	await(t, cleaned)

	var pe *PanicError
	if !errors.As(a.reason(), &pe) {
		t.Fatal(a.reason())
	}
}

func TestBindingsCalledOnceBeforeStartup(t *testing.T) {
	rt := runtimeFor(t)
	calls := 0
	addr, err := rt.Spawn(bindingsFunc(func() []Binding {
		calls++
		return []Binding{
			HandleMessage(func(ctx Context, _ Started) error {
				if err := ctx.actorContext().actor.addr().Send(context.Background(), get{}); !errors.Is(err, ErrStopped) {
					t.Error("startup admitted ordinary message", err)
				}
				return nil
			}),
			Handle(func(Context, get) (int, error) { return calls, nil }),
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	value, err := addr.Call[int](context.Background(), get{})
	if err != nil || value != 1 || calls != 1 {
		t.Fatal(value, err, calls)
	}
}

func TestDiscardPanicRemainsObservableAfterNormalStop(t *testing.T) {
	rt := runtimeFor(t)
	reported := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleMessage(func(ctx Context, _ get) error {
			_, err := StartTask(ctx.WithContext(context.Background()),
				func(context.Context) (int, error) { return 0, nil },
				func(int, error) bool { return false },
				func(int) {
					defer close(reported)

					panic("discard")
				},
			)
			return err
		})}
	})

	if err := a.addr().Send(context.Background(), get{}); err != nil {
		t.Fatal(err)
	}

	await(t, reported)

	// A serial barrier also waits for discard's panic to be recorded.
	a.mu.Lock()
	a.mu.Unlock()

	a.addr().Stop(nil)
	waitExitFor(t, a.addr())

	var pe *PanicError
	if !errors.As(a.reason(), &pe) {
		t.Fatal(a.reason())
	}
}
