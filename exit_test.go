package actorx

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"weak"
)

type exitWaitResult struct {
	exit Exit
	err  error
}

// Observe entry to a blocking Wait without sleeps or a canceled-context proxy.
type observedWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (ctx *observedWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func beginExitWait(t *testing.T, addr Addr, ctx context.Context) <-chan exitWaitResult {
	t.Helper()
	observed := &observedWaitContext{Context: ctx, waiting: make(chan struct{})}
	result := make(chan exitWaitResult, 1)
	go func() {
		exit, err := addr.Wait(observed)
		result <- exitWaitResult{exit, err}
	}()
	await(t, observed.waiting)
	return result
}

func pendingExitWait(t *testing.T, result <-chan exitWaitResult) {
	t.Helper()
	select {
	case got := <-result:
		t.Fatal("Wait completed before exit", got)
	default:
	}
}

func waitExitFor(t *testing.T, addr Addr) Exit {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	exit, err := addr.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return exit
}

func TestAddrWaitPassiveConcurrentAndRetry(t *testing.T) {
	rt := runtimeFor(t)
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{Handle(func(Context, get) (int, error) { return 9, nil })}
	})
	addr := a.addr()
	copyByValue := make(chan Addr, 1)
	copyByValue <- addr
	copy := <-copyByValue
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cancelCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()

	canceled := beginExitWait(t, addr, cancelCtx)
	waiters := make([]<-chan exitWaitResult, 16)
	for i := range waiters {
		waiters[i] = beginExitWait(t, copy, ctx)
	}

	cancelWait()
	if got := <-canceled; got.exit != (Exit{}) || !errors.Is(got.err, context.Canceled) {
		t.Fatal(got)
	}

	// A timeout only ends this wait; other waiters and message admission survive.
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Millisecond)
	defer timeoutCancel()

	if exit, err := addr.Wait(timeoutCtx); exit != (Exit{}) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(exit, err)
	}
	if got := call[int](t, a, get{}); got != 9 {
		t.Fatal(got)
	}

	for _, result := range waiters {
		pendingExitWait(t, result)
	}

	a.addr().Stop(context.DeadlineExceeded)
	for _, result := range waiters {
		got := <-result
		if got.err != nil || !errors.Is(got.exit.Reason, context.DeadlineExceeded) || got.exit.Reason != a.reason() {
			t.Fatal(got, a.reason())
		}
	}

	waitExitFor(t, a.addr())
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// The published result wins even after Close and with a pre-canceled context.
	for range 3 {
		exit, err := copy.Wait(cancelCtx)
		if err != nil || exit.Reason != a.reason() {
			t.Fatal(exit, err)
		}
	}
}

func TestAddrWaitValidationAndImportBoundary(t *testing.T) {
	rt := runtimeFor(t)
	a := spawnFor(t, rt, func() []Binding { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, invalid := range []Addr{{}, {id: a.id}, {channel: rt.calls}, {id: "a_bad", channel: rt.calls, exit: a.exit}} {
		if exit, err := invalid.Wait(ctx); exit != (Exit{}) || !errors.Is(err, ErrSetup) {
			t.Fatal(exit, err)
		}
	}

	data, err := ExportAddr(a.addr())
	if err != nil || string(data) != `{"id":"`+a.id+`"}` {
		t.Fatal(string(data), err)
	}

	for _, importer := range []*Runtime{rt, runtimeFor(t)} {
		imported, err := importer.ImportAddr(data)
		if err != nil {
			t.Fatal(err)
		}
		// Capability checks precede context checks, including same-Runtime import.
		if exit, err := imported.Wait(ctx); exit != (Exit{}) || err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrSetup) {
			t.Fatal(exit, err)
		}
	}

	a.addr().Stop(nil)
	if exit := waitExitFor(t, a.addr()); exit.Reason != nil {
		t.Fatal(exit)
	}
}

func TestAddrWaitStartupFailureWaitsForRollback(t *testing.T) {
	ch := &lifecycleReleaseChannel{failure: errors.New("release failed")}
	rt := runtimeFor(t, WithChannel(ch))
	failure, finalFailure := errors.New("start failed"), errors.New("final failed")
	workerRelease, discardRelease, finalRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
	for _, release := range []chan struct{}{workerRelease, discardRelease, finalRelease} {
		releaseAtCleanup(t, release)
	}

	stopping, discarding, final := make(chan struct{}), make(chan struct{}), make(chan struct{})
	early := make(chan *actor, 1)
	spawnResult := make(chan error, 1)
	go func() {
		a, err := rt.Spawn(bindingsFunc(func() []Binding {
			return []Binding{
				HandleMessage(func(ctx Context, _ Started) error {
					early <- ctx.actorContext().actor
					_, err := StartTask(ctx,
						func(context.Context) (int, error) {
							<-workerRelease
							return 7, nil
						},
						func(int, error) bool {
							t.Error("adopted failed-start result")
							return true
						},
						func(int) {
							close(discarding)
							<-discardRelease
							panic("discard failed")
						},
					)
					return errors.Join(failure, err)
				}),
				HandleMessage(func(Context, Stopping) error {
					close(stopping)
					panic("cleanup failed")
				}),
				HandleMessage(func(Context, Stopped) error {
					close(final)
					<-finalRelease
					return finalFailure
				}),
			}
		}))
		if a != (Addr{}) {
			t.Error("failed Spawn returned an actor")
		}

		spawnResult <- err
	}()
	a := <-early
	addr := a.addr()
	await(t, stopping)
	canceled, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if exit, err := addr.Wait(canceled); exit != (Exit{}) || !errors.Is(err, context.Canceled) {
		t.Fatal("canceling a wait after Stop", exit, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	waiting := beginExitWait(t, addr, ctx)
	checkPending := func() {
		t.Helper()
		pendingExitWait(t, waiting)
		select {
		case err := <-spawnResult:
			t.Fatal("Spawn returned before rollback", err)
		default:
		}

		select {
		case <-a.exit.done:
			t.Fatal("Done preceded rollback")
		default:
		}
	}
	checkPending() // worker still owns its result
	close(workerRelease)
	await(t, discarding)
	checkPending()
	close(discardRelease)
	await(t, final)
	checkPending() // binding released, but Stopped has not returned
	close(finalRelease)
	got := <-waiting
	spawnErr := <-spawnResult
	if got.err != nil || got.exit.Reason != spawnErr || got.exit.Reason != a.reason() {
		t.Fatal(got, spawnErr, a.reason())
	}

	for _, want := range []error{failure, finalFailure, ch.failure} {
		if !errors.Is(got.exit.Reason, want) {
			t.Fatal("lost exit cause", want, got.exit.Reason)
		}
	}

	for _, want := range []string{"cleanup failed", "discard failed"} {
		if !strings.Contains(got.exit.Reason.Error(), want) {
			t.Fatal(got.exit.Reason)
		}
	}
}

func TestAddrWaitCompletionAndCloseRace(t *testing.T) {
	for range 30 {
		rt := runtimeFor(t)
		a := spawnFor(t, rt, func() []Binding { return nil })
		addr := a.addr()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		waiting := beginExitWait(t, addr, ctx)
		cause := errors.New("Stop won Close race")
		stopResult := make(chan error, 1)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()

			<-start
			stopResult <- addr.Stop(cause)
		}()
		go func() {
			defer wg.Done()

			<-start
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer closeCancel()

			if err := rt.Close(closeCtx); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			cancel()
		}()
		close(start)
		wg.Wait()
		stopErr := <-stopResult
		if stopErr != nil && !errors.Is(stopErr, ErrClosed) {
			t.Fatal(stopErr)
		}

		got := <-waiting
		if got.err != nil && !errors.Is(got.err, context.Canceled) {
			t.Fatal(got)
		}

		exit, err := addr.Wait(ctx)
		if err != nil || (stopErr == nil && !errors.Is(exit.Reason, cause)) || (stopErr != nil && exit.Reason != nil) {
			t.Fatal("Close/Stop acceptance and final cause disagree", stopErr, exit, err)
		}
		if got.err == nil && got.exit != exit {
			t.Fatal("lost racing result", got, exit)
		}

		rt.mu.Lock()
		remaining := len(rt.actors)
		rt.mu.Unlock()

		if remaining != 0 {
			t.Fatal("Close left active actors", remaining)
		}
	}
}

func TestAddrWaitAfterStartupTaskExit(t *testing.T) {
	for _, panicCompletion := range []bool{false, true} {
		t.Run(map[bool]string{false: "stop", true: "panic"}[panicCompletion], func(t *testing.T) {
			rt := runtimeFor(t)
			workRelease := make(chan struct{})
			releaseAtCleanup(t, workRelease)
			failure := errors.New("asynchronous initialization failed")
			a := spawnFor(t, rt, func() []Binding {
				return []Binding{
					HandleMessage(func(ctx Context, _ Started) error {
						_, err := StartTask(ctx,
							func(context.Context) (int, error) {
								<-workRelease
								return 0, failure
							},
							func(_ int, err error) bool {
								if panicCompletion {
									panic(err)
								}

								ctx.Stop(err)
								return true
							},
							nil,
						)
						return err
					}),
					// The application reports Ready separately from startup success.
					Handle(func(Context, get) (bool, error) { return false, nil }),
				}
			})
			if call[bool](t, a, get{}) {
				t.Fatal("Spawn implied asynchronous readiness")
			}

			addr := a.addr()
			close(workRelease)
			waitExitFor(t, a.addr()) // Late Wait needs no pre-registered observer.
			exit := waitExitFor(t, addr)
			if panicCompletion {
				var p *PanicError
				if !errors.As(exit.Reason, &p) || p.Value != failure {
					t.Fatal(exit)
				}
			} else if !errors.Is(exit.Reason, failure) {
				t.Fatal(exit)
			}
		})
	}
}

func TestAddrWaitEnteredStopBeforeStarted(t *testing.T) {
	for _, closeRuntime := range []bool{false, true} {
		t.Run(map[bool]string{false: "Stop", true: "Close"}[closeRuntime], func(t *testing.T) {
			rt := runtimeFor(t)
			entered, release := make(chan struct{}), make(chan struct{})
			releaseAtCleanup(t, release)
			failure := errors.New("Started failed after cancellation")
			var order []string
			a := newActor(rt)
			if err := a.install([]Binding{
				HandleMessage(func(ctx Context, _ Started) error {
					if ctx.Err() != context.Canceled || ctx.State() != StateStopping {
						t.Error("Started missed pre-dispatch cancellation")
					}

					order = append(order, "Started")
					close(entered)
					<-release
					return failure
				}),
				HandleMessage(func(Context, Stopping) error {
					order = append(order, "Stopping")
					return nil
				}),
				HandleMessage(func(Context, Stopped) error {
					order = append(order, "Stopped")
					return nil
				}),
			}); err != nil {
				t.Fatal(err)
			}

			// Arrange the existing internal boundary before scheduling the loop.
			// This tests Stop-before-dispatch without a production scheduling hook.
			a.entered = true
			rt.mu.Lock()
			rt.actors[a.id] = a
			rt.mu.Unlock()

			if closeRuntime {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := rt.Close(ctx); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				if err := a.addr().Stop(nil); err != nil {
					t.Fatal(err)
				}
			}

			go a.run()
			await(t, entered)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			waiting := beginExitWait(t, a.addr(), ctx)
			pendingExitWait(t, waiting)
			if err := a.addr().Send(ctx, get{}); !errors.Is(err, ErrStopped) && !errors.Is(err, ErrClosed) {
				t.Fatal("opened admission", err)
			}

			close(release)
			got := <-waiting
			if got.err != nil || !errors.Is(got.exit.Reason, failure) || !errors.Is(got.exit.Reason, ErrStopped) {
				t.Fatal(got)
			}
			if closeRuntime && !errors.Is(got.exit.Reason, ErrClosed) {
				t.Fatal(got)
			}
			if strings.Join(order, ",") != "Started,Stopping,Stopped" {
				t.Fatal(order)
			}
		})
	}
}

func TestAddrWaitRecordDoesNotRetainActorOrRebind(t *testing.T) {
	rt := runtimeFor(t)
	addr, ref := func() (Addr, weak.Pointer[actor]) {
		a := spawnFor(t, rt, func() []Binding { return nil })
		addr, ref := a.addr(), weak.Make(a)
		a.addr().Stop(nil)
		waitExitFor(t, addr)
		return addr, ref
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.GC()
		if ref.Value() == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("original address retained exited execution object")
		}

		runtime.Gosched()
	}

	replacement := spawnFor(t, rt, func() []Binding {
		return []Binding{Handle(func(Context, get) (int, error) { return 11, nil })}
	})
	if addr.id == replacement.addr().id || waitExitFor(t, addr).Reason != nil || call[int](t, replacement, get{}) != 11 {
		t.Fatal("old record rebound to new instance")
	}

	runtime.KeepAlive(addr)
}
