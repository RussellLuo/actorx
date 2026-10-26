package actorx

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func releaseAtCleanup(t *testing.T, ch chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		select {
		case <-ch:
		default:
			close(ch)
		}
	})
}

func TestLifecycleAdmissionStateAndOrder(t *testing.T) {
	type startupStream struct{}
	rt := runtimeFor(t)
	ready := make(chan Context, 1)
	releaseStart := make(chan struct{})
	final := make(chan struct{})
	releaseFinal := make(chan struct{})
	releaseAtCleanup(t, releaseStart)
	releaseAtCleanup(t, releaseFinal)
	spawned := make(chan Addr, 1)
	var order []string // Only serial callbacks write; Done publishes the result.
	cause := errors.New("stop cause")
	go func() {
		a, err := rt.Spawn(bindingsFunc(func() []Binding {
			return []Binding{
				HandleMessage(func(ctx Context, _ Started) error {
					order = append(order, "started")
					if ctx.State() != StateStarted || ctx.Err() != nil {
						t.Error("incorrect startup context")
					}

					_, err := StartTask(ctx,
						func(context.Context) (int, error) { return 7, nil },
						func(v int, err error) bool {
							if ctx.State() != StateRunning || v != 7 || err != nil {
								t.Error("incorrect startup completion")
							}

							order = append(order, "completion")
							return true
						},
						nil,
					)
					ready <- ctx
					<-releaseStart
					return err
				}),
				HandleMessage(func(ctx Context, _ get) error {
					if ctx.State() != StateRunning {
						t.Error("handler outside Running")
					}

					order = append(order, "message")
					return nil
				}),
				HandleStream(func(Context, startupStream, StreamReply[int]) error {
					t.Error("stream admitted during startup")
					return nil
				}),
				HandleMessage(func(ctx Context, msg Stopping) error {
					if ctx.State() != StateStopping || ctx.Err() != context.Canceled || !errors.Is(msg.Reason, cause) {
						t.Error("incorrect Stopping context/reason")
					}

					_, err := StartTask(ctx.WithContext(context.Background()),
						func(context.Context) (int, error) { return 0, nil },
						func(int, error) bool { return true },
						nil,
					)
					if !errors.Is(err, ErrStopped) {
						t.Error("Stopping admitted work", err)
					}

					order = append(order, "stopping")
					return nil
				}),
				HandleMessage(func(ctx Context, msg Stopped) error {
					if ctx.State() != StateStopped || ctx.Err() != context.Canceled || !errors.Is(msg.Reason, cause) {
						t.Error("incorrect Stopped context/reason")
					}

					order = append(order, "stopped")
					close(final)
					<-releaseFinal
					return nil
				}),
			}
		}))
		if err != nil {
			t.Error(err)
		}

		spawned <- a
	}()
	ctx := <-ready
	a := ctx.actorContext().actor
	// Force the startup completion into the mailbox before opening admission.
	a.workers.Wait()
	for _, m := range []mode{sendMode, callMode, streamMode} {
		// Use a supported message for each mode so descriptor validation passes
		// and the test reaches the actor's startup admission barrier.
		var msg any = get{}
		if m == streamMode {
			msg = startupStream{}
		}
		if _, err := a.addr().prepare(context.Background(), msg, m); !errors.Is(err, ErrStopped) {
			t.Fatal("admitted before Started returned", m, err)
		}

		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := a.addr().prepare(canceled, msg, m); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}

	rt.calls.mu.Lock()
	if rt.calls.pending != 0 || len(rt.calls.calls) != 0 {
		t.Error("startup rejection leaked caller slots")
	}

	rt.calls.mu.Unlock()

	a.mu.Lock()
	if len(a.requests) != 0 || len(a.queue) != 1 {
		t.Error("startup rejection changed target requests or task completion")
	}

	a.mu.Unlock()

	select {
	case <-spawned:
		t.Fatal("Spawn returned before Started")
	default:
	}

	if !reflect.DeepEqual(order, []string{"started"}) {
		t.Fatal("startup completion executed early", order)
	}

	close(releaseStart)
	if addr := <-spawned; addr != a.addr() {
		t.Fatal("wrong spawned address")
	}

	call[any](t, a, get{})
	a.addr().Stop(cause)
	a.addr().Stop(errors.New("ignored second stop"))
	await(t, final)
	if ctx.State() != StateStopped {
		t.Fatal(ctx.State())
	}

	select {
	case <-a.exit.done:
		t.Fatal("Done preceded Stopped return")
	default:
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelWait()

	waiting := beginExitWait(t, a.addr(), waitCtx)
	pendingExitWait(t, waiting)
	close(releaseFinal)
	waitExitFor(t, a.addr())
	if got := <-waiting; got.err != nil || got.exit.Reason != a.reason() {
		t.Fatal(got, a.reason())
	}

	want := []string{"started", "completion", "message", "stopping", "stopped"}
	if !reflect.DeepEqual(order, want) || !errors.Is(a.reason(), cause) || strings.Contains(a.reason().Error(), "ignored second stop") {
		t.Fatal(order, a.reason())
	}
}

func TestLifecycleStartedFailureRollsBackLateTask(t *testing.T) {
	for _, tc := range []struct {
		name               string
		panicStart, queued bool
	}{
		{"error-late", false, false},
		{"panic-late", true, false},
		{"error-queued", false, true},
		{"panic-queued", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := runtimeFor(t, WithMailboxCapacity(1), WithPendingLimit(1), WithCallLimit(1), WithTaskLimit(1))
			failure := errors.New("startup failed")
			stopFailure := errors.New("stopping failed")
			finalFailure := errors.New("stopped failed")
			unblock := make(chan struct{})
			var discarded, delivered atomic.Int32
			var actor *actor
			var order []string
			_, err := rt.Spawn(bindingsFunc(func() []Binding {
				return []Binding{
					HandleMessage(func(ctx Context, _ Started) error {
						actor = ctx.actorContext().actor
						_, err := StartTask(ctx,
							func(ctx context.Context) (int, error) {
								<-unblock // Stopping must unblock before worker waiting.
								if !tc.queued && ctx.Err() != context.Canceled {
									t.Error("startup worker not cancelled")
								}

								return 42, nil
							},
							func(int, error) bool {
								delivered.Add(1)
								return true
							},
							func(v int) {
								if v != 42 {
									t.Error(v)
								}

								discarded.Add(1)
							},
						)
						if err != nil {
							return err
						}
						if tc.queued {
							close(unblock)
							ctx.actorContext().actor.workers.Wait() // Force a completion queued before startup failure.
						}
						if tc.panicStart {
							panic(failure)
						}

						return failure
					}),
					HandleMessage(func(ctx Context, msg Stopping) error {
						if msg.Reason == nil || ctx.State() != StateStopping {
							t.Error(msg.Reason, ctx.State())
						}

						order = append(order, "stopping")
						if !tc.queued {
							close(unblock)
						}

						return stopFailure
					}),
					HandleMessage(func(ctx Context, msg Stopped) error {
						if discarded.Load() != 1 || delivered.Load() != 0 {
							t.Error("unsettled startup result")
						}
						if !errors.Is(msg.Reason, stopFailure) {
							t.Error(msg.Reason)
						}

						order = append(order, "stopped")
						return finalFailure
					}),
				}
			}))
			if !errors.Is(err, stopFailure) || !errors.Is(err, finalFailure) {
				t.Fatal(err)
			}
			if tc.panicStart {
				var p *PanicError
				if !errors.As(err, &p) || p.Value != failure {
					t.Fatal(err)
				}
			} else if !errors.Is(err, failure) {
				t.Fatal(err)
			}

			waitExitFor(t, actor.addr()) // Spawn failure includes complete rollback.
			exit := waitExitFor(t, actor.addr())
			if exit.Reason != err || exit.Reason != actor.reason() {
				t.Fatal("startup result differs from exit", exit, err)
			}
			if !reflect.DeepEqual(order, []string{"stopping", "stopped"}) {
				t.Fatal(order)
			}
		})
	}
}

func TestLifecycleStopOrCloseDuringStarted(t *testing.T) {
	for _, closeRuntime := range []bool{false, true} {
		t.Run(map[bool]string{false: "Stop", true: "Close"}[closeRuntime], func(t *testing.T) {
			rt := runtimeFor(t)
			ready := make(chan Context, 1)
			cancelled := make(chan struct{})
			release := make(chan struct{})
			releaseAtCleanup(t, release)
			result := make(chan error, 1)
			var hooks atomic.Int32
			go func() {
				_, err := rt.Spawn(bindingsFunc(func() []Binding {
					return []Binding{
						HandleMessage(func(ctx Context, _ Started) error {
							ready <- ctx
							<-ctx.Done()
							close(cancelled)
							<-release
							if ctx.State() != StateStopping {
								t.Error("Stop overwritten", ctx.State())
							}

							return nil
						}),
						HandleMessage(func(Context, Stopping) error {
							hooks.Add(1)
							return nil
						}),
						HandleMessage(func(Context, Stopped) error {
							hooks.Add(1)
							return nil
						}),
					}
				}))
				result <- err
			}()
			ctx := <-ready
			if closeRuntime {
				waitCtx, cancel := context.WithCancel(context.Background())
				cancel()
				err := rt.Close(waitCtx)
				if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "phase=started") {
					t.Fatal(err)
				}
			} else {
				if err := ctx.actorContext().actor.addr().Stop(nil); err != nil {
					t.Fatal(err)
				}
			}

			await(t, cancelled)
			if ctx.State() != StateStopping || hooks.Load() != 0 {
				t.Fatal("overlapping callbacks")
			}

			select {
			case err := <-result:
				t.Fatal("Spawn returned before callback", err)
			default:
			}

			close(release)
			err := <-result
			if !errors.Is(err, ErrStopped) || (closeRuntime && !errors.Is(err, ErrClosed)) {
				t.Fatal(err)
			}
			if hooks.Load() != 2 || ctx.State() != StateStopped {
				t.Fatal(hooks.Load(), ctx.State())
			}

			await(t, ctx.actorContext().actor.exit.done)
		})
	}
}

func TestLifecyclePreStartFailureSkipsHooks(t *testing.T) {
	for _, stage := range []string{"invalid", "panic", "stop", "close", "binding"} {
		t.Run(stage, func(t *testing.T) {
			ch := &preStartChannel{stage: stage}
			rt := runtimeFor(t, WithChannel(ch))
			hooks := 0
			_, err := rt.Spawn(bindingsFunc(func() []Binding {
				entries := []Binding{
					HandleMessage(func(Context, Started) error {
						hooks++
						return nil
					}),
					HandleMessage(func(Context, Stopping) error {
						hooks++
						return nil
					}),
					HandleMessage(func(Context, Stopped) error {
						hooks++
						return nil
					}),
				}
				if stage == "panic" {
					panic("bindings failed")
				}
				if stage == "invalid" {
					entries = append(entries, Binding{})
				}
				return entries
			}))
			if err == nil || hooks != 0 {
				t.Fatal(err, hooks)
			}
			rt.mu.Lock()
			defer rt.mu.Unlock()
			if len(rt.actors) != 0 {
				t.Fatal("pre-start rollback retained execution state")
			}
		})
	}
}

type preStartChannel struct {
	LocalChannel
	stage string
}

func (ch *preStartChannel) Bind(ctx context.Context, id string, r Receiver) (func() error, error) {
	a := r.(actorReceiver).actor
	switch ch.stage {
	case "stop":
		a.stop(nil)
	case "close":
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := a.rt.Close(canceled); !errors.Is(err, context.Canceled) {
			return nil, err
		}
	case "binding":
		return nil, errors.New("binding failed")
	}
	return ch.LocalChannel.Bind(ctx, id, r)
}

func TestLifecycleBindingAndForgeryRejections(t *testing.T) {
	type startedPointer *Started
	var started atomic.Int32
	hook := HandleMessage(func(Context, Started) error {
		started.Add(1)
		return nil
	})
	for _, bindings := range [][]Binding{
		{hook, hook},
		{HandleMessage[Started](nil)},
		{HandleMessage[Stopping](nil)},
		{HandleMessage[Stopped](nil)},
		{HandleMessage(func(Context, *Started) error { return nil })},
		{HandleMessage(func(Context, **Stopping) error { return nil })},
		{HandleMessage(func(Context, startedPointer) error { return nil })},
		{Handle(func(Context, Stopped) (int, error) { return 0, nil })},
		{HandleDeferred(func(Context, Started, Reply[int]) error { return nil })},
		{HandleStream(func(Context, Stopping, StreamReply[int]) error { return nil })},
	} {
		rt := runtimeFor(t)
		if _, err := rt.Spawn(bindingsFunc(func() []Binding { return bindings })); !errors.Is(err, ErrSetup) {
			t.Fatal(err)
		}
	}

	rt := runtimeFor(t, WithCallLimit(1), WithPendingLimit(1))
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{hook, Handle(func(Context, get) (int, error) { return 7, nil })}
	})
	for _, msg := range []any{
		Started{},
		Stopping{},
		Stopped{},
		&Started{},
		(*Stopping)(nil),
		new(*Stopped),
		startedPointer(nil),
	} {
		for _, m := range []mode{sendMode, callMode, streamMode} {
			if _, err := a.addr().prepare(context.Background(), msg, m); !errors.Is(err, ErrUnhandled) {
				t.Fatal(msg, m, err)
			}

			for _, descriptor := range (actorReceiver{a}).Bindings() {
				if descriptor.MessageType() == reflect.TypeOf(msg) {
					t.Fatal("lifecycle event in ordinary bindings")
				}
			}

			if err := (actorReceiver{a}).Receive(context.Background(), Delivery{
				Target:      a.id,
				Correlation: "forged",
				Kind:        Kind(m),
				Message:     msg,
			}, correlationReply{}); !errors.Is(err, ErrUnhandled) {
				t.Fatal(err)
			}
		}
	}

	if started.Load() != 1 {
		t.Fatal("forged event invoked Started", started.Load())
	}
	if got := call[int](t, a, get{}); got != 7 {
		t.Fatal(got)
	}

	bindings := (actorReceiver{a}).Bindings()
	if len(bindings) != 1 || bindings[0] != Request[get, int]() {
		t.Fatal("ordinary bindings changed", bindings)
	}

}

type lifecycleReleaseChannel struct {
	LocalChannel
	panics   bool
	released atomic.Int32
	failure  error
}

func (d *lifecycleReleaseChannel) Bind(ctx context.Context, id string, r Receiver) (func() error, error) {
	release, err := d.LocalChannel.Bind(ctx, id, r)
	if err != nil {
		return nil, err
	}

	return func() error {
		d.released.Add(1)
		release()
		if d.panics {
			panic("release panic")
		}

		return d.failure
	}, nil
}

func TestLifecycleExitPanicsContinueAfterBindingRelease(t *testing.T) {
	for _, panicRelease := range []bool{false, true} {
		d := &lifecycleReleaseChannel{
			panics:  panicRelease,
			failure: errors.New("release failed"),
		}
		rt := runtimeFor(t, WithChannel(d))
		var cleaned bool
		addr, err := rt.Spawn(bindingsFunc(func() []Binding {
			// An actor with no Started binding still enters the startup stage.
			return []Binding{
				HandleMessage(func(Context, Stopping) error {
					defer func() { cleaned = true }()
					panic("stopping panic")
				}),
				HandleMessage(func(ctx Context, msg Stopped) error {
					if !cleaned || d.released.Load() != 1 || ctx.State() != StateStopped {
						t.Error("finalized before cleanup/release")
					}
					if msg.Reason == nil || !strings.Contains(msg.Reason.Error(), "stopping panic") {
						t.Error(msg.Reason)
					}
					if !panicRelease && !errors.Is(msg.Reason, d.failure) {
						t.Error(msg.Reason)
					}

					panic("stopped panic")
				}),
			}
		}))
		if err != nil {
			t.Fatal(err)
		}
		addr.Stop(nil)
		exit := waitExitFor(t, addr)
		if err := exit.Reason; err == nil || !strings.Contains(err.Error(), "stopped panic") ||
			(panicRelease && !strings.Contains(err.Error(), "release panic")) {
			t.Fatal(err)
		}
	}
}

func TestLifecycleStoppingWaitsForActiveHandler(t *testing.T) {
	rt := runtimeFor(t)
	active := make(chan Context, 1)
	observedStop := make(chan struct{})
	release := make(chan struct{})
	releaseAtCleanup(t, release)
	var stoppedHooks atomic.Int32
	var handled atomic.Int32
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleMessage(func(ctx Context, _ wait) error {
				active <- ctx
				<-ctx.Done()
				if ctx.State() != StateStopping {
					t.Error(ctx.State())
				}

				close(observedStop)
				<-release
				return nil
			}),
			HandleMessage(func(Context, get) error {
				handled.Add(1)
				return nil
			}),
			HandleMessage(func(Context, Stopping) error {
				stoppedHooks.Add(1)
				return nil
			}),
		}
	})
	if err := a.addr().Send(context.Background(), wait{}); err != nil {
		t.Fatal(err)
	}

	ctx := <-active
	if err := a.addr().Send(context.Background(), get{}); err != nil {
		t.Fatal(err)
	}

	a.addr().Stop(nil)
	await(t, observedStop)
	if ctx.State() != StateStopping || stoppedHooks.Load() != 0 {
		t.Fatal("overlapping lifecycle callback")
	}

	close(release)
	waitExitFor(t, a.addr())
	if handled.Load() != 0 || stoppedHooks.Load() != 1 {
		t.Fatal("queued work executed after Stop")
	}
}

func TestLifecycleClassificationAllowsRecursivePointerMessages(t *testing.T) {
	type recursive *recursive
	rt := runtimeFor(t)
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleMessage(func(Context, recursive) error { return nil })}
	})
	call[any](t, a, recursive(nil))
}
