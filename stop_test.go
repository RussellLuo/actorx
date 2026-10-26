package actorx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestAddrStopValidationAndEmptyActor(t *testing.T) {
	rt := runtimeFor(t)
	a := spawnFor(t, rt, func() []Binding { return []Binding{} })
	addr := a.addr()
	for _, invalid := range []Addr{{}, {id: a.id}, {channel: rt.calls}, {
		id:      "a_bad",
		channel: rt.calls,
		rt:      rt,
		exit:    a.exit,
	}} {
		if err := invalid.Stop(nil); !errors.Is(err, ErrStopped) {
			t.Fatal("invalid address", err)
		}
	}

	data, err := ExportAddr(addr)
	if err != nil {
		t.Fatal(err)
	}

	for _, importer := range []*Runtime{rt, runtimeFor(t)} {
		imported, err := importer.ImportAddr(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := imported.Stop(nil); err == nil || errors.Is(err, ErrStopped) || errors.Is(err, ErrClosed) {
			t.Fatal("imported address acquired stop capability", err)
		}
	}

	copyByValue := make(chan Addr, 1)
	copyByValue <- addr
	copy := <-copyByValue
	if err := copy.Stop(nil); err != nil {
		t.Fatal(err)
	}
	if exit := waitExitFor(t, addr); exit.Reason != nil {
		t.Fatal(exit)
	}
	if err := copy.Stop(errors.New("late")); err != nil || a.reason() != nil {
		t.Fatal("late Stop changed normal exit", err, a.reason())
	}
	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := addr.Stop(nil); !errors.Is(err, ErrClosed) {
		t.Fatal("Stop after Close", err)
	}
	if exit := waitExitFor(t, addr); exit.Reason != nil {
		t.Fatal(exit)
	}
}

func TestAddrStopFullBudgetsAndHandlerBarrier(t *testing.T) {
	rt := runtimeFor(t, WithMailboxCapacity(1), WithPendingLimit(1), WithCallLimit(1), WithTaskLimit(1))
	entered, release, workerCanceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseAtCleanup(t, release)
	var notices, cleanups atomic.Int32
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleDeferred(func(ctx Context, _ get, _ Reply[int]) error {
				if _, err := StartTask(ctx,
					func(workCtx context.Context) (int, error) {
						<-workCtx.Done()
						close(workerCanceled)
						return 0, workCtx.Err()
					},
					func(int, error) bool {
						t.Error("completion after stop")
						return true
					},
					nil,
				); err != nil {
					return err
				}

				close(entered)
				<-release
				return nil
			}),
			HandleMessage(func(Context, Stopping) error {
				notices.Add(1)
				return nil
			}),
			HandleMessage(func(Context, Stopped) error {
				cleanups.Add(1)
				notices.Add(1)
				return nil
			}),
		}
	})
	r, err := a.addr().prepare(context.Background(), get{}, callMode)
	if err != nil {
		t.Fatal(err)
	}

	await(t, entered)
	// The reserved completion fills the mailbox; the active Call holds the only
	// caller slot and worker slot. The active request also fills the target pending budget.
	if err := a.addr().Send(context.Background(), get{}); !errors.Is(err, ErrFull) {
		t.Fatal("full target accepted Send", err)
	}

	cause := errors.New("first stop")
	if err := a.addr().Stop(cause); err != nil {
		t.Fatal(err)
	}

	await(t, workerCanceled)
	await(t, r.done)
	if !errors.Is(r.err, ErrStopped) {
		t.Fatal(r.err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	waiting := beginExitWait(t, a.addr(), ctx)
	pendingExitWait(t, waiting)
	cancel()
	if got := <-waiting; !errors.Is(got.err, context.Canceled) {
		t.Fatal(got)
	}
	if err := a.addr().Send(context.Background(), get{}); !errors.Is(err, ErrStopped) {
		t.Fatal("admission stayed open", err)
	}

	ignored := errors.New("ignored repeated stop")
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() {
			if err := a.addr().Stop(ignored); err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()
	if notices.Load() != 0 || cleanups.Load() != 0 {
		t.Fatal("exit overtook active handler")
	}

	close(release)
	if exit := waitExitFor(t, a.addr()); !errors.Is(exit.Reason, cause) || errors.Is(exit.Reason, ignored) {
		t.Fatal(exit)
	}
	if notices.Load() != 2 || cleanups.Load() != 1 {
		t.Fatal("duplicate cleanup or notices", notices.Load(), cleanups.Load())
	}
}

func TestContextStopPhases(t *testing.T) {
	for _, phase := range []string{"Started", "handler", "completion"} {
		t.Run(phase, func(t *testing.T) {
			rt := runtimeFor(t)
			cause := errors.New("self stop")
			var early Addr
			var callbacks atomic.Int32
			stop := func(ctx Context) {
				ctx.Stop(cause)
				ctx.WithContext(context.Background()).Stop(errors.New("ignored"))
			}
			a, err := rt.Spawn(bindingsFunc(func() []Binding {
				return []Binding{
					HandleMessage(func(ctx Context, _ Started) error {
						early = ctx.actorContext().actor.addr()
						if phase == "Started" {
							stop(ctx)
						}
						if phase == "completion" {
							_, err := StartTask(ctx,
								func(context.Context) (int, error) { return 1, nil },
								func(int, error) bool {
									stop(ctx)
									callbacks.Add(1)
									return true
								},
								nil,
							)
							return err
						}

						return nil
					}),
					Handle(func(ctx Context, _ get) (int, error) {
						stop(ctx)
						return 1, nil
					}),
					HandleMessage(func(ctx Context, _ Stopping) error {
						ctx.Stop(errors.New("ignored stopping"))
						return nil
					}),
					HandleMessage(func(ctx Context, _ Stopped) error {
						ctx.Stop(errors.New("ignored stopped"))
						return nil
					}),
				}
			}))
			if phase == "Started" {
				if a != (Addr{}) || !errors.Is(err, ErrStopped) || !errors.Is(err, cause) {
					t.Fatal(a, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if phase == "handler" {
					if _, err := early.Call[int](context.Background(), get{}); !errors.Is(err, ErrStopped) {
						t.Fatal("self request", err)
					}
				}
			}
			if exit := waitExitFor(t, early); !errors.Is(exit.Reason, cause) {
				t.Fatal(exit)
			}
			if phase == "completion" && callbacks.Load() != 1 {
				t.Fatal("missing serial completion")
			}
		})
	}
}

type Stop struct{}

type stopBusiness struct{ calls int }

func (s *stopBusiness) Stop(Context, Stop) error {
	s.calls++
	return nil
}

func TestBusinessStopRemainsOrdinaryMessage(t *testing.T) {
	rt := runtimeFor(t)
	s := &stopBusiness{}
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleMessage(s.Stop), Handle(func(Context, get) (int, error) { return s.calls, nil })}
	})
	if err := a.addr().Send(context.Background(), Stop{}); err != nil {
		t.Fatal(err)
	}
	if got := call[int](t, a, get{}); got != 1 {
		t.Fatal(got)
	}
	if err := a.addr().Stop(nil); err != nil {
		t.Fatal(err)
	}
	if exit := waitExitFor(t, a.addr()); exit.Reason != nil {
		t.Fatal(exit)
	}
}

func TestAddrStopStartupPublicationRace(t *testing.T) {
	for range 40 {
		rt := runtimeFor(t)
		entered, release := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		t.Cleanup(resume)
		early := make(chan Addr, 1)
		type spawnResult struct {
			a   Addr
			err error
		}
		result := make(chan spawnResult, 1)
		var cleanups atomic.Int32
		go func() {
			a, err := rt.Spawn(bindingsFunc(func() []Binding {
				return []Binding{
					HandleMessage(func(Context, Stopped) error {
						cleanups.Add(1)
						return nil
					}),
					HandleMessage(func(ctx Context, _ Started) error {
						early <- ctx.actorContext().actor.addr()
						close(entered)
						<-release
						return nil
					}),
				}
			}))
			result <- spawnResult{a, err}
		}()
		addr := <-early
		await(t, entered)
		cause := errors.New("startup race stop")
		start := make(chan struct{})
		stopped := make(chan error, 1)
		go func() {
			<-start
			stopped <- addr.Stop(cause)
		}()
		go func() {
			<-start
			resume()
		}()
		close(start)
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}

		got := <-result
		if got.err != nil {
			if got.a != (Addr{}) || !errors.Is(got.err, ErrStopped) || !errors.Is(got.err, cause) {
				t.Fatal(got)
			}
		} else if got.a == (Addr{}) || got.a != addr {
			t.Fatal("successful startup lost original address", got)
		}
		if exit := waitExitFor(t, addr); !errors.Is(exit.Reason, cause) {
			t.Fatal(exit)
		}
		if cleanups.Load() != 1 {
			t.Fatal("cleanup count", cleanups.Load())
		}
	}
}
