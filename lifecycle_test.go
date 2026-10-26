package actorx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDoneWaitsForRequestSettlement(t *testing.T) {
	rt := runtimeFor(t)
	entered := make(chan struct{})
	returned := make(chan struct{})
	stopping := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleMessage(func(Context, Stopping) error {
				close(stopping)
				return nil
			}),
			HandleDeferred(func(ctx Context, _ get, _ Reply[int]) error { return nil }),
			HandleMessage(func(ctx Context, _ wait) error {
				close(entered)
				<-ctx.Done()
				close(returned)
				return nil
			}),
		}
	})

	r, err := a.addr().prepare(context.Background(), get{}, callMode)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.addr().Send(context.Background(), wait{}); err != nil {
		t.Fatal(err)
	}

	await(t, entered)

	// Pause result publication while the active handler responds to cancellation.
	r.watchMu.Lock()
	stopped := make(chan struct{})
	go func() {
		a.addr().Stop(nil)
		close(stopped)
	}()
	await(t, returned)

	deadline := time.Now().Add(3 * time.Second)
	for {
		rt.calls.mu.Lock()
		pending := rt.calls.pending
		rt.calls.mu.Unlock()

		if pending == 0 {
			break // finish has released the slot and is blocked at watchMu.
		}
		if time.Now().After(deadline) {
			r.watchMu.Unlock()

			t.Fatal("request settlement did not reach result publication")
		}

		time.Sleep(time.Millisecond)
	}

	select {
	case <-stopping:
		t.Error("Stopping ran before request settlement")
	default:
	}

	select {
	case <-a.exit.done:
		t.Error("Done closed before request settlement")
	default:
	}

	a.mu.Lock()
	tracked := false
	for target := range a.requests {
		if target.id == r.id {
			tracked = true
			break
		}
	}

	a.mu.Unlock()

	if !tracked {
		t.Error("unfinished result lost its owner record")
	}

	r.watchMu.Unlock()

	await(t, stopped)
	waitExitFor(t, a.addr())
	await(t, r.done)

	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.requests) != 0 || rt.calls.pending != 0 {
		t.Fatal("request or caller slot survived Done")
	}
}

func TestDoneSettlesAllPendingRequests(t *testing.T) {
	rt := runtimeFor(t, WithPendingLimit(1024), WithCallLimit(1024), WithMailboxCapacity(1024))
	entered := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{
			HandleDeferred(func(ctx Context, _ get, _ Reply[int]) error { return nil }),
			HandleMessage(func(ctx Context, _ wait) error {
				close(entered)
				<-ctx.Done()
				return nil
			}),
		}
	})

	var requests []*caller
	for i := 0; i < 1000; i++ {
		r, err := a.addr().prepare(context.Background(), get{}, callMode)
		if err != nil {
			t.Fatal(err)
		}

		requests = append(requests, r)
	}

	if err := a.addr().Send(context.Background(), wait{}); err != nil {
		t.Fatal(err)
	}

	await(t, entered)

	stopped := make(chan struct{})
	go func() {
		a.addr().Stop(nil)
		close(stopped)
	}()
	waitExitFor(t, a.addr())

	for _, r := range requests {
		select {
		case <-r.done:
		default:
			t.Error("Done preceded a pending result")
		}
	}

	await(t, stopped)
}

func TestActorUnregisterPublishesDone(t *testing.T) {
	rt := runtimeFor(t)
	for i := 0; i < 100; i++ {
		a := spawnFor(t, rt, func() []Binding { return nil })
		go a.addr().Stop(nil)
		for {
			rt.mu.Lock()
			_, registered := rt.actors[a.id]
			if !registered {
				select {
				case <-a.exit.done:
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					if exit, err := a.addr().Wait(ctx); err != nil || exit.Reason != a.reason() {
						t.Error("unregistration preceded exit publication", exit, err)
					}
				default:
					t.Error("unregistered actor has not published Done")
				}
			}

			rt.mu.Unlock()

			if !registered {
				break
			}

			<-a.exit.done
		}
	}
}

func TestCloseReportsBlockedActorAndPreservesCause(t *testing.T) {
	for _, phase := range []string{"setup", "handler", "stopping", "workers"} {
		t.Run(phase, func(t *testing.T) {
			rt := runtimeFor(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			defer close(release)

			spawned := make(chan Addr, 1)
			go func() {
				a, _ := rt.Spawn(bindingsFunc(func() []Binding {
					if phase == "setup" {
						close(entered)
						<-release
					}
					return []Binding{HandleMessage(func(Context, Stopping) error {
						if phase == "stopping" {
							close(entered)
							<-release
						}
						return nil
					}), HandleMessage(func(ctx Context, _ get) error {
						if phase == "handler" {
							close(entered)
							<-release
						}
						if phase == "workers" {
							_, err := StartTask(ctx.WithContext(context.Background()),
								func(context.Context) (int, error) {
									close(entered)
									<-release
									return 0, nil
								},
								func(int, error) bool { return true },
								nil,
							)
							return err
						}

						return nil
					})}
				}))
				spawned <- a
			}()
			if phase != "setup" {
				a := <-spawned
				if phase == "stopping" {
					a.Stop(nil)
				} else if err := a.Send(context.Background(), get{}); err != nil {
					t.Fatal(err)
				}
			}

			await(t, entered)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := rt.Close(ctx)
			// The worker may still be leaving its handler when Close snapshots it.
			wantPhase := "phase=" + phase
			if phase == "workers" {
				wantPhase = "work=1"
			}
			if !errors.Is(err, context.Canceled) ||
				!strings.Contains(err.Error(), "a_") ||
				!strings.Contains(err.Error(), wantPhase) {
				t.Fatalf("missing shutdown diagnostics: %v", err)
			}
		})
	}
}

func TestPendingAndCallerLimitsAreIndependent(t *testing.T) {
	rt := runtimeFor(t, WithPendingLimit(1), WithCallLimit(2))
	makeActor := func() *actor {
		return spawnFor(t, rt, func() []Binding {
			return []Binding{HandleDeferred(func(ctx Context, _ get, _ Reply[int]) error { return nil })}
		})
	}
	a, b, c := makeActor(), makeActor(), makeActor()

	// Exhaust a's request budget while leaving one global Call association free.
	if _, err := a.addr().prepare(context.Background(), get{}, callMode); err != nil {
		t.Fatal(err)
	}
	if err := a.addr().Send(context.Background(), get{}); !errors.Is(err, ErrFull) {
		t.Fatal("per actor bound was not enforced", err)
	}
	if _, err := b.addr().prepare(context.Background(), get{}, callMode); err != nil {
		t.Fatal("unrelated actor used the same request budget", err)
	}

	// Calls now exhaust the global budget, but Send needs no caller association.
	if _, err := c.addr().prepare(context.Background(), get{}, callMode); !errors.Is(err, ErrFull) {
		t.Fatal("global caller bound was not enforced", err)
	}
	if err := c.addr().Send(context.Background(), get{}); err != nil {
		t.Fatal("Send used a caller association", err)
	}
}

func TestStartedErrorIsReturnedOnceAfterSelfStop(t *testing.T) {
	rt := runtimeFor(t)
	failure := errors.New("started failed once")
	for _, stopFirst := range []bool{false, true} {
		_, err := rt.Spawn(bindingsFunc(func() []Binding {
			return []Binding{HandleMessage(func(ctx Context, _ Started) error {
				if stopFirst {
					ctx.Stop(nil)
				}
				return failure
			})}
		}))
		if !errors.Is(err, failure) || strings.Count(err.Error(), failure.Error()) != 1 {
			t.Fatal(err)
		}
	}
}
