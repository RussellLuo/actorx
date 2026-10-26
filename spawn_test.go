package actorx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type spawnFailureChannel struct {
	LocalChannel
	err error
}

func (ch *spawnFailureChannel) Bind(context.Context, string, Receiver) (func() error, error) {
	return nil, ch.err
}

func TestSpawnFailureReturnsZeroAddress(t *testing.T) {
	for _, stage := range []string{"nil", "invalid", "bindings-panic", "bind", "started-error", "started-panic"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("startup failure")
			var opts []Option
			if stage == "bind" {
				opts = append(opts, WithChannel(&spawnFailureChannel{err: failure}))
			}
			rt := runtimeFor(t, opts...)
			var early Addr
			var cleaned atomic.Bool
			var value Actor = bindingsFunc(func() []Binding {
				if stage == "invalid" {
					return []Binding{{}}
				}
				if stage == "bindings-panic" {
					panic(failure)
				}
				return []Binding{
					HandleMessage(func(ctx Context, _ Started) error {
						early = ctx.actorContext().actor.addr()
						if stage == "started-panic" {
							panic(failure)
						}
						return failure
					}),
					HandleMessage(func(Context, Stopped) error { cleaned.Store(true); return nil }),
				}
			})
			if stage == "nil" {
				value = nil
			}
			addr, err := rt.Spawn(value)
			if !addr.IsZero() || err == nil {
				t.Fatal(addr, err)
			}
			entered := stage == "started-error" || stage == "started-panic"
			if cleaned.Load() != entered {
				t.Fatal("incorrect lifecycle rollback", cleaned.Load(), stage)
			}
			if entered && waitExitFor(t, early).Reason != err {
				t.Fatal("Spawn did not wait for final result", err)
			}
			if stage == "nil" || stage == "invalid" || stage == "bindings-panic" {
				if !errors.Is(err, ErrSetup) {
					t.Fatal(err)
				}
			}
			if stage == "bindings-panic" || stage == "started-panic" {
				var p *PanicError
				if !errors.As(err, &p) || p.Value != failure {
					t.Fatal(err)
				}
			} else if stage != "nil" && stage != "invalid" && !errors.Is(err, failure) {
				t.Fatal(err)
			}
			rt.mu.Lock()
			defer rt.mu.Unlock()
			if len(rt.actors) != 0 {
				t.Fatal("Spawn returned before rollback")
			}
		})
	}
}

func TestContextSpawnStartupPublicationRace(t *testing.T) {
	for _, closeRuntime := range []bool{false, true} {
		for range 30 {
			rt := runtimeFor(t)
			started, release := make(chan Addr, 1), make(chan struct{})
			resume := sync.OnceFunc(func() { close(release) })
			t.Cleanup(resume)
			childResult := make(chan spawnOutcome, 1)
			cause, creatorFailure := errors.New("target stop"), errors.New("creator failed after target startup")
			parentResult := make(chan spawnOutcome, 1)
			go func() {
				addr, err := rt.Spawn(bindingsFunc(func() []Binding {
					return []Binding{HandleMessage(func(ctx Context, _ Started) error {
						child, err := ctx.Spawn(bindingsFunc(func() []Binding {
							return []Binding{HandleMessage(func(ctx Context, _ Started) error {
								started <- ctx.actorContext().actor.addr()
								<-release
								return nil
							})}
						}))
						childResult <- spawnOutcome{child, err}
						return creatorFailure
					})}
				}))
				parentResult <- spawnOutcome{addr, err}
			}()
			child := <-started
			var wg sync.WaitGroup
			wg.Go(resume)
			wg.Go(func() {
				if closeRuntime {
					if err := rt.Close(context.Background()); err != nil {
						t.Error(err)
					}
				} else if err := child.Stop(cause); err != nil {
					t.Error(err)
				}
			})
			wg.Wait()
			got, parent := <-childResult, <-parentResult
			if got.err == nil {
				if got.addr != child {
					t.Fatal("success lost instance identity", got)
				}
			} else if got.addr != (Addr{}) || (!errors.Is(got.err, ErrStopped) && !errors.Is(got.err, ErrClosed)) {
				t.Fatal(got)
			}
			if parent.addr != (Addr{}) || !errors.Is(parent.err, creatorFailure) {
				t.Fatal(parent)
			}

			exit := waitExitFor(t, child)
			if !closeRuntime && !errors.Is(exit.Reason, cause) {
				t.Fatal(exit)
			}
			if got.err != nil && got.err != exit.Reason {
				t.Fatal("startup failure returned before final exit", got, exit)
			}
		}
	}
}

type spawnOutcome struct {
	addr Addr
	err  error
}

func pendingSpawn(t *testing.T, result <-chan spawnOutcome) {
	t.Helper()
	select {
	case got := <-result:
		t.Fatal("Spawn returned before startup/rollback completed", got)
	default:
	}
}

func TestContextSpawnStartupAndAdmission(t *testing.T) {
	type startupStream struct{}
	rt := runtimeFor(t)
	release := make(chan struct{})
	releaseAtCleanup(t, release)
	childContext := make(chan Context, 1)
	parentResult := make(chan spawnOutcome, 1)
	var completions, handled atomic.Int32
	go func() {
		addr, err := rt.Spawn(bindingsFunc(func() []Binding {
			var worker Addr
			return []Binding{
				HandleMessage(func(ctx Context, _ Started) error {
					var err error
					worker, err = ctx.Spawn(bindingsFunc(func() []Binding {
						return []Binding{
							HandleMessage(func(ctx Context, _ Started) error {
								_, err := StartTask(ctx, func(context.Context) (int, error) { return 7, nil }, func(int, error) bool {
									completions.Add(1)
									return true
								}, nil)
								childContext <- ctx
								<-release
								return err
							}),
							Handle(func(Context, get) (int, error) {
								handled.Add(1)
								return 11, nil
							}),
							HandleStream(func(Context, startupStream, StreamReply[int]) error {
								handled.Add(1)
								return nil
							}),
						}
					}))
					return err
				}),
				Handle(func(Context, get) (Addr, error) { return worker, nil }),
			}
		}))
		parentResult <- spawnOutcome{addr, err}
	}()
	childCtx := <-childContext
	child := childCtx.actorContext().actor.addr()
	// A completed worker has queued its callback, but Started still owns execution.
	childCtx.actorContext().actor.workers.Wait()
	pendingSpawn(t, parentResult)
	for _, mode := range []mode{sendMode, callMode, streamMode} {
		var msg any = get{}
		if mode == streamMode {
			msg = startupStream{}
		}
		if _, err := child.prepare(context.Background(), msg, mode); !errors.Is(err, ErrStopped) {
			t.Fatal("admitted during Context.Spawn startup", mode, err)
		}
	}

	if completions.Load() != 0 || handled.Load() != 0 {
		t.Fatal("startup allowed business execution or task completion")
	}

	close(release)
	parent := <-parentResult
	if parent.err != nil || parent.addr.rt != rt || child.rt != rt {
		t.Fatal(parent)
	}

	worker, err := parent.addr.Call[Addr](context.Background(), get{})
	if err != nil || worker != child {
		t.Fatal("creator did not retain target address", worker, err)
	}
	if got, err := worker.Call[int](context.Background(), get{}); err != nil || got != 11 || completions.Load() != 1 {
		t.Fatal(got, err, completions.Load())
	}
	if err := parent.addr.Stop(nil); err != nil {
		t.Fatal(err)
	}

	waitExitFor(t, parent.addr)
	if _, err := worker.Call[int](context.Background(), get{}); err != nil {
		t.Fatal("creator stop cascaded to target", err)
	}
}

func TestContextSpawnFailurePropagationAndDegrade(t *testing.T) {
	for _, propagate := range []bool{false, true} {
		for _, panicStart := range []bool{false, true} {
			name := "degrade"
			if propagate {
				name = "propagate"
			}
			if panicStart {
				name += "-panic"
			}

			t.Run(name, func(t *testing.T) {
				rt := runtimeFor(t)
				failure, cleanupFailure := errors.New("target startup"), errors.New("target final handler")
				release, cleaning := make(chan struct{}), make(chan struct{})
				releaseAtCleanup(t, release)
				early := make(chan Addr, 1)
				childResult, parentResult := make(chan spawnOutcome, 1), make(chan spawnOutcome, 1)
				var parentCleanups atomic.Int32
				go func() {
					addr, err := rt.Spawn(bindingsFunc(func() []Binding {
						return []Binding{HandleMessage(func(Context, Stopped) error { parentCleanups.Add(1); return nil }), HandleMessage(func(ctx Context, _ Started) error {
							child, err := ctx.Spawn(bindingsFunc(func() []Binding {
								return []Binding{
									HandleMessage(func(ctx Context, _ Started) error {
										early <- ctx.actorContext().actor.addr()
										if panicStart {
											panic(failure)
										}

										return failure
									}),
									HandleMessage(func(Context, Stopped) error {
										close(cleaning)
										<-release
										return cleanupFailure
									}),
								}
							}))
							childResult <- spawnOutcome{child, err}
							if propagate {
								return err
							}

							return nil
						})}
					}))
					parentResult <- spawnOutcome{addr, err}
				}()
				child := <-early
				await(t, cleaning)
				pendingSpawn(t, childResult)
				pendingSpawn(t, parentResult)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				waiting := beginExitWait(t, child, ctx)
				pendingExitWait(t, waiting)
				if parentCleanups.Load() != 0 {
					t.Fatal("creator cleanup overtook target startup")
				}

				close(release)
				failed, parent := <-childResult, <-parentResult
				if failed.addr != (Addr{}) || failed.err == nil || !errors.Is(failed.err, cleanupFailure) {
					t.Fatal(failed)
				}
				if panicStart {
					var p *PanicError
					if !errors.As(failed.err, &p) || p.Value != failure {
						t.Fatal(failed.err)
					}
				} else if !errors.Is(failed.err, failure) {
					t.Fatal(failed.err)
				}
				if got := <-waiting; got.err != nil || got.exit.Reason != failed.err {
					t.Fatal(got, failed.err)
				}
				if propagate {
					if parent.addr != (Addr{}) || !errors.Is(parent.err, failed.err) || parentCleanups.Load() != 1 {
						t.Fatal(parent)
					}
				} else if parent.err != nil || parent.addr == (Addr{}) || parentCleanups.Load() != 0 {
					t.Fatal(parent)
				}
			})
		}
	}
}

func TestContextSpawnTargetSurvivesCreatorFailure(t *testing.T) {
	rt := runtimeFor(t)
	created := make(chan Addr, 1)
	failure := errors.New("creator startup failed after acquiring dependency")
	parent, err := rt.Spawn(bindingsFunc(func() []Binding {
		return []Binding{HandleMessage(func(ctx Context, _ Started) error {
			child, err := ctx.Spawn(bindingsFunc(func() []Binding {
				return []Binding{Handle(func(Context, get) (int, error) { return 7, nil })}
			}))
			if err != nil {
				return err
			}

			created <- child
			return failure
		})}
	}))
	if parent != (Addr{}) || !errors.Is(err, failure) {
		t.Fatal(parent, err)
	}

	child := <-created
	if got, err := child.Call[int](context.Background(), get{}); err != nil || got != 7 {
		t.Fatal("creator failure cascaded to independent target", got, err)
	}
	if err := child.Stop(nil); err != nil {
		t.Fatal(err)
	}

	waitExitFor(t, child)
}

func TestContextSpawnWaitCancellationAndClose(t *testing.T) {
	for _, action := range []string{"request-cancel", "creator-stop", "runtime-close"} {
		t.Run(action, func(t *testing.T) {
			rt := runtimeFor(t)
			started := make(chan Context, 1)
			release := make(chan struct{})
			releaseAtCleanup(t, release)
			result := make(chan spawnOutcome, 1)
			var handled atomic.Int32
			parent, err := rt.Spawn(bindingsFunc(func() []Binding {
				return []Binding{
					Handle(func(ctx Context, _ wait) (Addr, error) {
						child, err := ctx.Spawn(bindingsFunc(func() []Binding {
							return []Binding{
								HandleMessage(func(ctx Context, _ Started) error {
									started <- ctx
									<-release
									return nil
								}),
								Handle(func(Context, get) (int, error) { return 7, nil }),
							}
						}))
						result <- spawnOutcome{child, err}
						return child, err
					}),
					HandleMessage(func(Context, get) error {
						handled.Add(1)
						return nil
					}),
				}
			}))
			if err != nil {
				t.Fatal(err)
			}

			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()

			callResult := make(chan error, 1)
			go func() {
				_, err := parent.Call[Addr](requestCtx, wait{})
				callResult <- err
			}()
			childCtx := <-started
			child := childCtx.actorContext().actor.addr()
			if err := parent.Send(context.Background(), get{}); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			canceled, cancelWait := context.WithCancel(ctx)
			cancelWait()
			switch action {
			case "request-cancel":
				cancelRequest()
			case "creator-stop":
				if err := parent.Stop(nil); err != nil {
					t.Fatal(err)
				}
			case "runtime-close":
				if err := rt.Close(canceled); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}

			if err := <-callResult; err == nil {
				t.Fatal("pending request survived cancellation/stop")
			}

			pendingSpawn(t, result)
			waiting := beginExitWait(t, parent, ctx)
			pendingExitWait(t, waiting)
			if handled.Load() != 0 {
				t.Fatal("creator reentered while waiting")
			}
			if action == "runtime-close" {
				if childCtx.Err() != context.Canceled {
					t.Fatal("Close missed target")
				}
			} else if childCtx.Err() != nil {
				t.Fatal("creator cancellation stopped target", childCtx.Err())
			}

			close(release)
			got := <-result
			if action == "runtime-close" {
				if got.addr != (Addr{}) || !errors.Is(got.err, ErrClosed) {
					t.Fatal(got)
				}
				if err := rt.Close(ctx); err != nil {
					t.Fatal(err)
				}
				if exit := waitExitFor(t, child); !errors.Is(exit.Reason, ErrClosed) {
					t.Fatal(exit)
				}
			} else {
				if got.err != nil || got.addr != child {
					t.Fatal(got)
				}
				if _, err := child.Call[int](ctx, get{}); err != nil {
					t.Fatal("target did not remain independent", err)
				}
				if action == "request-cancel" {
					if err := parent.Stop(nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			if got := <-waiting; got.err != nil {
				t.Fatal(got)
			}
		})
	}
}

func TestContextSpawnValidationAndCompletion(t *testing.T) {
	var nilContext *localContext
	for _, ctx := range []Context{nilContext, &localContext{}, &localContext{Context: context.Background()}} {
		if addr, err := ctx.Spawn(nil); addr != (Addr{}) || !errors.Is(err, ErrSetup) {
			t.Fatal(addr, err)
		}
	}

	rt := runtimeFor(t)
	result := make(chan spawnOutcome, 1)
	stoppedContext := make(chan Context, 1)
	parent, err := rt.Spawn(bindingsFunc(func() []Binding {
		return []Binding{
			HandleMessage(func(ctx Context, _ Started) error {
				if addr, err := ctx.Spawn(nil); addr != (Addr{}) || !errors.Is(err, ErrSetup) {
					t.Error("invalid setup", addr, err)
				}

				_, err := StartTask(ctx, func(context.Context) (int, error) { return 1, nil }, func(int, error) bool {
					addr, err := ctx.Spawn(bindingsFunc(func() []Binding { return []Binding{} }))
					result <- spawnOutcome{addr, err}
					return true
				}, nil)
				return err
			}),
			HandleMessage(func(ctx Context, _ Stopping) error {
				if addr, err := ctx.Spawn(bindingsFunc(func() []Binding {
					t.Error("setup during stopping")
					return nil
				})); addr != (Addr{}) || !errors.Is(err, ErrStopped) {
					t.Error(addr, err)
				}

				return nil
			}),
			HandleMessage(func(ctx Context, _ Stopped) error {
				stoppedContext <- ctx
				return nil
			}),
		}
	}))
	if err != nil {
		t.Fatal(err)
	}

	child := <-result
	if child.err != nil || child.addr == (Addr{}) || child.addr.rt != rt {
		t.Fatal(child)
	}
	if err := parent.Stop(nil); err != nil {
		t.Fatal(err)
	}

	waitExitFor(t, parent)
	if addr, err := (<-stoppedContext).Spawn(nil); addr != (Addr{}) || !errors.Is(err, ErrStopped) {
		t.Fatal(addr, err)
	}

	// An otherwise usable owner context delegates Runtime closure validation.
	owner := newActor(rt)
	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if addr, err := (&localContext{Context: context.Background(), actor: owner}).Spawn(
		bindingsFunc(func() []Binding { return nil }),
	); addr != (Addr{}) || !errors.Is(err, ErrClosed) {
		t.Fatal(addr, err)
	}

	owner.cancel()
}
