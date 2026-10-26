package actorx_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

// This probe deliberately lives outside actorx and imports no integration layer.
// Embedding the public interface must preserve its private execution method.
type capabilityContext struct {
	actorx.Context
	token *int
}

func (c *capabilityContext) Token() *int { return c.token }

type executionProbe struct {
	bindings bindingsFunc
	wrap     actorx.ContextWrapper
}

func (a *executionProbe) Bindings() []actorx.Binding { return a.bindings() }

// The configured policy recognizes a private adapter; business actors only
// implement Bindings. Each wrapper is fixed once at instance admission.
func probeContextFactory(value actorx.Actor) (actorx.ContextWrapper, error) {
	if a, ok := value.(*executionProbe); ok {
		return a.wrap, nil
	}

	return func(c actorx.Context) actorx.Context { return c }, nil
}

func coreRuntime(t *testing.T, opts ...actorx.Option) *actorx.Runtime {
	t.Helper()

	rt, err := actorx.NewRuntime(context.Background(),
		append([]actorx.Option{actorx.WithContextFactory(probeContextFactory)}, opts...)...,
	)
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

func capabilityCheck(ctx actorx.Context, token *int) error {
	for _, c := range []actorx.Context{ctx, ctx.WithContext(context.Background()).WithContext(context.Background())} {
		if ext, ok := c.(interface{ Token() *int }); !ok || ext.Token() != token {
			return errors.New("capability or instance changed")
		}
		if c.State() != ctx.State() {
			return errors.New("derived context changed actor")
		}
	}

	return nil
}

type contextKey struct{}

type contextQuery struct{ value string }

func TestExternalContextAcrossRuntimesInstancesAndRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for range 2 {
		rt := coreRuntime(t)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				token := new(int)
				events := make(chan string, 4)
				check := func(ctx actorx.Context, phase string) error {
					if err := capabilityCheck(ctx, token); err != nil {
						return err
					}

					events <- phase
					return nil
				}
				a := &executionProbe{
					wrap: func(ctx actorx.Context) actorx.Context {
						ctx.State() // Reenters the actor lock: wrapping must run unlocked.
						return &capabilityContext{Context: ctx, token: token}
					},
					bindings: func() []actorx.Binding {
						return []actorx.Binding{
							actorx.HandleMessage(func(c actorx.Context, _ actorx.Started) error {
								if err := check(c, "started"); err != nil {
									return err
								}

								derived := c.WithContext(context.Background()).WithContext(context.Background())
								_, err := actorx.StartTask(derived,
									func(context.Context) (int, error) { return 7, nil },
									func(n int, err error) bool {
										if n != 7 || err != nil {
											t.Error(n, err)
										}
										if err := check(derived, "complete"); err != nil {
											t.Error(err)
										}

										return true
									},
									nil,
								)
								return err
							}),
							actorx.Handle(func(c actorx.Context, q contextQuery) (*int, error) {
								if c.Value(contextKey{}) != q.value {
									return nil, errors.New("request values mixed")
								}

								return token, capabilityCheck(c, token)
							}),
							actorx.HandleMessage(func(c actorx.Context, _ actorx.Stopping) error { return check(c, "stopping") }),
							actorx.HandleMessage(func(c actorx.Context, _ actorx.Stopped) error { return check(c, "stopped") }),
						}
					},
				}
				addr, err := rt.Spawn(a)
				if err != nil {
					t.Error(err)
					return
				}

				// Concurrent requests must retain their own values and the same instance token.
				var requests sync.WaitGroup
				for i := range 4 {
					requests.Add(1)
					go func() {
						defer requests.Done()

						value := fmt.Sprint(i)
						if got, err := addr.Call[*int](context.WithValue(ctx, contextKey{}, value), contextQuery{value}); err != nil || got != token {
							t.Error(got, err)
						}
					}()
				}

				requests.Wait()

				for _, want := range []string{"started", "complete"} {
					select {
					case got := <-events:
						if got != want {
							t.Error(got, want)
						}
					case <-ctx.Done():
						t.Error(ctx.Err())
						return
					}
				}

				// Complete startup work before checking the shutdown lifecycle sequence.
				_ = addr.Stop(nil)
				if exit, err := addr.Wait(ctx); err != nil || exit.Reason != nil {
					t.Error(exit, err)
				}

				for _, want := range []string{"stopping", "stopped"} {
					select {
					case got := <-events:
						if got != want {
							t.Error(got, want)
						}
					case <-ctx.Done():
						t.Error(ctx.Err())
						return
					}
				}
			}()
		}
	}

	wg.Wait()
}

func captureCoreContext(t *testing.T) actorx.Context {
	t.Helper()

	rt := coreRuntime(t)
	addr, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{actorx.Handle(func(c actorx.Context, _ contextQuery) (actorx.Context, error) { return c, nil })}
	}))
	if err != nil {
		t.Fatal(err)
	}

	c, err := addr.Call[actorx.Context](context.Background(), contextQuery{})
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func TestExternalContextWrapperFailures(t *testing.T) {
	foreign := captureCoreContext(t)
	invalid := []struct {
		name       string
		wrap       func(actorx.Context, actorx.Context) actorx.Context
		panicValue bool
	}{
		{"nil", func(_, _ actorx.Context) actorx.Context { return nil }, false},
		{"typed-nil", func(_, _ actorx.Context) actorx.Context { return (*capabilityContext)(nil) }, false},
		{"empty-embedded", func(_, _ actorx.Context) actorx.Context { return &capabilityContext{} }, false},
		{"typed-nil-embedded", func(_, _ actorx.Context) actorx.Context {
			return &capabilityContext{Context: (*capabilityContext)(nil)}
		}, false},
		{"other-runtime", func(_, _ actorx.Context) actorx.Context { return foreign }, false},
		{"old-context", func(_, old actorx.Context) actorx.Context { return old }, false},
		{"panic", func(_, _ actorx.Context) actorx.Context { panic("wrapper panic") }, true},
	}
	for _, tc := range invalid {
		for _, phase := range []string{"started", "handler", "derived", "stopping", "stopped"} {
			t.Run(tc.name+"/"+phase, func(t *testing.T) {
				rt := coreRuntime(t, actorx.WithInstanceLimit(1))

				// Count wrapper calls, including WithContext, to fail at one precise boundary.
				failAt := map[string]int{"started": 1, "handler": 2, "derived": 3, "stopping": 2, "stopped": 3}[phase]
				calls := 0
				var old actorx.Context
				invoked := map[string]int{}
				a := &executionProbe{
					wrap: func(c actorx.Context) actorx.Context {
						calls++
						if calls == failAt {
							return tc.wrap(c, old)
						}
						if old == nil {
							old = c
						}

						return &capabilityContext{Context: c}
					},
					bindings: func() []actorx.Binding {
						return []actorx.Binding{
							actorx.HandleMessage(func(actorx.Context, actorx.Started) error {
								invoked["started"]++
								return nil
							}),
							actorx.Handle(func(c actorx.Context, _ contextQuery) (int, error) {
								invoked["handler"]++
								if phase == "derived" {
									c.WithContext(context.Background())
									t.Error("derived failure returned")
								}

								return 1, nil
							}),
							actorx.HandleMessage(func(actorx.Context, actorx.Stopping) error {
								invoked["stopping"]++
								return nil
							}),
							actorx.HandleMessage(func(actorx.Context, actorx.Stopped) error {
								invoked["stopped"]++
								return nil
							}),
						}
					},
				}
				addr, spawnErr := rt.Spawn(a)
				err := spawnErr
				if phase == "started" {
					if !addr.IsZero() {
						t.Fatal("failed Spawn returned address")
					}
				} else {
					if spawnErr != nil {
						t.Fatal(spawnErr)
					}
					if phase == "handler" || phase == "derived" {
						if _, callErr := addr.Call[int](context.Background(), contextQuery{}); callErr == nil {
							t.Error("failed request succeeded")
						}
					} else {
						_ = addr.Stop(nil)
					}

					exit, waitErr := addr.Wait(context.Background())
					if waitErr != nil {
						t.Fatal(waitErr)
					}

					err = exit.Reason
				}
				if phase == "derived" {
					var p *actorx.PanicError
					if !errors.As(err, &p) {
						t.Fatal("missing handler panic", err)
					}

					var ok bool
					err, ok = p.Value.(error)
					if !ok {
						t.Fatal("panic did not carry setup error", p.Value)
					}
				}
				if !errors.Is(err, actorx.ErrSetup) {
					t.Fatal("missing setup error", err)
				}
				if tc.panicValue {
					var p *actorx.PanicError
					if !errors.As(err, &p) || p.Value != "wrapper panic" || len(p.Stack) == 0 {
						t.Fatal("panic diagnostic lost", err)
					}
				}

				for _, stage := range []string{"started", "handler", "stopping", "stopped"} {
					want := 1
					if stage == phase || (stage == "handler" && phase != "derived") {
						want = 0
					}
					if invoked[stage] != want {
						t.Errorf("%s invoked %d times, want %d", stage, invoked[stage], want)
					}
				}

				// Full exit, including failed startup rollback, must return the slot.
				if _, err := rt.Spawn(bindingsFunc(func() []actorx.Binding { return nil })); err != nil {
					t.Fatal("budget leaked", err)
				}
			})
		}
	}
}

func TestExternalStartTaskRejectsInvalidOwnershipBeforeReservation(t *testing.T) {
	rt := coreRuntime(t, actorx.WithTaskLimit(1), actorx.WithMailboxCapacity(1))
	valid := func(c actorx.Context) actorx.Context { return &capabilityContext{Context: c} }
	done := make(chan struct{})
	addr, err := rt.Spawn(&executionProbe{
		wrap: valid,
		bindings: func() []actorx.Binding {
			return []actorx.Binding{
				actorx.HandleMessage(func(c actorx.Context, _ actorx.Started) error {
					for _, bad := range []actorx.Context{
						nil,
						(*capabilityContext)(nil),
						&capabilityContext{},
						&capabilityContext{Context: (*capabilityContext)(nil)},
					} {
						cancel, err := actorx.StartTask(bad,
							func(context.Context) (int, error) {
								t.Error("invalid worker started")
								return 0, nil
							},
							func(int, error) bool {
								t.Error("invalid completion ran")
								return true
							},
							nil,
						)
						if cancel != nil || !errors.Is(err, actorx.ErrSetup) {
							t.Error(cancel, err)
						}
					}

					_, err := actorx.StartTask(c,
						func(context.Context) (int, error) { return 1, nil },
						func(int, error) bool {
							close(done)
							return true
						},
						nil,
					)
					return err
				}),
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("valid task did not complete after invalid inputs")
	}

	_ = addr.Stop(nil)
}

func TestExternalContextDerivedParentControlsTask(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			rt := coreRuntime(t)
			token := new(int)
			addr, err := rt.Spawn(&executionProbe{
				wrap: func(c actorx.Context) actorx.Context { return &capabilityContext{Context: c, token: token} },
				bindings: func() []actorx.Binding {
					return []actorx.Binding{
						actorx.HandleDeferred(func(c actorx.Context, q contextQuery, reply actorx.Reply[bool]) error {
							deadline := time.Now().Add(time.Second)
							if mode == "deadline" {
								deadline = time.Now().Add(20 * time.Millisecond)
							}

							parent, cancel := context.WithDeadline(context.WithValue(context.Background(), contextKey{}, q.value), deadline)
							derived := c.WithContext(context.Background()).WithContext(parent)
							_, err := actorx.StartTask(derived,
								func(work context.Context) (bool, error) {
									got, ok := work.Deadline()
									matches := ok && got.Equal(deadline) && work.Value(contextKey{}) == q.value
									<-work.Done()
									return matches, work.Err()
								},
								func(matches bool, err error) bool {
									defer cancel()
									want := context.Canceled
									if mode == "deadline" {
										want = context.DeadlineExceeded
									}

									reply.Complete(matches && errors.Is(err, want), capabilityCheck(derived, token))
									return true
								},
								nil,
							)
							if mode == "cancel" || err != nil {
								cancel()
							}

							return err
						}),
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if ok, err := addr.Call[bool](ctx, contextQuery{"new parent"}); !ok || err != nil {
				t.Fatal(ok, err)
			}
		})
	}
}

func TestWrapperFailureDoesNotReleaseBlockedWorkerBudget(t *testing.T) {
	rt := coreRuntime(t, actorx.WithInstanceLimit(1))
	unblock, working, failed, discarded := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(release)

	calls, stopping, stopped := 0, 0, 0
	addr, err := rt.Spawn(&executionProbe{
		wrap: func(c actorx.Context) actorx.Context {
			calls++
			if calls == 2 {
				// Reject Stopping so its handler cannot release the blocked worker.
				close(failed)
				return nil
			}

			return &capabilityContext{Context: c}
		},
		bindings: func() []actorx.Binding {
			return []actorx.Binding{
				actorx.HandleMessage(func(c actorx.Context, _ actorx.Started) error {
					_, err := actorx.StartTask(c,
						func(context.Context) (int, error) {
							close(working)
							<-unblock
							return 0, nil
						},
						func(int, error) bool {
							t.Error("completion ran after Stop")
							return true
						},
						func(int) { close(discarded) },
					)
					return err
				}),
				actorx.HandleMessage(func(actorx.Context, actorx.Stopping) error {
					stopping++
					release()
					return nil
				}),
				actorx.HandleMessage(func(actorx.Context, actorx.Stopped) error {
					stopped++
					return nil
				}),
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	<-working
	_ = addr.Stop(nil)
	<-failed

	// A failed Stopping wrapper must leave Wait and the instance budget pending.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := addr.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Wait did not join worker", err)
	}

	reads := 0
	fresh := bindingsFunc(func() []actorx.Binding {
		reads++
		return nil
	})
	if addr, err := rt.Spawn(fresh); !addr.IsZero() || !errors.Is(err, actorx.ErrFull) || reads != 0 {
		t.Fatal(addr, err, reads)
	}

	// Only worker settlement and discard may return the instance slot.
	release()
	if exit, err := addr.Wait(context.Background()); err != nil || !errors.Is(exit.Reason, actorx.ErrSetup) {
		t.Fatal(exit, err)
	}

	select {
	case <-discarded:
	default:
		t.Error("exit published before discard")
	}

	if stopping != 0 || stopped != 1 {
		t.Fatal(stopping, stopped)
	}
	if _, err := rt.Spawn(fresh); err != nil || reads != 1 {
		t.Fatal("slot not returned after complete exit", reads, err)
	}
}

func TestExternalRejectsRetainedRequestContext(t *testing.T) {
	for _, source := range []string{"same-instance", "other-instance"} {
		t.Run(source, func(t *testing.T) {
			rt := coreRuntime(t)
			var retained actorx.Context
			if source == "other-instance" {
				addr, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
					return []actorx.Binding{actorx.Handle(func(c actorx.Context, _ contextQuery) (actorx.Context, error) { return c, nil })}
				}))
				if err != nil {
					t.Fatal(err)
				}

				retained, err = addr.Call[actorx.Context](context.Background(), contextQuery{})
				if err != nil {
					t.Fatal(err)
				}
			}

			invoked := 0
			addr, err := rt.Spawn(&executionProbe{
				wrap: func(c actorx.Context) actorx.Context {
					if retained != nil {
						return retained
					}

					retained = c
					return &capabilityContext{Context: c}
				},
				bindings: func() []actorx.Binding {
					return []actorx.Binding{
						actorx.Handle(func(actorx.Context, contextQuery) (int, error) {
							invoked++
							return 1, nil
						}),
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if source == "same-instance" {
				if _, err := addr.Call[int](context.Background(), contextQuery{}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := addr.Call[int](context.Background(), contextQuery{}); !errors.Is(err, actorx.ErrSetup) {
				t.Fatal("retained request accepted", err)
			}

			exit, err := addr.Wait(context.Background())
			if err != nil || !errors.Is(exit.Reason, actorx.ErrSetup) {
				t.Fatal(exit, err)
			}

			want := 0
			if source == "same-instance" {
				want = 1
			}
			if invoked != want {
				t.Fatal("invalid request reached handler", invoked, want)
			}
		})
	}
}
