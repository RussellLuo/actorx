package actorx_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

// Keep channel admission visible independently of lifecycle invocation.
type factoryProbeChannel struct {
	*actorx.LocalChannel
	binds atomic.Int32
}

func (ch *factoryProbeChannel) Bind(ctx context.Context, id string, receiver actorx.Receiver) (func() error, error) {
	ch.binds.Add(1)
	return ch.LocalChannel.Bind(ctx, id, receiver)
}

func identityContext(c actorx.Context) actorx.Context { return c }

func TestContextFactoryOptionValidationAndOverride(t *testing.T) {
	for _, order := range []string{"nil", "valid-nil", "nil-valid", "first-last"} {
		t.Run(order, func(t *testing.T) {
			firstCalls, lastCalls := 0, 0
			first := actorx.WithContextFactory(func(actorx.Actor) (actorx.ContextWrapper, error) {
				firstCalls++
				return identityContext, nil
			})
			last := actorx.WithContextFactory(func(actorx.Actor) (actorx.ContextWrapper, error) {
				lastCalls++
				return identityContext, nil
			})
			nilFactory := actorx.WithContextFactory(nil)
			opts := map[string][]actorx.Option{
				"nil":        {nilFactory},
				"valid-nil":  {first, nilFactory},
				"nil-valid":  {nilFactory, last},
				"first-last": {first, last},
			}[order]
			ch := &seamChannel{}
			rt, err := actorx.NewRuntime(context.Background(), append(opts, actorx.WithChannel(ch))...)
			if order == "nil" || order == "valid-nil" {
				if rt != nil || !errors.Is(err, actorx.ErrSetup) || ch.opened {
					t.Fatal(rt, err, ch.opened)
				}

				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Close(context.Background())

			if firstCalls != 0 || lastCalls != 0 {
				t.Fatal("NewRuntime called factory")
			}
			if _, err := rt.Spawn(bindingsFunc(func() []actorx.Binding { return nil })); err != nil {
				t.Fatal(err)
			}
			if firstCalls != 0 || lastCalls != 1 {
				t.Fatal("factory options did not replace", firstCalls, lastCalls)
			}
		})
	}

	// An unconfigured Runtime preserves the plain core implementation.
	rt, err := actorx.NewRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close(context.Background())

	addr, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{
			actorx.Handle(func(c actorx.Context, _ contextQuery) (bool, error) {
				_, extended := c.WithContext(context.Background()).(*capabilityContext)
				return extended, nil
			}),
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	if extended, err := addr.Call[bool](context.Background(), contextQuery{}); extended || err != nil {
		t.Fatal(extended, err)
	}
}

func TestContextFactoryRunsOnlyAfterValidBindingsAndAdmission(t *testing.T) {
	calls := 0
	ch := &factoryProbeChannel{LocalChannel: actorx.NewLocalChannel()}
	var rt *actorx.Runtime
	rt = coreRuntime(t,
		actorx.WithInstanceLimit(1),
		actorx.WithChannel(ch),
		actorx.WithContextFactory(func(actorx.Actor) (actorx.ContextWrapper, error) {
			// Reenter Runtime admission: neither its mutex nor an actor lock is held.
			if _, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
				t.Error("full Spawn read Bindings")
				return nil
			})); !errors.Is(err, actorx.ErrFull) {
				t.Error(err)
			}

			calls++
			return identityContext, nil
		}),
	)
	valid := bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{actorx.HandleMessage(func(actorx.Context, contextQuery) error { return nil })}
	})
	if _, err := actorx.Describe(valid.Bindings()); err != nil || calls != 0 {
		t.Fatal("Describe called factory", calls, err)
	}

	for _, bad := range []actorx.Actor{
		nil,
		bindingsFunc(func() []actorx.Binding { return []actorx.Binding{{}} }),
		bindingsFunc(func() []actorx.Binding { panic("invalid bindings") }),
	} {
		if addr, err := rt.Spawn(bad); !addr.IsZero() || !errors.Is(err, actorx.ErrSetup) {
			t.Fatal(addr, err)
		}
	}

	if calls != 0 || ch.binds.Load() != 0 {
		t.Fatal("factory or channel ran for invalid Bindings", calls, ch.binds.Load())
	}

	addr, err := rt.Spawn(valid)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || ch.binds.Load() != 1 {
		t.Fatal(calls, ch.binds.Load())
	}
	if _, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		t.Error("full Spawn read Bindings")
		return nil
	})); !errors.Is(err, actorx.ErrFull) {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("full Spawn called factory", calls)
	}

	_ = addr.Stop(nil)
	if _, err := addr.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestContextFactoryFailureRollsBackBeforeChannelAndLifecycle(t *testing.T) {
	failure := errors.New("factory failed")
	for _, mode := range []string{"error", "panic", "nil-wrapper", "wrapper-and-error"} {
		t.Run(mode, func(t *testing.T) {
			ch := &factoryProbeChannel{LocalChannel: actorx.NewLocalChannel()}
			var lifecycle atomic.Int32
			value := bindingsFunc(func() []actorx.Binding {
				return []actorx.Binding{
					actorx.HandleMessage(func(actorx.Context, actorx.Started) error {
						lifecycle.Add(1)
						return nil
					}),
					actorx.HandleMessage(func(actorx.Context, actorx.Stopping) error {
						lifecycle.Add(1)
						return nil
					}),
					actorx.HandleMessage(func(actorx.Context, actorx.Stopped) error {
						lifecycle.Add(1)
						return nil
					}),
				}
			})
			calls := 0
			rt := coreRuntime(t,
				actorx.WithInstanceLimit(1),
				actorx.WithChannel(ch),
				actorx.WithContextFactory(func(actorx.Actor) (actorx.ContextWrapper, error) {
					calls++
					if calls != 1 {
						return identityContext, nil
					}

					switch mode {
					case "error":
						return nil, failure
					case "panic":
						panic("factory panic")
					case "wrapper-and-error":
						return func(c actorx.Context) actorx.Context {
							t.Error("failed factory wrapper ran")
							return c
						}, failure
					default:
						return nil, nil
					}
				}),
			)
			addr, err := rt.Spawn(value)
			if !addr.IsZero() || !errors.Is(err, actorx.ErrSetup) {
				t.Fatal(addr, err)
			}
			if mode == "error" || mode == "wrapper-and-error" {
				if !errors.Is(err, failure) {
					t.Fatal("factory error chain lost", err)
				}
			}
			if mode == "panic" {
				var p *actorx.PanicError
				if !errors.As(err, &p) || p.Value != "factory panic" || len(p.Stack) == 0 {
					t.Fatal("factory panic diagnostic lost", err)
				}
			}
			if lifecycle.Load() != 0 || ch.binds.Load() != 0 || calls != 1 {
				t.Fatal("failed binding entered lifecycle/channel", lifecycle.Load(), ch.binds.Load(), calls)
			}
			if _, err := rt.Spawn(value); err != nil {
				t.Fatal("factory rollback leaked instance slot", err)
			}
			if calls != 2 || ch.binds.Load() != 1 || lifecycle.Load() != 1 {
				t.Fatal(calls, ch.binds.Load(), lifecycle.Load())
			}
		})
	}
}

func TestContextFactoryBlockedDuringClose(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()

	var calls, wrappers, hooks atomic.Int32
	rt := coreRuntime(t,
		actorx.WithInstanceLimit(1),
		actorx.WithContextFactory(func(actorx.Actor) (actorx.ContextWrapper, error) {
			calls.Add(1)
			close(entered)
			<-unblock
			return func(c actorx.Context) actorx.Context {
				wrappers.Add(1)
				return c
			}, nil
		}),
	)
	type result struct {
		addr actorx.Addr
		err  error
	}
	finished := make(chan result, 1)
	go func() {
		addr, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
			return []actorx.Binding{
				actorx.HandleMessage(func(actorx.Context, actorx.Started) error {
					hooks.Add(1)
					return nil
				}),
				actorx.HandleMessage(func(actorx.Context, actorx.Stopping) error {
					hooks.Add(1)
					return nil
				}),
				actorx.HandleMessage(func(actorx.Context, actorx.Stopped) error {
					hooks.Add(1)
					return nil
				}),
			}
		}))
		finished <- result{addr, err}
	}()
	waitFor(t, entered)

	// The blocked factory still owns its instance slot, and Close must join it.
	if _, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		t.Error("blocked factory released its instance slot")
		return nil
	})); !errors.Is(err, actorx.ErrFull) {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := rt.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Close did not join factory", err)
	}

	select {
	case got := <-finished:
		t.Fatal("Spawn ended before factory returned", got)
	default:
	}

	// Close has timed out; let construction return and observe complete rollback.
	release()
	select {
	case got := <-finished:
		if !got.addr.IsZero() || !errors.Is(got.err, actorx.ErrClosed) {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Spawn did not roll back after factory returned")
	}

	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || wrappers.Load() != 0 || hooks.Load() != 0 {
		t.Fatal("closed factory entered execution", calls.Load(), wrappers.Load(), hooks.Load())
	}
}

type familyContext struct {
	actorx.Context
	runtimeToken *int
	instance     *familyProbe
}

type familyProbe struct{ id int } // Immutable factory input, independent of business state.

type familyQuery struct {
	remaining int
	token     *int
}

func (a *familyProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{
		actorx.Handle(a.descend),
		actorx.HandleMessage(func(c actorx.Context, _ actorx.Started) error { return a.check(c) }),
		actorx.HandleMessage(func(c actorx.Context, _ actorx.Stopping) error { return a.check(c) }),
		actorx.HandleMessage(func(c actorx.Context, _ actorx.Stopped) error { return a.check(c) }),
	}
}

func (a *familyProbe) check(c actorx.Context) error {
	ext, ok := c.(*familyContext)
	if !ok || ext.instance != a {
		return errors.New("child inherited another instance's wrapper")
	}

	for range 2 {
		c = c.WithContext(context.Background())
		derived, ok := c.(*familyContext)
		if !ok || derived.runtimeToken != ext.runtimeToken || derived.instance != a || c.State() != ext.State() {
			return errors.New("derived family environment changed")
		}
	}

	return nil
}

func (a *familyProbe) descend(c actorx.Context, q familyQuery) ([]int, error) {
	if err := a.check(c); err != nil {
		return nil, err
	}
	if c.(*familyContext).runtimeToken != q.token {
		return nil, errors.New("Runtime factory environment changed")
	}

	ids := []int{a.id}
	if q.remaining == 0 {
		return ids, nil
	}

	child, err := c.Spawn(&familyProbe{id: a.id + 1})
	if err != nil {
		return nil, err
	}

	defer child.Stop(nil)
	more, callErr := child.Call[[]int](c, familyQuery{remaining: q.remaining - 1, token: q.token})
	_ = child.Stop(nil)
	exit, waitErr := child.Wait(c)
	return append(ids, more...), errors.Join(callErr, waitErr, exit.Reason)
}

func TestContextFactoryBindsChildrenAndGrandchildrenOnce(t *testing.T) {
	for range 2 {
		token := new(int)
		var mu sync.Mutex
		bound := map[*familyProbe]int{}
		rt := coreRuntime(t, actorx.WithContextFactory(func(value actorx.Actor) (actorx.ContextWrapper, error) {
			instance, ok := value.(*familyProbe)
			if !ok {
				return nil, errors.New("factory received wrong Actor")
			}

			mu.Lock()
			bound[instance]++
			mu.Unlock()
			return func(c actorx.Context) actorx.Context {
				if _, alreadyWrapped := c.(*familyContext); alreadyWrapped {
					t.Error("WithContext stacked an old wrapper")
				}

				c.State() // The wrapper may safely acquire the actor lock.
				return &familyContext{Context: c, runtimeToken: token, instance: instance}
			}, nil
		}))
		root := &familyProbe{id: 10}
		addr, err := rt.Spawn(root)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		got, callErr := addr.Call[[]int](ctx, familyQuery{remaining: 2, token: token})
		_ = addr.Stop(nil)
		exit, waitErr := addr.Wait(ctx)
		cancel()
		if callErr != nil || waitErr != nil || exit.Reason != nil || !reflect.DeepEqual(got, []int{10, 11, 12}) {
			t.Fatal(got, callErr, waitErr, exit)
		}

		mu.Lock()
		if len(bound) != 3 {
			t.Error("wrong number of instance bindings", len(bound))
		}

		for instance, calls := range bound {
			if calls != 1 {
				t.Error("factory rebound on invocation or WithContext", instance.id, calls)
			}
		}

		mu.Unlock()
	}
}
