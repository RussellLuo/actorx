package actorx_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RussellLuo/actorx"
)

func TestRuntimeChannelStartup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "running"
		if fail {
			name = "rollback"
		}

		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			ch := &startupChannel{LocalChannel: actorx.NewLocalChannel()}
			rt, err := actorx.NewRuntime(ctx, actorx.WithChannel(ch))
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() {
				if err := rt.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})

			ready, release := make(chan struct{}), make(chan struct{})
			resume := sync.OnceFunc(func() { close(release) })
			defer resume() // Unblock Started before cleanup, including failed checks.

			result := make(chan error, 1)
			successful := make(chan actorx.Addr, 1)
			var hooks, handled atomic.Int32
			failure := errors.New("startup rollback")
			go func() {
				a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
					return []actorx.Binding{
						actorx.HandleMessage(func(*actorx.Context, actorx.Started) error {
							close(ready) // Bind is ready, but admission must remain closed.
							<-release
							if fail {
								return failure
							}

							return nil
						}),
						actorx.Handle(func(*actorx.Context, localQuery) (int, error) {
							handled.Add(1)
							return 7, nil
						}),
						actorx.HandleMessage(func(*actorx.Context, actorx.Stopping) error {
							hooks.Add(1)
							return nil
						}),
						actorx.HandleMessage(func(*actorx.Context, actorx.Stopped) error {
							hooks.Add(1)
							return nil
						}),
					}
				}))
				successful <- a
				result <- err
			}()
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			// Started is still blocked. Even an imported address must reject admission.
			data, err := json.Marshal(struct {
				ID string `json:"id"`
			}{ch.id})
			if err != nil {
				t.Fatal(err)
			}
			addr, err := rt.ImportAddr(data)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := addr.Call[int](ctx, localQuery{}); !errors.Is(err, actorx.ErrStopped) {
				t.Fatal("startup admitted Call", err)
			}
			if err := addr.Send(ctx, localQuery{}); !errors.Is(err, actorx.ErrStopped) {
				t.Fatal("startup admitted Send", err)
			}
			if handled.Load() != 0 {
				t.Fatal("handler ran during Started")
			}

			// Success opens admission; failure must finish rollback before Spawn returns.
			resume()
			select {
			case err := <-result:
				if (fail && !errors.Is(err, failure)) || (!fail && err != nil) {
					t.Fatal("Spawn outcome", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			if !fail {
				if got, err := addr.Call[int](ctx, localQuery{}); got != 7 || err != nil {
					t.Fatal(got, err)
				}

			}
			a := <-successful
			if !fail {
				a.Stop(nil)
				if _, err := a.Wait(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if hooks.Load() != 2 {
				t.Fatal("incomplete lifecycle cleanup", hooks.Load())
			}

			// A later Spawn must not revive the stopped or rolled-back address.
			next, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
				return []actorx.Binding{actorx.Handle(func(*actorx.Context, localQuery) (int, error) { return 11, nil })}
			}))
			if err != nil {
				t.Fatal(err)
			}
			if got, err := next.Call[int](ctx, localQuery{}); got != 11 || err != nil {
				t.Fatal("new instance", got, err)
			}
			if err := addr.Send(ctx, localQuery{}); err == nil {
				t.Fatal("old address revived")
			}
		})
	}
}

// Bind publishes the instance identity to this test before Started runs.
type startupChannel struct {
	*actorx.LocalChannel
	id string
}

func (ch *startupChannel) Bind(ctx context.Context, id string, r actorx.Receiver) (func() error, error) {
	ch.id = id
	return ch.LocalChannel.Bind(ctx, id, r)
}
