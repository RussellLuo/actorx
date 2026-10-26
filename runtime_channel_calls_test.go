package actorx_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

type localNotice struct{}

type localDeferred struct{}

type localChunks struct{}

type localUnknown struct{}

func TestRuntimeChannelCalls(t *testing.T) {
	for _, name := range []string{"Local", "External"} {
		t.Run(name, func(t *testing.T) {
			var ch actorx.Channel = actorx.NewLocalChannel()
			if name == "External" {
				ch = &seamChannel{}
			}
			testRuntimeChannelCalls(t, ch)
		})
	}
}

func testRuntimeChannelCalls(t *testing.T, ch actorx.Channel) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rt, err := actorx.NewRuntime(ctx, actorx.WithChannel(ch))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := rt.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})

	var handled atomic.Int32
	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{
			actorx.Handle(func(actorx.Context, localQuery) (int, error) {
				handled.Add(1)
				return 17, nil
			}),
			actorx.HandleMessage(func(actorx.Context, localNotice) error {
				handled.Add(1)
				return nil
			}),
			actorx.HandleDeferred(func(ctx actorx.Context, _ localDeferred, reply actorx.Reply[int]) error {
				_, err := actorx.StartTask(ctx,
					func(context.Context) (int, error) { return 23, nil },
					func(value int, err error) bool { return reply.Complete(value, err) },
					nil,
				)
				return err
			}),
			actorx.HandleStream(func(ctx actorx.Context, _ localChunks, reply actorx.StreamReply[int]) error {
				_, err := actorx.StartTask(ctx,
					func(work context.Context) (struct{}, error) {
						for i := range 3 {
							if err := reply.Write(work, i, 1); err != nil {
								return struct{}{}, err
							}
						}

						return struct{}{}, nil
					},
					func(_ struct{}, err error) bool { return reply.Complete(err) },
					nil,
				)
				return err
			}),
		}
	}))
	if err != nil {
		t.Fatal(err)
	}

	addr := a
	for _, invoke := range []func() error{
		func() error { return addr.Stream[int](ctx, localQuery{}, func(int) error { return nil }) },
		func() error { return addr.Send(ctx, localChunks{}) },
		func() error {
			_, err := addr.Call[int](ctx, localChunks{})
			return err
		},
	} {
		if err := invoke(); !errors.Is(err, actorx.ErrMode) {
			t.Fatal("mode", err)
		}
	}

	if handled.Load() != 0 {
		t.Fatal("rejected mode invoked handler")
	}
	if _, err := addr.Call[int](ctx, localUnknown{}); !errors.Is(err, actorx.ErrUnhandled) {
		t.Fatal("unknown type", err)
	}
	if got, err := addr.Call[int](ctx, localQuery{}); got != 17 || err != nil {
		t.Fatal(got, err)
	}
	if err := addr.Send(ctx, localQuery{}); err != nil {
		t.Fatal(err)
	}

	// A later Call is a serial mailbox barrier: the admitted Send must have run.
	if _, err := addr.Call[int](ctx, localQuery{}); err != nil {
		t.Fatal(err)
	}

	if got := handled.Load(); got != 3 {
		t.Fatal("Send/Call acceptance", got)
	}
	if err := addr.Send(ctx, localNotice{}); err != nil {
		t.Fatal(err)
	}
	if value, err := addr.Call[any](ctx, localNotice{}); value != nil || err != nil {
		t.Fatal(value, err)
	}
	if got, err := addr.Call[int](ctx, localDeferred{}); got != 23 || err != nil {
		t.Fatal("deferred", got, err)
	}

	// Stream returns only after all ordered callbacks and the final result.
	next := 0
	if err := addr.Stream[int](ctx, localChunks{}, func(value int) error {
		if value != next {
			return errors.New("out of order")
		}

		next++
		return nil
	}); err != nil || next != 3 {
		t.Fatal("stream completion", next, err)
	}

	failure := errors.New("consumer failure")
	if err := addr.Stream[int](ctx, localChunks{}, func(int) error { return failure }); !errors.Is(err, failure) {
		t.Fatal("callback error", err)
	}

	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := addr.Call[int](cancelled, localQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	a.Stop(nil)
	if _, err := a.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
