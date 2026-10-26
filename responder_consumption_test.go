package actorx_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/RussellLuo/actorx"
)

// Exercise the core consumption barrier through a minimal in-memory Channel.
// Backend-specific network credits are tested by the NATS module.
type localConsumingChannel struct{ actorx.Channel }

func (ch localConsumingChannel) Bind(ctx context.Context, id string, r actorx.Receiver) (func() error, error) {
	return ch.Channel.Bind(ctx, id, localConsumingReceiver{r})
}

type localConsumingReceiver struct{ actorx.Receiver }

func (r localConsumingReceiver) Receive(ctx context.Context, delivery actorx.Delivery, reply actorx.Responder) error {
	return r.Receiver.Receive(ctx, delivery, localConsumingReply{reply})
}

type localConsumingReply struct{ actorx.Responder }

func (r localConsumingReply) Write(ctx context.Context, value any, size int) error {
	return r.Responder.WriteConsumed(ctx, value, size)
}

func TestResponderConsumptionBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rt, err := actorx.NewRuntime(ctx, actorx.WithChannel(localConsumingChannel{&seamChannel{}}), actorx.WithCallLimit(1))
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	defer rt.Close(context.Background())

	releaseCallback := sync.OnceFunc(func() { close(release) })
	defer releaseCallback()

	wrote := make(chan error, 1)
	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{
			actorx.HandleStream(func(ctx actorx.Context, _ localChunks, reply actorx.StreamReply[int]) error {
				_, err := actorx.StartTask(ctx,
					func(work context.Context) (struct{}, error) {
						err := reply.Write(work, 7, 1)
						wrote <- err
						return struct{}{}, err
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

	entered := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- a.Stream[int](ctx, localChunks{}, func(value int) error {
			if value != 7 {
				t.Error("bad chunk", value)
			}

			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// The callback is blocked: neither consumption nor stream completion may publish.
	select {
	case err := <-wrote:
		t.Fatal("consumed before callback returned", err)
	case err := <-result:
		t.Fatal("completed during callback", err)
	case <-time.After(20 * time.Millisecond):
	}

	if err := a.Stream[int](ctx, localChunks{}, func(int) error { return nil }); !errors.Is(err, actorx.ErrFull) {
		t.Fatal("active callback lost budget", err)
	}

	// Returning from the callback permits both credit release and final completion.
	releaseCallback()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
