package dist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

// Run against an isolated server configured like examples/nats.conf:
// ACTORX_NATS_URL=nats://127.0.0.1:4222 go test -race -run TestConfiguredLimitsNATS
func TestConfiguredLimitsNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	newRuntime := func(t *testing.T, options ...Option) *Runtime {
		t.Helper()
		options = append(options, WithNATS(url))
		rt, err := NewRuntime(testContext(t), options...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := rt.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		return rt
	}

	t.Run("request admission and recovery", func(t *testing.T) {
		for _, capacity := range []int{1, 129} {
			t.Run(fmt.Sprint(capacity), func(t *testing.T) {
				typ := fmt.Sprintf("limits-%x", time.Now().UnixNano())
				replies := make(chan actorx.Reply[int], capacity)
				newRuntime(t, WithExecutionLimits(ExecutionLimits{Pending: capacity}),
					WithTransportLimits(TransportLimits{Requests: capacity}), WithActor(ActorSpec[*limitProbe]{
						Type: typ, New: func(ActorKey) *limitProbe { return &limitProbe{replies: replies} },
					}))
				client := newRuntime(t, WithTransportLimits(TransportLimits{Requests: capacity}))
				other := newRuntime(t)
				addr, otherAddr := mustAddr(t, client, typ, "one"), mustAddr(t, other, typ, "one")
				ctx := testContext(t)
				done := make(chan error, capacity)
				held := make([]actorx.Reply[int], 0, capacity)
				for range capacity {
					go func() { _, err := addr.Call[int](ctx, limitDeferred{}); done <- err }()
					select {
					case reply := <-replies:
						held = append(held, reply)
					case err := <-done:
						t.Fatal("request refused below configured capacity", len(held), err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if _, err := addr.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrFull) {
					t.Fatal("outgoing Requests exceeded", err)
				}
				if _, err := otherAddr.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrFull) || errors.Is(err, ErrOutcomeUnknown) {
					t.Fatal("incoming Requests did not explicitly reject before admission", err)
				}
				for _, reply := range held {
					reply.Complete(1, nil)
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				}
				if value, err := addr.Call[int](ctx, read{}); value != 1 || err != nil {
					t.Fatal("settlement did not restore request capacity", value, err)
				}
			})
		}
	})

	t.Run("stream admission and callback lifetime", func(t *testing.T) {
		typ := fmt.Sprintf("limits-%x", time.Now().UnixNano())
		producer, consumer := make(chan struct{}), make(chan struct{})
		var pOnce, cOnce sync.Once
		releaseProducer := func() { pOnce.Do(func() { close(producer) }) }
		releaseConsumer := func() { cOnce.Do(func() { close(consumer) }) }
		defer releaseProducer()
		defer releaseConsumer()
		newRuntime(t, WithTransportLimits(TransportLimits{Streams: 1}), WithActor(ActorSpec[*limitProbe]{
			Type: typ, New: func(ActorKey) *limitProbe { return &limitProbe{unblock: producer} },
		}))
		client := newRuntime(t, WithTransportLimits(TransportLimits{Streams: 1}))
		other := newRuntime(t)
		addr, otherAddr := mustAddr(t, client, typ, "one"), mustAddr(t, other, typ, "one")
		ctx := testContext(t)
		entered, done := make(chan struct{}), make(chan error, 1)
		go func() {
			done <- addr.Stream[int](ctx, limitStream{}, func(int) error {
				close(entered)
				<-consumer
				return nil
			})
		}()
		await(t, entered)
		if err := addr.Stream[int](ctx, limitStream{}, func(int) error { return nil }); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("outgoing Streams exceeded", err)
		}
		if err := otherAddr.Stream[int](ctx, limitStream{}, func(int) error { return nil }); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("incoming Streams exceeded", err)
		}
		releaseProducer()
		// Even after producer termination, the active consumer retains capacity.
		if err := addr.Stream[int](ctx, limitStream{}, func(int) error { return nil }); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("active callback released stream capacity", err)
		}
		releaseConsumer()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := addr.Stream[int](ctx, limitStream{}, func(int) error { return nil }); err != nil {
			t.Fatal("settlement did not restore stream capacity", err)
		}
	})

	t.Run("instances above the previous fixed limit", func(t *testing.T) {
		typ := fmt.Sprintf("limits-%x", time.Now().UnixNano())
		newRuntime(t, WithExecutionLimits(ExecutionLimits{Instances: 65}), counterOption(typ))
		client := newRuntime(t)
		for i := range 65 {
			if _, err := mustAddr(t, client, typ, fmt.Sprint(i)).Call[int](testContext(t), read{}); err != nil {
				t.Fatal(i, err)
			}
		}
		if _, err := mustAddr(t, client, typ, "overflow").Call[int](testContext(t), read{}); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("distributed activation exceeded Instances", err)
		}
	})
}
