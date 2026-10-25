package actorx_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/RussellLuo/actorx"
)

func TestRuntimeChannelAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rt, err := actorx.NewRuntime(ctx, actorx.WithMailboxCapacity(1), actorx.WithCallLimit(1))
	if err != nil {
		t.Fatal(err)
	}

	defer rt.Close(context.Background())

	entered, release, noticed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseHandler := sync.OnceFunc(func() { close(release) })
	defer releaseHandler()

	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{
			actorx.Handle(func(*actorx.Context, localQuery) (int, error) {
				close(entered)
				<-release // Deliberately hold the handler after caller cancellation.
				return 17, nil
			}),
			actorx.HandleMessage(func(*actorx.Context, localNotice) error {
				close(noticed)
				return nil
			}),
			actorx.Handle(func(*actorx.Context, localDeferred) (int, error) { return 23, nil }),
		}
	}))
	if err != nil {
		t.Fatal(err)
	}

	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()

	result := make(chan error, 1)
	go func() {
		_, err := a.Call[int](callCtx, localQuery{})
		result <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	cancelCall()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("caller cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// Caller cancellation frees the Call budget while its handler stays blocked.
	// Send can fill the single queue slot, but cannot exceed mailbox capacity.
	admissionCtx, cancelAdmission := context.WithCancel(ctx)
	defer cancelAdmission()

	if err := a.Send(admissionCtx, localNotice{}); err != nil {
		t.Fatal("mailbox admission", err)
	}

	cancelAdmission() // An accepted Send must survive cancellation of admission.
	if err := a.Send(ctx, localNotice{}); !errors.Is(err, actorx.ErrFull) {
		t.Fatal("mailbox overflow accepted", err)
	}

	releaseHandler()
	select {
	case <-noticed:
	case <-ctx.Done():
		t.Fatal("accepted Send was cancelled", ctx.Err())
	}

	if got, err := a.Call[int](ctx, localDeferred{}); got != 23 || err != nil {
		t.Fatal("caller capacity did not recover", got, err)
	}

	a.Stop(nil)
	if _, err := a.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
