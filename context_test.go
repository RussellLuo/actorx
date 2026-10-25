package actorx

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHandlerContextPreservesRequestDeadlineAndValues(t *testing.T) {
	type key struct{}
	type result struct {
		Value    any
		Deadline time.Time
	}

	rt := runtimeFor(t)
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleDeferred(func(ctx *Context, _ get, reply Reply[result]) error {
			// Standard context helpers and SDKs accept the handler context directly.
			parent, cancel := context.WithCancel(ctx)
			_, err := ctx.WithContext(parent).StartTask(
				func(ctx context.Context) (result, error) {
					defer cancel()

					deadline, _ := ctx.Deadline()
					return result{Value: ctx.Value(key{}), Deadline: deadline}, nil
				},
				func(value result, err error) bool {
					reply.Complete(value, err)
					return true
				},
				nil,
			)
			if err != nil {
				cancel()
			}

			return err
		})}
	})

	parent := context.WithValue(context.Background(), key{}, "request value")
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()

	wantDeadline, _ := ctx.Deadline()

	value, err := a.addr().Call[result](ctx, get{})
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != "request value" || !value.Deadline.Equal(wantDeadline) {
		t.Fatalf("request context lost: %+v", value)
	}
}

func TestOwnedTaskObservesRequestCancellation(t *testing.T) {
	rt := runtimeFor(t)
	started := make(chan struct{})
	stopped := make(chan struct{})
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleDeferred(func(ctx *Context, _ get, reply Reply[int]) error {
			_, err := ctx.StartTask(
				func(ctx context.Context) (int, error) {
					close(started)
					<-ctx.Done()
					close(stopped)
					return 0, ctx.Err()
				},
				func(value int, err error) bool {
					reply.Complete(value, err)
					return true
				},
				nil,
			)
			return err
		})}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		_, err := a.addr().Call[int](ctx, get{})
		result <- err
	}()
	await(t, started)

	cancel()
	await(t, stopped)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestDetachedTaskKeepsItsActorOwner(t *testing.T) {
	rt := runtimeFor(t)
	started := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	stopped := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	probes := [2]chan chan error{make(chan chan error, 1), make(chan chan error, 1)}
	var actors [2]*actor

	for i := range actors {
		actors[i] = spawnFor(t, rt, func() []Binding {
			return []Binding{HandleDeferred(func(ctx *Context, _ get, reply Reply[int]) error {
				// Remove request cancellation while retaining the current actor owner.
				ctx = ctx.WithContext(context.WithoutCancel(ctx))
				_, err := ctx.StartTask(
					func(ctx context.Context) (int, error) {
						close(started[i])
						defer close(stopped[i])

						for {
							select {
							case probe := <-probes[i]:
								probe <- ctx.Err()
							case <-ctx.Done():
								return 0, ctx.Err()
							}
						}
					},
					func(value int, err error) bool {
						reply.Complete(value, err)
						return true
					},
					nil,
				)
				return err
			})}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := make(chan error, len(actors))
	for _, a := range actors {
		go func() {
			_, err := a.addr().Call[int](ctx, get{})
			results <- err
		}()
	}

	await(t, started[0])
	await(t, started[1])

	// Settle the caller requests first; detached workers must remain usable.
	cancel()
	for range actors {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("caller did not observe cancellation")
		}
	}

	probe := func(i int) {
		t.Helper()
		reply := make(chan error, 1)
		probes[i] <- reply
		select {
		case err := <-reply:
			if err != nil {
				t.Fatal("detached worker canceled early", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("detached worker stopped responding")
		}
	}
	probe(0)
	probe(1)

	actors[0].addr().Stop(nil)
	await(t, stopped[0])
	waitExitFor(t, actors[0].addr())
	probe(1)

	actors[1].addr().Stop(nil)
	await(t, stopped[1])
	waitExitFor(t, actors[1].addr())
}

func TestStartTaskRejectsMissingActorContext(t *testing.T) {
	for _, ctx := range []*Context{nil, {}, {Context: context.Background()}} {
		_, err := ctx.StartTask(
			func(context.Context) (int, error) {
				t.Error("work started without an actor owner")
				return 0, nil
			},
			func(int, error) bool { return true },
			nil,
		)
		if !errors.Is(err, ErrSetup) {
			t.Fatal(err)
		}
	}
}
