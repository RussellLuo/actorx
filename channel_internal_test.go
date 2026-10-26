package actorx

import (
	"context"
	"errors"
	"testing"
)

func TestCallerAssociationsSettleOnFailureAndCancellation(t *testing.T) {
	rt := runtimeFor(t, WithCallLimit(1))
	a := spawnFor(t, rt, func() []Binding {
		return []Binding{HandleDeferred(func(Context, get, Reply[int]) error { return nil })}
	})
	if _, err := a.addr().Call[int](context.Background(), struct{}{}); !errors.Is(err, ErrUnhandled) {
		t.Fatal(err)
	}
	if err := a.addr().Stream[int](context.Background(), get{}, func(int) error { return nil }); !errors.Is(err, ErrMode) {
		t.Fatal(err)
	}

	rt.calls.mu.Lock()
	pending, records := rt.calls.pending, len(rt.calls.calls)
	rt.calls.mu.Unlock()

	if pending != 0 || records != 0 {
		t.Fatal("failed admission retained caller state", pending, records)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r, err := a.addr().prepare(ctx, get{}, callMode)
	if err != nil {
		t.Fatal(err)
	}

	cancel()
	await(t, r.done)
	a.addr().Stop(nil)
	waitExitFor(t, a.addr())
	rt.calls.mu.Lock()
	defer rt.calls.mu.Unlock()

	if rt.calls.pending != 0 || len(rt.calls.calls) != 0 {
		t.Fatal("cancelled association retained state")
	}
}
