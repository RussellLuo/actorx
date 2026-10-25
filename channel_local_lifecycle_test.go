package actorx_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RussellLuo/actorx"
)

type localQuery struct{}

// Observe public delivery without knowing the concrete Channel implementation.
type localDeliveryCounter struct {
	actorx.Channel
	deliveries atomic.Int32
}

func (ch *localDeliveryCounter) Deliver(ctx context.Context, d actorx.Delivery, r actorx.Responder) error {
	ch.deliveries.Add(1)
	return ch.Channel.Deliver(ctx, d, r)
}

type localReceiver struct{}

func (localReceiver) Addresses() actorx.AddressImporter { return nil }

func (localReceiver) Receive(context.Context, actorx.Delivery, actorx.Responder) error {
	return nil
}

func (localReceiver) Bindings() []actorx.CallDescriptor {
	return []actorx.CallDescriptor{actorx.Request[localQuery, int]()}
}

func TestRuntimeChannelOwnershipAndLocalControl(t *testing.T) {
	ch := &localDeliveryCounter{Channel: actorx.NewLocalChannel()}
	t.Cleanup(func() { ch.Close() })
	if _, err := ch.Bind(context.Background(), "unopened", nil); !errors.Is(err, actorx.ErrPhase) {
		t.Fatal("Bind before Open", err)
	}
	if err := ch.Deliver(context.Background(), actorx.Delivery{}, nil); !errors.Is(err, actorx.ErrPhase) {
		t.Fatal("Deliver before Open", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt, err := actorx.NewRuntime(ctx, actorx.WithChannel(ch))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { rt.Close(context.Background()) })
	cancel() // Successful initialization must not own the runtime lifetime.
	if _, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(ch)); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("duplicate claim", err)
	}

	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{actorx.Handle(func(*actorx.Context, localQuery) (int, error) { return 17, nil })}
	}))
	if err != nil {
		t.Fatal("owner damaged by duplicate claim/cancellation", err)
	}

	data, err := actorx.ExportAddr(a)
	if err != nil {
		t.Fatal(err)
	}

	addr, err := rt.ImportAddr(data)
	if err != nil {
		t.Fatal(err)
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer callCancel()

	if got, err := addr.Call[int](callCtx, localQuery{}); err != nil || got != 17 {
		t.Fatal(got, err)
	}

	canceled, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if exit, err := addr.Wait(canceled); exit != (actorx.Exit{}) || err == nil || errors.Is(err, context.Canceled) {
		t.Fatal("imported address gained exit capability", exit, err)
	}

	local := a // ordinary Go value copy retains the local exit record
	cause := errors.New("local shutdown")
	if err := addr.Stop(cause); err == nil {
		t.Fatal("imported address gained stop capability")
	}

	deliveries := ch.deliveries.Load()
	if err := local.Stop(cause); err != nil {
		t.Fatal(err)
	}
	if ch.deliveries.Load() != deliveries {
		t.Fatal("local Stop performed Channel delivery")
	}

	exit, err := local.Wait(callCtx)
	if err != nil || !errors.Is(exit.Reason, cause) {
		t.Fatal("local Wait", exit, err)
	}
	if err := rt.Close(callCtx); err != nil {
		t.Fatal(err)
	}
	if err := local.Stop(nil); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("Stop after Close", err)
	}
	if late, err := local.Wait(canceled); err != nil || late != exit {
		t.Fatal("lost exit after Close", late, err)
	}
	if _, err := rt.ImportAddr(data); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("import after Close", err)
	}
	if err := addr.Send(callCtx, localQuery{}); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("old Addr after Close", err)
	}
	if err := ch.Open(context.Background()); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("reopened", err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal("repeat Close", err)
	}
}

func TestRuntimeChannelLocalControlWithoutBindings(t *testing.T) {
	ch := &localDeliveryCounter{Channel: actorx.NewLocalChannel()}
	rt, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(ch))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { rt.Close(context.Background()) })
	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding { return []actorx.Binding{} }))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Stop(nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if exit, err := a.Wait(ctx); err != nil || exit.Reason != nil {
		t.Fatal(exit, err)
	}
	if ch.deliveries.Load() != 0 {
		t.Fatal("local lifecycle used business delivery")
	}
}

func TestLocalChannelCancelledOpen(t *testing.T) {
	ch := actorx.NewLocalChannel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := actorx.NewRuntime(ctx, actorx.WithChannel(ch)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := ch.Open(context.Background()); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("retried failed Open", err)
	}
	if err := ch.Deliver(context.Background(), actorx.Delivery{}, nil); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal(err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLocalChannelConcurrentOpen(t *testing.T) {
	ch := actorx.NewLocalChannel()
	var wg sync.WaitGroup
	type result struct {
		rt  *actorx.Runtime
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rt, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(ch))
			results <- result{rt, err}
		}()
	}

	wg.Wait()
	close(results)
	owners := 0
	for r := range results {
		if r.err == nil {
			owners++
			r.rt.Close(context.Background())
		} else if !errors.Is(r.err, actorx.ErrClosed) {
			t.Fatal(r.err)
		}
	}

	if owners != 1 {
		t.Fatal("owner count", owners)
	}
}

func TestLocalChannelBindingRelease(t *testing.T) {
	ch := actorx.NewLocalChannel()
	t.Cleanup(func() { ch.Close() })
	if err := ch.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	const id = "a_01234567890123456789012345678901"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if release, err := ch.Bind(ctx, id, localReceiver{}); !errors.Is(err, context.Canceled) || release != nil {
		t.Fatal("cancelled Bind", err)
	}

	release, err := ch.Bind(context.Background(), id, localReceiver{})
	if err != nil {
		t.Fatal("Bind rollback", err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}

	next, err := ch.Bind(context.Background(), id, localReceiver{})
	if err != nil {
		t.Fatal(err)
	}

	defer next()

	if err := release(); err != nil {
		t.Fatal("repeat release", err)
	}
	if extra, err := ch.Bind(context.Background(), id, localReceiver{}); !errors.Is(err, actorx.ErrSetup) {
		if extra != nil {
			extra()
		}

		t.Fatal("stale release removed a newer binding", err)
	}
}

func TestRuntimeChannelValidationBeforeOpen(t *testing.T) {
	ch := actorx.NewLocalChannel()
	if _, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(ch), actorx.WithCallLimit(0)); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}

	rt, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(ch))
	if err != nil {
		t.Fatal("invalid Runtime consumed Channel", err)
	}
	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
