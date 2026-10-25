package actorx_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RussellLuo/actorx"
)

// This fixture proves cross-package access and lifecycle boundaries only. Real
// NATS binding evidence lives in channel/nats and is never inferred from it.
type seamChannel struct {
	mu               sync.Mutex
	opened           bool
	targets          map[string]actorx.Receiver
	bind             func(context.Context, string, actorx.Receiver) error
	released, closed atomic.Int32
	last             actorx.Responder
	silent           bool
}

func (d *seamChannel) Open(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.opened {
		return actorx.ErrClosed
	}

	d.opened = true
	return ctx.Err()
}

func (d *seamChannel) Bind(ctx context.Context, id string, r actorx.Receiver) (func() error, error) {
	if d.bind != nil {
		if err := d.bind(ctx, id, r); err != nil {
			return nil, err
		}
	}

	d.mu.Lock()
	if d.targets == nil {
		d.targets = map[string]actorx.Receiver{}
	}

	d.targets[id] = r
	d.mu.Unlock()
	return func() error {
		d.mu.Lock()
		delete(d.targets, id)
		d.mu.Unlock()
		d.released.Add(1)
		return nil
	}, nil
}

func (d *seamChannel) Deliver(ctx context.Context, delivery actorx.Delivery, sink actorx.Responder) error {
	d.mu.Lock()
	r := d.targets[delivery.Target]
	d.last = sink
	d.mu.Unlock()

	if d.silent {
		return nil
	}
	if r == nil {
		return actorx.ErrStopped
	}

	return r.Receive(ctx, delivery, sink)
}

func (d *seamChannel) Close() error {
	d.closed.Add(1)
	return nil
}

func seamRuntime(t *testing.T, d *seamChannel) *actorx.Runtime {
	t.Helper()
	rt, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(d))
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

func waitFor(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
}

type seamQuery struct{}

type seamNotice struct{}

type seamDeferred struct{}

type seamChunks struct{}

type discardReply struct{}

func (discardReply) Addresses() actorx.AddressImporter { return nil }

func (discardReply) Complete(any, error) bool { return true }

func (discardReply) Write(context.Context, any, int) error { return nil }

func (discardReply) WriteConsumed(context.Context, any, int) error { return nil }

func TestExternalChannelAdmissionAndCorrelation(t *testing.T) {
	d := &seamChannel{}
	d.bind = func(ctx context.Context, id string, r actorx.Receiver) error {
		if err := r.Receive(ctx, actorx.Delivery{
			Target:      id,
			Correlation: "premature",
			Kind:        actorx.CallKind,
			Message:     seamQuery{},
		}, discardReply{}); !errors.Is(err, actorx.ErrStopped) {
			t.Errorf("half-published actor: %v", err)
		}

		descriptions := r.Bindings()
		want := []actorx.CallDescriptor{
			actorx.Request[seamQuery, int](),
			actorx.Message[seamNotice](),
			actorx.Request[seamDeferred, int](),
			actorx.Streaming[seamChunks, int](),
		}
		if !reflect.DeepEqual(descriptions, want) {
			t.Fatalf("ordinary binding descriptions: %v", descriptions)
		}
		if err := descriptions[0].Check(seamQuery{}, actorx.StreamKind); !errors.Is(err, actorx.ErrMode) {
			t.Fatal(err)
		}

		descriptions[0] = actorx.Message[seamNotice]()
		if r.Bindings()[0].MessageType() != reflect.TypeFor[seamQuery]() {
			t.Fatal("mutable receiver metadata")
		}

		return nil
	}
	rt := seamRuntime(t, d)
	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{
			actorx.Handle(func(*actorx.Context, seamQuery) (int, error) { return 7, nil }),
			actorx.HandleMessage(func(*actorx.Context, seamNotice) error { return nil }),
			actorx.HandleDeferred(func(_ *actorx.Context, _ seamDeferred, reply actorx.Reply[int]) error {
				reply.Complete(9, nil)
				return nil
			}),
			actorx.HandleStream(func(_ *actorx.Context, _ seamChunks, reply actorx.StreamReply[int]) error {
				reply.Complete(nil)
				return nil
			}),
			actorx.HandleMessage(func(*actorx.Context, actorx.Started) error { return nil }),
			actorx.HandleMessage(func(*actorx.Context, actorx.Stopping) error { return nil }),
			actorx.HandleMessage(func(*actorx.Context, actorx.Stopped) error { return nil }),
		}
	}))
	if err != nil {
		t.Fatal(err)
	}

	got, err := a.Call[int](context.Background(), seamQuery{})
	if err != nil || got != 7 {
		t.Fatal(got, err)
	}

	d.mu.Lock()
	late := d.last
	d.mu.Unlock()

	if late.Complete(8, nil) {
		t.Fatal("late reply completed a settled association")
	}

	a.Stop(nil)
	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if exit, err := a.Wait(waitCtx); err != nil || exit.Reason != nil {
		t.Fatal(exit, err)
	}

	if d.released.Load() != 1 {
		t.Fatal("binding release count", d.released.Load())
	}
}

func TestExternalBindingCloseCancelsWithoutRuntimeLock(t *testing.T) {
	entered := make(chan struct{})
	var bindings atomic.Int32
	d := &seamChannel{bind: func(ctx context.Context, _ string, _ actorx.Receiver) error {
		if bindings.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
		}

		// Return a successful binding after cancellation to test publication rollback.
		return nil
	}}
	rt := seamRuntime(t, d)
	result := make(chan error, 1)
	go func() {
		_, err := rt.Spawn(bindingsFunc(func() []actorx.Binding { return nil }))
		result <- err
	}()
	waitFor(t, entered)
	other := make(chan struct{})
	go func() {
		_, err := rt.Spawn(bindingsFunc(func() []actorx.Binding { return nil }))
		if err != nil {
			t.Error(err)
		}

		close(other)
	}()
	waitFor(t, other) // A blocked network bind must not hold rt.mu.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, actorx.ErrClosed) {
		t.Fatal(err)
	}
	if d.released.Load() != 2 || d.closed.Load() != 1 {
		t.Fatal("leaked bindings/channel", d.released.Load(), d.closed.Load())
	}
}

func TestExternalCallerCloseWithoutLocalTarget(t *testing.T) {
	d := &seamChannel{silent: true}
	rt := seamRuntime(t, d)
	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding { return nil }))
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := a.Call[int](context.Background(), seamQuery{})
		result <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		d.mu.Lock()
		waiting := d.last != nil
		d.mu.Unlock()

		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no caller association")
		}

		time.Sleep(time.Millisecond)
	}

	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, actorx.ErrClosed) {
		t.Fatal(err)
	}
}

func TestAddressImportDoesNotBindOrProbe(t *testing.T) {
	source := seamRuntime(t, &seamChannel{})
	a, err := source.Spawn(bindingsFunc(func() []actorx.Binding { return nil }))
	if err != nil {
		t.Fatal(err)
	}

	data, err := actorx.ExportAddr(a)
	if err != nil {
		t.Fatal(err)
	}

	d := &seamChannel{}
	rt := seamRuntime(t, d)
	addr, err := rt.ImportAddr(data)
	if err != nil {
		t.Fatal(err)
	}

	roundtrip, err := actorx.ExportAddr(addr)
	if err != nil || string(data) != string(roundtrip) {
		t.Fatal(string(roundtrip), err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := addr.Stop(nil); err == nil {
		t.Fatal("import gained stop capability")
	}
	if _, err := addr.Wait(ctx); err == nil || errors.Is(err, context.Canceled) {
		t.Fatal("import gained exit capability", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.targets) != 0 || d.last != nil {
		t.Fatal("import performed channel work")
	}

	for _, bad := range []string{
		`null`,
		`{}`,
		`{"id":"ca.a_123"}`,
		`{"id":"a_ABCDEF01234567890123456789012345"}`,
		`{"id":"a_0123456789012345678901234567890*"}`,
	} {
		if _, err := rt.ImportAddr([]byte(bad)); err == nil {
			t.Fatal("invalid ID accepted", bad)
		}
	}
}

// No wrapper or private entry point is available to this external implementation.
type openProbeChannel struct {
	*actorx.LocalChannel
	failure                    error
	opened, rolledBack, closed atomic.Int32
	closing                    chan struct{}
	releaseClose               <-chan struct{}
}

func (ch *openProbeChannel) Open(ctx context.Context) error {
	ch.opened.Add(1)
	if ch.failure != nil {
		ch.rolledBack.Add(1) // The implementation owns rollback on Open failure.
		return ch.failure
	}

	return ch.LocalChannel.Open(ctx)
}

func (ch *openProbeChannel) Close() error {
	ch.closed.Add(1)
	if ch.closing != nil {
		close(ch.closing)
		<-ch.releaseClose
	}

	return ch.LocalChannel.Close()
}

func TestExternalChannelOpenFailureDoesNotCloseUnownedChannel(t *testing.T) {
	failure := errors.New("initialization failed")
	ch := &openProbeChannel{LocalChannel: actorx.NewLocalChannel(), failure: failure}
	if _, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(ch)); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if ch.opened.Load() != 1 || ch.rolledBack.Load() != 1 || ch.closed.Load() != 0 {
		t.Fatal("wrong rollback owner")
	}

	var typedNil *actorx.LocalChannel
	for _, invalid := range []actorx.Channel{nil, typedNil} {
		if _, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(invalid)); !errors.Is(err, actorx.ErrSetup) {
			t.Fatal("nil channel accepted", err)
		}
	}
}

func TestExternalChannelCloseTimeoutContinuesCleanup(t *testing.T) {
	release := make(chan struct{})
	ch := &openProbeChannel{
		LocalChannel: actorx.NewLocalChannel(),
		closing:      make(chan struct{}),
		releaseClose: release,
	}
	rt, err := actorx.NewRuntime(context.Background(), actorx.WithChannel(ch))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rt.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	waitFor(t, ch.closing)
	close(release)
	if err := rt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ch.closed.Load() != 1 {
		t.Fatal("channel closed more than once")
	}
}

func TestExternalReceiverRejectsDifferentInstance(t *testing.T) {
	var receiver actorx.Receiver
	var target string
	d := &seamChannel{bind: func(_ context.Context, id string, r actorx.Receiver) error {
		receiver, target = r, id
		return nil
	}}
	rt := seamRuntime(t, d)
	var invoked atomic.Int32
	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{actorx.Handle(func(*actorx.Context, seamQuery) (int, error) {
			invoked.Add(1)
			return 7, nil
		})}
	}))
	if err != nil {
		t.Fatal(err)
	}

	wrong := "a_01234567890123456789012345678901"
	if wrong == target {
		wrong = "a_11234567890123456789012345678901"
	}
	if err := receiver.Receive(context.Background(), actorx.Delivery{
		Target:      wrong,
		Correlation: "wrong-instance",
		Kind:        actorx.CallKind,
		Message:     seamQuery{},
	}, discardReply{}); !errors.Is(err, actorx.ErrStopped) {
		t.Fatal("receiver accepted another instance", err)
	}
	if invoked.Load() != 0 {
		t.Fatal("wrong instance invoked handler")
	}
	if got, err := a.Call[int](context.Background(), seamQuery{}); got != 7 || err != nil {
		t.Fatal(got, err)
	}
}

// Complete deliberately reenters Runtime control before blocking. This catches
// holding Runtime.mu across cancellation/settlement, including Addr.Stop.
type controlBarrierReply struct {
	discardReply
	complete func(error)
}

func (r controlBarrierReply) Complete(_ any, err error) bool {
	r.complete(err)
	return true
}

func TestExternalAddrStopSettlementDoesNotHoldRuntimeLock(t *testing.T) {
	d := &seamChannel{}
	rt := seamRuntime(t, d)
	stopping := make(chan struct{})
	a, err := rt.Spawn(bindingsFunc(func() []actorx.Binding {
		return []actorx.Binding{
			actorx.HandleDeferred(func(*actorx.Context, seamQuery, actorx.Reply[int]) error { return nil }),
			actorx.HandleMessage(func(*actorx.Context, actorx.Stopping) error {
				close(stopping)
				return nil
			}),
		}
	}))
	if err != nil {
		t.Fatal(err)
	}

	b, err := rt.Spawn(bindingsFunc(func() []actorx.Binding { return []actorx.Binding{} }))
	if err != nil {
		t.Fatal(err)
	}

	data, err := actorx.ExportAddr(a)
	if err != nil {
		t.Fatal(err)
	}

	var identity struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &identity); err != nil {
		t.Fatal(err)
	}

	d.mu.Lock()
	receiver := d.targets[identity.ID]
	d.mu.Unlock()

	settling, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()

	cause := errors.New("external stop")
	sink := controlBarrierReply{complete: func(err error) {
		if !errors.Is(err, actorx.ErrStopped) {
			t.Error("pending request", err)
		}
		// Admission must already be closed; this reentrant Stop must complete.
		if err := a.Stop(errors.New("ignored reentrant cause")); err != nil {
			t.Error(err)
		}
		if err := b.Stop(nil); err != nil {
			t.Error(err)
		}

		close(settling)
		<-release
	}}
	if err := receiver.Receive(context.Background(), actorx.Delivery{
		Target:      identity.ID,
		Correlation: "blocked-reply",
		Kind:        actorx.CallKind,
		Message:     seamQuery{},
	}, sink); err != nil {
		t.Fatal(err)
	}

	stopResult := make(chan error, 1)
	go func() { stopResult <- a.Stop(cause) }()
	waitFor(t, settling)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := b.Wait(ctx); err != nil {
		t.Fatal("other control stalled", err)
	}
	if err := a.Send(ctx, seamQuery{}); !errors.Is(err, actorx.ErrStopped) {
		t.Fatal("admission open", err)
	}

	select {
	case <-stopping:
		t.Fatal("Stopping overtook settlement")
	default:
	}

	canceled, cancelWait := context.WithCancel(ctx)
	cancelWait()
	if exit, err := a.Wait(canceled); exit != (actorx.Exit{}) || !errors.Is(err, context.Canceled) {
		t.Fatal("Wait overtook settlement", exit, err)
	}

	// Close must establish its boundary and return a timed-out wait while the
	// external responder remains blocked. It cannot deadlock on Runtime.mu.
	closeResult := make(chan error, 1)
	go func() { closeResult <- rt.Close(canceled) }()
	select {
	case err := <-closeResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close blocked on settlement")
	}

	if err := a.Stop(nil); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal(err)
	}

	resume()
	if err := <-stopResult; err != nil {
		t.Fatal(err)
	}

	exit, err := a.Wait(ctx)
	if err != nil || !errors.Is(exit.Reason, cause) {
		t.Fatal(exit, err)
	}
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if d.released.Load() != 2 || d.closed.Load() != 1 {
		t.Fatal("duplicate cleanup", d.released.Load(), d.closed.Load())
	}
}
