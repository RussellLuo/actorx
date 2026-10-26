package actorx

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestDescribeAndSpawnShareValidation(t *testing.T) {
	message := HandleMessage(func(Context, get) error { panic("invalid set executed") })
	started := HandleMessage(func(Context, Started) error { panic("invalid set started") })
	cases := map[string][]Binding{
		"zero":                    {{}},
		"zero after valid":        {message, {}},
		"nil message handler":     {HandleMessage[get](nil)},
		"nil request handler":     {Handle[get, int](nil)},
		"nil deferred handler":    {HandleDeferred[get, int](nil)},
		"nil stream handler":      {HandleStream[get, int](nil)},
		"interface message":       {HandleMessage(func(Context, any) error { return nil })},
		"duplicate message":       {message, message},
		"duplicate across modes":  {message, HandleStream(func(Context, get, StreamReply[int]) error { return nil })},
		"duplicate result types":  {Handle(func(Context, get) (int, error) { return 0, nil }), Handle(func(Context, get) (string, error) { return "", nil })},
		"duplicate lifecycle":     {started, started},
		"nil started handler":     {HandleMessage[Started](nil)},
		"nil stopping handler":    {HandleMessage[Stopping](nil)},
		"nil stopped handler":     {HandleMessage[Stopped](nil)},
		"pointer lifecycle":       {HandleMessage(func(Context, *Started) error { return nil })},
		"lifecycle request":       {Handle(func(Context, Stopping) (int, error) { return 0, nil })},
		"lifecycle deferred":      {HandleDeferred(func(Context, Stopped, Reply[int]) error { return nil })},
		"lifecycle stream":        {HandleStream(func(Context, Started, StreamReply[int]) error { return nil })},
		"lifecycle result":        {Handle(func(Context, get) (Stopped, error) { return Stopped{}, nil })},
		"lifecycle chunk":         {HandleStream(func(Context, get, StreamReply[*Stopping]) error { return nil })},
		"invalid after lifecycle": {started, {}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			if out, err := Describe(entries); out != nil || !errors.Is(err, ErrSetup) {
				t.Fatal(out, err)
			}
			rt := runtimeFor(t)
			if addr, err := rt.Spawn(bindingsFunc(func() []Binding { return entries })); !addr.IsZero() || !errors.Is(err, ErrSetup) {
				t.Fatal(addr, err)
			}
		})
	}
}

func TestDescribeEmptyAndLifecycleOnly(t *testing.T) {
	calls := 0
	for _, entries := range [][]Binding{
		nil,
		{},
		{
			HandleMessage(func(Context, Started) error { calls++; return nil }),
			HandleMessage(func(Context, Stopping) error { calls++; return nil }),
			HandleMessage(func(Context, Stopped) error { calls++; return nil }),
		},
	} {
		out, err := Describe(entries)
		if len(out) != 0 || err != nil || calls != 0 {
			t.Fatal(out, err, calls)
		}
		rt := runtimeFor(t)
		addr, err := rt.Spawn(bindingsFunc(func() []Binding { return entries }))
		if err != nil {
			t.Fatal(err)
		}
		if err := rt.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if exit, err := addr.Wait(context.Background()); err != nil || exit.Reason != nil {
			t.Fatal(exit, err)
		}
	}
	if calls != 3 {
		t.Fatal("lifecycle dispatch changed", calls)
	}
}

type describeReceiver struct {
	n       int
	padding [64]byte // Keep this allocation out of the tiny allocator for the GC test.
}

func (a *describeReceiver) Bindings() []Binding {
	return []Binding{HandleMessage(a.started), Handle(a.read), Handle(a.add)}
}

func (a *describeReceiver) started(Context, Started) error { a.n++; return nil }
func (a *describeReceiver) read(Context, get) (int, error) { return a.n, nil }
func (a *describeReceiver) add(_ Context, msg add) (int, error) {
	a.n += msg.N
	return a.n, nil
}

func TestDescribeNilReceiverAndIndependentSnapshots(t *testing.T) {
	entries := (*describeReceiver)(nil).Bindings()
	want := []CallDescriptor{Request[get, int](), Request[add, int]()}
	out, err := Describe(entries)
	if err != nil || !reflect.DeepEqual(out, want) {
		t.Fatal(out, err)
	}
	// A second call owns its own output. Neither mutation can change a Binding.
	again, err := Describe(entries)
	if err != nil {
		t.Fatal(err)
	}
	out[0] = Message[string]()
	if !reflect.DeepEqual(again, want) || entries[1].Descriptor() != want[0] {
		t.Fatal("output aliases input or another snapshot")
	}
	entries[1] = Binding{}
	if !reflect.DeepEqual(again, want) {
		t.Fatal("input mutation changed descriptions")
	}

	rt := runtimeFor(t)
	var addresses []Addr
	for _, n := range []int{10, 20} {
		value := &describeReceiver{n: n}
		live, err := Describe(value.Bindings())
		if err != nil || !reflect.DeepEqual(live, want) {
			t.Fatal("nil/live contract differs", live, err)
		}
		addr, err := rt.Spawn(value)
		if err != nil {
			t.Fatal(err)
		}
		addresses = append(addresses, addr)
	}
	for i, addr := range addresses {
		n := (i + 1) * 10
		if got, err := addr.Call[int](context.Background(), add{N: 2}); err != nil || got != n+3 {
			t.Fatal(got, err)
		}
	}
	for i, addr := range addresses {
		n := (i + 1) * 10
		if got, err := addr.Call[int](context.Background(), get{}); err != nil || got != n+3 {
			t.Fatal(got, err)
		}
	}
}

//go:noinline
func describeCollectibleReceiver(collected chan struct{}) ([]CallDescriptor, error) {
	value := &describeReceiver{}
	runtime.SetFinalizer(value, func(*describeReceiver) { close(collected) })
	return Describe(value.Bindings())
}

func TestDescribeDoesNotRetainReceiver(t *testing.T) {
	collected := make(chan struct{})
	out, err := describeCollectibleReceiver(collected)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		runtime.GC()
		select {
		case <-collected:
			if out[0] != Request[get, int]() {
				t.Fatal(out)
			}
			runtime.KeepAlive(out)
			return
		case <-deadline.C:
			t.Fatal("descriptions retained the bound receiver")
		case <-tick.C:
		}
	}
}
