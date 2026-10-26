package generate

import "testing"

// Exercise static Describe and real dispatch through generated bindings. All four
// signatures retain value receivers, imported types, and nil metadata safety.
func TestGeneratedBindingsNilReceiverAndInstanceIsolation(t *testing.T) {
	dir := fixture(t, map[string]string{
		"a.go": `package fixture
import (
 ax "github.com/RussellLuo/actorx"
 "time"
)
type Add struct{ N int }
type Read struct{}
type Notice struct{}
type Deferred struct{}
type Chunks struct{}
//actorx:actor
type A struct{ N int }
var _ ax.Actor = (*A)(nil)
var constructed int
func newA(n int) *A { constructed++; return &A{N: n} }
func (a *A) Add(_ ax.Context, q Add) (int, error) { a.N += q.N; return a.N, nil }
func (a A) Read(ax.Context, Read) (int, error) { return a.N, nil }
func (a A) Notice(ax.Context, Notice) error { return nil }
func (a A) Deferred(_ ax.Context, _ Deferred, reply ax.Reply[time.Duration]) error { reply.Complete(time.Second, nil); return nil }
func (a A) Chunks(_ ax.Context, _ Chunks, reply ax.StreamReply[time.Duration]) error { reply.Complete(nil); return nil }
func (a A) Started(ax.Context, ax.Started) error { _ = a.N; return nil }
func (a *A) Stopping(ax.Context, ax.Stopping) error { _ = a.N; return nil }
func (a *A) Stopped(ax.Context, ax.Stopped) error { _ = a.N; return nil }
`,
		"a_test.go": `package fixture
import (
 "context"
 "reflect"
 "testing"
 "time"
 ax "github.com/RussellLuo/actorx"
)
func TestBindings(t *testing.T) {
 var nilA *A
 entries := nilA.Bindings() // Must not call or implicitly dereference the receiver.
 descriptions, err := ax.Describe(entries)
 if err != nil { t.Fatal(err) }
 want := []ax.CallDescriptor{
  ax.Request[Add, int](), ax.Streaming[Chunks, time.Duration](),
  ax.Request[Deferred, time.Duration](), ax.Message[Notice](), ax.Request[Read, int](),
 }
 if len(entries) != 8 || constructed != 0 || !reflect.DeepEqual(descriptions, want) { t.Fatal(entries, descriptions, constructed) }
 first, second := newA(10), newA(20)
 for _, value := range []*A{first, second} {
  actual, err := ax.Describe(value.Bindings())
  if err != nil || !reflect.DeepEqual(actual, descriptions) { t.Fatal(actual, err) }
 }
 // Changing a metadata snapshot cannot replace executable bindings.
 descriptions[0] = ax.Message[string]()
 rt, err := ax.NewRuntime(context.Background())
 if err != nil { t.Fatal(err) }; defer rt.Close(context.Background())
 a, err := rt.Spawn(first); if err != nil { t.Fatal(err) }
 b, err := rt.Spawn(second); if err != nil { t.Fatal(err) }
 if n, err := a.Call[int](context.Background(), Add{N: 3}); err != nil || n != 13 { t.Fatal(n, err) }
 // A value receiver must read the current state at dispatch, not a copy captured by Bindings.
 if n, err := a.Call[int](context.Background(), Read{}); err != nil || n != 13 { t.Fatal(n, err) }
 if n, err := b.Call[int](context.Background(), Read{}); err != nil || n != 20 { t.Fatal(n, err) }
 if err := a.Send(context.Background(), Notice{}); err != nil { t.Fatal(err) }
 if n, err := a.Call[time.Duration](context.Background(), Deferred{}); err != nil || n != time.Second { t.Fatal(n, err) }
 if err := a.Stream[time.Duration](context.Background(), Chunks{}, func(time.Duration) error { return nil }); err != nil { t.Fatal(err) }
}
`,
	})
	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}
	if err := Run(dir, "actorx.gen.go", true); err != nil {
		t.Fatal(err)
	}
	buildFixture(t, dir)
}

func TestValueReceiverSignatureNamesDoNotCollide(t *testing.T) {
	dir := fixture(t, map[string]string{"a.go": `package fixture
import (
 ax "github.com/RussellLuo/actorx"
 "time"
)
type a struct{}
type actorx struct{}
type actorxType0 struct{}
//actorx:actor
type A struct{}
func (A) Read(ax.Context, a) (time.Duration, error) { return 0, nil }
func (A) Reset(ax.Context, actorx) error { return nil }
func (A) More(ax.Context, actorxType0) error { return nil }
`})
	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}
	if err := Run(dir, "actorx.gen.go", true); err != nil {
		t.Fatal(err)
	}
	buildFixture(t, dir)
}

func TestBindingsReservedNameConflict(t *testing.T) {
	for _, marker := range []string{"", "//actorx:ignore\n"} {
		dir := fixture(t, map[string]string{"a.go": `package fixture
import ax "github.com/RussellLuo/actorx"
type Q struct{}
//actorx:actor
type A struct{}
func (*A) Read(ax.Context, Q) error { return nil }
` + marker + `func (*A) Bindings() []ax.Binding { return nil }
`})
		if err := Run(dir, "actorx.gen.go", false); err == nil {
			t.Fatal("reserved Bindings method accepted")
		}
	}
}
