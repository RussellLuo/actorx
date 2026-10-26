package actorx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/RussellLuo/actorx"
)

type limitQuery struct{}

type limitProbe struct {
	stopped chan struct{}
	unblock chan struct{}
	fail    bool
	read    *int
}

func (p *limitProbe) Bindings() []actorx.Binding {
	if p.read != nil {
		*p.read++
	}
	return []actorx.Binding{actorx.HandleMessage(p.start), actorx.HandleMessage(p.stop), actorx.Handle(p.child)}
}

func (p *limitProbe) start(actorx.Context, actorx.Started) error {
	if p.fail {
		return errors.New("start failed")
	}
	return nil
}

func (p *limitProbe) stop(actorx.Context, actorx.Stopped) error {
	if p.stopped != nil {
		close(p.stopped)
		<-p.unblock
	}
	return nil
}

func (*limitProbe) child(ctx actorx.Context, _ limitQuery) (bool, error) {
	_, err := ctx.Spawn(&limitProbe{})
	return errors.Is(err, actorx.ErrFull), nil
}

func TestInstanceLimitHeldThroughStoppedAndRollback(t *testing.T) {
	ctx := context.Background()
	if rt, err := actorx.NewRuntime(ctx, actorx.WithInstanceLimit(-1)); rt != nil || !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(rt, err)
	}
	rt, err := actorx.NewRuntime(ctx, actorx.WithInstanceLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close(ctx)
	if _, err := rt.Spawn(&limitProbe{fail: true}); err == nil {
		t.Fatal("expected rollback")
	}
	stopped, unblock := make(chan struct{}), make(chan struct{})
	addr, err := rt.Spawn(&limitProbe{stopped: stopped, unblock: unblock})
	if err != nil {
		t.Fatal(err)
	}
	if full, err := addr.Call[bool](ctx, limitQuery{}); err != nil || !full {
		t.Fatal(full, err)
	}
	_ = addr.Stop(nil)
	<-stopped
	reads := 0
	if addr, err := rt.Spawn(&limitProbe{read: &reads}); !addr.IsZero() || !errors.Is(err, actorx.ErrFull) || reads != 0 {
		t.Fatal(addr, err, reads)
	}
	close(unblock)
	if _, err := addr.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Spawn(&limitProbe{}); err != nil {
		t.Fatal("slot not released", err)
	}
}
